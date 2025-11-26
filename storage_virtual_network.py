#!/usr/bin/env python3
"""
Threaded Network Server with Socket Communication
Runs on a specified localhost port and accepts node connections
"""

import socket
import threading
import json
import time
import hashlib
from typing import Dict, Any
from collections import defaultdict
import argparse


class ThreadedNetworkServer:
    """Network coordinator that listens on a socket port"""
    
    def __init__(self, host='localhost', port=5500):
        self.host = host
        self.port = port
        self.nodes: Dict[str, str] = {}  # node_id -> "host:port"
        self.node_status: Dict[str, str] = {}  # node_id -> "online"/"offline"
        self.connections: Dict[str, Dict[str, int]] = defaultdict(dict)
        self.active_transfers: Dict[str, dict] = {}
        self.running = False
        self.server_socket = None
        self.lock = threading.Lock()
        self.health_check_interval = 5  # seconds
        
    def start(self):
        """Start the network coordinator server"""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        self.running = True
        
        print(f"[Network] Coordinator started on {self.host}:{self.port}")
        
        # Start accepting connections in a separate thread
        accept_thread = threading.Thread(target=self._accept_connections, daemon=True)
        accept_thread.start()
        
        # Start health check thread
        health_thread = threading.Thread(target=self._health_check_loop, daemon=True)
        health_thread.start()
        
    def stop(self):
        """Stop the network coordinator server"""
        self.running = False
        if self.server_socket:
            self.server_socket.close()
        print("[Network] Coordinator stopped")
        
    def _accept_connections(self):
        """Accept incoming client connections"""
        while self.running:
            try:
                client_socket, address = self.server_socket.accept()
                # Handle each client in a separate thread
                client_thread = threading.Thread(
                    target=self._handle_client,
                    args=(client_socket, address),
                    daemon=True
                )
                client_thread.start()
            except:
                break
                
    def _handle_client(self, client_socket, address):
        """Handle individual client requests"""
        try:
            while True:
                # Receive data
                data = client_socket.recv(4096)
                if not data:
                    break
                    
                # Parse request
                request = json.loads(data.decode('utf-8'))
                command = request.get('command')
                args = request.get('args', {})
                
                # Process command
                response = self._process_command(command, args)
                
                # Send response
                client_socket.sendall(json.dumps(response).encode('utf-8'))
                
                # Break after handling one request (don't keep connection open)
                break
                
        except Exception as e:
            print(f"[Network] Error handling client: {e}")
        finally:
            client_socket.close()
            
    def _process_command(self, command: str, args: Dict[str, Any]) -> Dict[str, Any]:
        """Process commands from clients"""
        try:
            if command == "health":
                with self.lock:
                    return {"status": "healthy", "total_nodes": len(self.nodes)}
            
            elif command == "register_node":
                node_id = args.get('node_id')
                node_address = args.get('node_address')
                
                if not node_id or not node_address:
                    return {"error": "Missing node_id or node_address"}
                
                with self.lock:
                    self.nodes[node_id] = node_address
                    self.node_status[node_id] = "online"
                
                print(f"[Network] Registered {node_id} at {node_address}")
                return {"success": True, "registered": node_id}
            
            elif command == "unregister_node":
                node_id = args.get('node_id')
                
                with self.lock:
                    if node_id in self.nodes:
                        del self.nodes[node_id]
                        if node_id in self.node_status:
                            del self.node_status[node_id]
                        print(f"[Network] Unregistered {node_id}")
                        return {"success": True, "unregistered": node_id}
                return {"error": "Node not found"}
            
            elif command == "list_nodes":
                with self.lock:
                    nodes_with_status = {}
                    for node_id, address in self.nodes.items():
                        nodes_with_status[node_id] = {
                            "address": address,
                            "status": self.node_status.get(node_id, "unknown")
                        }
                return {"nodes": nodes_with_status}
            
            elif command == "create_connection":
                node1_id = args.get('node1_id')
                node2_id = args.get('node2_id')
                bandwidth = args.get('bandwidth')
                
                if not all([node1_id, node2_id, bandwidth]):
                    return {"error": "Missing required fields"}
                
                with self.lock:
                    if node1_id not in self.nodes or node2_id not in self.nodes:
                        return {"error": "One or both nodes not registered"}
                    
                    self.connections[node1_id][node2_id] = bandwidth
                    self.connections[node2_id][node1_id] = bandwidth
                
                # Tell both nodes about the connection (outside lock)
                self._send_to_node(node1_id, {
                    "command": "add_connection",
                    "args": {"node_id": node2_id, "bandwidth": bandwidth}
                })
                
                self._send_to_node(node2_id, {
                    "command": "add_connection",
                    "args": {"node_id": node1_id, "bandwidth": bandwidth}
                })
                
                return {
                    "success": True,
                    "connection": f"{node1_id} <-> {node2_id}",
                    "bandwidth": bandwidth
                }
            
            elif command == "initiate_transfer":
                source_node_id = args.get('source_node_id')
                target_node_id = args.get('target_node_id')
                file_name = args.get('file_name')
                file_size = args.get('file_size')
                
                if not all([source_node_id, target_node_id, file_name, file_size]):
                    return {"error": "Missing required fields"}
                
                with self.lock:
                    if source_node_id not in self.nodes or target_node_id not in self.nodes:
                        return {"error": "One or both nodes not registered"}
                    
                    # Generate unique file ID
                    file_id = hashlib.md5(f"{file_name}-{time.time()}".encode()).hexdigest()
                    
                    # Create transfer record
                    self.active_transfers[file_id] = {
                        "file_id": file_id,
                        "source_node_id": source_node_id,
                        "target_node_id": target_node_id,
                        "file_name": file_name,
                        "file_size": file_size,
                        "total_chunks": 0,
                        "completed_chunks": 0,
                        "status": "in_progress",
                        "started_at": time.time()
                    }
                
                # Tell target node to prepare (outside lock)
                result = self._send_to_node(target_node_id, {
                    "command": "initiate_transfer",
                    "args": {
                        "file_id": file_id,
                        "file_name": file_name,
                        "file_size": file_size,
                        "source_node": source_node_id
                    }
                })
                
                if 'error' in result:
                    with self.lock:
                        if file_id in self.active_transfers:
                            del self.active_transfers[file_id]
                    return {"error": f"Failed to initiate transfer: {result['error']}"}
                
                # Update with chunk info
                with self.lock:
                    if file_id in self.active_transfers:
                        self.active_transfers[file_id]['total_chunks'] = result.get('total_chunks', 0)
                
                return {
                    "success": True,
                    "file_id": file_id,
                    "total_chunks": result.get('total_chunks', 0)
                }
            
            elif command == "process_transfer":
                file_id = args.get('file_id')
                chunks_to_process = args.get('chunks_to_process', 1)
                
                with self.lock:
                    if not file_id or file_id not in self.active_transfers:
                        return {"error": "Transfer not found"}
                    
                    transfer = self.active_transfers[file_id]
                    target_node_id = transfer['target_node_id']
                    source_node_id = transfer['source_node_id']
                    start_chunk = transfer['completed_chunks']
                    end_chunk = min(start_chunk + chunks_to_process, transfer['total_chunks'])
                
                # Process chunks (outside lock)
                chunks_processed = 0
                for chunk_id in range(start_chunk, end_chunk):
                    result = self._send_to_node(target_node_id, {
                        "command": "process_chunk",
                        "args": {
                            "file_id": file_id,
                            "chunk_id": chunk_id,
                            "source_node": source_node_id
                        }
                    })
                    
                    if result.get('success'):
                        chunks_processed += 1
                        with self.lock:
                            transfer['completed_chunks'] += 1
                            
                            if result.get('completed'):
                                transfer['status'] = 'completed'
                                transfer['completed_at'] = time.time()
                                break
                    else:
                        with self.lock:
                            transfer['status'] = 'failed'
                        break
                
                with self.lock:
                    return {
                        "success": True,
                        "chunks_processed": chunks_processed,
                        "completed_chunks": transfer['completed_chunks'],
                        "total_chunks": transfer['total_chunks'],
                        "status": transfer['status'],
                        "completed": transfer['status'] == 'completed'
                    }
            
            elif command == "transfer_status":
                file_id = args.get('file_id')
                
                with self.lock:
                    if file_id not in self.active_transfers:
                        return {"error": "Transfer not found"}
                    
                    transfer = self.active_transfers[file_id]
                    progress = (transfer['completed_chunks'] / transfer['total_chunks']) * 100
                    
                    return {
                        "file_id": file_id,
                        "file_name": transfer['file_name'],
                        "status": transfer['status'],
                        "progress_percent": progress,
                        "completed_chunks": transfer['completed_chunks'],
                        "total_chunks": transfer['total_chunks'],
                        "source_node": transfer['source_node_id'],
                        "target_node": transfer['target_node_id']
                    }
            
            elif command == "network_stats":
                total_storage = 0
                used_storage = 0
                online_nodes = 0
                
                with self.lock:
                    node_ids = list(self.nodes.keys())
                
                for node_id in node_ids:
                    with self.lock:
                        if self.node_status.get(node_id) == "online":
                            online_nodes += 1
                    
                    result = self._send_to_node(node_id, {
                        "command": "storage_stats",
                        "args": {}
                    })
                    
                    if 'error' not in result:
                        total_storage += result.get('total_bytes', 0)
                        used_storage += result.get('used_bytes', 0)
                
                with self.lock:
                    return {
                        "total_nodes": len(self.nodes),
                        "online_nodes": online_nodes,
                        "offline_nodes": len(self.nodes) - online_nodes,
                        "total_storage_bytes": total_storage,
                        "used_storage_bytes": used_storage,
                        "storage_utilization_percent": (used_storage / total_storage * 100) if total_storage > 0 else 0,
                        "active_transfers": len([t for t in self.active_transfers.values() if t['status'] == 'in_progress']),
                        "completed_transfers": len([t for t in self.active_transfers.values() if t['status'] == 'completed'])
                    }
            
            elif command == "tick":
                with self.lock:
                    node_ids = list(self.nodes.keys())
                
                for node_id in node_ids:
                    self._send_to_node(node_id, {"command": "tick", "args": {}})
                
                return {"success": True}
            
            else:
                return {"error": f"Unknown command: {command}"}
                
        except Exception as e:
            return {"error": str(e)}
            if command == "register_node":
                node_id = args.get('node_id')
                node_address = args.get('node_address')
                
                if not node_id or not node_address:
                    return {"error": "Missing node_id or node_address"}
                
                self.nodes[node_id] = node_address
                self.node_status[node_id] = "online"
                print(f"[Network] Registered {node_id} at {node_address}")
                return {"success": True, "registered": node_id}
            
            elif command == "unregister_node":
                node_id = args.get('node_id')
                
                if node_id in self.nodes:
                    del self.nodes[node_id]
                    if node_id in self.node_status:
                        del self.node_status[node_id]
                    print(f"[Network] Unregistered {node_id}")
                    return {"success": True, "unregistered": node_id}
                return {"error": "Node not found"}
            
            elif command == "list_nodes":
                nodes_with_status = {}
                for node_id, address in self.nodes.items():
                    nodes_with_status[node_id] = {
                        "address": address,
                        "status": self.node_status.get(node_id, "unknown")
                    }
                return {"nodes": nodes_with_status}
            
            elif command == "create_connection":
                node1_id = args.get('node1_id')
                node2_id = args.get('node2_id')
                bandwidth = args.get('bandwidth')
                
                if not all([node1_id, node2_id, bandwidth]):
                    return {"error": "Missing required fields"}
                
                if node1_id not in self.nodes or node2_id not in self.nodes:
                    return {"error": "One or both nodes not registered"}
                
                # Add connection to both nodes (without lock to avoid blocking)
                try:
                    # Release lock before making network calls
                    self.connections[node1_id][node2_id] = bandwidth
                    self.connections[node2_id][node1_id] = bandwidth
                except Exception as e:
                    return {"error": f"Failed to update connection registry: {str(e)}"}
                
                # Tell both nodes about the connection (outside of lock)
                # Use a separate thread to avoid blocking
                def notify_nodes():
                    self._send_to_node(node1_id, {
                        "command": "add_connection",
                        "args": {"node_id": node2_id, "bandwidth": bandwidth}
                    })
                    
                    self._send_to_node(node2_id, {
                        "command": "add_connection",
                        "args": {"node_id": node1_id, "bandwidth": bandwidth}
                    })
                
                import threading
                thread = threading.Thread(target=notify_nodes, daemon=True)
                thread.start()
                
                return {
                    "success": True,
                    "connection": f"{node1_id} <-> {node2_id}",
                    "bandwidth": bandwidth
                }
            
            elif command == "initiate_transfer":
                source_node_id = args.get('source_node_id')
                target_node_id = args.get('target_node_id')
                file_name = args.get('file_name')
                file_size = args.get('file_size')
                
                if not all([source_node_id, target_node_id, file_name, file_size]):
                    return {"error": "Missing required fields"}
                
                if source_node_id not in self.nodes or target_node_id not in self.nodes:
                    return {"error": "One or both nodes not registered"}
                
                # Generate unique file ID
                file_id = hashlib.md5(f"{file_name}-{time.time()}".encode()).hexdigest()
                
                # Tell target node to prepare for transfer
                result = self._send_to_node(target_node_id, {
                    "command": "initiate_transfer",
                    "args": {
                        "file_id": file_id,
                        "file_name": file_name,
                        "file_size": file_size,
                        "source_node": source_node_id
                    }
                })
                
                if 'error' in result:
                    return result
                
                # Track transfer
                self.active_transfers[file_id] = {
                    "file_id": file_id,
                    "source_node_id": source_node_id,
                    "target_node_id": target_node_id,
                    "file_name": file_name,
                    "file_size": file_size,
                    "total_chunks": result.get('total_chunks', 0),
                    "completed_chunks": 0,
                    "status": "in_progress",
                    "started_at": time.time()
                }
                
                return {
                    "success": True,
                    "file_id": file_id,
                    "total_chunks": result.get('total_chunks', 0)
                }
            
            elif command == "process_transfer":
                file_id = args.get('file_id')
                chunks_to_process = args.get('chunks_to_process', 1)
                
                if not file_id or file_id not in self.active_transfers:
                    return {"error": "Transfer not found"}
                
                transfer = self.active_transfers[file_id]
                chunks_processed = 0
                
                # Process chunks
                for chunk_id in range(transfer['completed_chunks'],
                                        min(transfer['completed_chunks'] + chunks_to_process,
                                            transfer['total_chunks'])):
                    result = self._send_to_node(transfer['target_node_id'], {
                        "command": "process_chunk",
                        "args": {
                            "file_id": file_id,
                            "chunk_id": chunk_id,
                            "source_node": transfer['source_node_id']
                        }
                    })
                    
                    if result.get('success'):
                        chunks_processed += 1
                        transfer['completed_chunks'] += 1
                        
                        if result.get('completed'):
                            transfer['status'] = 'completed'
                            transfer['completed_at'] = time.time()
                            break
                    else:
                        transfer['status'] = 'failed'
                        break
                
                return {
                    "success": True,
                    "chunks_processed": chunks_processed,
                    "completed_chunks": transfer['completed_chunks'],
                    "total_chunks": transfer['total_chunks'],
                    "status": transfer['status'],
                    "completed": transfer['status'] == 'completed'
                }
            
            elif command == "network_stats":
                total_storage = 0
                used_storage = 0
                online_nodes = 0
                
                for node_id in self.nodes:
                    if self.node_status.get(node_id) == "online":
                        online_nodes += 1
                        result = self._send_to_node(node_id, {
                            "command": "storage_stats",
                            "args": {}
                        })
                        if 'error' not in result:
                            total_storage += result.get('total_bytes', 0)
                            used_storage += result.get('used_bytes', 0)
                
                return {
                    "total_nodes": len(self.nodes),
                    "online_nodes": online_nodes,
                    "offline_nodes": len(self.nodes) - online_nodes,
                    "total_storage_bytes": total_storage,
                    "used_storage_bytes": used_storage,
                    "storage_utilization_percent": (used_storage / total_storage * 100) if total_storage > 0 else 0,
                    "active_transfers": len([t for t in self.active_transfers.values() if t['status'] == 'in_progress']),
                    "completed_transfers": len([t for t in self.active_transfers.values() if t['status'] == 'completed'])
                }
            
            elif command == "tick":
                for node_id in self.nodes:
                    self._send_to_node(node_id, {"command": "tick", "args": {}})
                return {"success": True}
            
            else:
                return {"error": f"Unknown command: {command}"}
                
        except Exception as e:
            return {"error": str(e)}
    
    def _send_to_node(self, node_id: str, request: dict) -> dict:
        """Send a request to a specific node"""
        if node_id not in self.nodes:
            return {"error": "Node not found"}
        
        try:
            node_address = self.nodes[node_id]
            host, port = node_address.split(':')
            port = int(port)
            
            # Connect to node
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(10)
            sock.connect((host, port))
            
            # Send request
            sock.sendall(json.dumps(request).encode('utf-8'))
            
            # Receive response
            data = sock.recv(4096)
            response = json.loads(data.decode('utf-8'))
            
            sock.close()
            
            # Mark node as online if successful
            with self.lock:
                self.node_status[node_id] = "online"
            
            return response
            
        except Exception as e:
            # Mark node as offline if communication fails
            with self.lock:
                self.node_status[node_id] = "offline"
            return {"error": f"Failed to communicate with node: {str(e)}"}
    
    def _health_check_loop(self):
        """Periodically check health of all registered nodes"""
        while self.running:
            time.sleep(self.health_check_interval)
            
            with self.lock:
                nodes_to_check = list(self.nodes.keys())
            
            for node_id in nodes_to_check:
                # Send health check
                result = self._send_to_node(node_id, {
                    "command": "health",
                    "args": {}
                })
                
                # Status is already updated in _send_to_node
                if 'error' in result:
                    current_status = self.node_status.get(node_id)
                    if current_status == "online":
                        print(f"[Network] ⚠ Node {node_id} is now OFFLINE")
                else:
                    current_status = self.node_status.get(node_id)
                    if current_status == "offline":
                        print(f"[Network] ✓ Node {node_id} is back ONLINE")


def main():
    parser = argparse.ArgumentParser(description='Threaded Network Coordinator Server')
    parser.add_argument('--host', default='localhost', help='Host to bind to (default: localhost)')
    parser.add_argument('--port', type=int, default=5500, help='Port to bind to (default: 5500)')
    
    args = parser.parse_args()
    
    server = ThreadedNetworkServer(host=args.host, port=args.port)
    server.start()
    
    print(f"\nNetwork Coordinator running. Press Ctrl+C to stop.\n")
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n\nStopping server...")
        server.stop()


if __name__ == '__main__':
    main()