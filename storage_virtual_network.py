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
        self.node_status: Dict[str, str] = {}  # node_id -> "online"/"offline" (manual control)
        self.connections: Dict[str, Dict[str, int]] = defaultdict(dict)
        self.active_transfers: Dict[str, dict] = {}
        self.running = False
        self.server_socket = None
        self.lock = threading.Lock()
        
    def start(self):
        """Start the network coordinator server"""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        self.running = True
        
        print(f"[Network] Coordinator started on {self.host}:{self.port}")
        print(f"[Network] Node status is MANUAL - use set_node_status command to change online/offline states")
        
        # Start accepting connections in a separate thread
        accept_thread = threading.Thread(target=self._accept_connections, daemon=True)
        accept_thread.start()
        
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
                    # Register node as online by default
                    self.nodes[node_id] = node_address
                    self.node_status[node_id] = "online"
                
                print(f"[Network] Registered {node_id} at {node_address} [ONLINE]")
                return {"success": True, "registered": node_id, "status": "online"}
            
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
            
            elif command == "set_node_status":
                node_id = args.get('node_id')
                status = args.get('status')  # 'online' or 'offline'
                
                if not node_id or status not in ['online', 'offline']:
                    return {"error": "Missing node_id or invalid status"}
                
                with self.lock:
                    if node_id in self.nodes:
                        old_status = self.node_status.get(node_id, 'unknown')
                        self.node_status[node_id] = status
                        
                        # Log the status change
                        if old_status != status:
                            print(f"[Network] Node {node_id} status changed: {old_status} -> {status}")
                            
                            # If setting to offline, cancel any active transfers involving this node
                            if status == "offline":
                                self._cancel_transfers_for_node(node_id)
                        else:
                            print(f"[Network] Node {node_id} status remains: {status}")
                        
                        return {
                            "success": True, 
                            "node_id": node_id, 
                            "status": status, 
                            "old_status": old_status,
                            "message": f"Node {node_id} is now {status.upper()}"
                        }
                    else:
                        return {"error": "Node not found"}
            
            elif command == "get_node_status":
                node_id = args.get('node_id')
                
                with self.lock:
                    if node_id in self.nodes:
                        status = self.node_status.get(node_id, 'unknown')
                        return {
                            "success": True, 
                            "node_id": node_id, 
                            "status": status
                        }
                    else:
                        return {"error": "Node not found"}
            
            elif command == "create_connection":
                node1_id = args.get('node1_id')
                node2_id = args.get('node2_id')
                bandwidth = args.get('bandwidth')
                
                if not all([node1_id, node2_id, bandwidth]):
                    return {"error": "Missing required fields"}
                
                with self.lock:
                    if node1_id not in self.nodes or node2_id not in self.nodes:
                        return {"error": "One or both nodes not registered"}
                    
                    # Check if both nodes are online
                    if self.node_status.get(node1_id) != "online":
                        return {"error": f"Source node {node1_id} is offline"}
                    if self.node_status.get(node2_id) != "online":
                        return {"error": f"Target node {node2_id} is offline"}
                    
                    self.connections[node1_id][node2_id] = bandwidth
                    self.connections[node2_id][node1_id] = bandwidth
                
                # Tell both nodes about the connection
                result1 = self._send_to_node(node1_id, {
                    "command": "add_connection",
                    "args": {"node_id": node2_id, "bandwidth": bandwidth}
                })
                
                result2 = self._send_to_node(node2_id, {
                    "command": "add_connection",
                    "args": {"node_id": node1_id, "bandwidth": bandwidth}
                })
                
                # Check if connection setup was successful
                if 'error' in result1 or 'error' in result2:
                    # Rollback connection if one node failed
                    with self.lock:
                        if node1_id in self.connections and node2_id in self.connections[node1_id]:
                            del self.connections[node1_id][node2_id]
                        if node2_id in self.connections and node1_id in self.connections[node2_id]:
                            del self.connections[node2_id][node1_id]
                    
                    errors = []
                    if 'error' in result1:
                        errors.append(f"{node1_id}: {result1['error']}")
                    if 'error' in result2:
                        errors.append(f"{node2_id}: {result2['error']}")
                    
                    return {"error": f"Failed to establish connection: {', '.join(errors)}"}
                
                print(f"[Network] Created connection {node1_id} <-> {node2_id} @ {bandwidth}Mbps")
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
                    
                    # Check if both nodes are online
                    if self.node_status.get(source_node_id) != "online":
                        return {"error": f"Source node {source_node_id} is offline"}
                    if self.node_status.get(target_node_id) != "online":
                        return {"error": f"Target node {target_node_id} is offline"}
                    
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
                
                # Tell target node to prepare
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
                
                print(f"[Network] Initiated transfer {file_id[:8]}...: {source_node_id} -> {target_node_id} ({file_name})")
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
                
                # Check if nodes are still online before processing
                with self.lock:
                    if self.node_status.get(source_node_id) != "online":
                        transfer['status'] = 'failed'
                        return {"error": f"Source node {source_node_id} is offline"}
                    if self.node_status.get(target_node_id) != "online":
                        transfer['status'] = 'failed'
                        return {"error": f"Target node {target_node_id} is offline"}
                
                # Process chunks
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
                                print(f"[Network] Transfer {file_id[:8]}... completed: {source_node_id} -> {target_node_id}")
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
                    progress = (transfer['completed_chunks'] / transfer['total_chunks']) * 100 if transfer['total_chunks'] > 0 else 0
                    
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
                    online_nodes = sum(1 for node_id in node_ids if self.node_status.get(node_id) == "online")
                
                # Only try to get storage stats from online nodes
                for node_id in node_ids:
                    with self.lock:
                        if self.node_status.get(node_id) != "online":
                            continue
                    
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
                        "completed_transfers": len([t for t in self.active_transfers.values() if t['status'] == 'completed']),
                        "failed_transfers": len([t for t in self.active_transfers.values() if t['status'] == 'failed'])
                    }
            
            elif command == "tick":
                with self.lock:
                    node_ids = list(self.nodes.keys())
                
                # Only send tick to online nodes
                for node_id in node_ids:
                    with self.lock:
                        if self.node_status.get(node_id) != "online":
                            continue
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
        
        # Check if node is manually set to offline
        with self.lock:
            if self.node_status.get(node_id) != "online":
                return {"error": f"Node {node_id} is offline"}
        
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
            
            return response
            
        except Exception as e:
            return {"error": f"Failed to communicate with node: {str(e)}"}
    
    def _cancel_transfers_for_node(self, node_id: str):
        """Cancel all active transfers involving the specified node"""
        transfers_to_cancel = []
        
        # Find transfers involving this node
        for file_id, transfer in self.active_transfers.items():
            if (transfer['source_node_id'] == node_id or 
                transfer['target_node_id'] == node_id):
                transfers_to_cancel.append(file_id)
        
        # Cancel the transfers
        for file_id in transfers_to_cancel:
            transfer = self.active_transfers[file_id]
            transfer['status'] = 'failed'
            print(f"[Network] Cancelled transfer {file_id[:8]}... due to node {node_id} going offline")


def main():
    parser = argparse.ArgumentParser(description='Threaded Network Coordinator Server')
    parser.add_argument('--host', default='localhost', help='Host to bind to (default: localhost)')
    parser.add_argument('--port', type=int, default=5500, help='Port to bind to (default: 5500)')
    
    args = parser.parse_args()
    
    server = ThreadedNetworkServer(host=args.host, port=args.port)
    server.start()
    
    print(f"\nNetwork Coordinator running with MANUAL node status control.")
    print(f"Use 'set_node_status' command to change node online/offline states.")
    print(f"Press Ctrl+C to stop.\n")
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n\nStopping server...")
        server.stop()


if __name__ == '__main__':
    main()