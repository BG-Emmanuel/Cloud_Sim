#!/usr/bin/env python3
"""
Threaded Node Server with Socket Communication and Real File Storage
Each node creates a directory and stores actual files
"""

import socket
import threading
import json
import time
import argparse
import os
import shutil
import random
from typing import Dict, Any
from storage_virtual_node import StorageVirtualNode, TransferStatus, FileTransfer, FileChunk


class ThreadedNodeServer:
    """Node server that listens on an automatically assigned socket port"""
    
    def __init__(self, node: StorageVirtualNode, host='localhost', port=0, storage_path=None):
        self.node = node
        self.host = host
        self.port = port  # 0 means auto-assign
        self.actual_port = None
        self.running = False
        self.server_socket = None
        self.lock = threading.Lock()
        self.network_host = None
        self.network_port = None
        self.registered = False
        self.simulated_offline = False  # New flag for simulated offline status
        
        # Set up storage directory
        if storage_path is None:
            storage_path = os.path.join(os.getcwd(), f"storage_{node.node_id}")
        
        self.storage_path = storage_path
        
        # Create storage directory if it doesn't exist
        os.makedirs(self.storage_path, exist_ok=True)
        print(f"[Node {self.node.node_id}] Storage directory: {self.storage_path}")
        
    def start(self):
        """Start the node server on an automatically assigned port"""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.actual_port = self.server_socket.getsockname()[1]  # Get the actual assigned port
        self.server_socket.listen(5)
        self.running = True
        
        print(f"[Node {self.node.node_id}] Server started on {self.host}:{self.actual_port}")
        
        # Start accepting connections in a separate thread
        accept_thread = threading.Thread(target=self._accept_connections, daemon=True)
        accept_thread.start()
        
        return self.actual_port
        
    def stop(self):
        """Stop the node server"""
        self.running = False
        
        # Unregister from network before stopping
        if self.registered and self.network_host and self.network_port:
            self._unregister_from_network()
        
        if self.server_socket:
            self.server_socket.close()
        print(f"[Node {self.node.node_id}] Server stopped")
        
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
            # Receive data
            data = client_socket.recv(4096)
            if not data:
                return
                
            # Parse request
            request = json.loads(data.decode('utf-8'))
            command = request.get('command')
            args = request.get('args', {})
            
            # Process command
            response = self._process_command(command, args)
            
            # Send response
            client_socket.sendall(json.dumps(response).encode('utf-8'))
            
        except Exception as e:
            print(f"[Node {self.node.node_id}] Error handling client: {e}")
        finally:
            client_socket.close()
            
    def _process_command(self, command: str, args: Dict[str, Any]) -> Dict[str, Any]:
        """Process commands from clients"""
        with self.lock:
            try:
                # Check if node is simulated offline (except for health checks and status changes)
                if self.simulated_offline and command not in ['health', 'set_online_status']:
                    return {"error": f"Node {self.node.node_id} is currently offline"}
                
                if command == "health":
                    return {"status": "healthy", "node_id": self.node.node_id}
                
                elif command == "info":
                    return {
                        "node_id": self.node.node_id,
                        "cpu_capacity": self.node.cpu_capacity,
                        "memory_capacity": self.node.memory_capacity,
                        "total_storage": self.node.total_storage,
                        "bandwidth": self.node.bandwidth,
                        "connections": list(self.node.connections.keys())
                    }
                
                elif command == "add_connection":
                    node_id = args.get('node_id')
                    bandwidth = args.get('bandwidth')
                    self.node.add_connection(node_id, bandwidth)
                    return {"success": True, "connected_to": node_id}
                
                elif command == "initiate_transfer":
                    file_id = args.get('file_id')
                    file_name = args.get('file_name')
                    file_size = args.get('file_size')
                    source_node = args.get('source_node')
                    
                    transfer = self.node.initiate_file_transfer(
                        file_id, file_name, file_size, source_node
                    )
                    
                    if not transfer:
                        return {"error": "Insufficient storage space"}
                    
                    return {
                        "success": True,
                        "file_id": transfer.file_id,
                        "total_chunks": len(transfer.chunks),
                        "chunk_size": transfer.chunks[0].size if transfer.chunks else 0
                    }
                
                elif command == "process_chunk":
                    file_id = args.get('file_id')
                    chunk_id = args.get('chunk_id')
                    source_node = args.get('source_node')
                    
                    success = self.node.process_chunk_transfer(file_id, chunk_id, source_node)
                    
                    if not success:
                        return {"error": "Failed to process chunk"}
                    
                    # Save chunk to disk
                    transfer = self.node.active_transfers.get(file_id) or self.node.stored_files.get(file_id)
                    
                    if transfer:
                        # Create file directory
                        file_dir = os.path.join(self.storage_path, file_id)
                        os.makedirs(file_dir, exist_ok=True)
                        
                        # Save chunk to disk
                        chunk_path = os.path.join(file_dir, f"chunk_{chunk_id}.dat")
                        chunk = transfer.chunks[chunk_id]
                        
                        # Write actual data to disk (simulate with zeros for now)
                        with open(chunk_path, 'wb') as f:
                            f.write(b'\0' * chunk.size)
                        
                        # Check if transfer is complete
                        is_complete = transfer.status == TransferStatus.COMPLETED
                        
                        if is_complete:
                            # Merge chunks into final file
                            final_file_path = os.path.join(self.storage_path, transfer.file_name)
                            with open(final_file_path, 'wb') as final_file:
                                for i in range(len(transfer.chunks)):
                                    chunk_path = os.path.join(file_dir, f"chunk_{i}.dat")
                                    if os.path.exists(chunk_path):
                                        with open(chunk_path, 'rb') as chunk_file:
                                            final_file.write(chunk_file.read())
                            
                            # Clean up chunk files
                            shutil.rmtree(file_dir)
                            
                            print(f"[Node {self.node.node_id}] File saved: {final_file_path} ({transfer.total_size} bytes)")
                        
                        return {
                            "success": True,
                            "chunk_id": chunk_id,
                            "completed": is_complete
                        }
                    
                    return {"error": "Transfer not found"}
                
                elif command == "storage_stats":
                    stats = self.node.get_storage_utilization()
                    
                    # Add real disk usage
                    if os.path.exists(self.storage_path):
                        total_size = 0
                        for dirpath, dirnames, filenames in os.walk(self.storage_path):
                            for filename in filenames:
                                filepath = os.path.join(dirpath, filename)
                                if os.path.exists(filepath):
                                    total_size += os.path.getsize(filepath)
                        
                        stats['actual_disk_usage_bytes'] = total_size
                        stats['actual_disk_usage_mb'] = total_size / (1024 * 1024)
                    
                    return stats
                
                elif command == "network_stats":
                    return self.node.get_network_utilization()
                
                elif command == "performance_stats":
                    return self.node.get_performance_metrics()
                
                elif command == "tick":
                    self.node.network_utilization = 0
                    return {"success": True}
                
                elif command == "create_file":
                    # New command: Create a file locally on this node
                    return self._create_local_file(args)
                
                elif command == "list_files":
                    # New command: List files stored on this node
                    return self._list_local_files()

                elif command == "delete_file":
                    # Delete a local file by name or file_id
                    return self._delete_local_file(args)
                
                elif command == "set_online_status":
                    # Special command to change online/offline status
                    online = args.get('online', True)
                    old_status = "online" if not self.simulated_offline else "offline"
                    self.simulated_offline = not online
                    new_status = "online" if online else "offline"
                    
                    # Notify network coordinator about status change
                    if self.registered and self.network_host and self.network_port:
                        try:
                            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                            sock.settimeout(5)
                            sock.connect((self.network_host, self.network_port))
                            
                            request = {
                                "command": "set_node_status",
                                "args": {
                                    "node_id": self.node.node_id,
                                    "status": new_status
                                }
                            }
                            
                            sock.sendall(json.dumps(request).encode('utf-8'))
                            data = sock.recv(4096)
                            response = json.loads(data.decode('utf-8'))
                            
                            sock.close()
                            
                            if response.get('success'):
                                print(f"[Node {self.node.node_id}] Network coordinator notified: {old_status} -> {new_status}")
                            else:
                                print(f"[Node {self.node.node_id}] Failed to notify network: {response.get('error')}")
                        except Exception as e:
                            print(f"[Node {self.node.node_id}] Error notifying network: {e}")
                    
                    print(f"[Node {self.node.node_id}] Status changed: {old_status} -> {new_status}")
                    return {"success": True, "node_id": self.node.node_id, "status": new_status, "old_status": old_status}
                
                elif command == "download_file":
                    # New command: Download a file from another node
                    return self._download_file_from_network(args)
                
                else:
                    return {"error": f"Unknown command: {command}"}
                    
            except Exception as e:
                return {"error": str(e)}
    
    def _create_local_file(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Create a file locally on this node"""
        file_name = args.get('file_name')
        file_size_mb = args.get('file_size_mb', 10)
        content_type = args.get('content_type', 'random')  # random, text, binary
        
        if not file_name:
            return {"error": "Missing file_name parameter"}
        
        file_size_bytes = file_size_mb * 1024 * 1024
        file_path = os.path.join(self.storage_path, file_name)
        
        # Check if we have enough storage space
        if self.node.used_storage + file_size_bytes > self.node.total_storage:
            return {"error": "Insufficient storage space"}
        
        try:
            # Create the file with specified content
            if content_type == 'text':
                # Create text file with repeating pattern
                text_content = "This is a sample text file created by node {}. ".format(self.node.node_id)
                text_content += "This line is repeated to fill the file. "
                
                with open(file_path, 'w') as f:
                    while f.tell() < file_size_bytes:
                        f.write(text_content)
                        
            elif content_type == 'binary':
                # Create binary file with random data
                chunk_size = 1024 * 1024  # 1MB chunks
                with open(file_path, 'wb') as f:
                    remaining = file_size_bytes
                    while remaining > 0:
                        chunk = min(chunk_size, remaining)
                        f.write(os.urandom(chunk))
                        remaining -= chunk
            else:  # random (default)
                # Create file with mix of text and binary
                chunk_size = 1024 * 1024  # 1MB chunks
                with open(file_path, 'wb') as f:
                    remaining = file_size_bytes
                    while remaining > 0:
                        chunk = min(chunk_size, remaining)
                        # Mix of random bytes and some text
                        if random.random() > 0.7:
                            # Add some structured data occasionally
                            header = f"Chunk at position {f.tell()}\n".encode()
                            f.write(header)
                            f.write(os.urandom(chunk - len(header)))
                        else:
                            f.write(os.urandom(chunk))
                        remaining -= chunk
            
            # Update node storage metrics
            self.node.used_storage += file_size_bytes
            
            # Create a file transfer record for consistency
            file_id = f"local_{file_name}_{int(time.time())}"
            chunks = self.node._generate_chunks(file_id, file_size_bytes)
            
            transfer = FileTransfer(
                file_id=file_id,
                file_name=file_name,
                total_size=file_size_bytes,
                chunks=chunks,
                status=TransferStatus.COMPLETED,
                completed_at=time.time()
            )
            
            # Mark all chunks as completed
            for chunk in chunks:
                chunk.status = TransferStatus.COMPLETED
                chunk.stored_node = self.node.node_id
            
            self.node.stored_files[file_id] = transfer
            self.node.total_requests_processed += 1
            
            actual_size = os.path.getsize(file_path)
            
            # Notify network coordinator of updated metadata (best-effort)
            try:
                if self.registered and self.network_host and self.network_port:
                    # compute current usage and files count
                    total_bytes = self.node.total_storage
                    # recalc actual disk usage
                    total_size = 0
                    for dirpath, dirnames, filenames in os.walk(self.storage_path):
                        for filename in filenames:
                            filepath = os.path.join(dirpath, filename)
                            if os.path.exists(filepath):
                                total_size += os.path.getsize(filepath)

                    files_count = len([f for f in os.listdir(self.storage_path) if os.path.isfile(os.path.join(self.storage_path, f))])

                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.settimeout(5)
                    sock.connect((self.network_host, self.network_port))
                    request = {
                        "command": "node_update",
                        "args": {
                            "node_id": self.node.node_id,
                            "address": f"{self.host}:{self.actual_port}",
                            "used_bytes": total_size,
                            "total_bytes": total_bytes,
                            "files_count": files_count,
                            "timestamp": time.time()
                        }
                    }
                    sock.sendall(json.dumps(request).encode('utf-8'))
                    try:
                        _ = sock.recv(4096)
                    except Exception:
                        pass
                    sock.close()
            except Exception:
                pass

            return {
                "success": True,
                "file_name": file_name,
                "file_path": file_path,
                "expected_size_bytes": file_size_bytes,
                "actual_size_bytes": actual_size,
                "file_id": file_id,
                "message": f"File created successfully on node {self.node.node_id}"
            }
            
        except Exception as e:
            return {"error": f"Failed to create file: {str(e)}"}

    def _delete_local_file(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Delete a file locally on this node. Accepts `file_name` or `file_id`."""
        file_name = args.get('file_name')
        file_id = args.get('file_id')

        # Prefer file_name if provided
        if not file_name and not file_id:
            return {"error": "Missing file_name or file_id parameter"}

        try:
            # If file_name provided, delete that file in storage dir
            if file_name:
                target_path = os.path.join(self.storage_path, file_name)
                if not os.path.exists(target_path):
                    return {"error": f"File '{file_name}' not found"}

                size = os.path.getsize(target_path)
                os.remove(target_path)

                # Remove stored_files entries that reference this file_name
                to_remove = [fid for fid, t in list(self.node.stored_files.items()) if t.file_name == file_name]
                for fid in to_remove:
                    try:
                        del self.node.stored_files[fid]
                    except KeyError:
                        pass

                # Remove any chunk directories named after file_ids
                for fid in to_remove:
                    chunk_dir = os.path.join(self.storage_path, fid)
                    if os.path.exists(chunk_dir):
                        try:
                            shutil.rmtree(chunk_dir)
                        except Exception:
                            pass

                # Update used storage (don't go negative)
                self.node.used_storage = max(0, self.node.used_storage - size)

                self.node.total_requests_processed += 1
                # Notify network coordinator about update
                try:
                    if self.registered and self.network_host and self.network_port:
                        total_bytes = self.node.total_storage
                        # recalc actual disk usage
                        total_size = 0
                        for dirpath, dirnames, filenames in os.walk(self.storage_path):
                            for filename in filenames:
                                filepath = os.path.join(dirpath, filename)
                                if os.path.exists(filepath):
                                    total_size += os.path.getsize(filepath)

                        files_count = len([f for f in os.listdir(self.storage_path) if os.path.isfile(os.path.join(self.storage_path, f))])

                        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sock.settimeout(5)
                        sock.connect((self.network_host, self.network_port))
                        request = {
                            "command": "node_update",
                            "args": {
                                "node_id": self.node.node_id,
                                "address": f"{self.host}:{self.actual_port}",
                                "used_bytes": total_size,
                                "total_bytes": total_bytes,
                                "files_count": files_count,
                                "timestamp": time.time()
                            }
                        }
                        sock.sendall(json.dumps(request).encode('utf-8'))
                        try:
                            _ = sock.recv(4096)
                        except Exception:
                            pass
                        sock.close()
                except Exception:
                    pass

                return {"success": True, "file_name": file_name, "deleted_bytes": size}

            # If file_id provided, try to remove based on stored_files record
            if file_id:
                transfer = self.node.stored_files.get(file_id)
                if not transfer:
                    return {"error": f"File id '{file_id}' not found"}

                # Remove actual file if exists
                file_path = os.path.join(self.storage_path, transfer.file_name)
                deleted_bytes = 0
                if os.path.exists(file_path):
                    deleted_bytes = os.path.getsize(file_path)
                    os.remove(file_path)

                # Remove chunk dir if present
                chunk_dir = os.path.join(self.storage_path, file_id)
                if os.path.exists(chunk_dir):
                    try:
                        shutil.rmtree(chunk_dir)
                    except Exception:
                        pass

                # Remove stored_files record
                try:
                    del self.node.stored_files[file_id]
                except KeyError:
                    pass

                # Update used storage
                self.node.used_storage = max(0, self.node.used_storage - deleted_bytes)
                self.node.total_requests_processed += 1
                # Notify network coordinator about update
                try:
                    if self.registered and self.network_host and self.network_port:
                        total_bytes = self.node.total_storage
                        # recalc actual disk usage
                        total_size = 0
                        for dirpath, dirnames, filenames in os.walk(self.storage_path):
                            for filename in filenames:
                                filepath = os.path.join(dirpath, filename)
                                if os.path.exists(filepath):
                                    total_size += os.path.getsize(filepath)

                        files_count = len([f for f in os.listdir(self.storage_path) if os.path.isfile(os.path.join(self.storage_path, f))])

                        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sock.settimeout(5)
                        sock.connect((self.network_host, self.network_port))
                        request = {
                            "command": "node_update",
                            "args": {
                                "node_id": self.node.node_id,
                                "address": f"{self.host}:{self.actual_port}",
                                "used_bytes": total_size,
                                "total_bytes": total_bytes,
                                "files_count": files_count,
                                "timestamp": time.time()
                            }
                        }
                        sock.sendall(json.dumps(request).encode('utf-8'))
                        try:
                            _ = sock.recv(4096)
                        except Exception:
                            pass
                        sock.close()
                except Exception:
                    pass

                return {"success": True, "file_id": file_id, "deleted_bytes": deleted_bytes}

        except Exception as e:
            return {"error": f"Failed to delete file: {str(e)}"}
    
    def _list_local_files(self) -> Dict[str, Any]:
        """List all files stored locally on this node"""
        try:
            files = []
            total_size = 0
            
            if os.path.exists(self.storage_path):
                for item in os.listdir(self.storage_path):
                    item_path = os.path.join(self.storage_path, item)
                    if os.path.isfile(item_path):
                        file_size = os.path.getsize(item_path)
                        files.append({
                            "name": item,
                            "size_bytes": file_size,
                            "size_mb": file_size / (1024 * 1024),
                            "created_time": os.path.getctime(item_path)
                        })
                        total_size += file_size
            
            return {
                "success": True,
                "node_id": self.node.node_id,
                "files": files,
                "total_files": len(files),
                "total_size_bytes": total_size,
                "total_size_mb": total_size / (1024 * 1024)
            }
        except Exception as e:
            return {"error": f"Failed to list files: {str(e)}"}
    
    def _download_file_from_network(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Download a file from another node in the network"""
        source_node_id = args.get('source_node_id')
        file_name = args.get('file_name')
        bandwidth = args.get('bandwidth', 1000)
        
        if not source_node_id or not file_name:
            return {"error": "Missing source_node_id or file_name"}
        
        # Check if we're registered with network
        if not self.registered or not self.network_host or not self.network_port:
            return {"error": "Node not registered with network"}
        
        try:
            # Step 1: Connect to network coordinator to find source node
            network_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            network_sock.settimeout(10)
            network_sock.connect((self.network_host, self.network_port))
            
            # Get node list to find source node address
            request = {
                "command": "list_nodes",
                "args": {}
            }
            
            network_sock.sendall(json.dumps(request).encode('utf-8'))
            data = network_sock.recv(4096)
            nodes_response = json.loads(data.decode('utf-8'))
            network_sock.close()
            
            if 'error' in nodes_response:
                return {"error": f"Failed to get node list: {nodes_response['error']}"}
            
            nodes = nodes_response.get('nodes', {})
            if source_node_id not in nodes:
                return {"error": f"Source node {source_node_id} not found"}
            
            source_node_info = nodes[source_node_id]
            if isinstance(source_node_info, dict):
                source_address = source_node_info.get('address', '')
                source_status = source_node_info.get('status', 'unknown')
            else:
                source_address = source_node_info
                source_status = 'online'
            
            if source_status != 'online':
                return {"error": f"Source node {source_node_id} is offline"}
            
            if ':' not in source_address:
                return {"error": f"Invalid address for source node {source_node_id}"}
            
            source_host, source_port = source_address.split(':')
            source_port = int(source_port)
            
            # Step 2: Connect to source node to get file info
            source_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            source_sock.settimeout(10)
            source_sock.connect((source_host, source_port))
            
            # First, list files on source node to find the file
            list_request = {
                "command": "list_files",
                "args": {}
            }
            
            source_sock.sendall(json.dumps(list_request).encode('utf-8'))
            data = source_sock.recv(4096)
            list_response = json.loads(data.decode('utf-8'))
            
            if not list_response.get('success'):
                source_sock.close()
                return {"error": f"Failed to list files on {source_node_id}: {list_response.get('error')}"}
            
            # Find the requested file
            source_files = list_response.get('files', [])
            file_info = None
            for f in source_files:
                if f['name'] == file_name:
                    file_info = f
                    break
            
            if not file_info:
                source_sock.close()
                return {"error": f"File '{file_name}' not found on {source_node_id}"}
            
            source_sock.close()
            
            file_size_bytes = file_info['size_bytes']
            
            # Step 3: Create connection through network coordinator
            network_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            network_sock.settimeout(10)
            network_sock.connect((self.network_host, self.network_port))
            
            conn_request = {
                "command": "create_connection",
                "args": {
                    "node1_id": source_node_id,
                    "node2_id": self.node.node_id,
                    "bandwidth": bandwidth
                }
            }
            
            network_sock.sendall(json.dumps(conn_request).encode('utf-8'))
            data = network_sock.recv(4096)
            conn_response = json.loads(data.decode('utf-8'))
            
            if not conn_response.get('success'):
                network_sock.close()
                return {"error": f"Failed to create connection: {conn_response.get('error')}"}
            
            # Step 4: Initiate transfer through network coordinator
            transfer_request = {
                "command": "initiate_transfer",
                "args": {
                    "source_node_id": source_node_id,
                    "target_node_id": self.node.node_id,
                    "file_name": file_name,
                    "file_size": file_size_bytes
                }
            }
            
            network_sock.sendall(json.dumps(transfer_request).encode('utf-8'))
            data = network_sock.recv(4096)
            transfer_response = json.loads(data.decode('utf-8'))
            
            if not transfer_response.get('success'):
                network_sock.close()
                return {"error": f"Failed to initiate transfer: {transfer_response.get('error')}"}
            
            file_id = transfer_response['file_id']
            total_chunks = transfer_response['total_chunks']
            
            # Step 5: Process the transfer
            print(f"[Node {self.node.node_id}] Downloading {file_name} from {source_node_id}...")
            
            chunks_processed = 0
            chunks_per_step = 3
            
            while chunks_processed < total_chunks:
                process_request = {
                    "command": "process_transfer",
                    "args": {
                        "file_id": file_id,
                        "chunks_to_process": chunks_per_step
                    }
                }
                
                network_sock.sendall(json.dumps(process_request).encode('utf-8'))
                data = network_sock.recv(4096)
                process_response = json.loads(data.decode('utf-8'))
                
                if not process_response.get('success'):
                    network_sock.close()
                    return {"error": f"Transfer failed: {process_response.get('error')}"}
                
                new_chunks = process_response.get('chunks_processed', 0)
                chunks_processed += new_chunks
                
                progress = (chunks_processed / total_chunks) * 100
                print(f"[Node {self.node.node_id}] Download progress: {progress:.1f}% ({chunks_processed}/{total_chunks} chunks)")
                
                if process_response.get('completed'):
                    break
            
            network_sock.close()
            
            # Verify file was downloaded
            downloaded_file_path = os.path.join(self.storage_path, file_name)
            if os.path.exists(downloaded_file_path):
                actual_size = os.path.getsize(downloaded_file_path)
                return {
                    "success": True,
                    "file_name": file_name,
                    "file_path": downloaded_file_path,
                    "source_node": source_node_id,
                    "file_size_bytes": actual_size,
                    "file_size_mb": actual_size / (1024 * 1024),
                    "message": f"File downloaded successfully from {source_node_id}"
                }
            else:
                return {"error": "Download completed but file not found locally"}
                
        except Exception as e:
            return {"error": f"Download failed: {str(e)}"}
    
    def register_with_network(self, network_host, network_port):
        """Register this node with the network coordinator"""
        self.network_host = network_host
        self.network_port = network_port
        
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(10)
            sock.connect((network_host, network_port))
            
            request = {
                "command": "register_node",
                "args": {
                    "node_id": self.node.node_id,
                    "node_address": f"{self.host}:{self.actual_port}"
                }
            }
            
            sock.sendall(json.dumps(request).encode('utf-8'))
            data = sock.recv(4096)
            response = json.loads(data.decode('utf-8'))
            
            sock.close()
            
            if response.get('success'):
                self.registered = True
                print(f"[Node {self.node.node_id}] Successfully registered with network at {network_host}:{network_port}")
                # Send initial metadata update after registering
                try:
                    # compute current usage and files count
                    total_bytes = self.node.total_storage
                    total_size = 0
                    for dirpath, dirnames, filenames in os.walk(self.storage_path):
                        for filename in filenames:
                            filepath = os.path.join(dirpath, filename)
                            if os.path.exists(filepath):
                                total_size += os.path.getsize(filepath)

                    files_count = len([f for f in os.listdir(self.storage_path) if os.path.isfile(os.path.join(self.storage_path, f))])

                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.settimeout(5)
                    sock.connect((self.network_host, self.network_port))
                    request = {
                        "command": "node_update",
                        "args": {
                            "node_id": self.node.node_id,
                            "address": f"{self.host}:{self.actual_port}",
                            "used_bytes": total_size,
                            "total_bytes": total_bytes,
                            "files_count": files_count,
                            "timestamp": time.time()
                        }
                    }
                    sock.sendall(json.dumps(request).encode('utf-8'))
                    try:
                        _ = sock.recv(4096)
                    except Exception:
                        pass
                    sock.close()
                except Exception:
                    pass
                return True
            else:
                print(f"[Node {self.node.node_id}] Failed to register: {response}")
                return False
                
        except Exception as e:
            print(f"[Node {self.node.node_id}] Error registering with network: {e}")
            return False
    
    def _unregister_from_network(self):
        """Unregister this node from the network coordinator"""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            sock.connect((self.network_host, self.network_port))
            
            request = {
                "command": "unregister_node",
                "args": {
                    "node_id": self.node.node_id
                }
            }
            
            sock.sendall(json.dumps(request).encode('utf-8'))
            data = sock.recv(4096)
            response = json.loads(data.decode('utf-8'))
            
            sock.close()
            
            if response.get('success'):
                print(f"[Node {self.node.node_id}] Successfully unregistered from network")
            
        except Exception as e:
            print(f"[Node {self.node.node_id}] Error unregistering from network: {e}")


def interactive_mode(server: ThreadedNodeServer):
    """Interactive mode for creating files and managing the node"""
    print(f"\n{'='*60}")
    print(f"INTERACTIVE MODE - Node {server.node.node_id}")
    print(f"{'='*60}")
    
    while True:
        print(f"\nOptions for Node {server.node.node_id}:")
        print("  1. Create a new file")
        print("  2. List local files")
        print("  3. Show storage statistics")
        print("  4. Show network statistics")
        print("  5. Show performance metrics")
        print("  6. Set online/offline status")
        print("  7. Download file from network")
        print("  8. Delete a local file")
        print("  9. Exit interactive mode")
        
        try:
            choice = input("\nEnter your choice (1-8): ").strip()
            
            if choice == '1':
                # Create file
                file_name = input("Enter file name: ").strip()
                if not file_name:
                    print("File name cannot be empty!")
                    continue
                
                try:
                    size_mb = float(input("Enter file size in MB: ").strip())
                    if size_mb <= 0:
                        print("File size must be positive!")
                        continue
                except ValueError:
                    print("Invalid file size!")
                    continue
                
                print("Content types:")
                print("  1. Random data (default)")
                print("  2. Text data")
                print("  3. Binary data")
                content_choice = input("Choose content type (1-3, default 1): ").strip()
                
                content_type = 'random'
                if content_choice == '2':
                    content_type = 'text'
                elif content_choice == '3':
                    content_type = 'binary'
                
                # Create the file
                result = server._create_local_file({
                    'file_name': file_name,
                    'file_size_mb': size_mb,
                    'content_type': content_type
                })
                
                if result.get('success'):
                    print(f"✓ File created successfully!")
                    print(f"  Name: {result['file_name']}")
                    print(f"  Size: {result['actual_size_bytes'] / (1024*1024):.2f} MB")
                    print(f"  Path: {result['file_path']}")
                else:
                    print(f"✗ Failed to create file: {result.get('error', 'Unknown error')}")
            
            elif choice == '2':
                # List files
                result = server._list_local_files()
                if result.get('success'):
                    files = result['files']
                    if files:
                        print(f"\nFiles stored on node {server.node.node_id}:")
                        print("-" * 60)
                        for file_info in files:
                            created_time = time.strftime('%Y-%m-%d %H:%M:%S', 
                                                        time.localtime(file_info['created_time']))
                            print(f"  {file_info['name']}")
                            print(f"    Size: {file_info['size_mb']:.2f} MB")
                            print(f"    Created: {created_time}")
                            print()
                    else:
                        print("No files stored on this node.")
                    print(f"Total: {result['total_files']} files, {result['total_size_mb']:.2f} MB")
                else:
                    print(f"✗ Failed to list files: {result.get('error', 'Unknown error')}")
            
            elif choice == '3':
                # Storage stats
                result = server._process_command("storage_stats", {})
                if 'error' not in result:
                    print(f"\nStorage Statistics for Node {server.node.node_id}:")
                    print("-" * 40)
                    print(f"Used Storage:  {result['used_bytes'] / (1024**3):.2f} GB")
                    print(f"Total Storage: {result['total_bytes'] / (1024**3):.2f} GB")
                    print(f"Utilization:   {result['utilization_percent']:.1f}%")
                    print(f"Files Stored:  {result['files_stored']}")
                    if 'actual_disk_usage_mb' in result:
                        print(f"Actual Disk:   {result['actual_disk_usage_mb']:.2f} MB")
                else:
                    print(f"✗ Failed to get storage stats: {result['error']}")
            
            elif choice == '4':
                # Network stats
                result = server._process_command("network_stats", {})
                if 'error' not in result:
                    print(f"\nNetwork Statistics for Node {server.node.node_id}:")
                    print("-" * 40)
                    print(f"Current Usage: {result['current_utilization_bps'] / 1000000:.2f} Mbps")
                    print(f"Max Bandwidth: {result['max_bandwidth_bps'] / 1000000:.2f} Mbps")
                    print(f"Utilization:   {result['utilization_percent']:.1f}%")
                    print(f"Connections:   {', '.join(result['connections'])}")
                else:
                    print(f"✗ Failed to get network stats: {result['error']}")
            
            elif choice == '5':
                # Performance stats
                result = server._process_command("performance_stats", {})
                if 'error' not in result:
                    print(f"\nPerformance Metrics for Node {server.node.node_id}:")
                    print("-" * 40)
                    print(f"Requests Processed: {result['total_requests_processed']}")
                    print(f"Data Transferred:   {result['total_data_transferred_bytes'] / (1024**2):.2f} MB")
                    print(f"Failed Transfers:   {result['failed_transfers']}")
                    print(f"Active Transfers:   {result['current_active_transfers']}")
                else:
                    print(f"✗ Failed to get performance stats: {result['error']}")
            
            elif choice == '6':
                # Set online/offline status
                current_status = "online" if not server.simulated_offline else "offline"
                print(f"\nCurrent status: {current_status.upper()}")
                print("1. Go ONLINE (accept requests)")
                print("2. Go OFFLINE (reject requests)")
                
                status_choice = input("Select status (1-2): ").strip()
                if status_choice == '1':
                    result = server._process_command("set_online_status", {"online": True})
                    if result.get('success'):
                        print("✓ Node set to ONLINE - accepting requests")
                        print("  Network coordinator has been notified")
                    else:
                        print(f"✗ Failed to set node online: {result.get('error')}")
                elif status_choice == '2':
                    result = server._process_command("set_online_status", {"online": False})
                    if result.get('success'):
                        print("✓ Node set to OFFLINE - rejecting requests")
                        print("  Network coordinator has been notified")
                    else:
                        print(f"✗ Failed to set node offline: {result.get('error')}")
                else:
                    print("Invalid choice!")
            
            elif choice == '7':
                # Download file from network
                if not server.registered:
                    print("✗ Node is not registered with network. Cannot download files.")
                    continue
                
                # Get available nodes from network
                try:
                    network_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    network_sock.settimeout(10)
                    network_sock.connect((server.network_host, server.network_port))
                    
                    request = {
                        "command": "list_nodes",
                        "args": {}
                    }
                    
                    network_sock.sendall(json.dumps(request).encode('utf-8'))
                    data = network_sock.recv(4096)
                    nodes_response = json.loads(data.decode('utf-8'))
                    network_sock.close()
                    
                    if 'error' in nodes_response:
                        print(f"✗ Failed to get node list: {nodes_response['error']}")
                        continue
                    
                    nodes = nodes_response.get('nodes', {})
                    online_nodes = {}
                    
                    for node_id, node_info in nodes.items():
                        if isinstance(node_info, dict):
                            status = node_info.get('status', 'unknown')
                            address = node_info.get('address', '')
                        else:
                            status = 'online'
                            address = node_info
                        
                        if status == 'online' and node_id != server.node.node_id:
                            online_nodes[node_id] = address
                    
                    if not online_nodes:
                        print("✗ No other online nodes available for download")
                        continue
                    
                    print(f"\nAvailable source nodes:")
                    node_list = list(online_nodes.keys())
                    for i, node_id in enumerate(node_list, 1):
                        print(f"  {i}. {node_id}")
                    
                    try:
                        node_choice = int(input(f"\nSelect source node (1-{len(node_list)}): ")) - 1
                        source_node_id = node_list[node_choice]
                    except (ValueError, IndexError):
                        print("Invalid selection!")
                        continue
                    
                    # Connect to source node to list files
                    source_address = online_nodes[source_node_id]
                    if ':' not in source_address:
                        print(f"Invalid address for node {source_node_id}")
                        continue
                    
                    source_host, source_port = source_address.split(':')
                    source_port = int(source_port)
                    
                    source_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    source_sock.settimeout(10)
                    source_sock.connect((source_host, source_port))
                    
                    list_request = {
                        "command": "list_files",
                        "args": {}
                    }
                    
                    source_sock.sendall(json.dumps(list_request).encode('utf-8'))
                    data = source_sock.recv(4096)
                    list_response = json.loads(data.decode('utf-8'))
                    source_sock.close()
                    
                    if not list_response.get('success'):
                        print(f"✗ Failed to list files on {source_node_id}: {list_response.get('error')}")
                        continue
                    
                    source_files = list_response.get('files', [])
                    if not source_files:
                        print(f"✗ No files available on {source_node_id}")
                        continue
                    
                    print(f"\nFiles available on {source_node_id}:")
                    for i, file_info in enumerate(source_files, 1):
                        print(f"  {i}. {file_info['name']} ({file_info['size_mb']:.2f} MB)")
                    
                    try:
                        file_choice = int(input(f"\nSelect file to download (1-{len(source_files)}): ")) - 1
                        selected_file = source_files[file_choice]
                    except (ValueError, IndexError):
                        print("Invalid selection!")
                        continue
                    
                    try:
                        bandwidth = int(input("\nEnter download bandwidth in Mbps (default: 1000): ") or "1000")
                    except ValueError:
                        bandwidth = 1000
                    
                    # Start download
                    print(f"\nStarting download from {source_node_id}...")
                    result = server._download_file_from_network({
                        'source_node_id': source_node_id,
                        'file_name': selected_file['name'],
                        'bandwidth': bandwidth
                    })
                    
                    if result.get('success'):
                        print(f"✓ Download completed successfully!")
                        print(f"  File: {result['file_name']}")
                        print(f"  Size: {result['file_size_mb']:.2f} MB")
                        print(f"  Path: {result['file_path']}")
                        print(f"  Source: {result['source_node']}")
                    else:
                        print(f"✗ Download failed: {result.get('error', 'Unknown error')}")
                        
                except Exception as e:
                    print(f"✗ Error during download setup: {e}")
            
            elif choice == '8':
                # Delete a local file
                list_result = server._list_local_files()
                if not list_result.get('success'):
                    print(f"✗ Failed to list files: {list_result.get('error')}")
                    continue

                files = list_result.get('files', [])
                if not files:
                    print("No files available to delete.")
                    continue

                print(f"\nFiles stored on node {server.node.node_id}:")
                for i, f in enumerate(files, 1):
                    print(f"  {i}. {f['name']} ({f['size_mb']:.2f} MB)")

                try:
                    del_choice = int(input(f"\nSelect file to delete (1-{len(files)}): ")) - 1
                    target = files[del_choice]
                except (ValueError, IndexError):
                    print("Invalid selection!")
                    continue

                confirm = input(f"Are you sure you want to delete '{target['name']}'? (y/N): ").strip().lower()
                if confirm != 'y':
                    print("Deletion cancelled.")
                    continue

                result = server._delete_local_file({'file_name': target['name']})
                if result.get('success'):
                    print(f"✓ Deleted '{target['name']}' ({result.get('deleted_bytes', 0)} bytes)")
                else:
                    print(f"✗ Failed to delete file: {result.get('error', 'Unknown error')}")

            elif choice == '9':
                print("Exiting interactive mode...")
                break
            
            else:
                print("Invalid choice! Please enter 1-8.")
                
        except KeyboardInterrupt:
            print("\nExiting interactive mode...")
            break
        except Exception as e:
            print(f"Error: {e}")


def main():
    parser = argparse.ArgumentParser(description='Threaded Node Server with Auto Port Assignment')
    parser.add_argument('--node-id', required=True, help='Node identifier')
    parser.add_argument('--network-host', default='localhost', 
                       help='Network coordinator host (default: localhost)')
    parser.add_argument('--network-port', type=int, default=5500,
                       help='Network coordinator port (default: 5500)')
    parser.add_argument('--host', default='localhost', 
                       help='Host to bind to (default: localhost)')
    parser.add_argument('--cpu', type=int, default=4, 
                       help='CPU capacity in vCPUs (default: 4)')
    parser.add_argument('--memory', type=int, default=16, 
                       help='Memory capacity in GB (default: 16)')
    parser.add_argument('--storage', type=int, default=500, 
                       help='Storage capacity in GB (default: 500)')
    parser.add_argument('--bandwidth', type=int, default=1000, 
                       help='Bandwidth in Mbps (default: 1000)')
    parser.add_argument('--storage-path', type=str, default=None,
                       help='Custom storage directory path (default: ./storage_<node-id>)')
    parser.add_argument('--interactive', action='store_true',
                       help='Start in interactive mode after initialization')
    
    args = parser.parse_args()
    
    print("=" * 70)
    print(f"Starting Storage Node: {args.node_id}")
    print("=" * 70)
    
    # Create the storage node
    node = StorageVirtualNode(
        node_id=args.node_id,
        cpu_capacity=args.cpu,
        memory_capacity=args.memory,
        storage_capacity=args.storage,
        bandwidth=args.bandwidth
    )
    
    print(f"\nNode Configuration:")
    print(f"  CPU:       {args.cpu} vCPUs")
    print(f"  Memory:    {args.memory} GB")
    print(f"  Storage:   {args.storage} GB")
    print(f"  Bandwidth: {args.bandwidth} Mbps")
    
    # Create and start the server (port auto-assigned)
    server = ThreadedNodeServer(node, host=args.host, port=0, storage_path=args.storage_path)
    assigned_port = server.start()
    
    print(f"\n✓ Server listening on {args.host}:{assigned_port} (auto-assigned)")
    
    # Register with network coordinator
    print(f"\nRegistering with network coordinator at {args.network_host}:{args.network_port}...")
    
    # Retry registration a few times
    max_retries = 5
    for attempt in range(max_retries):
        if server.register_with_network(args.network_host, args.network_port):
            break
        else:
            if attempt < max_retries - 1:
                print(f"Retrying in 2 seconds... (attempt {attempt + 2}/{max_retries})")
                time.sleep(2)
            else:
                print(f"\n⚠ Warning: Could not register with network coordinator.")
                print(f"   Node is running but not connected to network.")
                print(f"   Make sure network coordinator is running on {args.network_host}:{args.network_port}")
    
    print(f"\n{'=' * 70}")
    print(f"Node '{args.node_id}' is ready and running!")
    print(f"Press Ctrl+C to stop.")
    print(f"{'=' * 70}\n")
    
    # Start interactive mode if requested
    if args.interactive:
        interactive_mode(server)
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n\nStopping server...")
        server.stop()
        print("✓ Server stopped\n")


if __name__ == '__main__':
    main()