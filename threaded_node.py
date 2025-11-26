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
from storage_virtual_node import StorageVirtualNode, TransferStatus


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
        print("  6. Exit interactive mode")
        
        try:
            choice = input("\nEnter your choice (1-6): ").strip()
            
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
                print("Exiting interactive mode...")
                break
            
            else:
                print("Invalid choice! Please enter 1-6.")
                
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