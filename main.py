#!/usr/bin/env python3
"""
Threaded Storage Virtual Network - Main Client
Connects to network coordinator and discovers nodes automatically
Supports real file transfers with interactive mode
"""

import socket
import json
import time
import argparse
import os
from tqdm import tqdm


class NetworkClient:
    """Client to communicate with network coordinator"""
    
    def __init__(self, host='localhost', port=5500):
        self.host = host
        self.port = port
        
    def _send_request(self, command: str, args: dict = None) -> dict:
        """Send a request to the network coordinator"""
        if args is None:
            args = {}
            
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(30)
            sock.connect((self.host, self.port))
            
            request = {"command": command, "args": args}
            sock.sendall(json.dumps(request).encode('utf-8'))
            
            data = sock.recv(4096)
            response = json.loads(data.decode('utf-8'))
            
            sock.close()
            return response
            
        except Exception as e:
            return {"error": f"Failed to communicate with network: {str(e)}"}
    
    def list_nodes(self):
        """List all registered nodes"""
        return self._send_request("list_nodes")
    
    def create_connection(self, node1_id: str, node2_id: str, bandwidth: int):
        """Create connection between two nodes"""
        return self._send_request("create_connection", {
            "node1_id": node1_id,
            "node2_id": node2_id,
            "bandwidth": bandwidth
        })
    
    def initiate_transfer(self, source_node_id: str, target_node_id: str, 
                         file_name: str, file_size: int):
        """Initiate a file transfer"""
        return self._send_request("initiate_transfer", {
            "source_node_id": source_node_id,
            "target_node_id": target_node_id,
            "file_name": file_name,
            "file_size": file_size
        })
    
    def process_transfer(self, file_id: str, chunks_to_process: int = 1):
        """Process chunks of a transfer"""
        return self._send_request("process_transfer", {
            "file_id": file_id,
            "chunks_to_process": chunks_to_process
        })
    
    def network_stats(self):
        """Get network statistics"""
        return self._send_request("network_stats")
    
    def tick(self):
        """Reset network utilization"""
        return self._send_request("tick")


class NodeClient:
    """Client to communicate with individual nodes"""
    
    def __init__(self, host, port):
        self.host = host
        self.port = port
        
    def _send_request(self, command: str, args: dict = None) -> dict:
        """Send a request to a node"""
        if args is None:
            args = {}
            
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(10)
            sock.connect((self.host, self.port))
            
            request = {"command": command, "args": args}
            sock.sendall(json.dumps(request).encode('utf-8'))
            
            data = sock.recv(4096)
            response = json.loads(data.decode('utf-8'))
            
            sock.close()
            return response
            
        except Exception as e:
            return {"error": f"Failed to communicate with node: {str(e)}"}
    
    def info(self):
        """Get node information"""
        return self._send_request("info")
    
    def storage_stats(self):
        """Get storage statistics"""
        return self._send_request("storage_stats")
    
    def performance_stats(self):
        """Get performance metrics"""
        return self._send_request("performance_stats")
    
    def list_files(self):
        """List files stored on this node"""
        return self._send_request("list_files")
    
    def create_file(self, file_name: str, file_size_mb: int, content_type: str = 'random'):
        """Create a file on this node"""
        return self._send_request("create_file", {
            'file_name': file_name,
            'file_size_mb': file_size_mb,
            'content_type': content_type
        })


def transfer_file(network_client, source_node_id, target_node_id, file_name, file_size_bytes, chunks_per_step=3):
    """Transfer a file between nodes with progress display"""
    print(f"\nInitiating transfer: {file_name} ({file_size_bytes / (1024*1024):.2f}MB)")
    print(f"Source: {source_node_id} → Target: {target_node_id}")
    
    transfer_result = network_client.initiate_transfer(
        source_node_id=source_node_id,
        target_node_id=target_node_id,
        file_name=file_name,
        file_size=file_size_bytes
    )
    
    if not transfer_result.get('success'):
        print(f"✗ Failed to initiate transfer: {transfer_result}")
        return False
    
    file_id = transfer_result['file_id']
    total_chunks = transfer_result['total_chunks']
    print(f"✓ Transfer initiated (ID: {file_id[:8]}...)")
    print(f"✓ Total chunks: {total_chunks}")
    
    # Process transfer with progress bar
    print(f"\nProcessing transfer with {chunks_per_step} chunks per step...")
    print("-" * 70)
    
    with tqdm(total=total_chunks, desc="Transferring", unit="chunk",
              bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]',
              ncols=70) as pbar:
        completed = False
        total_start_time = time.time()
        
        while not completed:
            # Tick the network
            network_client.tick()
            
            # Process chunks
            try:
                result = network_client.process_transfer(file_id, chunks_per_step)
                
                if result.get('success'):
                    chunks_processed = result['chunks_processed']
                    pbar.update(chunks_processed)
                    completed = result.get('completed', False)
                    
                    if completed:
                        total_time = time.time() - total_start_time
                        print(f"\n✓ Transfer completed successfully!")
                        print(f"  Total time: {total_time:.2f} seconds")
                        print(f"  Average speed: {(file_size_bytes / (1024*1024) / total_time):.2f} MB/s")
                        print(f"  File stored on {target_node_id}")
                        return True
                else:
                    print(f"\n✗ Transfer failed: {result}")
                    return False
                    
            except Exception as e:
                print(f"\n✗ Error during transfer: {e}")
                return False
            
            time.sleep(0.1)
    
    return False


def interactive_mode(network_client, network_host, network_port):
    """Interactive mode for file transfers"""
    print(f"\n{'='*70}")
    print("INTERACTIVE FILE TRANSFER MODE")
    print(f"{'='*70}")
    
    while True:
        print(f"\nOptions:")
        print("  1. List all nodes and their files")
        print("  2. Transfer file between nodes")
        print("  3. Create file on a node")
        print("  4. Show network statistics")
        print("  5. Exit")
        
        try:
            choice = input("\nEnter your choice (1-5): ").strip()
            
            if choice == '1':
                list_nodes_and_files(network_client, network_host, network_port)
            
            elif choice == '2':
                transfer_file_interactive(network_client, network_host, network_port)
            
            elif choice == '3':
                create_file_interactive(network_client, network_host, network_port)
            
            elif choice == '4':
                show_network_stats(network_client)
            
            elif choice == '5':
                print("Exiting interactive mode...")
                break
            
            else:
                print("Invalid choice! Please enter 1-5.")
                
        except KeyboardInterrupt:
            print("\nExiting interactive mode...")
            break
        except Exception as e:
            print(f"Error: {e}")


def list_nodes_and_files(network_client, network_host, network_port):
    """List all nodes and the files they contain"""
    print(f"\n{'='*70}")
    print("NODES AND FILES OVERVIEW")
    print(f"{'='*70}")
    
    # Get nodes from network
    nodes_list = network_client.list_nodes()
    
    if 'error' in nodes_list:
        print(f"✗ Failed to connect to network: {nodes_list['error']}")
        return
    
    registered_nodes = nodes_list.get('nodes', {})
    
    if len(registered_nodes) == 0:
        print("No nodes registered with the network")
        return
    
    # Parse node information
    nodes_info = {}
    for node_id, node_data in registered_nodes.items():
        if isinstance(node_data, dict):
            nodes_info[node_id] = {
                'address': node_data.get('address', ''),
                'status': node_data.get('status', 'unknown')
            }
        else:
            nodes_info[node_id] = {
                'address': node_data,
                'status': 'online'
            }
    
    # Create node clients and get file lists
    node_clients = {}
    for node_id, info in nodes_info.items():
        if info['status'] == 'online' and ':' in info['address']:
            host, port = info['address'].split(':')
            node_clients[node_id] = NodeClient(host, int(port))
    
    # Display nodes and their files
    for node_id, client in node_clients.items():
        print(f"\n● {node_id.upper()} [{nodes_info[node_id]['status'].upper()}]:")
        print(f"  Address: {nodes_info[node_id]['address']}")
        
        # Get node info
        node_info = client.info()
        if 'error' not in node_info:
            print(f"  Storage: {node_info.get('total_storage', 0) / (1024**3):.0f} GB")
        
        # Get files list
        files_result = client.list_files()
        if files_result.get('success'):
            files = files_result.get('files', [])
            if files:
                print(f"  Files ({len(files)}):")
                for file_info in files:
                    print(f"    - {file_info['name']} ({file_info['size_mb']:.2f} MB)")
            else:
                print("  Files: No files stored")
        else:
            print("  Files: Unable to retrieve file list")
    
    print(f"\nTotal nodes: {len(registered_nodes)}")
    online_count = sum(1 for info in nodes_info.values() if info['status'] == 'online')
    print(f"Online nodes: {online_count}")


def transfer_file_interactive(network_client, network_host, network_port):
    """Interactive file transfer between nodes"""
    print(f"\n{'='*70}")
    print("FILE TRANSFER")
    print(f"{'='*70}")
    
    # Get available nodes
    nodes_list = network_client.list_nodes()
    if 'error' in nodes_list:
        print(f"✗ Failed to get nodes: {nodes_list['error']}")
        return
    
    registered_nodes = nodes_list.get('nodes', {})
    online_nodes = {node_id: info for node_id, info in registered_nodes.items() 
                   if isinstance(info, dict) and info.get('status') == 'online'}
    
    if len(online_nodes) < 2:
        print("Need at least 2 online nodes for file transfer")
        return
    
    # Select source node
    print("\nAvailable source nodes:")
    node_list = list(online_nodes.keys())
    for i, node_id in enumerate(node_list, 1):
        print(f"  {i}. {node_id}")
    
    try:
        source_choice = int(input(f"\nSelect source node (1-{len(node_list)}): ")) - 1
        source_node_id = node_list[source_choice]
    except (ValueError, IndexError):
        print("Invalid selection!")
        return
    
    # Get files from source node
    source_info = online_nodes[source_node_id]
    if ':' not in source_info['address']:
        print("Invalid node address")
        return
    
    host, port = source_info['address'].split(':')
    source_client = NodeClient(host, int(port))
    
    files_result = source_client.list_files()
    if not files_result.get('success'):
        print(f"✗ Failed to get files from {source_node_id}: {files_result.get('error')}")
        return
    
    source_files = files_result.get('files', [])
    if not source_files:
        print(f"✗ No files available on {source_node_id}")
        return
    
    # Select file from source node
    print(f"\nFiles available on {source_node_id}:")
    for i, file_info in enumerate(source_files, 1):
        print(f"  {i}. {file_info['name']} ({file_info['size_mb']:.2f} MB)")
    
    try:
        file_choice = int(input(f"\nSelect file to transfer (1-{len(source_files)}): ")) - 1
        selected_file = source_files[file_choice]
    except (ValueError, IndexError):
        print("Invalid selection!")
        return
    
    # Select target node
    print(f"\nAvailable target nodes (excluding {source_node_id}):")
    target_nodes = [node_id for node_id in node_list if node_id != source_node_id]
    for i, node_id in enumerate(target_nodes, 1):
        print(f"  {i}. {node_id}")
    
    try:
        target_choice = int(input(f"\nSelect target node (1-{len(target_nodes)}): ")) - 1
        target_node_id = target_nodes[target_choice]
    except (ValueError, IndexError):
        print("Invalid selection!")
        return
    
    # Get bandwidth
    try:
        bandwidth = int(input("\nEnter connection bandwidth in Mbps (default: 1000): ") or "1000")
    except ValueError:
        bandwidth = 1000
    
    # Create connection
    print(f"\nCreating connection {source_node_id} <-> {target_node_id} @ {bandwidth}Mbps...")
    conn_result = network_client.create_connection(source_node_id, target_node_id, bandwidth)
    if not conn_result.get('success'):
        print(f"✗ Failed to create connection: {conn_result}")
        return
    print("✓ Connection created successfully!")
    
    # Transfer file
    file_name = selected_file['name']
    file_size_bytes = selected_file['size_bytes']
    
    success = transfer_file(
        network_client=network_client,
        source_node_id=source_node_id,
        target_node_id=target_node_id,
        file_name=file_name,
        file_size_bytes=file_size_bytes,
        chunks_per_step=3
    )
    
    if success:
        print(f"\n✓ File transfer completed: {file_name} from {source_node_id} to {target_node_id}")
    else:
        print(f"\n✗ File transfer failed")


def create_file_interactive(network_client, network_host, network_port):
    """Create a file on a specific node"""
    print(f"\n{'='*70}")
    print("CREATE FILE ON NODE")
    print(f"{'='*70}")
    
    # Get available nodes
    nodes_list = network_client.list_nodes()
    if 'error' in nodes_list:
        print(f"✗ Failed to get nodes: {nodes_list['error']}")
        return
    
    registered_nodes = nodes_list.get('nodes', {})
    online_nodes = {node_id: info for node_id, info in registered_nodes.items() 
                   if isinstance(info, dict) and info.get('status') == 'online'}
    
    if not online_nodes:
        print("No online nodes available")
        return
    
    # Select node
    print("\nAvailable nodes:")
    node_list = list(online_nodes.keys())
    for i, node_id in enumerate(node_list, 1):
        print(f"  {i}. {node_id}")
    
    try:
        node_choice = int(input(f"\nSelect node (1-{len(node_list)}): ")) - 1
        selected_node_id = node_list[node_choice]
    except (ValueError, IndexError):
        print("Invalid selection!")
        return
    
    # Get node client
    node_info = online_nodes[selected_node_id]
    if ':' not in node_info['address']:
        print("Invalid node address")
        return
    
    host, port = node_info['address'].split(':')
    node_client = NodeClient(host, int(port))
    
    # Get file details
    file_name = input("\nEnter file name: ").strip()
    if not file_name:
        print("File name cannot be empty!")
        return
    
    try:
        file_size_mb = float(input("Enter file size in MB: ").strip())
        if file_size_mb <= 0:
            print("File size must be positive!")
            return
    except ValueError:
        print("Invalid file size!")
        return
    
    print("\nContent types:")
    print("  1. Random data (default)")
    print("  2. Text data")
    print("  3. Binary data")
    content_choice = input("Choose content type (1-3, default 1): ").strip()
    
    content_type = 'random'
    if content_choice == '2':
        content_type = 'text'
    elif content_choice == '3':
        content_type = 'binary'
    
    # Create file
    print(f"\nCreating file '{file_name}' ({file_size_mb} MB) on {selected_node_id}...")
    result = node_client.create_file(file_name, file_size_mb, content_type)
    
    if result.get('success'):
        print(f"✓ File created successfully!")
        print(f"  Node: {selected_node_id}")
        print(f"  File: {result['file_name']}")
        print(f"  Size: {result['actual_size_bytes'] / (1024*1024):.2f} MB")
        print(f"  Path: {result['file_path']}")
    else:
        print(f"✗ Failed to create file: {result.get('error', 'Unknown error')}")


def show_network_stats(network_client):
    """Display network statistics"""
    print(f"\n{'='*70}")
    print("NETWORK STATISTICS")
    print(f"{'='*70}")
    
    try:
        stats = network_client.network_stats()
        
        print(f"\nNetwork Overview:")
        print(f"  Total Nodes:      {stats['total_nodes']}")
        print(f"  Online Nodes:     {stats.get('online_nodes', 0)}")
        print(f"  Offline Nodes:    {stats.get('offline_nodes', 0)}")
        print(f"  Storage Used:     {stats['used_storage_bytes'] / (1024**3):.2f}GB / " +
              f"{stats['total_storage_bytes'] / (1024**3):.2f}GB " +
              f"({stats['storage_utilization_percent']:.1f}%)")
        print(f"  Active Transfers: {stats['active_transfers']}")
        print(f"  Completed:        {stats['completed_transfers']}")
        
    except Exception as e:
        print(f"Could not retrieve network stats: {e}")


def main():
    parser = argparse.ArgumentParser(description='Threaded Storage Network Client')
    parser.add_argument('--network-host', default='localhost',
                       help='Network coordinator host (default: localhost)')
    parser.add_argument('--network-port', type=int, default=5500,
                       help='Network coordinator port (default: 5500)')
    parser.add_argument('--source-node', default='node1',
                       help='Source node ID for transfer (default: node1)')
    parser.add_argument('--target-node', default='node2',
                       help='Target node ID for transfer (default: node2)')
    parser.add_argument('--file-size-mb', type=int, default=100,
                       help='File size in MB to transfer (default: 100)')
    parser.add_argument('--file-path', type=str, default=None,
                       help='Path to actual file to transfer (optional)')
    parser.add_argument('--chunks-per-step', type=int, default=3,
                       help='Number of chunks to process per step (default: 3)')
    parser.add_argument('--connection-bandwidth', type=int, default=1000,
                       help='Connection bandwidth in Mbps (default: 1000)')
    parser.add_argument('--interactive', action='store_true',
                       help='Start in interactive mode')
    
    args = parser.parse_args()
    
    # Create network client
    network_client = NetworkClient(args.network_host, args.network_port)
    
    # Test connection to network
    nodes_list = network_client.list_nodes()
    if 'error' in nodes_list:
        print(f"✗ Failed to connect to network: {nodes_list['error']}")
        print(f"\nMake sure network coordinator is running:")
        print(f"  python storage_virtual_network.py --port {args.network_port}")
        return
    
    print("=" * 70)
    print("THREADED STORAGE VIRTUAL NETWORK CLIENT")
    print("=" * 70)
    print(f"Connected to network at {args.network_host}:{args.network_port}")
    
    # Start interactive mode if requested
    if args.interactive:
        interactive_mode(network_client, args.network_host, args.network_port)
        return
    
    # Otherwise run the original automated transfer
    print("Running automated file transfer...")
    
    # Determine file info
    if args.file_path and os.path.exists(args.file_path):
        file_size_bytes = os.path.getsize(args.file_path)
        file_name = os.path.basename(args.file_path)
        file_size_mb = file_size_bytes / (1024 * 1024)
        print(f"Using actual file: {args.file_path} ({file_size_mb:.2f}MB)")
    else:
        file_size_bytes = args.file_size_mb * 1024 * 1024
        file_name = "large_dataset.zip"
        file_size_mb = args.file_size_mb
        if args.file_path:
            print(f"Warning: File '{args.file_path}' not found, using simulated transfer")
    
    print(f"\nTransfer: {args.source_node} → {args.target_node}")
    print(f"File: {file_name} ({file_size_mb:.2f}MB)")
    print(f"Chunks per step: {args.chunks_per_step}")
    
    # ========================================================================
    # STEP 1: Discover registered nodes
    # ========================================================================
    print("\n" + "=" * 70)
    print("[1/5] DISCOVERING REGISTERED NODES")
    print("=" * 70)
    
    nodes_list = network_client.list_nodes()
    registered_nodes = nodes_list.get('nodes', {})
    
    # Parse node information (handle both dict and string formats)
    nodes_info = {}
    for node_id, node_data in registered_nodes.items():
        if isinstance(node_data, dict):
            nodes_info[node_id] = {
                'address': node_data.get('address', ''),
                'status': node_data.get('status', 'unknown')
            }
        else:
            nodes_info[node_id] = {
                'address': node_data,
                'status': 'online'
            }
    
    print(f"✓ Found {len(nodes_info)} registered node(s):")
    online_count = 0
    offline_count = 0
    
    for node_id, info in nodes_info.items():
        status = info['status']
        address = info['address']
        status_symbol = "●" if status == "online" else "○"
        status_text = "ONLINE" if status == "online" else "OFFLINE"
        
        print(f"  {status_symbol} {node_id} at {address} [{status_text}]")
        
        if status == "online":
            online_count += 1
        else:
            offline_count += 1
    
    print(f"\n  Online: {online_count} | Offline: {offline_count}")
    
    # Check if required nodes exist and are online
    if args.source_node not in nodes_info:
        print(f"\n✗ Source node '{args.source_node}' not found")
        print(f"Available nodes: {', '.join(nodes_info.keys())}")
        return
    
    if nodes_info[args.source_node]['status'] != 'online':
        print(f"\n✗ Source node '{args.source_node}' is OFFLINE")
        return
    
    if args.target_node not in nodes_info:
        print(f"\n✗ Target node '{args.target_node}' not found")
        print(f"Available nodes: {', '.join(nodes_info.keys())}")
        return
    
    if nodes_info[args.target_node]['status'] != 'online':
        print(f"\n✗ Target node '{args.target_node}' is OFFLINE")
        return
    
    # ========================================================================
    # STEP 2: Create network connections
    # ========================================================================
    print("\n" + "=" * 70)
    print("[2/5] CREATING NETWORK CONNECTIONS")
    print("=" * 70)
    
    result = network_client.create_connection(
        args.source_node, 
        args.target_node, 
        args.connection_bandwidth
    )
    
    if result.get('success'):
        print(f"✓ Connected {args.source_node} <-> {args.target_node} @ {args.connection_bandwidth}Mbps")
    else:
        print(f"✗ Failed to connect nodes: {result}")
        return
    
    # ========================================================================
    # STEP 3: File transfer operation
    # ========================================================================
    print("\n" + "=" * 70)
    print("[3/5] FILE TRANSFER OPERATION")
    print("=" * 70)
    
    success = transfer_file(
        network_client=network_client,
        source_node_id=args.source_node,
        target_node_id=args.target_node,
        file_name=file_name,
        file_size_bytes=file_size_bytes,
        chunks_per_step=args.chunks_per_step
    )
    
    if not success:
        return
    
    # ========================================================================
    # STEP 4: Display final statistics
    # ========================================================================
    print("\n" + "=" * 70)
    print("[4/5] FINAL STATISTICS")
    print("=" * 70)
    
    show_network_stats(network_client)
    
    print("\n" + "=" * 70)
    print("\n✓ Simulation completed successfully!\n")


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
    except Exception as e:
        print(f"\n\nUnexpected error: {e}")
        import traceback
        traceback.print_exc()