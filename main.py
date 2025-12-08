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
    
    def set_node_status(self, node_id: str, status: str):
        """Set node status (online/offline)"""
        return self._send_request("set_node_status", {
            "node_id": node_id,
            "status": status
        })
    
    def get_node_status(self, node_id: str):
        """Get node status"""
        return self._send_request("get_node_status", {
            "node_id": node_id
        })


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

    def delete_file(self, file_name: str):
        """Delete a file on this node by name"""
        return self._send_request("delete_file", {"file_name": file_name})
    
    def set_online_status(self, online: bool):
        """Set node online/offline status"""
        return self._send_request("set_online_status", {
            'online': online
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
        print("  5. Manage node online/offline status")
        print("  6. Delete file on a node")
        print("  7. Exit")
        
        try:
            choice = input("\nEnter your choice (1-6): ").strip()
            
            if choice == '1':
                list_nodes_and_files(network_client, network_host, network_port)
            
            elif choice == '2':
                transfer_file_interactive(network_client, network_host, network_port)
            
            elif choice == '3':
                create_file_interactive(network_client, network_host, network_port)
            
            elif choice == '4':
                show_network_stats(network_client)
            
            elif choice == '5':
                manage_node_status(network_client, network_host, network_port)
            elif choice == '6':
                # Delete a file on a reachable node
                nodes_list = network_client.list_nodes()
                if 'error' in nodes_list:
                    print(f"✗ Failed to get nodes: {nodes_list['error']}")
                    continue

                registered_nodes = nodes_list.get('nodes', {})
                reachable_nodes = {}
                for node_id, node_data in registered_nodes.items():
                    if isinstance(node_data, dict) and node_data.get('status') == 'online' and ':' in node_data.get('address', ''):
                        host, port = node_data['address'].split(':')
                        client = NodeClient(host, int(port))
                        if not _check_simulated_offline(client):
                            reachable_nodes[node_id] = (host, int(port))

                if not reachable_nodes:
                    print("No reachable nodes available")
                    continue

                print("\nAvailable nodes:")
                node_list = list(reachable_nodes.keys())
                for i, nid in enumerate(node_list, 1):
                    print(f"  {i}. {nid}")

                try:
                    choice_idx = int(input(f"\nSelect node (1-{len(node_list)}): ")) - 1
                    selected_node = node_list[choice_idx]
                except (ValueError, IndexError):
                    print("Invalid selection!")
                    continue

                host, port = reachable_nodes[selected_node]
                node_client = NodeClient(host, port)

                files_result = node_client.list_files()
                if not files_result.get('success'):
                    print(f"✗ Failed to list files: {files_result.get('error')}")
                    continue

                files = files_result.get('files', [])
                if not files:
                    print("No files available on selected node")
                    continue

                print(f"\nFiles on {selected_node}:")
                for i, f in enumerate(files, 1):
                    print(f"  {i}. {f['name']} ({f['size_mb']:.2f} MB)")

                try:
                    file_choice = int(input(f"\nSelect file to delete (1-{len(files)}): ")) - 1
                    target = files[file_choice]
                except (ValueError, IndexError):
                    print("Invalid selection!")
                    continue

                confirm = input(f"Are you sure you want to delete '{target['name']}' on {selected_node}? (y/N): ").strip().lower()
                if confirm != 'y':
                    print("Deletion cancelled.")
                    continue

                del_res = node_client.delete_file(target['name'])
                if del_res.get('success'):
                    print(f"✓ Deleted '{target['name']}' from {selected_node}")
                else:
                    print(f"✗ Failed to delete file: {del_res.get('error', 'Unknown error')}")

            elif choice == '7':
                print("Exiting interactive mode...")
                break
            
            else:
                print("Invalid choice! Please enter 1-6.")
                
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
    for node_id, info in nodes_info.items():
        status_symbol = "●" if info['status'] == 'online' else "○"
        simulated_status = ""
        
        if node_id in node_clients:
            simulated_offline = _check_simulated_offline(node_clients[node_id])
            if simulated_offline:
                simulated_status = " [SIMULATED OFFLINE]"
                status_symbol = "○"
        
        print(f"\n{status_symbol} {node_id.upper()} [{info['status'].upper()}]{simulated_status}:")
        print(f"  Address: {info['address']}")
        
        # Only try to get files if node is not simulated offline
        if node_id in node_clients and not _check_simulated_offline(node_clients[node_id]):
            # Get node info
            node_info = node_clients[node_id].info()
            if 'error' not in node_info:
                print(f"  Storage: {node_info.get('total_storage', 0) / (1024**3):.0f} GB")
            
            # Get files list
            files_result = node_clients[node_id].list_files()
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
        else:
            print("  Files: Node is offline")
    
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
    
    # Create node clients and check simulated offline status
    node_clients = {}
    reachable_nodes = {}
    
    for node_id, node_data in registered_nodes.items():
        if isinstance(node_data, dict) and node_data.get('status') == 'online' and ':' in node_data.get('address', ''):
            address = node_data['address']
            host, port = address.split(':')
            client = NodeClient(host, int(port))
            node_clients[node_id] = client
            
            # Check if node is actually reachable and not simulated offline
            if not _check_simulated_offline(client):
                reachable_nodes[node_id] = node_data
    
    if len(reachable_nodes) < 2:
        print("Need at least 2 reachable nodes for file transfer")
        if len(reachable_nodes) == 1:
            print("Only one node is reachable:", list(reachable_nodes.keys())[0])
        return
    
    # Select source node
    print("\nAvailable source nodes:")
    node_list = list(reachable_nodes.keys())
    for i, node_id in enumerate(node_list, 1):
        print(f"  {i}. {node_id}")
    
    try:
        source_choice = int(input(f"\nSelect source node (1-{len(node_list)}): ")) - 1
        source_node_id = node_list[source_choice]
    except (ValueError, IndexError):
        print("Invalid selection!")
        return
    
    # Get files from source node
    source_info = reachable_nodes[source_node_id]
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
    
    # Create node clients and check simulated offline status
    node_clients = {}
    reachable_nodes = {}
    
    for node_id, node_data in registered_nodes.items():
        if isinstance(node_data, dict) and node_data.get('status') == 'online' and ':' in node_data.get('address', ''):
            address = node_data['address']
            host, port = address.split(':')
            client = NodeClient(host, int(port))
            node_clients[node_id] = client
            
            # Check if node is actually reachable and not simulated offline
            if not _check_simulated_offline(client):
                reachable_nodes[node_id] = node_data
    
    if not reachable_nodes:
        print("No reachable nodes available")
        return
    
    # Select node
    print("\nAvailable nodes:")
    node_list = list(reachable_nodes.keys())
    for i, node_id in enumerate(node_list, 1):
        print(f"  {i}. {node_id}")
    
    try:
        node_choice = int(input(f"\nSelect node (1-{len(node_list)}): ")) - 1
        selected_node_id = node_list[node_choice]
    except (ValueError, IndexError):
        print("Invalid selection!")
        return
    
    # Get node client
    node_info = reachable_nodes[selected_node_id]
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


def manage_node_status(network_client, network_host, network_port):
    """Manage node online/offline status"""
    print(f"\n{'='*70}")
    print("MANAGE NODE ONLINE/OFFLINE STATUS")
    print(f"{'='*70}")
    
    # Get available nodes
    nodes_list = network_client.list_nodes()
    if 'error' in nodes_list:
        print(f"✗ Failed to get nodes: {nodes_list['error']}")
        return
    
    registered_nodes = nodes_list.get('nodes', {})
    
    if not registered_nodes:
        print("No nodes registered with the network")
        return
    
    # Display nodes and their current status
    print("\nCurrent Node Status:")
    print("-" * 50)
    
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
    
    # Create node clients for nodes that are reachable
    node_clients = {}
    for node_id, info in nodes_info.items():
        if info['status'] == 'online' and ':' in info['address']:
            host, port = info['address'].split(':')
            node_clients[node_id] = NodeClient(host, int(port))
    
    # Display nodes
    for i, (node_id, info) in enumerate(nodes_info.items(), 1):
        status_symbol = "●" if info['status'] == 'online' else "○"
        simulated_status = " (simulated offline)" if node_id in node_clients and _check_simulated_offline(node_clients[node_id]) else ""
        print(f"  {i}. {status_symbol} {node_id} - {info['status'].upper()}{simulated_status}")
    
    # Select node to manage
    try:
        node_choice = int(input(f"\nSelect node to manage (1-{len(nodes_info)}): ")) - 1
        selected_node_id = list(nodes_info.keys())[node_choice]
        selected_node_info = nodes_info[selected_node_id]
    except (ValueError, IndexError):
        print("Invalid selection!")
        return
    
    # Get current status details
    current_network_status = selected_node_info['status']
    current_simulated_status = "unknown"
    
    if selected_node_id in node_clients:
        simulated_offline = _check_simulated_offline(node_clients[selected_node_id])
        current_simulated_status = "offline" if simulated_offline else "online"
    
    print(f"\nNode: {selected_node_id}")
    print(f"Network Status: {current_network_status.upper()}")
    if selected_node_id in node_clients:
        print(f"Simulated Status: {current_simulated_status.upper()}")
    print(f"Address: {selected_node_info['address']}")
    
    # Show management options
    print(f"\nManagement Options for {selected_node_id}:")
    print("  1. Set node ONLINE")
    print("  2. Set node OFFLINE")
    print("  3. Test node connectivity")
    
    try:
        action_choice = input("\nSelect action (1-3): ").strip()
        
        if action_choice == '1':
            # Set node online - BOTH network and node
            print(f"\nSetting {selected_node_id} to ONLINE...")
            
            # First update network coordinator
            network_result = network_client.set_node_status(selected_node_id, "online")
            if network_result.get('success'):
                print(f"✓ Network coordinator: {selected_node_id} -> ONLINE")
                old_status = network_result.get('old_status', 'unknown')
                if old_status != 'online':
                    print(f"  Status changed from {old_status} to online")
            else:
                print(f"✗ Failed to update network coordinator: {network_result.get('error')}")
                return
            
            # Then update the node itself if reachable
            if selected_node_id in node_clients:
                try:
                    node_result = node_clients[selected_node_id].set_online_status(True)
                    if node_result.get('success'):
                        print(f"✓ Node {selected_node_id} set to ONLINE")
                        old_node_status = node_result.get('old_status', 'unknown')
                        if old_node_status != 'online':
                            print(f"  Node status changed from {old_node_status} to online")
                    else:
                        print(f"⚠ Could not update node directly: {node_result.get('error')}")
                except Exception as e:
                    print(f"⚠ Could not reach node for direct update: {e}")
            else:
                print(f"⚠ Node {selected_node_id} is not reachable for direct status update")
            
            print(f"\n✓ {selected_node_id} is now ONLINE in the network")
        
        elif action_choice == '2':
            # Set node offline - BOTH network and node
            print(f"\nSetting {selected_node_id} to OFFLINE...")
            
            # First update network coordinator
            network_result = network_client.set_node_status(selected_node_id, "offline")
            if network_result.get('success'):
                print(f"✓ Network coordinator: {selected_node_id} -> OFFLINE")
                old_status = network_result.get('old_status', 'unknown')
                if old_status != 'offline':
                    print(f"  Status changed from {old_status} to offline")
            else:
                print(f"✗ Failed to update network coordinator: {network_result.get('error')}")
                return
            
            # Then update the node itself if reachable
            if selected_node_id in node_clients:
                try:
                    node_result = node_clients[selected_node_id].set_online_status(False)
                    if node_result.get('success'):
                        print(f"✓ Node {selected_node_id} set to OFFLINE")
                        old_node_status = node_result.get('old_status', 'unknown')
                        if old_node_status != 'offline':
                            print(f"  Node status changed from {old_node_status} to offline")
                    else:
                        print(f"⚠ Could not update node directly: {node_result.get('error')}")
                except Exception as e:
                    print(f"⚠ Could not reach node for direct update: {e}")
            else:
                print(f"⚠ Node {selected_node_id} is not reachable for direct status update")
            
            print(f"\n✓ {selected_node_id} is now OFFLINE in the network")
        
        elif action_choice == '3':
            # Test connectivity
            print(f"\nTesting connectivity to {selected_node_id}...")
            if selected_node_id in node_clients:
                # Test basic health
                health_result = node_clients[selected_node_id]._send_request("health", {})
                if 'error' in health_result:
                    print(f"✗ Node is unreachable: {health_result['error']}")
                else:
                    print(f"✓ Node is reachable and healthy")
                
                # Test simulated status
                simulated_offline = _check_simulated_offline(node_clients[selected_node_id])
                status = "OFFLINE" if simulated_offline else "ONLINE"
                print(f"  Simulated status: {status}")
                
                # Test other commands
                info_result = node_clients[selected_node_id].info()
                if 'error' in info_result:
                    print(f"  Info command: Failed ({info_result['error']})")
                else:
                    print(f"  Info command: Success")
            else:
                print(f"✗ Node is not reachable at {selected_node_info['address']}")
        
        else:
            print("Invalid action!")
            
    except Exception as e:
        print(f"Error during management: {e}")
    """Manage node online/offline status"""
    print(f"\n{'='*70}")
    print("MANAGE NODE ONLINE/OFFLINE STATUS")
    print(f"{'='*70}")
    
    # Get available nodes
    nodes_list = network_client.list_nodes()
    if 'error' in nodes_list:
        print(f"✗ Failed to get nodes: {nodes_list['error']}")
        return
    
    registered_nodes = nodes_list.get('nodes', {})
    
    if not registered_nodes:
        print("No nodes registered with the network")
        return
    
    # Display nodes and their current status
    print("\nCurrent Node Status:")
    print("-" * 50)
    
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
    
    # Create node clients for nodes that are reachable
    node_clients = {}
    for node_id, info in nodes_info.items():
        if ':' in info['address']:
            host, port = info['address'].split(':')
            node_clients[node_id] = NodeClient(host, int(port))
    
    # Display nodes
    for i, (node_id, info) in enumerate(nodes_info.items(), 1):
        status_symbol = "●" if info['status'] == 'online' else "○"
        # Check actual node status if reachable
        actual_status = info['status']
        if node_id in node_clients:
            try:
                node_status_result = node_clients[node_id]._send_request("health", {})
                if 'error' in node_status_result:
                    actual_status = "offline"
                else:
                    # Try to get more detailed status
                    info_result = node_clients[node_id].info()
                    if 'error' in info_result and 'offline' in info_result['error'].lower():
                        actual_status = "offline"
            except:
                actual_status = "offline"
        
        status_text = f"{info['status'].upper()}"
        if info['status'] != actual_status:
            status_text += f" (node reports: {actual_status.upper()})"
        
        print(f"  {i}. {status_symbol} {node_id} - {status_text}")
    
    # Select node to manage
    try:
        node_choice = int(input(f"\nSelect node to manage (1-{len(nodes_info)}): ")) - 1
        selected_node_id = list(nodes_info.keys())[node_choice]
        selected_node_info = nodes_info[selected_node_id]
    except (ValueError, IndexError):
        print("Invalid selection!")
        return
    
    # Get current status details
    current_network_status = selected_node_info['status']
    
    print(f"\nNode: {selected_node_id}")
    print(f"Network Status: {current_network_status.upper()}")
    print(f"Address: {selected_node_info['address']}")
    
    # Show management options
    print(f"\nManagement Options for {selected_node_id}:")
    print("  1. Set node ONLINE")
    print("  2. Set node OFFLINE")
    print("  3. Test node connectivity")
    
    try:
        action_choice = input("\nSelect action (1-3): ").strip()
        
        if action_choice == '1':
            # Set node online - BOTH network and node
            print(f"\nSetting {selected_node_id} to ONLINE...")
            
            # First update network coordinator
            network_result = network_client.set_node_status(selected_node_id, "online")
            if network_result.get('success'):
                print(f"✓ Network coordinator: {selected_node_id} -> ONLINE")
                old_status = network_result.get('old_status', 'unknown')
                if old_status != 'online':
                    print(f"  Status changed from {old_status} to online")
            else:
                print(f"✗ Failed to update network coordinator: {network_result.get('error')}")
                return
            
            # Then update the node itself if reachable
            if selected_node_id in node_clients:
                try:
                    node_result = node_clients[selected_node_id].set_online_status(True)
                    if node_result.get('success'):
                        print(f"✓ Node {selected_node_id} set to ONLINE")
                        old_node_status = node_result.get('old_status', 'unknown')
                        if old_node_status != 'online':
                            print(f"  Node status changed from {old_node_status} to online")
                    else:
                        print(f"⚠ Could not update node directly: {node_result.get('error')}")
                except Exception as e:
                    print(f"⚠ Could not reach node for direct update: {e}")
            else:
                print(f"⚠ Node {selected_node_id} is not reachable for direct status update")
            
            print(f"\n✓ {selected_node_id} is now ONLINE in the network")
        
        elif action_choice == '2':
            # Set node offline - BOTH network and node
            print(f"\nSetting {selected_node_id} to OFFLINE...")
            
            # First update network coordinator
            network_result = network_client.set_node_status(selected_node_id, "offline")
            if network_result.get('success'):
                print(f"✓ Network coordinator: {selected_node_id} -> OFFLINE")
                old_status = network_result.get('old_status', 'unknown')
                if old_status != 'offline':
                    print(f"  Status changed from {old_status} to offline")
            else:
                print(f"✗ Failed to update network coordinator: {network_result.get('error')}")
                return
            
            # Then update the node itself if reachable
            if selected_node_id in node_clients:
                try:
                    node_result = node_clients[selected_node_id].set_online_status(False)
                    if node_result.get('success'):
                        print(f"✓ Node {selected_node_id} set to OFFLINE")
                        old_node_status = node_result.get('old_status', 'unknown')
                        if old_node_status != 'offline':
                            print(f"  Node status changed from {old_node_status} to offline")
                    else:
                        print(f"⚠ Could not update node directly: {node_result.get('error')}")
                except Exception as e:
                    print(f"⚠ Could not reach node for direct update: {e}")
            else:
                print(f"⚠ Node {selected_node_id} is not reachable for direct status update")
            
            print(f"\n✓ {selected_node_id} is now OFFLINE in the network")
        
        elif action_choice == '3':
            # Test connectivity
            print(f"\nTesting connectivity to {selected_node_id}...")
            if selected_node_id in node_clients:
                # Test basic health
                health_result = node_clients[selected_node_id]._send_request("health", {})
                if 'error' in health_result:
                    print(f"✗ Node is unreachable: {health_result['error']}")
                else:
                    print(f"✓ Node is reachable and healthy")
                
                # Test info command to check if node rejects requests
                info_result = node_clients[selected_node_id].info()
                if 'error' in info_result:
                    if 'offline' in info_result['error'].lower():
                        print(f"  Node status: OFFLINE (rejecting requests)")
                    else:
                        print(f"  Info command: Failed ({info_result['error']})")
                else:
                    print(f"  Info command: Success")
                    print(f"  Node status: ONLINE (accepting requests)")
            else:
                print(f"✗ Node is not reachable at {selected_node_info['address']}")
        
        else:
            print("Invalid action!")
            
    except Exception as e:
        print(f"Error during management: {e}")
    """Manage node online/offline status"""
    print(f"\n{'='*70}")
    print("MANAGE NODE ONLINE/OFFLINE STATUS")
    print(f"{'='*70}")
    
    # Get available nodes
    nodes_list = network_client.list_nodes()
    if 'error' in nodes_list:
        print(f"✗ Failed to get nodes: {nodes_list['error']}")
        return
    
    registered_nodes = nodes_list.get('nodes', {})
    
    if not registered_nodes:
        print("No nodes registered with the network")
        return
    
    # Display nodes and their current status
    print("\nCurrent Node Status:")
    print("-" * 50)
    
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
    
    # Create node clients for nodes that are reachable
    node_clients = {}
    for node_id, info in nodes_info.items():
        if info['status'] == 'online' and ':' in info['address']:
            host, port = info['address'].split(':')
            node_clients[node_id] = NodeClient(host, int(port))
    
    # Display nodes
    for i, (node_id, info) in enumerate(nodes_info.items(), 1):
        status_symbol = "●" if info['status'] == 'online' else "○"
        simulated_status = " (simulated offline)" if node_id in node_clients and _check_simulated_offline(node_clients[node_id]) else ""
        print(f"  {i}. {status_symbol} {node_id} - {info['status'].upper()}{simulated_status}")
    
    # Select node to manage
    try:
        node_choice = int(input(f"\nSelect node to manage (1-{len(nodes_info)}): ")) - 1
        selected_node_id = list(nodes_info.keys())[node_choice]
        selected_node_info = nodes_info[selected_node_id]
    except (ValueError, IndexError):
        print("Invalid selection!")
        return
    
    # Get current status details
    current_network_status = selected_node_info['status']
    current_simulated_status = "unknown"
    
    if selected_node_id in node_clients:
        simulated_offline = _check_simulated_offline(node_clients[selected_node_id])
        current_simulated_status = "offline" if simulated_offline else "online"
    
    print(f"\nNode: {selected_node_id}")
    print(f"Network Status: {current_network_status.upper()}")
    if selected_node_id in node_clients:
        print(f"Simulated Status: {current_simulated_status.upper()}")
    print(f"Address: {selected_node_info['address']}")
    
    # Show management options - MERGED OPTIONS
    print(f"\nManagement Options for {selected_node_id}:")
    print("  1. Set node ONLINE")
    print("  2. Set node OFFLINE")
    print("  3. Test node connectivity")
    
    try:
        action_choice = input("\nSelect action (1-3): ").strip()
        
        if action_choice == '1':
            # Set node online - BOTH network and node
            print(f"\nSetting {selected_node_id} to ONLINE...")
            
            # First update network coordinator
            network_result = network_client.set_node_status(selected_node_id, "online")
            if network_result.get('success'):
                print(f"✓ Network coordinator updated: {selected_node_id} -> ONLINE")
            else:
                print(f"✗ Failed to update network coordinator: {network_result.get('error')}")
            
            # Then update the node itself if reachable
            if selected_node_id in node_clients:
                node_result = node_clients[selected_node_id].set_online_status(True)
                if node_result.get('success'):
                    print(f"✓ Node {selected_node_id} set to ONLINE")
                else:
                    print(f"✗ Failed to set node status: {node_result.get('error')}")
            else:
                print(f"⚠ Node {selected_node_id} is not reachable for direct status update")
        
        elif action_choice == '2':
            # Set node offline - BOTH network and node
            print(f"\nSetting {selected_node_id} to OFFLINE...")
            
            # First update network coordinator
            network_result = network_client.set_node_status(selected_node_id, "offline")
            if network_result.get('success'):
                print(f"✓ Network coordinator updated: {selected_node_id} -> OFFLINE")
            else:
                print(f"✗ Failed to update network coordinator: {network_result.get('error')}")
            
            # Then update the node itself if reachable
            if selected_node_id in node_clients:
                node_result = node_clients[selected_node_id].set_online_status(False)
                if node_result.get('success'):
                    print(f"✓ Node {selected_node_id} set to OFFLINE")
                else:
                    print(f"✗ Failed to set node status: {node_result.get('error')}")
            else:
                print(f"⚠ Node {selected_node_id} is not reachable for direct status update")
        
        elif action_choice == '3':
            # Test connectivity
            print(f"\nTesting connectivity to {selected_node_id}...")
            if selected_node_id in node_clients:
                # Test basic health
                health_result = node_clients[selected_node_id]._send_request("health", {})
                if 'error' in health_result:
                    print(f"✗ Node is unreachable: {health_result['error']}")
                else:
                    print(f"✓ Node is reachable and healthy")
                    
                # Test simulated status
                simulated_offline = _check_simulated_offline(node_clients[selected_node_id])
                status = "OFFLINE" if simulated_offline else "ONLINE"
                print(f"  Simulated status: {status}")
                
                # Test other commands
                info_result = node_clients[selected_node_id].info()
                if 'error' in info_result:
                    print(f"  Info command: Failed ({info_result['error']})")
                else:
                    print(f"  Info command: Success")
            else:
                print(f"✗ Node is not reachable at {selected_node_info['address']}")
        
        else:
            print("Invalid action!")
            
    except Exception as e:
        print(f"Error during management: {e}")
def _check_simulated_offline(node_client):
    """Check if a node is in simulated offline mode"""
    try:
        # Try to get info - if it fails with offline message, node is simulated offline
        result = node_client.info()
        if 'error' in result and 'offline' in result['error'].lower():
            return True
        return False
    except:
        return False


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