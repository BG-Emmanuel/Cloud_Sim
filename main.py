#!/usr/bin/env python3
"""
Threaded Storage Virtual Network - Main Client
Connects to network coordinator and discovers nodes automatically
Supports real file transfers
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
    
    args = parser.parse_args()
    
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
    
    print("=" * 70)
    print("THREADED STORAGE VIRTUAL NETWORK - FILE TRANSFER SIMULATION")
    print("=" * 70)
    print(f"\nConnecting to network at {args.network_host}:{args.network_port}")
    print(f"Transfer: {args.source_node} → {args.target_node}")
    print(f"File: {file_name} ({file_size_mb:.2f}MB)")
    print(f"Chunks per step: {args.chunks_per_step}")
    
    # Create network client
    network_client = NetworkClient(args.network_host, args.network_port)
    
    # ========================================================================
    # STEP 1: Discover registered nodes
    # ========================================================================
    print("\n" + "=" * 70)
    print("[1/5] DISCOVERING REGISTERED NODES")
    print("=" * 70)
    
    nodes_list = network_client.list_nodes()
    
    if 'error' in nodes_list:
        print(f"✗ Failed to connect to network: {nodes_list['error']}")
        print(f"\nMake sure network coordinator is running:")
        print(f"  python threaded_network_server.py --port {args.network_port}")
        return
    
    registered_nodes = nodes_list.get('nodes', {})
    
    if len(registered_nodes) == 0:
        print("✗ No nodes registered with the network yet")
        print(f"\nStart some nodes first:")
        print(f"  python threaded_node_server.py --node-id node1 --network-port {args.network_port}")
        print(f"  python threaded_node_server.py --node-id node2 --network-port {args.network_port}")
        return
    
    # Parse node information (handle both dict and string formats)
    nodes_info = {}
    for node_id, node_data in registered_nodes.items():
        if isinstance(node_data, dict):
            # New format with status
            nodes_info[node_id] = {
                'address': node_data.get('address', ''),
                'status': node_data.get('status', 'unknown')
            }
        else:
            # Old format (just address string)
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
    # STEP 2: Display network topology
    # ========================================================================
    print("\n" + "=" * 70)
    print("[2/5] NETWORK TOPOLOGY")
    print("=" * 70)
    
    # Create clients for each node to get detailed info
    node_clients = {}
    for node_id, info in nodes_info.items():
        address = info['address']
        if ':' in address:
            host, port = address.split(':')
            node_clients[node_id] = NodeClient(host, int(port))
    
    for node_id, client in node_clients.items():
        node_info = client.info()
        status = nodes_info[node_id]['status']
        status_symbol = "●" if status == "online" else "○"
        
        if 'error' not in node_info:
            print(f"\n{status_symbol} {node_id.upper()} [{status.upper()}]:")
            print(f"  Address:   {nodes_info[node_id]['address']}")
            print(f"  CPU:       {node_info.get('cpu_capacity')} vCPUs")
            print(f"  Memory:    {node_info.get('memory_capacity')} GB")
            print(f"  Storage:   {node_info.get('total_storage', 0) / (1024**3):.0f} GB")
            print(f"  Bandwidth: {node_info.get('bandwidth', 0) / 1000000:.0f} Mbps")
        else:
            print(f"\n{status_symbol} {node_id.upper()} [{status.upper()}]:")
            print(f"  Address:   {nodes_info[node_id]['address']}")
            print(f"  Status:    Unable to retrieve info (node may be offline)")
    
    # ========================================================================
    # STEP 3: Create network connections
    # ========================================================================
    print("\n" + "=" * 70)
    print("[3/5] CREATING NETWORK CONNECTIONS")
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
    
    print("\n" + "=" * 70)
    print("[4/5] FILE TRANSFER OPERATION")
    print("=" * 70)
    
    print(f"\nInitiating transfer: {file_name} ({file_size_mb:.2f}MB)")
    print(f"Source: {args.source_node} → Target: {args.target_node}")
    
    transfer_result = network_client.initiate_transfer(
        source_node_id=args.source_node,
        target_node_id=args.target_node,
        file_name=file_name,
        file_size=file_size_bytes
    )
    
    if not transfer_result.get('success'):
        print(f"✗ Failed to initiate transfer: {transfer_result}")
        return
    
    file_id = transfer_result['file_id']
    total_chunks = transfer_result['total_chunks']
    print(f"✓ Transfer initiated (ID: {file_id[:8]}...)")
    print(f"✓ Total chunks: {total_chunks}")
    
    # Process transfer with progress bar
    print(f"\nProcessing transfer with {args.chunks_per_step} chunks per step...")
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
                result = network_client.process_transfer(file_id, args.chunks_per_step)
                
                if result.get('success'):
                    chunks_processed = result['chunks_processed']
                    pbar.update(chunks_processed)
                    completed = result.get('completed', False)
                    
                    if completed:
                        total_time = time.time() - total_start_time
                        print(f"\n✓ Transfer completed successfully!")
                        print(f"  Total time: {total_time:.2f} seconds")
                        print(f"  Average speed: {(file_size_mb / total_time):.2f} MB/s")
                        print(f"\n  File stored on {args.target_node}")
                        
                        # Get target node info to show where file is stored
                        if args.target_node in node_clients:
                            storage = node_clients[args.target_node].storage_stats()
                            if 'actual_disk_usage_mb' in storage:
                                print(f"  Actual disk usage: {storage['actual_disk_usage_mb']:.2f} MB")
                        
                        break
                else:
                    print(f"\n✗ Transfer failed: {result}")
                    break
                    
            except Exception as e:
                print(f"\n✗ Error during transfer: {e}")
                break
            
            time.sleep(0.1)
    
    # ========================================================================
    # STEP 5: Display final statistics
    # ========================================================================
    print("\n" + "=" * 70)
    print("[5/5] FINAL STATISTICS")
    print("=" * 70)
    
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
        
        # Get node-specific stats
        print(f"\nNode-Specific Details:")
        print("-" * 70)
        
        for node_id in [args.source_node, args.target_node]:
            if node_id in node_clients:
                try:
                    storage = node_clients[node_id].storage_stats()
                    perf = node_clients[node_id].performance_stats()
                    
                    print(f"\n{node_id.upper()}:")
                    print(f"  Storage:  {storage['used_bytes'] / (1024**3):.2f}GB / " +
                          f"{storage['total_bytes'] / (1024**3):.2f}GB " +
                          f"({storage['utilization_percent']:.1f}%)")
                    print(f"  Files:    {storage['files_stored']}")
                    print(f"  Data Tx:  {perf['total_data_transferred_bytes'] / (1024**2):.2f}MB")
                    print(f"  Requests: {perf['total_requests_processed']}")
                    
                except Exception as e:
                    print(f"  Error getting stats: {e}")
                
    except Exception as e:
        print(f"Could not retrieve network stats: {e}")
    
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