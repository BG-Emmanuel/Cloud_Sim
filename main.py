from storage_virtual_network import StorageVirtualNetwork
from storage_virtual_node import StorageVirtualNode
import sys

def print_progress_bar(iteration, total, prefix='', suffix='', length=50, fill='█'):
    """Print a progress bar to the console"""
    percent = 100 * (iteration / float(total))
    filled_length = int(length * iteration // total)
    bar = fill * filled_length + '-' * (length - filled_length)
    sys.stdout.write(f'\r{prefix} |{bar}| {percent:.1f}% {suffix}')
    sys.stdout.flush()
    if iteration == total:
        print()  # New line when complete
network = StorageVirtualNetwork()

# Create nodes
node1 = StorageVirtualNode("node1", cpu_capacity=4, memory_capacity=16, storage_capacity=500, bandwidth=1000)
node2 = StorageVirtualNode("node2", cpu_capacity=8, memory_capacity=32, storage_capacity=1000, bandwidth=2000)

# Add nodes to network
network.add_node(node1)
network.add_node(node2)

# Connect nodes with 1Gbps link
network.connect_nodes("node1", "node2", bandwidth=1000)

print("=== File Transfer Simulation ===\n")

# Initiate file transfer (100MB file from node1 to node2)
transfer = network.initiate_file_transfer(
    source_node_id="node1",
    target_node_id="node2",
    file_name="large_dataset.zip",
    file_size=100 * 1024 * 1024  # 100MB
)

if transfer:
    print(f"Transfer initiated: {transfer.file_id}")
    print(f"File: {transfer.file_name} ({transfer.total_size / (1024*1024):.2f} MB)")
    print(f"Total chunks: {len(transfer.chunks)}\n")
    
    step = 0
    total_chunks = len(transfer.chunks)
    chunks_completed = 0
    completed = False
    
    print("Transferring file...")
    print_progress_bar(0, total_chunks, prefix='Progress:', suffix='Complete', length=50)
    
    # Process transfer in chunks
    while not completed:
        step += 1
        chunks_done, completed = network.process_file_transfer(
            source_node_id="node1",
            target_node_id="node2",
            file_id=transfer.file_id,
            chunks_per_step=3  # Process 3 chunks at a time
        )
        
        # Update progress counter
        chunks_completed += chunks_done
        
        # Update progress bar
        print_progress_bar(
            chunks_completed, 
            total_chunks, 
            prefix='Progress:', 
            suffix=f'Complete ({chunks_completed}/{total_chunks} chunks)', 
            length=50
        )
        
        if completed:
            print(f"\n✓ Transfer completed successfully in {step} steps!")
            break
        
    # Get final network stats
    print("\n=== Final Network Statistics ===")
    stats = network.get_network_stats()
    print(f"Total nodes: {stats['total_nodes']}")
    print(f"Network utilization: {stats['bandwidth_utilization']:.2f}%")
    print(f"Storage utilization: {stats['storage_utilization']:.2f}%")
    print(f"Total data transferred: {stats['used_storage_bytes'] / (1024*1024):.2f} MB")
    
    print("\n=== Node 2 Statistics ===")
    storage_util = node2.get_storage_utilization()
    print(f"Storage used: {storage_util['used_bytes'] / (1024*1024):.2f} MB / {storage_util['total_bytes'] / (1024*1024*1024):.2f} GB")
    print(f"Storage utilization: {storage_util['utilization_percent']:.2f}%")
    print(f"Files stored: {storage_util['files_stored']}")
    
    perf = node2.get_performance_metrics()
    print(f"Total requests processed: {perf['total_requests_processed']}")
    print(f"Total data transferred: {perf['total_data_transferred_bytes'] / (1024*1024):.2f} MB")
else:
    print("Failed to initiate transfer!")