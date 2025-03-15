#!/usr/bin/env python3
# filepath: /Users/stefanosgiannakopoulos/Desktop/Distributed_Project/Distributed_Systems_2024-2025/insert_experiment.py
import os
import time
import concurrent.futures
import requests
import urllib

# Mapping of node IDs to their IP and port.
node_mapping = {
    "00": ('10.0.17.96', 5000),
    "01": ('10.0.17.245', 5001),
    "02": ('10.0.17.158', 5002),
    "03": ('10.0.17.171', 5003),
    "04": ('10.0.17.209', 5004),
    "05": ('10.0.17.96', 5005),
    "06": ('10.0.17.245', 5006),
    "07": ('10.0.17.158', 5007),
    "08": ('10.0.17.171', 5008),
    "09": ('10.0.17.209', 5009)
}

# Define the experiments.
experiments = [
   # {"consistency": "", "k": 1}
  #  {"consistency": "chain replication", "k": 3}
  #  {"consistency": "chain replication", "k": 5}
   # {"consistency": "", "k": 1}
  # {"consistency": "eventual consistency", "k": 3}
    {"consistency": "eventual consistency", "k": 5}
]

def query_keys_from_file(node_id, config):
    """
    
    """
    ip, port = node_mapping[node_id]
    file_name = f"query_{node_id}.txt"
    count = 0
    if not os.path.exists(file_name):
        print(f"File {file_name} not found!")
        return (0, 0)
    
    start_read = time.time()
    with open(file_name, "r", encoding="utf-8") as f:
        lines = f.readlines()
    file_read_time = time.time() - start_read

    fixed_value = "1"  # Change this if needed.
    for line in lines:
        song_name = line.strip()
        if song_name:
            encoded_song = urllib.parse.quote(song_name, safe='')  # This should encode all unsafe characters.
            url = f"http://{ip}:{port}/query/{encoded_song}"
            # Requote the full URL to ensure any stray characters are properly encoded.
            url = requests.utils.requote_uri(url)
            try:
                response = requests.get(url, timeout=5)
                if response.status_code == 200:
                    count += 1
                else:
                    print(f"Node {node_id} failed to insert '{song_name}': {response.text}")
            except Exception as e:
                print(f"Exception on node {node_id} inserting '{song_name}': {str(e)}")
    return (count, file_read_time)

def run_experiment(config):
    """
    Starts concurrent inserts from all nodes, measures total elapsed time, subtracts file-read time,
    and then calculates throughput based on the effective insertion time.
    Assumes that nodes are preconfigured with config's 'consistency' and replication factor 'k'.
    """
    print(f"Running experiment with config: {config}")
    total_inserts = 0
    total_file_read_time = 0.0
    start_time = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for node_id in node_mapping.keys():
            futures.append(executor.submit(query_keys_from_file, node_id, config))
        for future in concurrent.futures.as_completed(futures):
            count, file_read_time = future.result()
            total_inserts += count
            total_file_read_time += file_read_time

    elapsed = time.time() - start_time
    effective_time = elapsed - total_file_read_time
    throughput = total_inserts / effective_time if effective_time > 0 else 0
    print(f"Config: {config} | Inserts: {total_inserts} | Total Elapsed: {elapsed:.2f} sec")
    print(f"Total File Reading Time: {total_file_read_time:.2f} sec")
    print(f"Effective Read Time (elapsed - reading): {effective_time:.2f} sec")
    print(f"Throughput: {throughput:.2f} inserts/sec")
    print("---------------------------------------------------")
    return total_inserts, elapsed, throughput

if __name__ == "__main__":
    # Run each experiment one by one.
    results = []
    for config in experiments:
        print("Starting experiment:", config)
        res = run_experiment(config)
        results.append((config, res))