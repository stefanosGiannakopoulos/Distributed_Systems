import os
import time
import random
import requests
import urllib.parse

# Node mapping for our 10 nodes.
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

REQUESTS_DIR = "./"

def get_random_node():
    """
    Returns a random node_id from the node_mapping
    """
    node_id = random.choice(list(node_mapping.keys()))
    return node_id

def process_request(request_line, expected_values):
    """
    Process a single request line by selecting a random node
    and sending the request to it
    """
    if not request_line.strip():
        return 0, 0
    
    # Select a random node for this request
    node_id = get_random_node()
    ip, port = node_mapping[node_id]
    
    # Split by comma and strip extra spaces
    parts = [p.strip() for p in request_line.split(",")]
    command = parts[0].lower()
    
    stale_read = 0
    success = 0
    
    if command == "insert" and len(parts) >= 3:
        key = parts[1]
        value = parts[2]
        # Update expected value: simply assign the latest value for the key
        expected_values[key] = value
        # URL-encode the key
        encoded_key = urllib.parse.quote(key, safe='')
        url = f"http://{ip}:{port}/insert/{encoded_key}/{value}"
        url = requests.utils.requote_uri(url)
        
        try:
            response = requests.post(url)
            if response.status_code == 200:
                success = 1
                print(f"Node {node_id} successful insert '{key}': {value}")
            else:
                print(f"Node {node_id} failed insert '{key}': {response.text}")
        except Exception as e:
            print(f"Exception on node {node_id} inserting '{key}': {str(e)}")
            
    elif command == "query" and len(parts) >= 2:
        key = parts[1]
        encoded_key = urllib.parse.quote(key, safe='')
        url = f"http://{ip}:{port}/query/{encoded_key}"
        url = requests.utils.requote_uri(url)
        
        try:
            response = requests.get(url)
            if response.status_code == 200:
                success = 1
                try:
                    data = response.json()
                    # Extract the value from the response
                    returned_value = data.get("value", "")
                    expected = expected_values.get(key)
                    
                    # Extract just the numeric value if returned_value is a list
                    actual_value = returned_value
                    if isinstance(returned_value, list) and len(returned_value) > 1:
                        actual_value = str(returned_value[1])
                    else:
                        actual_value = str(returned_value)
                    
                    print(f"Node {node_id} query '{key}': got '{actual_value}'")
                    
                    if expected is not None and actual_value != expected:
                        print(f"Node {node_id} STALE READ for '{key}': expected '{expected}', got '{actual_value}'")
                        stale_read = 1
                except Exception as e:
                    print(f"Node {node_id} query '{key}' JSON parse error: {str(e)}")
            else:
                print(f"Node {node_id} failed query '{key}': {response.text}")
        except Exception as e:
            print(f"Exception on node {node_id} querying '{key}': {str(e)}")
    else:
        print(f"Unrecognized command: {request_line}")
    
    return success, stale_read

def collect_all_requests():
    """
    Reads all request files and returns a list of all requests
    """
    all_requests = []
    
    for node_id in node_mapping.keys():
        file_name = os.path.join(REQUESTS_DIR, f"requests_{node_id}.txt")
        if os.path.exists(file_name):
            with open(file_name, "r", encoding="utf-8") as f:
                lines = [line.strip() for line in f.readlines() if line.strip()]
                all_requests.extend(lines)
        else:
            print(f"File {file_name} not found for node {node_id}!")
    
    return all_requests

def run_request_experiment():
    """
    Reads all requests, then processes them sequentially by
    randomly choosing a node for each request
    """
    print("Collecting all requests...")
    all_requests = collect_all_requests()
    print(f"Found {len(all_requests)} total requests")
    
    expected_values = {}
    total_requests = 0
    total_stale_reads = 0
    total_queries = 0
    
    print("Starting request processing...")
    start_time = time.time()
    
    for request in all_requests:
        success, stale = process_request(request, expected_values)
        total_requests += success
        total_stale_reads += stale
        if "query" in request.lower() and success:
            total_queries += 1
    
    elapsed_time = time.time() - start_time
    throughput = total_requests / elapsed_time if elapsed_time > 0 else 0
    
    print(f"\nResults:")
    print(f"Total Requests Processed: {total_requests}")
    print(f"Total Elapsed Time: {elapsed_time:.2f} sec")
    print(f"Total Stale Reads: {total_stale_reads}")
    
    if total_queries > 0:
        print(f"Stale Read Percentage: {(total_stale_reads/total_queries)*100:.2f}% of successful queries")
    else:
        print("No successful queries processed.")

if __name__ == "__main__":
    run_request_experiment()