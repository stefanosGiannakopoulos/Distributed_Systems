from flask import Flask, request, jsonify
from node import BootstrapNode  
import utils
import os
import requests
import threading
from dotenv import load_dotenv
import sys
import time

app = Flask(__name__)

k = utils.k # replication factor

consistency = utils.consistency # chain replication

number_of_nodes = 1

network_nodes = [{
    "ip": os.getenv("BOOTSTRAP_IP"),
    "port": os.getenv("BOOTSTRAP_PORT"),
    "key": utils.hash_function(f"{os.getenv('BOOTSTRAP_IP')}:{os.getenv('BOOTSTRAP_PORT')}")
}]

def belongs_to_me(key, current, predecessor):
    if predecessor == current:
        # single node ring covers everything
        return True
    if predecessor < current:
        # normal no wrap
        return predecessor < key <= current
    else:
        # wrap around case
        return key > predecessor or key <= current

@app.route('/')
def home():
    return jsonify({"message": "Chordify DHT Node Running"})


@app.route('/insert/<string:song_name>/<int:value>', methods=['POST'])
def insert_song(song_name: str, value: int = 0):
    key = utils.hash_function(song_name)  # compute hash key for routing
    current_id = node.get_identifier()
    
    predecessor = node.get_predecessor() 
    
    if predecessor in (None, []):
        print("Only One Node: Insert Locally")
        node.insert(song_name, value)
        return jsonify({"message": f"Inserted '{song_name}' at node {node.get_ip()} and port {node.get_port()}"}), 200
    
    print(f"Predecessor IP: {predecessor[0]}, Predecessor Port: {predecessor[1]}")

    predecessor = utils.hash_function(f"{predecessor[0]}:{predecessor[1]}")
    
     
    print(f"--- INSERT REQUEST ---")
    print(f"Song: {song_name}")
    print(f"Computed Key: {key}")
    print(f"Current Node ID: {current_id} (IP: {node.get_ip()}, Port: {node.get_port()})")
    print(f"Predecessor Key: {predecessor}")
    print(f"Successor Key: {utils.hash_function(f'{node.get_successor()[0]}:{node.get_successor()[1]}')}")
    
    # Check if the key falls in the interval (predecessor, current_id]
    if belongs_to_me(key,  current_id, predecessor,):
        print("Responsible : Inserting locally.")
        node.insert(song_name, value)
        # Check for the consistency model 
        if consistency == 'chain replication':
            print(f"Consistency Model: {consistency} and we start from the primary node of the key {node.get_ip()}:{node.get_port()}")
            # The Last Node in the chain has to return the response to the client
            if node.get_successor() == []:
                return jsonify({"message": f"Inserted '{song_name}' at node {node.get_ip()} and port {node.get_port()}"}), 200
            else:
                try: 
                    packet = {"song_name": song_name, "value": node.get_song_list()[song_name], "k": k-1} 
                    response = requests.post(f"http://{node.get_successor()[0]}:{node.get_successor()[1]}/chain_replicated_insert",json=packet)
                    response.raise_for_status()
                    return response.json(), response.status_code
                except requests.RequestException as e:
                    return jsonify({"error": f"Failed to forward request to node {node.get_successor()[0]}:{node.get_successor()[1]}: {str(e)}"}), 500          
        
        elif consistency == "eventual consistency": # eventual consistency
            print(f"Consistency Model: {consistency} and we start from the primary node of the key {node.get_ip()}:{node.get_port()}")
            # The First Node in the chain has to return the response to the client and then return the response to the client, meanwhile a thread has been opened to replicate the song to the next k-1 nodes
            if node.get_successor() == []:
                return jsonify({"message": f"Inserted '{song_name}' at node {node.get_ip()} and port {node.get_port()}"}), 200
            else:
                packet = {"song_name": song_name, "value": node.get_song_list()[song_name], "k": k-1} # prepare the packet that is the argument of the thread
                thread = threading.Thread(target=eventual_insertion_background, args=(packet,)) # our argument is the function that we want to run in the thread, so from the function we hit the endpoint
                thread.start() # start the thread to replicate the song to the next k-1 nodes
                return jsonify({"message": f"Inserted '{song_name}' at node {node.get_ip()} and port {node.get_port()}"}), 200
              
             
        else:  # no cosistency model
             print("Responsible : Inserting locally. No consistency model")
             return jsonify({"message": f"Inserted '{song_name}' at node {node.get_ip()} and port {node.get_port()}"}), 200
        
    else:
        print("Not responsible : Forwarding to successor.")
        # Forward the request to the successor.
        successor = node.get_successor()
        if successor == []:  #if no successor insert locally.
            node.insert(song_name, value)
            return jsonify({"message": f"Inserted '{song_name}' locally at node {node.get_ip()} and port {node.get_port()} (alone in ring)"}), 200
        
        print(f'Forwarding to successor {successor[0]}:{successor[1]}')
        successor_url = f"http://{successor[0]}:{successor[1]}/insert/{song_name}/{value}"
        try:
            response = requests.post(successor_url)
            response.raise_for_status()
            return response.json()  # Return the response from the successor.
        except requests.RequestException as e:
            return jsonify({"error": f"Failed to forward request to node {successor[0]}:{successor[1]}: {str(e)}"}), 500


def eventual_insertion_background(packet):
    time.sleep(0.01)  # 10 millisecond delay
    successor = node.get_successor()
    if not successor or successor == []:
        print("No valid successor found, replication stops here.")
        return jsonify({"message": "Replication ended at this node (no successor)"}), 200
    try:
        response = requests.post(f"http://{successor[0]}:{successor[1]}/eventual_insertion", json=packet)
        response.raise_for_status()
        print("Response from endpoint of :", response.json())
    except Exception as e:
        print("Error calling endpoint:", e)
        # threads in Python terminate automatically when the function call ends, so no need to do anything else here for the thread
    return response.json()


# Have to correct the eventual_insertion endpoint ... 

@app.route('/eventual_insertion', methods=['POST'])
def eventual_insertion():
    data = request.get_json()
    if not data or "song_name" not in data or "value" not in data or "k" not in data:
        return jsonify({"error": "Invalid replication packet data"}), 400
    
    song_name = data["song_name"]
    value = data["value"]
    counter = data["k"]
    
    print(f"Received chain replication packet for '{song_name}' with k = {counter} at node {node.get_ip()}:{node.get_port()}")
    
    if counter == 1: # Last node and we return the response 
        print(f"Last Node that the song replication happens {node.get_ip()}:{node.get_port()}")
        node.set_song_to_song_list(song_name, value)
        return jsonify({"message": f"Inserted '{song_name}' at node {node.get_ip()} and port {node.get_port()} -> Last Node that the song was eventually inserted ..."}), 200
    
    else: # not the last node in the chain of k
        node.set_song_to_song_list(song_name, value)
        new_counter = counter - 1
        new_packet = {"song_name": song_name, "value": value, "k": new_counter}
        successor = node.get_successor()
        if not successor or successor == []:
            print("No valid successor found, replication stops here.")
            return jsonify({"message": "Replication ended at this node (no successor)"}), 200
        
        replication_url = f"http://{successor[0]}:{successor[1]}/eventual_insertion"
        
        try:
            print(f"Node {node.get_ip()}:{node.get_port()} forwarding chain replication packet for '{song_name}' with new k = {new_counter} to node {successor[0]}:{successor[1]}.")
            response = requests.post(replication_url, json=new_packet)
            response.raise_for_status()
            return jsonify({
                "message": f"Forwarded chain replication packet for '{song_name}' to node {successor[0]}:{successor[1]}",
                "next_response": response.json()
            }), 200
        except requests.RequestException as e:
            return jsonify({"error": f"Failed to forward chain replication packet to node {successor}: {str(e)}"}), 500
        
    
    

@app.route('/chain_replicated_insert', methods=['POST'])
def chain_replicated_insert():
    data = request.get_json()
    if not data or "song_name" not in data or "value" not in data or "k" not in data:
        return jsonify({"error": "Invalid replication packet data"}), 400
    
    song_name = data["song_name"]
    value = data["value"]
    counter = data["k"]
    
    time.sleep(0.01)  # 10 millisecond delay at each hop

    
    print(f"Received chain replication packet for '{song_name}' with k = {counter} at node {node.get_ip()}:{node.get_port()}")
    
    if counter == 1: # Last node in the chain and we return the response to the client
        print(f"Last Node that the song replication happens {node.get_ip()}:{node.get_port()}")
        node.set_song_to_song_list(song_name, value)
        return jsonify({"message": f"Inserted '{song_name}' at node {node.get_ip()} and port {node.get_port()} -> Last Node of the Chain"}), 200
    
    else:
        node.set_song_to_song_list(song_name, value)
        new_counter = counter - 1
        new_packet = {"song_name": song_name, "value": value, "k": new_counter}
        successor = node.get_successor()
        if not successor or successor == []:
            print("No valid successor found, replication stops here.")
            return jsonify({"message": "Replication ended at this node (no successor)"}), 200
        
        replication_url = f"http://{successor[0]}:{successor[1]}/chain_replicated_insert"
        
        try:
            print(f"Node {node.get_ip()}:{node.get_port()} forwarding chain replication packet for '{song_name}' with new k = {new_counter} to node {successor[0]}:{successor[1]}.")
            response = requests.post(replication_url, json=new_packet)
            response.raise_for_status()
            return jsonify({
                "message": f"Forwarded chain replication packet for '{song_name}' to node {successor[0]}:{successor[1]}",
                "next_response": response.json()
            }), 200
        except requests.RequestException as e:
            return jsonify({"error": f"Failed to forward chain replication packet to node {successor}: {str(e)}"}), 500
    

@app.route('/query/<string:song_name>', methods=['GET'])
def query_song(song_name: str):
    if song_name == "*":
        # Use a visited set to avoid loops
        visited_param = request.args.get('visited', '')
        visited_set = set(visited_param.split(',')) if visited_param else set()

        current_node_id = f"{node.get_ip()}:{node.get_port()}"
        if current_node_id in visited_set:
            return jsonify({"songs": {}}), 200
        visited_set.add(current_node_id)

        local_songs = node.get_song_list().copy()
        this_node_id = node.get_identifier()
        predecessor = node.get_predecessor()
        
        # Single node case
        if predecessor in (None, []):
            return jsonify({"songs": local_songs}), 200
        
        if isinstance(predecessor, list):
            predecessor_id = utils.hash_function(f"{predecessor[0]}:{predecessor[1]}")
        else:
            predecessor_id = predecessor or this_node_id

        successor = node.get_successor()
        if not successor or successor == []:
            return jsonify({"songs": local_songs}), 200

        successor_url = (
            f"http://{successor[0]}:{successor[1]}/query/*"
            f"?visited={','.join(visited_set)}"
        )
        try:
            response = requests.get(successor_url)
            response.raise_for_status()
            successor_songs = response.json().get("songs", {})
        except requests.RequestException as e:
            return jsonify({
                "error": f"Failed to forward query to node {successor}: {str(e)}"
            }), 500

        local_songs.update(successor_songs)
        return jsonify({"songs": local_songs}), 200

    else: # Query for a specific song 
        key = utils.hash_function(song_name)
        current_id = node.get_identifier()
        predecessor = node.get_predecessor()

        # If we have no predecessor, the node is alone.
        if not predecessor or predecessor == []:
            print("Only One Node: Query Locally")
            result = node.query(song_name)
            if result is None:
                return jsonify({"message": f"Song '{song_name}' not found in DHT"}), 404
            return jsonify({
                "message": f"Song found at node {node.get_ip()}:{node.get_port()}",
                "value": result
            }), 200

        if isinstance(predecessor, list):
            predecessor_id = utils.hash_function(f"{predecessor[0]}:{predecessor[1]}")
       
        print(f"--- QUERY REQUEST (Clockwise) ---")
        print(f"Song: {song_name}")
        print(f"Computed Key: {key}")
        print(f"Current Node ID: {current_id} (IP: {node.get_ip()}, Port: {node.get_port()})")
        print(f"Predecessor Key: {predecessor_id}")
        
        
        if consistency == "eventual consistency":
            print(f"Consistency Model: {consistency} and we start from a random node {node.get_ip()}:{node.get_port()}")
            # Get visited nodes from query parameters (if any)
            visited_param = request.args.get("visited", "")
            visited_set = set(visited_param.split(',')) if visited_param else set()
            current_node_id = f"{node.get_ip()}:{node.get_port()}"
            if current_node_id in visited_set:
                # A full circle has been completed and the song was not found.
                return jsonify({"message": f"Song '{song_name}' not found in DHT (cycle complete)"}), 404
            visited_set.add(current_node_id)

            result = node.query(song_name)
            if result is None:
                successor = node.get_successor()
                if successor == []:
                    return jsonify({"message": f"Song '{song_name}' not found in DHT"}), 404
                try:
                    # Forward the request while appending the visited set.
                    successor_url = f"http://{successor[0]}:{successor[1]}/query/{song_name}?visited={','.join(visited_set)}"
                    response = requests.get(successor_url)
                    response.raise_for_status()
                    return response.json(), response.status_code
                except requests.RequestException as e:
                    if e.response is not None and e.response.status_code == 404:
                        return jsonify({"message": f"Song '{song_name}' not found in DHT"}), 404
                    return jsonify({
                        "error": f"Failed to forward query to node {successor}: {str(e)}"
                    }), 500
            else:
                return jsonify({
                    "message": f"Song found at node {node.get_ip()}:{node.get_port()}",
                    "value": result
                }), 200

        
        # If the key falls in our interval, we are responsible.
        if belongs_to_me(key, current_id, predecessor_id):
            # If chain replication is enabled, delegate read to the tail.
            if consistency == "chain replication":
                if node.get_successor() and node.get_successor() != []:
                    successor = node.get_successor()
                    packet = {"song_name": song_name, "k": k-1}
                    chain_query_url = f"http://{successor[0]}:{successor[1]}/chain_replicated_query?song_name={song_name}&k={k-1}"
                    try:
                        print(f"Chain replication enabled; forwarding query for '{song_name}' to tail via node {successor[0]}:{successor[1]}.")
                        response = requests.get(chain_query_url)
                        response.raise_for_status()
                        return response.json(), response.status_code
                    except requests.RequestException as e:
                        return jsonify({"error": f"Failed to forward chain replicated query: {str(e)}"}), 500
                else:
                    # No successor means this node is the tail.
                    result = node.query(song_name)
                    if result is None:
                        return jsonify({"message": f"Song '{song_name}' not found in DHT"}), 404
                    return jsonify({
                        "message": f"Song found at tail node {node.get_ip()}:{node.get_port()}",
                        "value": result
                    }), 200
            
            else: # no consistency model
                print("Responsible : Querying locally.")
                result = node.query(song_name)
                if result is None:
                    return jsonify({"message": f"Song '{song_name}' not found in DHT"}), 404
                return jsonify({
                    "message": f"Song found at node {node.get_ip()}:{node.get_port()}",
                    "value": result
                }), 200
            
        else: # Does not belong to us (we are not responsible, meaning we are not the primary node for this song)
            successor = node.get_successor()
            if successor == []:
                return jsonify({"message": f"Song '{song_name}' not found in DHT"}), 404

            successor_url = f"http://{successor[0]}:{successor[1]}/query/{song_name}"
            try:
                response = requests.get(successor_url)
                if response.status_code == 404:
                    return jsonify({"message": f"Song '{song_name}' not found in DHT"}), 404
                response.raise_for_status()
                return response.json(), response.status_code
            except requests.RequestException as e:
                return jsonify({
                    "error": f"Failed to forward query to node {successor}: {str(e)}"
                }), 500

@app.route('/chain_replicated_query', methods=['GET'])
def chain_replicated_query():
    song_name = request.args.get("song_name")
    counter = int(request.args.get("k"))
    
    if counter == 1:
        print(f"Last Node that the song query happens {node.get_ip()}:{node.get_port()}")
        result = node.query(song_name)
        if result is None:
            return jsonify({"message": f"Song '{song_name}' not found in DHT"}), 404
        return jsonify({
            "message": f"Song found at tail node -> {node.get_ip()}:{node.get_port()}",
            "value": result
        }), 200
    
    else:
        successor = node.get_successor()
        if not successor or successor == []:
            return jsonify({"message": f"Song '{song_name}' not found in DHT"}), 404
         
        # Otherwise, forward the query further along the chain.
        chain_query_url = f"http://{successor[0]}:{successor[1]}/chain_replicated_query?song_name={song_name}&k={counter-1}"
        try:
            response = requests.get(chain_query_url)
           
            response.raise_for_status()
            return response.json(), response.status_code
        except requests.RequestException as e:
             
            if response.status_code == 404:
                return response.json(), 404
            
            return jsonify({"error": f"Failed to forward chain replicated query to node {successor}: {str(e)}"}), 500



@app.route('/delete/<string:song_name>', methods=['DELETE'])
def delete(song_name: str):
    key = utils.hash_function(song_name)
    current_ip = node.get_ip()
    current_id = node.get_identifier()
    # Use predecessor to define responsibility.
    pred = node.get_predecessor()
    
    if pred in (None, []):
        print("Only One Node: Delete Locally")
        result = node.delete(song_name)
        if result:
            return jsonify({"message": f"Deleted song '{song_name}' from node {current_ip}:{node.get_port()}"}), 200
        else:
            return jsonify({"message": f"Song '{song_name}' not found in the DHT"}), 404
    
    if isinstance(pred, list):
        pred = utils.hash_function(f"{pred[0]}:{pred[1]}")
    
    if belongs_to_me(key, current_id ,pred):
        result = node.delete(song_name)
        if result == True:
            if consistency == "chain replication":
                print(f"Consistency Model: {consistency} and we start from the primary node of the key {node.get_ip()}:{node.get_port()}")
                # The Last Node in the chain has to return the response to the client
                if node.get_successor() == []:
                    return jsonify({"message": f"Deleted '{song_name}' at node {node.get_ip()} and port {node.get_port()}"}), 200
                else:
                    try: 
                        packet = {"song_name": song_name, "k": k-1}
                        response = requests.post(f"http://{node.get_successor()[0]}:{node.get_successor()[1]}/chain_replicated_delete",json=packet)
                        response.raise_for_status()
                        return response.json(), response.status_code
                    except requests.RequestException as e:
                        return jsonify({"error": f"Failed to forward request to node {node.get_successor()[0]}:{node.get_successor()[1]}: {str(e)}"}), 500

            elif consistency == "eventual consistency": # eventual consistency
                print(f"Consistency Model: {consistency} and we start from the primary node of the key {node.get_ip()}:{node.get_port()}")
                # The First Node in the chain has to return the response to the client and then return the response to the client, meanwhile a thread has been opened to replicate the song to the next k-1 nodes
                if node.get_successor() == []:
                    return jsonify({"message": f"Deleted '{song_name}' at node {node.get_ip()} and port {node.get_port()}"}), 200
                packet = {"song_name": song_name , "k": k-1}  # prepare the packet that is the argument of the thread
                thread = threading.Thread(target=eventual_deletion_background, args=(packet,)) # our argument is the function that we want to run in the thread, so from the function we hit the endpoint
                thread.start() # start the thread to replicate the song to the next k-1 nodes
                return jsonify({"message": f"Deleted '{song_name}' at node {node.get_ip()} and port {node.get_port()}"}), 200
            
            else:  # no cosistency model
                print("Responsible : Deleting locally. No consistency model")
                node.delete(song_name)
                return jsonify({"message": f"Deleted '{song_name}' at node {node.get_ip()} and port {node.get_port()}"}), 200
            
        else:
            return jsonify({"message": f"Song '{song_name}' not found in the DHT"}), 404
    
    else:
        # Forward the delete request to the successor.
        successor = node.get_successor()
        if successor == []:
            return jsonify({"message": f"Song '{song_name}' not found in the DHT"}), 404
        
        successor_url = f"http://{successor[0]}:{successor[1]}/delete/{song_name}"
    
    try:
        response = requests.delete(successor_url)
        
        # If the successor reports a 404, return a 404 response here as well.
        if response.status_code == 404:
            return jsonify({"message": f"Song '{song_name}' not found. Could not delete."}), 404
        
        response.raise_for_status()
        return jsonify({
            "message": f"Forwarded delete request to node {successor[0]}:{successor[1]}",
            "result": response.json()
        }), 200

    except requests.RequestException as e:
        return jsonify({"error": f"Failed to forward delete request to node {successor}: {str(e)}"}), 500


def eventual_deletion_background(packet):
    successor = node.get_successor()
    if not successor or successor == []:
        print("No valid successor found, deletion stops here.")
        return jsonify({"message": "Deletion ended at this node (no successor)"}), 200
    try:
        response = requests.post(f"http://{successor[0]}:{successor[1]}/eventual_deletion", json=packet)
        response.raise_for_status()
        print("Response from endpoint of :", response.json())
    except Exception as e:
        print("Error calling endpoint:", e)
        # threads in Python terminate automatically when the function call ends, so no need to do anything else here for the thread
    return response.json()

@app.route('/eventual_deletion', methods=['POST'])
def eventual_deletion():
    data = request.get_json()
    if not data or "song_name" not in data or "k" not in data:
        return jsonify({"error": "Invalid replication packet data"}), 400
    song_name = data["song_name"]
    counter = data["k"]
    
    if counter == 1:
        print(f"Last Node that the song deletion happens {node.get_ip()}:{node.get_port()}")
        result = node.delete(song_name)
        if result:
            return jsonify({"message": f"Deleted song '{song_name}' from node {node.get_ip()}:{node.get_port()} -> Last Node of the Chain"}), 200
        else:
            return jsonify({"message": f"Song '{song_name}' not found in the DHT"}), 404
    else:
        result = node.delete(song_name)
        if result:
            new_counter = counter - 1
            new_packet = {"song_name": song_name, "k": new_counter}
            successor = node.get_successor()
            if not successor or successor == []:
                print("No valid successor found, deletion stops here.")
                return jsonify({"message": "Deletion ended at this node (no successor)"}), 200
            
            replication_url = f"http://{successor[0]}:{successor[1]}/eventual_deletion"
            try:
                print(f"Node {node.get_ip()}:{node.get_port()} forwarding chain deletion packet for '{song_name}' with new k = {new_counter} to node {successor[0]}:{successor[1]}.")
                response = requests.post(replication_url, json=new_packet)
                response.raise_for_status()
                return jsonify({
                    "message": f"Forwarded chain deletion packet for '{song_name}' to node {successor[0]}:{successor[1]}",
                    "next_response": response.json()
                }), 200
            except requests.RequestException as e:
                return jsonify({"error": f"Failed to forward chain deletion packet to node {successor}: {str(e)}"}), 500
        else:
            return jsonify({"message": f"Song '{song_name}' not found in the DHT"}), 404
    

@app.route('/chain_replicated_delete', methods=['POST'])
def chain_replicated_delete():
    data = request.get_json()
    if not data or "song_name" not in data or "k" not in data:
        return jsonify({"error": "Invalid replication packet data"}), 400
    
    song_name = data["song_name"]
    counter = data["k"]
    
    print(f"Received chain replication packet for '{song_name}' with k = {counter} at node {node.get_ip()}:{node.get_port()}")
    
    if counter == 1:
        print(f"Last Node that the song deletion happens {node.get_ip()}:{node.get_port()}")
        result = node.delete(song_name)
        if result:
            return jsonify({"message": f"Deleted song '{song_name}' from node {node.get_ip()}:{node.get_port()} -> Last Node of the Chain"}), 200
        else:
            return jsonify({"message": f"Song '{song_name}' not found in the DHT"}), 404
    else:
        result = node.delete(song_name)
        if result:
            new_counter = counter - 1
            new_packet = {"song_name": song_name, "k": new_counter}
            successor = node.get_successor()
            if not successor or successor == []:
                print("No valid successor found, deletion stops here.")
                return jsonify({"message": "Deletion ended at this node (no successor)"}), 200
            
            replication_url = f"http://{successor[0]}:{successor[1]}/chain_replicated_delete"
            try:
                print(f"Node {node.get_ip()}:{node.get_port()} forwarding chain deletion packet for '{song_name}' with new k = {new_counter} to node {successor[0]}:{successor[1]}.")
                response = requests.post(replication_url, json=new_packet)
                response.raise_for_status()
                return jsonify({
                    "message": f"Forwarded chain deletion packet for '{song_name}' to node {successor[0]}:{successor[1]}",
                    "next_response": response.json()
                }), 200
            except requests.RequestException as e:
                return jsonify({"error": f"Failed to forward chain deletion packet to node {successor}: {str(e)}"}), 500
        else:
            return jsonify({"message": f"Song '{song_name}' not found in the DHT"}), 404


"""------------------------------------------------------Join Request--------------------------------------------------------------------------------"""
@app.route('/join_network', methods=['POST'])
def join_network():
    requests_data = request.get_json()
    candidate_node_ip = requests_data.get("ip")
    candidate_node_port = requests_data.get("port")
    candidate_node_key = requests_data.get("key_to_join")
    
    if not candidate_node_ip or not candidate_node_port or not candidate_node_key:
        return jsonify({"error": "Invalid join request; insufficient data"}), 400
    
    print(f"Join request from {candidate_node_ip}:{candidate_node_port} with key {candidate_node_key}")
    
    for node_info in network_nodes:
        if node_info["key"] == candidate_node_key:
            return jsonify({"error": "Node already exists in network"}), 400
    
    candidate_node = {
        "ip": candidate_node_ip,
        "port": candidate_node_port,
        "key": candidate_node_key
    }
    network_nodes.append(candidate_node)
    network_nodes.sort(key=lambda n: n["key"])
    
    candidate_index = next(i for i, n in enumerate(network_nodes) if n["key"] == candidate_node_key)
    predecessor_index = (candidate_index - 1) % len(network_nodes)
    successor_index = (candidate_index + 1) % len(network_nodes)
    
    predecessor = network_nodes[predecessor_index]
    successor = network_nodes[successor_index]
    
    response_data = {
        "message": "Node successfully joined the network",
        "predecessor": [predecessor["ip"], predecessor["port"]],
        "successor": [successor["ip"], successor["port"]],
    }
    
    global number_of_nodes
    number_of_nodes += 1
    
    return jsonify(response_data), 200

@app.route('/join', methods=['POST'])
def join():
    # Fetch bootstrap node details from environment variables.
    bootstrap_node_ip = os.getenv("BOOTSTRAP_IP")
    bootstrap_node_port = os.getenv("BOOTSTRAP_PORT")
    
    # Compute keys based on "IP:port" strings.
    bootstrap_node_key = utils.hash_function(f"{bootstrap_node_ip}:{bootstrap_node_port}")
    my_key = utils.hash_function(f"{node.get_ip()}:{node.get_port()}")
    
    # Prevent joining the network as the bootstrap node.
    if my_key == bootstrap_node_key:
        return jsonify({"error": "Node is already the bootstrap node"}), 400
    
    # Construct URL for bootstrap node's join_network endpoint.
    bootstrap_url = f"http://{bootstrap_node_ip}:{bootstrap_node_port}/join_network"
    try:
        response = requests.post(bootstrap_url, json={
            "ip": node.get_ip(),
            "port": node.get_port(),
            "key_to_join": my_key
        })
        response.raise_for_status()
    except requests.RequestException as e:
        return jsonify({"error": f"Failed to join the network: {str(e)}"}), 500
    
    data_from_bootstrap = response.json()
    
    # Update pointers based on bootstrap node's response.
    if "successor" in data_from_bootstrap:
        node.set_successor(data_from_bootstrap["successor"])
    if "predecessor" in data_from_bootstrap:
        node.set_predecessor(data_from_bootstrap["predecessor"])
    
    
    # Update neighboring nodes.
    if node.get_predecessor():
        pred_url = f"http://{node.get_predecessor()[0]}:{node.get_predecessor()[1]}/update_successor"
        try:
            response = requests.post(pred_url, json={
                "ip": node.get_ip(),
                "port": node.get_port()
            })
            response.raise_for_status()
        except requests.RequestException as e:
            return jsonify({"error": f"Failed to update predecessor's successor: {str(e)}"}), 500
    
    if node.get_successor():
        succ_url = f"http://{node.get_successor()[0]}:{node.get_successor()[1]}/update_predecessor"
        try:
            response = requests.post(succ_url, json={
                "ip": node.get_ip(),
                "port": node.get_port()
            })
            response.raise_for_status()
        except requests.RequestException as e:
            return jsonify({"error": f"Failed to update successor's predecessor: {str(e)}"}), 500
    
    # start the replication process
    print('Starting replication process...\n')
    
    pred = node.get_predecessor()
    if pred and isinstance(pred, list):
        pred_param = f"{pred[0]},{pred[1]}"
    else:
        pred_param = ""
    
    
    
    try: 
        request = requests.get(f"http://{os.getenv('BOOTSTRAP_IP')}:{os.getenv('BOOTSTRAP_PORT')}/get_nodes")
        number_of_nodes = request.json().get("number_of_nodes", 1)
    except requests.RequestException as e:
        return jsonify({"error": f"Failed to get number of nodes from bootstrap node: {str(e)}"}), 500
    
    # handle the case of under-replication, all nodes have the same song_list
    if number_of_nodes <= k:
        print('Under-replication detected, starting replication process...\n')
        pred = node.get_predecessor()
        pred_song_list = {}
        try:
            request = requests.get(f"http://{pred[0]}:{pred[1]}/show_song_list")
            pred_song_list = request.json()
        except requests.RequestException as e:
            return jsonify({"error": f"Failed to get song list from predecessor: {str(e)}"}), 500
        
        node.set_song_list(pred_song_list)
        return jsonify({"message": "Join request processed"}), 200
    
    
    if node.get_successor() != [] and node.get_predecessor != []:
     # 1. get the songs from the successor that are supposed to be in this node (if any ...)
        successor = node.get_successor()
        successor_url = f"http://{successor[0]}:{successor[1]}/replicate?pred={pred_param}"       
        try:
            response = requests.get(successor_url)
            response.raise_for_status()
        except requests.RequestException as e:
            return jsonify({"error": f"Failed to replicate songs from successor: {str(e)}"}), 500
        
        songs_to_replicate = response.json().get("songs_that_belong_to_me", {})
        if songs_to_replicate == None or songs_to_replicate == {}:
            print ('No songs to replicate, ending replication process...\n')
            return jsonify({"status": "No songs to replicate"}), 200
         
        else:    #2. send a packet for each song I replicated to the whole DHT starting from The Primary Node of this Key , which is consisted of the song its value and a counter each time k-1, and if k == 0 then delete it from the list because it violates the replication factor of k 
            print('First Step done...\n')   
            print(f"Songs to replicate: {songs_to_replicate}")
            # first we insert the songs to our node_list and then we send the aforementioned packet to the whole DHT
            for song in songs_to_replicate:
                node.insert(song, songs_to_replicate[song])
                packet = {"song_name": song, "value": songs_to_replicate[song], "k":k-1}
                # send the packet to the whole DHT (until we get again to this node starting from the node that actually is responsible for the key), to see if we have to delete the song from the list of the node that gets the request
                # If this node is responsible for the song, start the replication process here.
                song_key = utils.hash_function(song)
                current_id = node.get_identifier()
                predecessor_id = utils.hash_function(f"{pred[0]}:{pred[1]}")
                if belongs_to_me(song_key, current_id, predecessor_id):
                    successor = node.get_successor()
                    if not successor or successor == []:
                        print("No valid successor found; replication aborted.")
                        return jsonify({"message": "Replication aborted (no successor)"}), 200
                    
                    primary_url = f"http://{successor[0]}:{successor[1]}/start_replication"
                    try:
                        print(f"Node {node.get_ip()}:{node.get_port()} is primary for '{song}', starting replication.")
                        response = requests.post(primary_url, json=packet)
                        response.raise_for_status()
                        print(f"Replication packet for '{song}' successfully initiated.")
                    except requests.RequestException as e:
                        print(f"Error starting replication for song '{song}': {str(e)}")
                else:
                    # this node is not the primary for the song, forward the replication packet to our successor, in order to find the primary node for this song 
                    successor = node.get_successor()
                    if not successor or successor == []:
                        print("No valid successor found; replication aborted.")
                        continue
                    replication_url = f"http://{successor[0]}:{successor[1]}/find_primary"
                    try:
                        print(f"Forwarding replication packet for '{song}' (not primary here) to node {successor[0]}:{successor[1]}.")
                        response = requests.post(replication_url, json=packet)
                        response.raise_for_status()
                    except requests.RequestException as e:
                        print(f"Error forwarding replication for song '{song}': {str(e)}")
                
                print('Replication process done...\n')

    return jsonify({"message": "Join request processed"}), 200


@app.route('/find_primary', methods=['POST'])
def find_primary():
    data = request.get_json()
    if not data or "song_name" not in data or "value" not in data or "k" not in data:
        return jsonify({"error": "Invalid replication packet data"}), 400
    
    song_name = data["song_name"]
    value = data["value"]
    counter = data["k"]
    
    print(f"Received replication packet for '{song_name}' with k = {counter} at node {node.get_ip()}:{node.get_port()}")
     
    # check if we are the primary node for this song
    song_key = utils.hash_function(song_name)
    current_id = node.get_identifier()
    predecessor = node.get_predecessor()
    
    if not belongs_to_me(song_key, current_id, utils.hash_function(f"{predecessor[0]}:{predecessor[1]}")):
        # this node is not the primary for the song , forward the packet to our successor
        successor = node.get_successor()
        if not successor or successor == []:
            print("No valid successor found, replication aborted.")
            return jsonify({"message": "Replication aborted (no successor)"}), 200
        else:
            replication_url = f"http://{successor[0]}:{successor[1]}/find_primary"
            try:
                print(f"Forwarding replication packet for '{song_name}' (not primary here) to node {successor[0]}:{successor[1]}.")
                response = requests.post(replication_url, json=data)
                response.raise_for_status()
                return jsonify({
                    "message": f"Forwarded replication packet for '{song_name}' to node {successor[0]}:{successor[1]}",
                    "next_response": response.json()
                }), 200
            except requests.RequestException as e:
                return jsonify({"error": f"Failed to forward replication packet to node {successor}: {str(e)}"}), 500
    
    else:
        # This node is the primary node for the song, now we send the packet the next k-1 nodes and replicate it , and after the k-1 nodes we delete it from the list of the node that gets the request if exists till we get to the primary node of the key again
            new_counter = counter - 1
            new_packet = {"song_name": song_name, "value": value, "k": new_counter}
            successor = node.get_successor()
            if not successor or successor == []:
                print("No valid successor found, replication stops here.")
                return jsonify({"message": "Replication ended at this node (no successor)"}), 200
            replication_url = f"http://{successor[0]}:{successor[1]}/start_replication"
            try:
                print(f"Primary node starting the replication process for '{song_name}' with new k = {new_counter} to node {successor[0]}:{successor[1]}.")
                response = requests.post(replication_url, json=new_packet)
                response.raise_for_status()
                return jsonify({
                    "message": f"Forwarded replication packet for '{song_name}' from primary node to {successor[0]}:{successor[1]}",
                    "next_response": response.json()
                }), 200
            except requests.RequestException as e:
                return jsonify({"error": f"Primary node failed to forward replication packet: {str(e)}"}), 500
 
            
# last step of the replication process, we copy to the next k-1 nodes the song and its value and the counter, and if the counter is 0 we delete it from the list of the node that gets the request, all of these till we reach again the primary node of the key    
@app.route('/start_replication', methods=['POST'])
def start_replication():
    data = request.get_json()
    if not data or "song_name" not in data or "value" not in data or "k" not in data:
        return jsonify({"error": "Invalid replication packet data"}), 400
    
    song_name = data["song_name"]
    value = data["value"]
    counter = data["k"]
    print(f"Received replication packet for '{song_name}' with k = {counter} at node {node.get_ip()}:{node.get_port()}")
    
    #check if we are the primary node for this song, if we are then we stop the recursion and return
    song_key = utils.hash_function(song_name)
    current_id = node.get_identifier()
    predecessor = node.get_predecessor()
    
    print(f"I am {node.get_port()}")
    
    # base case of our recursion, we are the primary node for the song (the node the requests started from)
    if belongs_to_me(song_key, current_id, utils.hash_function(f"{predecessor[0]}:{predecessor[1]}")):
        print(f"Primary node reached for '{song_name}', stopping replication.")
        return jsonify({"message": f"Primary node reached again for '{song_name}',replication has been finished"}), 200
    
    
    # we are not responsible for the song, so we delete it and forward the packet to our successor
    if counter <= 0:
        print(f'counter is {counter} , deleting song {song_name} from node {node.get_ip()}:{node.get_port()}')
        node.delete(song_name)
        print(f"Song '{song_name}' deleted from node {node.get_ip()}:{node.get_port()} (k = 0), so we are not responsible for it")
        new_counter = counter - 1
        new_packet = {"song_name": song_name, "value": value, "k": new_counter}
        successor = node.get_successor()
        if not successor or successor == []:
            print("No valid successor found, replication stops here.")
            return jsonify({"message": "Replication ended at this node (no successor)"}), 200
        replication_url = f"http://{successor[0]}:{successor[1]}/start_replication"
        try:
            print(f"Node {node.get_ip()}:{node.get_port()} forwarding replication packet for '{song_name}' with new k = {new_counter} to node {successor[0]}:{successor[1]}.")
            response = requests.post(replication_url, json=new_packet)
            response.raise_for_status()
            return jsonify({
                "message": f"Forwarded replication packet for '{song_name}' to node {successor[0]}:{successor[1]}",
                "next_response": response.json()
            }), 200
        except requests.RequestException as e:
            return jsonify({"error": f"Failed to forward replication packet to node {successor}: {str(e)}"}), 500
    
    # we are responsible for the song, so we insert it and forward the packet to our successor
    else:
        if node.query(song_name) is not None:
            print(f"Song '{song_name}' already exists in this node, updating value.")
            node.get_song_list()[song_name] = value # we want all the responsible nodes to have the same value for the same song
        
        else: # song does not exist in this node, so we can simply insert it
            node.insert(song_name, value)
        
        new_counter = counter - 1
        new_packet = {"song_name": song_name, "value": value, "k": new_counter}
        successor = node.get_successor()
        if not successor or successor == []:
            print("No valid successor found, replication stops here.")
            return jsonify({"message": "Replication ended at this node (no successor)"}), 200
        replication_url = f"http://{successor[0]}:{successor[1]}/start_replication"
        try:
            print(f"Node {node.get_ip()}:{node.get_port()} forwarding replication packet for '{song_name}' with new k = {new_counter} to node {successor[0]}:{successor[1]}.")
            response = requests.post(replication_url, json=new_packet)
            response.raise_for_status()
            return jsonify({
                "message": f"Forwarded replication packet for '{song_name}' to node {successor[0]}:{successor[1]}",
                "next_response": response.json()
            }), 200
        except requests.RequestException as e:
            return jsonify({"error": f"Failed to forward replication packet to node {successor}: {str(e)}"}), 500
        

# returns the songs that are supposed to be in the node that hit the request but are in the node that got the request
@app.route('/replicate', methods=['GET'])
def replicate():
    # Get the songs that are supposed to be in the predecessor node
    songs = node.get_song_list()
    songs_to_replicate = {}
    pred_param = request.args.get('pred', '')
    pred_of_pred = [] 
    if pred_param:
        pred_list = pred_param.split(',')
        pred_of_pred = pred_list
    else:
        return jsonify({"error": "No predecessor can not proceed... "}), 400
    
    if pred_of_pred == None or pred_of_pred == []:
        return jsonify({"error": "No predecessor of predecessor can not proceed... "}), 400
    pred = node.get_predecessor()
    for song in songs:
        song_key = utils.hash_function(song)
        if belongs_to_me(song_key,utils.hash_function(f"{pred[0]}:{pred[1]}") , utils.hash_function(f"{pred_of_pred[0]}:{pred_of_pred[1]}")):
            songs_to_replicate[song] = songs[song]
    return jsonify({"songs_that_belong_to_me": songs_to_replicate}), 200


@app.route('/update_predecessor', methods=['POST'])
def update_predecessor():
    requests_data = request.get_json()
    pred_ip = requests_data.get("ip")
    pred_port = requests_data.get("port")
    if not pred_ip or not pred_port:
        return jsonify({"error": "Not sufficient amount of data to update predecessor"}), 400
    node.set_predecessor([pred_ip, pred_port])
    return jsonify({"message": "Predecessor updated"}), 200


@app.route('/update_successor', methods=['POST'])
def update_successor():
    requests_data = request.get_json()
    succ_ip = requests_data.get("ip")
    succ_port = requests_data.get("port")
    if not succ_ip or not succ_port:
        return jsonify({"error": "Not sufficient amount of data to update successor"}), 400
    node.set_successor([succ_ip, succ_port])
    return jsonify({"message": "Successor updated"}), 200

@app.route('/pred_suc', methods=['POST'])
def pred_suc():
    pred = node.get_predecessor()
    suc = node.get_successor()
    if pred != [] and suc != []:
        return jsonify({"predecessor key": utils.hash_function(f'{pred[0]}:{pred[1]}'), 
                    "successor key": utils.hash_function(f'{suc[0]}:{suc[1]}'),
                        "pred":pred, "suc":suc , "current_key":node.get_identifier()}), 200
    else:
        return jsonify({"pred": pred, "suc":suc}), 404


@app.route('/overlay', methods=['GET'])
def overlay():
    # get the visited nodes as a comma‐separated string from query parameters
    visited = request.args.get('visited', '')
    visited_set = set(visited.split(',')) if visited else set()
    
    current_node = f"{node.get_ip()}:{node.get_port()}"
    
    # if we've already visited the current node, stop the chain
    if current_node in visited_set:
        return jsonify({"overlay": []}), 200
    
    # add the current node to visited set
    visited_set.add(current_node)
    
    # start the overlay list with current node info
    overlay_list = [{
        "ip": node.get_ip(),
        "port": node.get_port()
    }]
    
    # get the successor pointer
    successor = node.get_successor()
    
    if not successor or successor == []:
        return jsonify({"overlay": overlay_list}), 200
    
    # prepare the query parameter for visited nodes
    params = {'visited': ','.join(visited_set)}
    successor_url = f"http://{successor[0]}:{successor[1]}/overlay"
    
    try:
        response = requests.get(successor_url, params=params)
        response.raise_for_status()
        # merge overlay info from successor with current node's info
        successor_overlay = response.json().get("overlay", [])
        overlay_list.extend(successor_overlay)
    except requests.RequestException as e:
        return jsonify({"error": f"Failed to forward overlay request to node {successor[0]}:{successor[1]}: {str(e)}"}), 500
    
    return jsonify({"overlay": overlay_list}), 200


@app.route('/get_nodes', methods=['GET'])
def get_nodes():
  if node.get_identifier() == utils.hash_function(f"{os.getenv('BOOTSTRAP_IP')}:{os.getenv('BOOTSTRAP_PORT')}"):
    return jsonify({"number_of_nodes": number_of_nodes}), 200
  else:
      try:
         data =  requests.get(f"http://{os.getenv('BOOTSTRAP_IP')}:{os.getenv('BOOTSTRAP_PORT')}/get_nodes")
      except requests.RequestException as e:
          return jsonify({"error": f"Failed to get nodes from bootstrap node: {str(e)}"}), 500
      
      num_of_nodes = data.json().get("number_of_nodes", 0)
      return jsonify({"number_of_nodes": number_of_nodes}), 200


@app.route('/decrease_num_of_nodes', methods=['POST'])
def decrease_num_of_nodes():
    global number_of_nodes
    number_of_nodes -= 1
    return jsonify({"message": "Number of nodes decreased"}), 200

@app.route('/give_songs', methods=['POST'])
def give_songs():
    # Get the songs that are supposed to be in the predecessor node
    data = request.get_json()
    if data == None or data == {}:
        return jsonify({"error": "No songs to give"}), 400
    for song in data:
        node.set_song_to_song_list(song, data[song])
    return jsonify({"message": "Songs given successfully"}), 200

@app.route('/same_image', methods=['POST'])
def same_image():
     # Get the song list from the request payload.
    song_list = request.get_json()
    if song_list is None:
        return jsonify({"error": "No song list provided"}), 400

    # Update local song list.
    node.set_song_list(song_list)
    print(f"Node {node.get_ip()}:{node.get_port()} updated song list to: {node.get_song_list()}")

    # Get visited nodes from query parameters to avoid cycles.
    visited_param = request.args.get('visited', '')
    visited = set(visited_param.split(',')) if visited_param else set()

    current_node = f"{node.get_ip()}:{node.get_port()}"
    if current_node in visited:
        # Cycle complete; return the updated song list.
        print(f"Cycle complete at node {current_node}.")
        return jsonify({"message": "Same image cycle complete", "song_list": node.get_song_list()}), 200

    # Add current node to visited set.
    visited.add(current_node)
    # If there is a valid successor, forward the same image packet.
    successor = node.get_successor()
    if not successor or successor == []:
        print("No valid successor found; same image propagation ends here.")
        return jsonify({"message": "No successor to propagate same image", "song_list": node.get_song_list()}), 200

    same_image_url = f"http://{successor[0]}:{successor[1]}/same_image?visited={','.join(visited)}"
    try:
        print(f"Forwarding same image from node {current_node} to successor {successor[0]}:{successor[1]}")
        response = requests.post(same_image_url, json=song_list)
        response.raise_for_status()
        return jsonify({"message": "Propagated same image", "next_response": response.json()}), 200
    except requests.RequestException as e:
        return jsonify({"error": f"Failed to forward same image to node {successor}: {str(e)}"}), 500
    
    

# for testing purposes
@app.route('/show_song_list', methods=['GET'])
def show_song_list():
    return jsonify(node.get_song_list()), 200
                   
if __name__ == '__main__':
    print(f"Starting Flask for Bootstrap on port: 5000")
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 5000

    node = BootstrapNode(None, None, port)
    app.run(host="0.0.0.0", port=port, debug=True)
