import requests
import sys

# Use the bootstrap node as the default entry point:
BASE_URL = "http://10.0.17.96:5000"

node_mapping = {
    1: ('10.0.17.96', 5000),
    2: ('10.0.17.245', 5001),
    3: ('10.0.17.158', 5002),
    4: ('10.0.17.171', 5003),
    5: ('10.0.17.209', 5004),
    6: ('10.0.17.96', 5005),
    7: ('10.0.17.245', 5006),
    8: ('10.0.17.158', 5007),
    9: ('10.0.17.171', 5008),
    10: ('10.0.17.209', 5009)
}

def print_help():
    print("Available commands:")
    print("  insert <key> <value>  - Insert a new (key,value) pair into the DHT.")
    print("  delete <key>          - Delete the (key,value) pair with the given key.")
    print("  query <key>           - Query the DHT for a key. Use '*' to return all pairs.")
    print("  depart                - Gracefully depart from the DHT.")
    print("  overlay               - Display the network overlay (Chord ring topology).")
    print("  setnode <node id>     - Set the target node (from available mapping) to hit.")
    print("  listnodes             - List all available node mappings.")
    print("  help                  - Show this help message.")
    print("  exit                  - Exit the CLI.")

def list_nodes():
    print("Available nodes:")
    for node_id, (ip, port) in node_mapping.items():
        print(f"  {node_id}: http://{ip}:{port}")

def setnode_command(node_id_str):
    global BASE_URL
    try:
        node_id = int(node_id_str)
        if node_id in node_mapping:
            ip, port = node_mapping[node_id]
            BASE_URL = f"http://{ip}:{port}"
            print(f"Target node set to: {BASE_URL}")
        else:
            print("Invalid node id!")
    except ValueError:
        print("Please provide a valid node id (an integer).")

def insert_command(key, value):
    url = f"{BASE_URL}/insert/{key}/{value}"
    try:
        response = requests.post(url)
        print(response.json())
    except Exception as e:
        print(f"Error during insert: {e}")

def delete_command(key):
    url = f"{BASE_URL}/delete/{key}"
    try:
        response = requests.delete(url)
        print(response.json())
    except Exception as e:
        print(f"Error during delete: {e}")

def query_command(key):
    url = f"{BASE_URL}/query/{key}"
    try:
        response = requests.get(url)
        print(response.json())
    except Exception as e:
        print(f"Error during query: {e}")

def depart_command():
    url = f"{BASE_URL}/depart"
    try:
        response = requests.post(url)
        print(response.json())
    except Exception as e:
        print(f"Error during depart: {e}")

def overlay_command():
    url = f"{BASE_URL}/overlay"
    try:
        response = requests.get(url)
        print(response.json())
    except Exception as e:
        print(f"Error fetching overlay: {e}")

def main():
    print("Welcome to the DHT CLI. Type 'help' to get started.")
    while True:
        try:
            cmd_line = input("DHT> ")
        except KeyboardInterrupt:
            print("\nExiting...")
            break
        tokens = cmd_line.strip().split()
        if not tokens:
            continue
        command = tokens[0].lower()
        if command == "insert":
            if len(tokens) < 3:
                print("Usage: insert <key> <value>")
                continue
            key = tokens[1]
            value = " ".join(tokens[2:])
            insert_command(key, value)
        elif command == "delete":
            if len(tokens) != 2:
                print("Usage: delete <key>")
                continue
            delete_command(tokens[1])
        elif command == "query":
            if len(tokens) != 2:
                print("Usage: query <key>")
                continue
            query_command(tokens[1])
        elif command == "depart":
            depart_command()
        elif command == "overlay":
            overlay_command()
        elif command == "setnode":
            if len(tokens) != 2:
                print("Usage: setnode <node id>")
                continue
            setnode_command(tokens[1])
        elif command == "listnodes":
            list_nodes()
        elif command == "help":
            print_help()
        elif command in ("exit", "quit"):
            print("Exiting CLI.")
            break
        else:
            print("Unknown command. Type 'help' for a list of commands.")

if __name__ == "__main__":
    main()