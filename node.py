from utils import hash_function,get_local_ip

""" 
Each Node is a FLASK server, so this Class Implements the Node as a unit of the DHT. The logic is implemented
in the API. From the API we know which nodes to reach out to ...
"""
   
class Node:
    def __init__(self,identifier = None,ip = None,port = None,predecessor = [], successor = [], song_list = {}):
        self.ip = get_local_ip()
        self.port = port
        self.identifier = hash_function(f'{self.ip}:{self.port}')
        self._predecessor = predecessor      
        self._successor = successor 
        self._song_list = song_list
        
    def insert(self, key, value:int):
        if key not in self._song_list:
            self._song_list[key] = value
        else:
            self._song_list[key] += value
        print(f"Song {value} added to {key}\n")
        
    
    def query(self,key):
        if key == '*':
            return self._song_list
        else:
            if key in self._song_list:
                print(f"Song found in this node {self.identifier}\n")
                return key, self._song_list[key]
            else:
                return None
    
    
    def delete(self,key) -> bool:
        # We want to delete the song_name and its value from the song_list 
        if key in self._song_list:
            del self._song_list[key]
            print(f"Song {key} deleted from this node {self.identifier}\n")
            return True
        else:
            print(f"Song not found in this node {self.identifier}\n")
            return False
           
        
    def set_predecessor(self,predecessor):
        self._predecessor = predecessor
    
    def set_successor(self,successor):
        self._successor = successor
    
    
    def set_song_list(self,song_list):
        self._song_list = song_list
    
    def set_song_to_song_list(self,key,value):
        self._song_list[key] = value
        
    def get_predecessor(self):
        return self._predecessor
    
      
    def get_successor(self):
        return self._successor
     
    def get_identifier(self):
        return self.identifier
     
    def get_ip(self):
        return self.ip
    
    def get_port(self):
        return self.port
      
    def get_song_list(self):
        return self._song_list


      
class BootstrapNode(Node):
    def __init__(self,identifier,host,port):
        super().__init__(identifier,host,port)
        self._predecessor = None
        self._successor = None
        self._song_list = {}
        self._replica_list = {}        