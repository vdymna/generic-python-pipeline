from collections import deque

class DAG:
    
    def __init__(self):
        self.graph = {}
    
    def add(self, node, points_to=None):
        if node not in self.graph:          
            self.graph[node] = []
            
        if points_to:            
            if points_to not in self.graph:
                self.graph[points_to] = []  
            self.graph[node].append(points_to)

        if len(self.sort()) != len(self.graph):
            raise Exception('A cycle is detected in the graph') 

    def sort(self):
        self.in_degrees()
        nodes_to_visit = deque()
        for node, pointers in self.degrees.items():
            if pointers == 0:
                nodes_to_visit.append(node)
        
        sorted_nodes = []
        while nodes_to_visit:
            node = nodes_to_visit.popleft()
            for pointer in self.graph[node]:
                self.degrees[pointer] -= 1
                if self.degrees[pointer] == 0:
                    nodes_to_visit.append(pointer)
            sorted_nodes.append(node)
        return sorted_nodes

    def in_degrees(self):
        self.degrees = {}      
        for node in self.graph:
            if node not in self.degrees:
                self.degrees[node] = 0
            for pointed in self.graph[node]:
                if pointed not in self.degrees:
                    self.degrees[pointed] = 0
                self.degrees[pointed] += 1

    