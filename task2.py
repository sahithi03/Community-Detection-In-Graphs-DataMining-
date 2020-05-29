from pyspark import SparkContext
import sys
import time
import collections
import copy
from itertools import combinations
start = time.time()
sc = SparkContext('local[*]','task2')
sc.setLogLevel("ERROR")

input_file_name = sys.argv[1]
bet_output_file_name = sys.argv[2]
com_output_file_name = sys.argv[3]
data = sc.textFile(input_file_name)

def get_betweeness(root,graph):

    queue = collections.deque()
    level = {}
    shortest_path_count = {}
    queue.append(root)
    level[root] = 0
    shortest_path_count[root] = 1
    node_parents = collections.defaultdict(list)
    while queue:
        node = queue.popleft()
        current_level = level[node]
        for nei in graph[node]:
            if nei not in level:
                queue.append(nei)
                level[nei] = current_level + 1
                node_parents[nei] = [node]
                shortest_path_count[nei] = shortest_path_count[node]
            else:
                if (level[nei] == current_level + 1):
                    node_parents[nei].append(node)
                    shortest_path_count[nei] += shortest_path_count[node]
    #level -> nodes
    level_nodes_dict = collections.defaultdict(list)
    max_level = float("-inf")
    for k,v in level.items():
        level_nodes_dict[v].append(k)
        max_level = max(max_level,v)
    #calculate betweeness
    node_weight_dict = {}
    curr_level = max_level

    betweenness_dict = {}
    while curr_level >= 0:
        curr_nodes = level_nodes_dict[curr_level]
        for curr_node in curr_nodes:
            if curr_node not in node_weight_dict:
                node_weight_dict[curr_node] = 1
            if  curr_node in node_parents:
                parents = node_parents[curr_node]
                temp = 0
                for parent in parents:
                    temp += shortest_path_count[parent]
                for parent in parents:
                    src = curr_node
                    dest = parent
                    edge = tuple(sorted((src,dest))) 
                    if edge not in betweenness_dict:
                        betweenness_dict[edge] = node_weight_dict[src] * shortest_path_count[dest] / temp
                    else:
                        betweenness_dict[edge] += node_weight_dict[src] * shortest_path_count[dest] / temp
                    if dest not in node_weight_dict:
                        node_weight_dict[dest] = 1
                    node_weight_dict[dest] += node_weight_dict[src] * shortest_path_count[dest] / temp
        curr_level -= 1
    betweeness_list = [[k,v] for k,v in betweenness_dict.items()]
    

    return betweeness_list


def bfs(root, adjacency_matrix):
    queue = collections.deque()
    queue.append(root)
    visited.add(root)
    v_count = []
    e_count = []
    v_count.append(root)
    while queue:
        node = queue.popleft()
        for nei in adjacency_matrix[node]:
            pair = sorted((node,nei))
            if pair not in e_count:
                e_count.append(pair)
            if nei not in visited:
                visited.add(nei)
                v_count.append(nei)
                queue.append(nei)

    return (v_count,e_count)

def get_connected_components(adjacency_matrix):
    vertices = []
    for keys in adjacency_matrix.keys():
        vertices.append(keys)
    connected_components = []
    for vertex in vertices:
        if vertex not in visited:

            component = bfs(vertex,adjacency_matrix)
            connected_components.append(component)

    return connected_components

def remove_edge(adjacency_matrix,edge_to_be_removed):
    src = edge_to_be_removed[0]
    dst = edge_to_be_removed[1]
    if src in adjacency_matrix:
        if dst in adjacency_matrix[src]:
            adjacency_matrix[src].remove(dst)
    if dst in adjacency_matrix:
        if src in adjacency_matrix[dst]:
            adjacency_matrix[dst].remove(src)
    return adjacency_matrix



def calculate_modularity(original_graph, connected_components, m):
    modularity = 0
    for component in connected_components:
        com_vertices = component[0]
        for i in range(len(com_vertices)):
            for j in range(len(com_vertices)):
                Aij = 0
                if com_vertices[j] in original_graph[com_vertices[i]]:
                    Aij = 1

                ki = len(original_graph[com_vertices[i]])
                kj = len(original_graph[com_vertices[j]])

                modularity += Aij - (ki*kj)/(2*m)

    modularity = modularity / (2*m)
    return modularity

rdd = data.map(lambda x:x.split(' ')).map(lambda x:(str(x[0]),[str(x[1])])).reduceByKey(lambda x,y:x+y)
vertices_rdd1 = rdd.map(lambda x:(x[0])).collect()
vertices_rdd2 = data.map(lambda x:x.split(' ')).map(lambda x:(str(x[1]),[str(x[0])])).reduceByKey(lambda x,y:x+y).map(lambda x:(x[0])).collect()
list_of_vertices = list(set(vertices_rdd1+vertices_rdd2))
vertices_rdd = sc.parallelize(list_of_vertices)

edges_rdd = data.map(lambda x:x.split(' ')).map(lambda x:(int(x[0]),int(x[1])))

graph = data.map(lambda x:x.split(' ')).map((lambda x:(x[0],[x[1]]))).reduceByKey(lambda x,y:x+y).collectAsMap()
graph_rdd2 = data.map(lambda x:x.split(' ')).map((lambda x:(x[1],[x[0]]))).reduceByKey(lambda x,y:x+y).collectAsMap()
for key,value in graph_rdd2.items():
    if key in graph:
        graph[key].extend(value)
    else:
        graph[key] = value

betweeness_rdd = vertices_rdd.flatMap(lambda x:(get_betweeness(x,graph))).reduceByKey(lambda x,y:x+y).map(lambda x:(x[0],x[1]/2)).sortByKey().\
    map(lambda x: (x[1],x[0])).sortByKey(ascending=False).map(lambda x: (x[1],x[0]))
betweeness_result = betweeness_rdd.collect()

m = edges_rdd.count()
visited = set()

first_edge_to_remove = betweeness_result[0][0]
adjacency_matrix = copy.deepcopy(graph)
#connected_components = get_connected_components(adjacency_matrix)
#visited = set()
#print("Initial Connected Components",len(connected_components))
#modularity = calculate_modularity(graph,connected_components,m)
#print("Initial Modularity",modularity)
max_modularity = -1
communities = []
j = 0

while j < m:

    adjacency_matrix = remove_edge(adjacency_matrix,first_edge_to_remove)
    #print("After Removal of",first_edge_to_remove)
    connected_components = get_connected_components(adjacency_matrix)
    #print("No of connected Components",len(connected_components))
    visited = set()
    modularity = calculate_modularity(graph,connected_components,m)
    #print("New Modularity",modularity)
    if (modularity > max_modularity):
        max_modularity = modularity
        communities = connected_components
    new_matrix = collections.defaultdict(list)
    for c in connected_components:
        c_edges = c[1]
        for edge in c_edges:
            new_matrix[edge[0]].append(edge[1])
            new_matrix[edge[1]].append(edge[0])
    adjacency_matrix = new_matrix.copy()
    temp = []
    for i in adjacency_matrix.keys():
        temp.append(i)
        
    betweeness_values = []
    for vertex in temp:
        ans  = get_betweeness(vertex,adjacency_matrix)
        betweeness_values.extend(ans)
    betweeness_dict = collections.defaultdict(float)
    for l in betweeness_values:
        betweeness_dict[l[0]] += l[1]
    sorted_betweeness = sorted(betweeness_dict.items(),key=lambda x:x[1],reverse=True)
    if sorted_betweeness:
        first_edge_to_remove = sorted_betweeness[0][0]
    
    j = j + 1

sorted_communities= []
for i in communities :
	sorted_communities.append((sorted(i[0]),len(i[0])))
    
sorted_communities.sort()
sorted_communities.sort(key=lambda x:x[1])

#print(max_modularity)
#print(len(communities))


f = open(com_output_file_name,"w")
for i in sorted_communities:
    f.write(str(i[0]).replace("[","").replace("]",""))
    f.write("\n")
f.close()

f = open(bet_output_file_name,"w")
for i in betweeness_result:
    f.write(str(i[0])+ ", " + str(i[1]))
    f.write("\n")
f.close()
print("Execution Time",time.time() - start)
