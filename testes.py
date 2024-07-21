import timeit
from nodeft import Node, tabelaRot, hash_key

def setup_nodes():
    nodes = []
    for nodeID in tabelaRot.keys():
        nodeBase = Node(nodeID)
        nodes.append(nodeBase)
    return nodes

def test_put(nodes, key):
    nodes[0].put(key)

def test_get(nodes, key):
    nodes[0].get(key)

print("aqui")

# Configuração inicial
nodes = setup_nodes()
key = hash_key("some_key")

# Medir desempenho de PUT
put_time = timeit.timeit(lambda: test_put(nodes, key), number=10)
print(f"Tempo médio de PUT: {put_time / 100:.6f} segundos")

# Medir desempenho de GET
get_time = timeit.timeit(lambda: test_get(nodes, key), number=10)
print(f"Tempo médio de GET: {get_time / 100:.6f} segundos")
