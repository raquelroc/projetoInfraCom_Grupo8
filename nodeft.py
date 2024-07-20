import socket
import threading
import hashlib
import time

tabelaRot = {
    1: {"vizinhos": [5, 2], "host": "127.0.0.1", "port": 1231},
    2: {"vizinhos": [1, 3], "host": "127.0.0.2", "port": 1232},
    3: {"vizinhos": [2, 4], "host": "127.0.0.3", "port": 1233},
    4: {"vizinhos": [3, 5], "host": "127.0.0.4", "port": 1234},
    5: {"vizinhos": [4, 1], "host": "127.0.0.5", "port": 1235}
}

threads = [] # usado para finalizar as threads
stop_event = threading.Event()

def hash_key(key, lowerBound=0, upperBound=5, decimals=2):

    # Gera um hash em int
    hashInt = int(hashlib.sha1(key.encode()).hexdigest(), 16) 
    # normaliza o valor para o intervalo [0, 1)
    normalizedValue = hashInt / float(2**160) 
    # escala o valor para o intervalor [0, 5]
    scaledValue = 0 + (upperBound - lowerBound) * normalizedValue
    # arredonda o valor para duas casas decimais
    roundedValue = round(scaledValue, decimals) 

    return roundedValue

# OBS: ctrl+C para finalizar o programa
class Node:
    def __init__(self, nodeID):
        # criação dos atributos do nó
        self.identifier = nodeID
        self.nodeInfo = tabelaRot[nodeID]
        self.hashTable = {} # a hashTable do nó possui as chaves do intervalo n-1 até n menos o n 
        self.fingerTable = self.create_fingerTable()
        self.sock = None

        # Iniciando o server do nó
        self.serverThread = threading.Thread(target=self.run_server)
        self.serverThread.daemon = True

        threads.append(self.serverThread) # para o problema com as threads

        self.serverThread.start()

    def create_fingerTable(self):
        tempFingerTable = {}
        for i in range(3):
            row = [i+1, self.identifier + 2 ** i]
            if row[1] > len(tabelaRot):
                row[1] -= len(tabelaRot)
            tempFingerTable[row[0]] = row[1]
        return tempFingerTable

    # Funçao para criação do servidor desse nó
    def run_server(self):
        # Criação do server do nó
        host, port = self.nodeInfo["host"], self.nodeInfo["port"]
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind((host, port))
        sock.listen(5)
        self.sock = sock

        print(f"No {self.identifier} escutando em {host}: {port}")
        
        # recebimento dos pacotes, são tratados no handle_client
        while not stop_event.is_set():

            nodeSock, nodeAddr = sock.accept()

            # dentro de nodeSock é possivel ver as informações de host e port que nós escolhemos
            clientThread = threading.Thread(target=self.handle_client, args=(nodeSock,))
            clientThread.daemon = True
            
            threads.append(clientThread) # para o problema com as threads

            clientThread.start()

    
    # Função de tratamento dos pacotes recebidos
    def handle_client(self, nodeSock):
        try:
            arquivo = nodeSock.recv(1024).decode()
            
            # tratando casos de chaves inteiros
            requestedKey = 0
            requestedKey += int(arquivo[3])
            if len(arquivo) > 4 : requestedKey += int(arquivo[5]) * 0.1
            if len(arquivo) > 5 : requestedKey += int(arquivo[6]) * 0.01

            # comandos
            # exemplo de comando enviado por pacote "PUT3" -> comando + key
            # caso um pacote chegue com o comando put a função put é chamada
            match arquivo[0:3]:
                case "PUT":
                    self.put(requestedKey)

        finally:
            # pacotes normais
            print(f"o no {self.identifier} recebeu um arquivo do endereco {nodeSock.getsockname()}: {arquivo}")
            nodeSock.close()

    #Função que manda pacotes responsaveis por comando para outros nós
    def send_command(self, id, key, command): # parametro data
        try:
            Sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            Sock.connect((tabelaRot[id]["host"], tabelaRot[id]["port"]))

            arquivo = f"{command}{key}"

            Sock.send(arquivo.encode())
            Sock.close()
        except ConnectionRefusedError:
                print(f"o no {id} nao esta disponivel")
        except Exception as e:
            print(f"aqui {e}")
            print(f"erro ao enviar arquivo do no {nodeID} para o no {id}")

    # Função que verifica se a chave está no intervalo desse nó
    def in_interval(self, key, limiteInferior, limiteSuperior):
        if self.identifier == 1 and (key < 1 or key > len(tabelaRot)):
            return True
        return limiteInferior <= key < limiteSuperior

    # Função PUT utilizando a finger table
    def put(self, key): # parametro data
        if self.in_interval(key, self.identifier-1, self.identifier-1 + 0.99):
            print(f"o nó {self.identifier} armazenou o arquivo 'teste' com a chave {key}")
            self.hashTable[key] = 'teste'
            return

        # checa a finger table pelo nó que deve enviar
        nextNode = len(tabelaRot) + 2
        for node in self.fingerTable.values():
            if abs(node - key) < nextNode:
                nextNode = node

        self.send_command(nextNode, key, "PUT")   

    # atualiza a finger table e a tabela de roteamento com os id's do nó novo
    def update_routingTable(self):
        self.fingerTable = self.create_fingerTable()
        tabelaRot[self.identifier]["vizinhos"] = [self.identifier - 1, self.identifier + 1]
        if self.identifier == 1:
            tabelaRot[self.identifier]["vizinhos"][0] = len(tabelaRot)
        if self.identifier == len(tabelaRot):
            tabelaRot[self.identifier]["vizinhos"][1] = 1
    
    # redistribui os dados da hashTable para o novo nó
    def redistribute_keys(self):
        for data in self.hashTable.keys():
            if self.in_interval(data, self.identifier, self.identifier + 0.99):
                self.put(self, data)
     
# Função que adiciona um novo nó no final da rede
def add_node(host, port):
    tabelaRot[len(nodes)+1] = {"vizinhos": [], "host": host, "port": port}
    new_node = Node(len(nodes)+1)
    nodes.append(new_node)
    for node in nodes:
       node.update_routingTable()
    nodes[-1].redistribute_keys()

# Criação dos 5 nós
nodes = []
for nodeID in tabelaRot.keys():
    nodeBase = Node(nodeID)
    nodes.append(nodeBase)

# teste para o funcionamento da função PUT
nodes[0].put(1)

add_node("127.0.0.6", 1236)

try:
    while not stop_event.is_set():
        time.sleep(3)
except KeyboardInterrupt:
    stop_event.set()
    print("Finalizando threads...")

print("final")

