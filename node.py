import socket
import threading

tabelaRot = {
    1: {"vizinhos": [5, 2], "host": "127.0.0.1", "port": 1231},
    2: {"vizinhos": [1, 3], "host": "127.0.0.2", "port": 1232},
    3: {"vizinhos": [2, 4], "host": "127.0.0.3", "port": 1233},
    4: {"vizinhos": [3, 5], "host": "127.0.0.4", "port": 1234},
    5: {"vizinhos": [4, 1], "host": "127.0.0.5", "port": 1235}
}


threads = [] # usado para finalizar as threads

# OBS:
# - Ao rodar o programa ele fica aberto, para rodar novamente feche o terminal 
# - fiz isso para evitar um problema onde as threads daemon ainda estavam
# - em execução enquanto o interpretador Python finalizava 


class node:
    def __init__(self, nodeID):
        # criação dos atributos do nó
        self.identifier = nodeID
        self.nodeInfo = tabelaRot[nodeID]
        self.hashTable = self.create_hash_table()

        # Iniciando o server do nó
        self.serverThread = threading.Thread(target=self.run_server)
        self.serverThread.daemon = True

        threads.append(self.serverThread) # para o problema com as threads

        self.serverThread.start()

    # Função para criaçao da hashtable
    # - a hash table armazena apenas os dados "host" e "port" dos vizinhos desse nó
    def create_hash_table(self):
        hash_table = {}

        for vizinho in self.nodeInfo["vizinhos"]:
            hash_table[vizinho] = (tabelaRot[vizinho]["host"], tabelaRot[vizinho]["port"])
        
        return hash_table

    # Funçao para criação do servidor desse nó
    def run_server(self):
        # Criação do server do nó
        host, port = self.nodeInfo["host"], self.nodeInfo["port"]
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind((host, port))
        sock.listen(5)

        print(f"No {self.identifier} escutando em {host}: {port}")
        
        # recebimento dos pacotes, são tratados no handle_client
        while True:
            nodeSock, nodeAddr = sock.accept()

            # dentro de nodeSock é possivel ver as informações de host e port que nós escolhemos
            #print(nodeAddr, nodeSock)
            #print(f"aqui {nodeSock.getpeername()}")

            clientThread = threading.Thread(target=self.handle_client, args=(nodeSock, nodeAddr))
            clientThread.daemon = True
            
            threads.append(clientThread) # para o problema com as threads

            clientThread.start()
    
    # Função de tratamento dos pacotes recebidos
    def handle_client(self, nodeSock, nodeAddr):
        arquivo = nodeSock.recv(1024).decode()
        
        # comandos
        # exemplo de comando enviado por pacote "PUT3" -> comando + key
        # caso um pacote chegue com o comando put a função put é chamada
        if arquivo[0:2] in "PUT":
            self.put(int(arquivo[3]))

        # pacotes normais
        print(f"o no {self.identifier} recebeu um arquivo do endereco {nodeAddr}: {arquivo}")
        nodeSock.close()

    # Função para a função PUT *não funcionando*
    # - caso o nó atual não seja o desejado ele irá identificar qual vizinho
    # - é o melhor para enviar o pacote e mandara um pacote com o comando para o vizinho
    # PROBLEMA : o programa fica em um loop nos nós 5 e 1 devido a decisão baseada no maior/menor
    def put(self, key):
        vizinhoId = 0

        # verificando se o nó atual é o certo
        if self.identifier == key:
            print("a")
            return
        
        # decidindo para qual vizinho enviar o pacote
        elif key < self.identifier:
            vizinhoId = self.hashTable[min(self.hashTable.keys())]
        elif key > self.identifier:
            vizinhoId = self.hashTable[max(self.hashTable.keys())]

        # enviando o pacote
        try:
            maiorSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            maiorSock.connect((vizinhoId[0], vizinhoId[1]))

            arquivo = f"PUT{key}"

            maiorSock.send(arquivo.encode())
            maiorSock.close()
        except ConnectionRefusedError:
                print(f"o no {max(self.hashTable.keys())} nao esta disponivel")
        except Exception as e:
            print(e)
            print(f"erro ao enviar arquivo do no {nodeID} para o no {max(self.hashTable.keys())}")
            

    # Função para testar a conexão entre os vizinhos
    # - cada nó envia um pacote com uma mensagem para seus vizinhos imediatos
    def send_hi(self):
        for vizinhoID in self.nodeInfo["vizinhos"]:
            vizinhoInfo = self.hashTable[vizinhoID]
            try:
                vizinhoSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                vizinhoSock.connect((vizinhoInfo[0], vizinhoInfo[1]))

                arquivo = f"nome arquivo, enviado por {self.identifier}"

                vizinhoSock.send(arquivo.encode())
                vizinhoSock.close()

            except ConnectionRefusedError:
                print(f"o no {vizinhoID} nao esta disponivel")
            except Exception as e:
                print(f"erro ao enviar arquivo do no {nodeID} para o no {vizinhoID}")
    
    

# Criação dos 5 nós
nodes = []
for nodeID in tabelaRot.keys():
    nodeBase = node(nodeID)
    nodes.append(nodeBase)

# teste para a conexão dos nós
for realnode in nodes:
    realnode.send_hi()

# teste para o funcionamento da função PUT *não funcionando*
nodes[0].put(2)

# verificar criação das hash tables
"""for realnode in nodes:
    print(realnode.hashTable)"""

# para o problema com as threads
for thread in threads:
    thread.join()
