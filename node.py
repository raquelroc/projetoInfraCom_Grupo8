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

def hash_key(key, lower_bound=0, upper_bound=5, decimals=2):

    # Gera um hash em int
    hashInt = int(hashlib.sha1(key.encode()).hexdigest(), 16) 

    # normaliza o valor para o intervalo [0, 1)
    normalized_value = hashInt / float(2**160) 

    # escala o valor para o intervalor [0, 5]
    scaled_value = lower_bound + (upper_bound - lower_bound) * normalized_value

    # arredonda o valor para duas casas decimais
    rounded_value = round(scaled_value, decimals) 

    return rounded_value

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
        self.hashTable = {} # a hashTable do nó possui as chaves do intervalo n-1 até n menos o n 

        # Iniciando o server do nó
        self.serverThread = threading.Thread(target=self.run_server)
        self.serverThread.daemon = True

        threads.append(self.serverThread) # para o problema com as threads

        self.serverThread.start()

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
            self.put(int(arquivo[3]) + int(arquivo[5]) * 0.1 + int(arquivo[6]) * 0.01)
        
        if arquivo[0:2] in "GET":
            self.get(int(arquivo[3]) + int(arquivo[5]) * 0.1 + int(arquivo[6]) * 0.01)
            

        # pacotes normais
        print(f"o no {self.identifier} recebeu um arquivo do endereco {nodeSock.getsockname()}: {arquivo}")
        nodeSock.close()

    # Função para a função PUT
    # - caso o nó atual não seja o desejado ele irá identificar 
    # - qual vizinho está mais perto do intervalo desejado
    def put(self, key):
        vizinhoId = 0

        # caso onde a key esteja no intervalo
        if key < self.identifier and key >= self.nodeInfo["vizinhos"][0]:
            self.hashTable[key] = 'teste'
            print(f"o nó {self.identifier} armazenou o arquivo 'teste' com a chave {key}")
            return

        # caso tratando a conexão 5 - 1
        if self.identifier == 1:
            if key < self.identifier or key > self.nodeInfo["vizinhos"][0]:
                self.hashTable[key] = 'teste'
                print(f"o nó {self.identifier} armazenou o arquivo 'teste' com a chave {key}")
                return

        if key >= self.identifier:
            vizinhoId = self.nodeInfo["vizinhos"][1]

        if key < self.nodeInfo["vizinhos"][0]:
            vizinhoId = self.nodeInfo["vizinhos"][0]

        # enviando o pacote
        try:
            maiorSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            maiorSock.connect((tabelaRot[vizinhoId]["host"], tabelaRot[vizinhoId]["port"]))

            arquivo = f"PUT{key}"

            maiorSock.send(arquivo.encode())
            maiorSock.close()
        except ConnectionRefusedError:
                print(f"o no {vizinhoId} nao esta disponivel")
        except Exception as e:
            print(f"aqui {e}")
            print(f"erro ao enviar arquivo do no {nodeID} para o no {vizinhoId}")

    # Função para a função GET *não esta funcinando*
    # - caso o nó atual não seja o desejado ele irá identificar 
    # - qual vizinho está mais perto do intervalo desejado 
    # PROBLEMA: ainda não foi implementado o retorno da busca 
    # - para o nó que fez a solicitação inicialmente      
    def get(self, key):
        vizinhoId = 0

        # caso onde a key esteja no intervalo
        if key < self.identifier and key >= self.nodeInfo["vizinhos"][0]:
            if key in self.hashTable.keys():
                return self.hashTable[key]
            else:
                return "chave não existente"
            
        # caso tratando a conexão 5 - 1
        if self.identifier == 1:
            if key < self.identifier or key > self.nodeInfo["vizinhos"][0]:
                if key in self.hashTable.keys():
                    return self.hashTable[key]
                else:
                    return "chave não existente"
        
        if key >= self.identifier:
            vizinhoId = self.nodeInfo["vizinhos"][1]

        if key < self.nodeInfo["vizinhos"][0]:  
            vizinhoId = self.nodeInfo["vizinhos"][0]

        try:
            menorSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            menorSock.connect((tabelaRot[vizinhoId]["host"], tabelaRot[vizinhoId]["port"]))

            arquivo = f"GET{key}"

            menorSock.send(arquivo.encode())
            menorSock.close()
        except ConnectionRefusedError:
                print(f"o no {vizinhoId} nao esta disponivel")
        except Exception as e:
            print("e")
            print(f"erro ao enviar arquivo do no {nodeID} para o no {vizinhoId}")

    # Função para testar a conexão entre os vizinhos
    # - cada nó envia um pacote com uma mensagem para seus vizinhos imediatos
    def send_hi(self):
        for vizinhoID in self.nodeInfo["vizinhos"]:
            vizinhoInfo = self.nodeInfo["vizinhos"]
            try:
                vizinhoSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                vizinhoSock.connect((tabelaRot[vizinhoID]["host"], tabelaRot[vizinhoID]["port"]))

                arquivo = f"nome arquivo, enviado por {self.identifier}"

                vizinhoSock.send(arquivo.encode())
                vizinhoSock.close()

            except ConnectionRefusedError:
                print(f"o no {vizinhoID} nao esta disponivel")
            except Exception as e:
                print(e)
                print(f"erro ao enviar arquivo do no {nodeID} para o no {vizinhoID}")
    
    

# Criação dos 5 nós
nodes = []
for nodeID in tabelaRot.keys():
    nodeBase = node(nodeID)
    nodes.append(nodeBase)

# teste para a conexão dos nós
"""for realnode in nodes:
    realnode.send_hi()
"""
# teste para o funcionamento da função PUT
print(hash_key("teste"))
nodes[1].put(hash_key("teste"))

time.sleep(1)

# teste para o funcionamento da função GET
print(nodes[0].hashTable)
print(nodes[1].get(hash_key("teste")))


# verificar criação das hash tables
"""for realnode in nodes:
    print(realnode.hashTable)"""

# para o problema com as threads
for thread in threads:
    thread.join()
