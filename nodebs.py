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
    
    def node_operation(node_id, node_port, file_path, neighbors):
        # Iniciar servidor
        server_thread = threading.Thread(target=start_server, args=(node_id, node_port, file_path))
        server_thread.start()

        # Aguardar servidor iniciar
        time.sleep(2)

        # Solicitar arquivos dos vizinhos
        for neighbor_id, neighbor_port in neighbors:
            save_path = f"Nó{node_id}/arquivo_recebido_de_Nó{neighbor_id}.txt"
            threading.Thread(target=request_file, args=(neighbor_id, neighbor_port, save_path)).start()
    # Funçao para criação do servidor desse nó
    def run_server(self, file_path):
        # Criação do server do nó
        host, port = self.nodeInfo["host"], self.nodeInfo["port"]
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind((host, port))
        sock.listen(5)

        print(f"No {self.identifier} escutando em {host}: {port}")
        
        # recebimento dos pacotes, são tratados no handle_client
        while True:
            nodeSock, nodeAddr = sock.accept()
            print(f"Conexão recebida de {nodeAddr}")

            with open(file_path, 'rb') as file:
                data = file.read()
                nodeSock.sendall(data)
        
            nodeSock.close()
            print(f"Arquivo enviado para {nodeAddr}")
            
            # dentro de nodeSock é possivel ver as informações de host e port que nós escolhemos
            clientThread = threading.Thread(target=self.handle_client, args=(nodeSock,))
            clientThread.daemon = True
            
            threads.append(clientThread) # para o problema com as threads

            clientThread.start()
    
    
    # Função de tratamento dos pacotes recebidos
    def handle_client(self, nodeSock):
        nodeSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        nodeSock.connect(('localhost', 1231))

        try:
            with open(self.hashTable, 'wb') as file:
                while True:
                    arquivo = nodeSock.recv(1024).decode()
                    if not arquivo:
                        break
                    file.write(arquivo)
    
            nodeSock.close()
            print(f"Arquivo recebido de Nó {self.identifier} e salvo em {self.hashTable}")

            # transformando a string recebida em numero 
            requestedKey = 0
            requestedKey += int(arquivo[3])
            if len(arquivo) > 4 : requestedKey += int(arquivo[5]) * 0.1
            if len(arquivo) > 5 : requestedKey += int(arquivo[6]) * 0.01

            # comandos
            # exemplo de comando enviado por pacote "PUT3" -> comando + key
            # caso um pacote chegue com o comando put a função put é chamada
            match arquivo[0:3]:
                case "PUT":
                    self.put(requestedKey, arquivo)

                case "GET":
                    self.get(requestedKey)
        finally:
            # pacotes normais
            print(f"o no {self.identifier} recebeu um arquivo do endereco {nodeSock.getsockname()}: {arquivo}")
            nodeSock.close()

    #Função que manda pacotes responsaveis por comandos para outros nós
    def send_command(self, id, key, command):
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
        if self.identifier == 1 and (key < 1 or key > 5):
            return True
        return limiteInferior <= key < limiteSuperior

    # Função PUT utilizando busca sequencial
    def put(self, key):
        nextNode = self.nodeInfo["vizinhos"][1]
        
        if self.in_interval(key, self.identifier-1, self.identifier-1 + 0.99):
            self.hashTable[key] = 'teste'
            print(f"o nó {self.identifier} armazenou o arquivo 'teste' com a chave {key}")
            return

        self.send_command(nextNode, key, "PUT")

    # Função para a função GET *não esta funcinando*
    # - caso o nó atual não seja o desejado ele irá identificar 
    # - qual vizinho está mais perto do intervalo desejado 
    # PROBLEMA: ainda não foi implementado o retorno da busca 
    # - para o nó que fez a solicitação inicialmente      
    def get(self, key):
        nextNode = self.nodeInfo["vizinhos"][1]

        if self.in_interval(key, self.identifier-1, self.identifier-1 + 0.99):
            if key in self.hashTable.keys():
                return self.hashTable[key]
            else:
                return "chave não existente"
            
        self.send_command(nextNode, key, "GET")
    


# Criação dos 5 nós
nodes = []
for nodeID in tabelaRot.keys():
    nodeBase = node(nodeID)
    nodes.append(nodeBase)

try:
    while True:
        time.sleep(3)
except KeyboardInterrupt:
    stop_event.set()
    print("Finalizando threads...")

# para o problema com as threads
for thread in threads:
    thread.join()

print("aquif")

