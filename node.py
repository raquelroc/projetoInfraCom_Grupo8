#nos enviam e recebem mensagens dos vizinhos respectivos infinitamente ou ate que algum err aconteca
#implementar envio de arquivos
#implementar uso de ips diferentes(?)

import socket
import threading
import time

#tabela de roteamento
#cada no tem 2 vizinhos, com portas diferentes
#e o endereco ip do localhost
tabelaRot = {
    1: {"vizinhos": [5, 2], "host": "127.0.0.1", "port": 1231},
    2: {"vizinhos": [1, 3], "host": "127.0.0.1", "port": 1232},
    3: {"vizinhos": [2, 4], "host": "127.0.0.1", "port": 1233},
    4: {"vizinhos": [3, 5], "host": "127.0.0.1", "port": 1234},
    5: {"vizinhos": [4, 1], "host": "127.0.0.1", "port": 1235}
}

#mensagens enviadas a cada 3s com espaço de 1s pro
#no que recebeu a mensagem ficar pronto pra receber outra
def node(nodeID):
    # Inicializa o socket do nó
    nodeInfo = tabelaRot[nodeID]
    host, port = nodeInfo["host"], nodeInfo["port"]
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind((host, port))
    sock.listen(5)
    print(f"No {nodeID} escutando em {host}: {port}")

    def listen():
        while True:
            nodeSock, nodeAddr = sock.accept()
            #teste pra ver se a mensagem tava sendo enviada
            arquivo = nodeSock.recv(1024).decode()
            print(f"o no {nodeID} recebeu um arquivo do endereco {nodeAddr}: {arquivo}")
            nodeSock.close()

    def sendFile():
        while True:
            time.sleep(3)
            for vizinhoID in nodeInfo["vizinhos"]:
                vizinhoInfo = tabelaRot[vizinhoID]
                try:
                    vizinhoSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    vizinhoSock.connect((vizinhoInfo["host"], vizinhoInfo["port"]))
                    #teste so pra saber qual no tava enviando
                    arquivo = f"nome arquivo enviado por {nodeID}"
                    vizinhoSock.send(arquivo.encode())
                    vizinhoSock.close()
                    print(f"o no {nodeID} enviou um arquivo para o no {vizinhoID}: {arquivo}")
                except ConnectionRefusedError:
                    print(f"o no {vizinhoID} nao esta disponivel para envio de arquivos no momento")
                except Exception as e:
                    print(f"erro ao enviar arquivo do no {nodeID} para o no {vizinhoID}")

    threading.Thread(target=listen).start()
    time.sleep(1)
    threading.Thread(target=sendFile).start()

#inicializacao dos nos
for nodeID in tabelaRot.keys():
    threading.Thread(target=node, args=(nodeID,)).start()
