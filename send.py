import socket
import threading
import os
import time

# Função para iniciar o servidor
def start_server(node_id, node_port, file_path):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(('localhost', node_port))
    server_socket.listen(5)
    
    print(f"Nó {node_id} esperando conexões na porta {node_port}...")
    
    while True:
        client_socket, client_address = server_socket.accept()
        print(f"Conexão recebida de {client_address}")
        
        with open(file_path, 'rb') as file:
            data = file.read()
            client_socket.sendall(data)
        
        client_socket.close()
        print(f"Arquivo enviado para {client_address}")

# Função para solicitar arquivo de outro nó
def request_file(server_node_id, server_node_port, save_path):
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect(('localhost', 8001))
    
    with open(save_path, 'wb') as file:
        while True:
            data = client_socket.recv(1024)
            if not data:
                break
            file.write(data)
    
    client_socket.close()
    print(f"Arquivo recebido de Nó {server_node_id} e salvo em {save_path}")

# Função para um nó atuar como servidor e cliente
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

if __name__ == "__main__":
    # Exemplo de configuração para o Nó 1
    node_id = 1
    node_port = 8001
    file_path = 'Nó1/arquivo.txt'
    neighbors = [(2, 8002), (3, 8003), (4, 8004), (5, 8005)]

    os.makedirs(f"Nó{node_id}", exist_ok=True)
    node_operation(node_id, node_port, file_path, neighbors)