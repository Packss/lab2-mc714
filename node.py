import socket
import threading
import json
import time
import sys


class Node:
    def __init__(self, id_node, porta, peers):
        self.id_node = id_node
        self.porta = porta
        self.peers = peers
        self.lock = threading.Lock()

    def log(self, mensagem):
        print(f"[Nó {self.id_node}] {mensagem}")

    def incia_servidor(self):
        servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        servidor.bind(('0.0.0.0', self.porta))
        servidor.listen(5)
        self.log(f"Servidor escutando na porta {self.porta}")

        while True:
            conn, _ = servidor.accept()
            # tava travando o servidor então resolvi usar thread
            threading.Thread(taget=self.prepara_cliente, args=(
                conn,), daemon=True).start()

    def prepara_cliente(self, conn):
        try:
            data = conn.recv(1024).decode('utf-8')
            if data:
                msg = json.loads(data)
                self.recebe_mensagem(msg)
        except Exception as erro:
            self.log(f"Erro ao receber dados: {erro}")
        finally:
            conn.close()

    def manda_mensagem(self, id_peer, tipo_msg, conteudo={}):
        if id_peer not in self.peers:
            self.log(f"Erro: Nó {id_peer} não é um peer conhecido.")
            return False

        msg = {
            "sender": self.id_node,
            "type": tipo_msg,
            "content": conteudo
        }

        try:
            ip, porta = self.peers[id_peer]
            cliente = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            cliente.settimeout(2.0)
            cliente.connect((ip, porta))
            cliente.send(json.dumps(msg).encode('utf-8'))
            cliente.close()
            return True
        except (socket.timeout, ConnectionRefusedError):
            self.log(f"Falha ao conectar com o nó {id_peer}")
            return False

    def recebe_mensagem(self, msg):
        sender = msg["sender"]
        tipo_msg = msg["type"]
        conteudo = msg["content"]

        self.log(f"Mensagem recebida do nó {sender} | Tipo: {tipo_msg} | Conteúdo: {conteudo}")


if __name__ == "__main__":
    my_id = int(sys.argv[1])
    my_port = int(sys.argv[2])

    try:
        peers_dict = json.loads(sys.argv[3])
        peers_dict = {int(k): v for k, v in peers_dict.items()}
    except Exception as erro:
        print(f"Dicionario de peers errado: {erro}")
        sys.exit(1)

    node = Node(my_id, my_port, peers_dict)

    # Para poder passar comandos, iniciamos servidor em outra thread
    threading.Thread(target=node.incia_servidor, daemon=True).start()
    time.sleep(1)

    while True:
        comando = input("\nComandos: [1] Mensagem de texto, [2] Sair\n")
        if comando == "1":
            try:
                alvo = int(input("ID do nó destino: "))
                texto = input("Digite a mensagem a enviar: ")
                node.manda_mensagem(alvo, "TESTE", {"texto": texto})
            except ValueError:
                print("Valores invalidos")
        elif comando == "2":
            print("Desconectando nó")
            break
