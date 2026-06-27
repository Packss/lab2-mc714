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
        self.lamport_clock = 0

        # coordenador com base no proprio id e id peers
        self.coordenador = max(list(peers.keys()) + [id_node])
        self.eleicao_ativa = False
        self.recebeu_resposta = False

        # exclusão mutua
        self.estado = "RELEASED"
        self.relogio_pedido = 0
        self.respostas_necessarias = 0
        self.respostas_adiadas = []

    def log(self, mensagem):
        print(f"[Nó {self.id_node}] [Relogio: {self.lamport_clock}] {mensagem}")

    def incrementa_relogio(self):
        with self.lock:
            self.lamport_clock += 1
        self.log("Relogio atualizado")

    def incia_servidor(self):
        servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        servidor.bind(('0.0.0.0', self.porta))
        servidor.listen(5)
        self.log(f"Servidor escutando na porta {self.porta}")

        while True:
            conn, _ = servidor.accept()
            # tava travando o servidor então resolvi usar thread
            threading.Thread(target=self.prepara_cliente, args=(conn,), daemon=True).start()

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

        # evento então incrementamos antes de enviar
        with self.lock:
            self.lamport_clock += 1
            relogio_atual = self.lamport_clock

        msg = {
            "sender": self.id_node,
            "type": tipo_msg,
            "clock": relogio_atual,
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
        relogio_recebido = msg["clock"]

        self.incrementa_relogio()

        self.log(f"Mensagem recebida do nó {sender} | Tipo: {tipo_msg} | Conteúdo: {conteudo}")

        if tipo_msg == "BULLY_ELECTION":
            if self.id_node > sender:
                self.manda_mensagem(sender, "BULLY_ANSWER")
                if not self.eleicao_ativa:
                    threading.Thread(target=self.comeca_eleicao, daemon=True).start()

        elif tipo_msg == "BULLY_ANSWER":
            self.recebeu_resposta = True

        elif tipo_msg == "BULLY_COORDINATOR":
            self.coordenador = sender
            self.eleicao_ativa = False
            self.log(f"BULLY: Novo lider definido: nó {self.coordenador}")

        elif tipo_msg == "RA_REQUEST":
            relogio_req = conteudo["req_clock"]

            with self.lock:
                nossa_prioridade = (self.estado == "HELD" or (self.estado == "WANTED" and (
                    self.relogio_recebido < relogio_req or (self.relogio_recebido == relogio_req and self.id_node < sender))))

            if nossa_prioridade:
                self.log(f"Exclusão mutua: Adiou resposta para nó {sender} (Estou usando ou quero usar com mais prioridade")
                with self.lock:
                    self.respostas_adiadas.append(sender)
            else:
                self.log(f"Exclusão mutua: Enviando OK para o nó {sender}")
                self.manda_mensagem(sender, "RA_REPLY")

        elif tipo_msg == "RA_REPLY":
            gatilho = False
            with self.lock:
                self.respostas_necessarias -= 1
                self.log(f"Exclusão mutua: Recebemos OK. Restam {self.respostas_necessarias}")
                if self.respostas_necessarias == 0 and self.estado == "WANTED":
                    gatilho = True
            if gatilho:
                threading.Thread(target=self.entrar_secao_critica, daemon=True).start()

    def comeca_eleicao(self):
        self.log("BULLY: Iniciando eleição")
        self.eleicao_ativa = True
        self.recebeu_resposta = False

        peers_maiores = [id for id in self.peers.keys() if id > self.id_node]
        if not peers_maiores:
            self.vira_coordenador()
            return

        for id in peers_maiores:
            self.log(f"BULLY: Enviando ELECTION para o nó {id}")
            self.manda_mensagem(id, "BULLY_ELECTION")

        time.sleep(2)

        if not self.recebeu_resposta and self.eleicao_ativa:
            self.vira_coordenador()

    def vira_coordenador(self):
        self.coordenador = self.id_node
        self.eleicao_ativa = False
        self.log("BULLY: Venci a eleição, notificando outros nós")
        for id in self.peers.keys():
            self.manda_mensagem(id, "BULLY_COORDINATOR")

    def pedir_secao_critica(self):
        if self.estado != "RELEASED":
            self.log("Exclusão mutua: Requisição ignorada. Já na fila ou usando o recurso")
            return

        self.estado = "WANTED"
        self.relogio_pedido = self.lamport_clock
        self.respostas_necessarias = len(self.peers)

        self.log(f"Exclusão mutua: Requisitando região crítica com relogio [{self.relogio_pedido}]")
        if self.respostas_necessarias == 0:
            self.entrar_secao_critica()
            return

        for id in self.peers.keys():
            if not self.manda_mensagem(id, "RA_REQUEST", {"req_clock": self.relogio_pedido}):
                self.log(f"Exclusão mutua: Nó {id} parece offline. Desconsiderando resposta necessaria")
                with self.lock:
                    self.respostas_necessarias -= 1
                    if self.respostas_necessarias == 0 and self.estado == "WANTED":
                        self.entrar_secao_critica()

    def entrar_secao_critica(self):
        with self.lock:
            self.estado = "HELD"
        self.log("Entramos na região crítica (acessando recurso compartilhado)")

        # simulando o uso de um recurso
        time.sleep(5)

        self.log("Saindo da região crítica")
        with self.lock:
            self.estado = "RELEASED"
            respostas_a_enviar = list(self.respostas_adiadas)
            self.respostas_adiadas = []

        for id in respostas_a_enviar:
            self.log(f"Exclusão mutua: Liberando nó {id} que estava aguardando")
            self.manda_mensagem(id, "RA_REPLY")


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
        print(f"Comandos interativos (Nó: {node.id_node} | Lider: {node.coordenador}")
        print("[1] Gerar evento local")
        print("[2] Enviar ping (sincroniza lamport)")
        print("[3] Forçar eleição")
        print("[4] Requisitar região crítica (Exclusão mutua)")
        print("[5] Mandar uma mensagem de texto")
        print("[6] Sair")
        comando = input("Selecione uma opção: ")

        if comando == "1":
            node.incrementa_relogio()
            node.log("Evento interno gerado")
        elif comando == "2":
            try:
                alvo = int(input("ID do nó destino: "))
                node.manda_mensagem(alvo, "PING")
            except ValueError: pass
        elif comando == "3":
            node.comeca_eleicao()
        elif comando == "4":
            threading.Thread(target=node.pedir_secao_critica, daemon=True).start()
        elif comando == "5":
            try:
                alvo = int(input("ID do nó destino: "))
                texto = input("Digite a mensagem a enviar: ")
                node.manda_mensagem(alvo, "TESTE", {"texto": texto})
            except ValueError:
                print("Valores invalidos")
        elif comando == "6":
            print("Desconectando nó")
            break
