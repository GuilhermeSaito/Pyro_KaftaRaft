# pyro4-ns
# python broker.py 1 Líder
# python broker.py 2 Votante
# python broker.py 3 Votante
# python broker.py 4 Observador
# python publisher.py
# python consumer.py

import Pyro4
from time import sleep, time
from collections import defaultdict
from threading import Thread, Lock

'''
Tem que colocar 2 logs no lider:
    - Log uncommited
    - Log commited
        - Esse log uncommited eh o log q tem agr
        - O log commited vai receber o dado somente quando o dado for replicado pelo lider e a maioria dos votantes
'''

class Broker:
    def __init__(self, broker_id, state):
        self.broker_id = broker_id
        self.state = state
        self.log = []  # Lista de dados armazenados dos membros
        self.uncommited_log = []
        self.commited_log = []
        self.epoch = 1 
        self.leader = None
        self.members = []
        self.confirmations = defaultdict(set)
        self.quorum_size = 1
        self.maioria_quorum = 0
        self.heartbeats = {}
        self.lock = Lock()

    def set_leader(self, leader_uri):
        """
            Define a referência do líder no broker.
        """
        self.leader = Pyro4.Proxy(leader_uri)
        print(f"{self.state} {self.broker_id} registrado ao líder.")

    @Pyro4.expose
    def register_member(self, member_uri, state, broker_id):
        """
            Registra um novo membro no líder.
        """
        member_proxy = Pyro4.Proxy(member_uri)
        self.members.append((state, member_proxy, broker_id))
        if state == "Votante":
            self.maioria_quorum = len([s for s, _, _ in self.members if s == "Votante"]) // 2 + 1
            self.quorum_size = self.quorum_size + 1
        print(f"{state} registrado no cluster com ID: {member_uri}")

    @Pyro4.expose
    def publish(self, data):
        """
            Processo para o líder armazenar dados e notificar os votantes.
        """
        if self.state == "Líder":
            self.uncommited_log.append(data)
            print(f"Líder {self.broker_id} armazenou dados: {data}")
            self.notify_members()

    def notify_members(self):
        """
            Notifica os votantes sobre novos dados no log.
        """
        for state, member, _ in self.members:
            if state == "Votante":
                try:
                    member.notify(self.epoch)
                except Pyro4.errors.CommunicationError:
                    print(f"Erro ao notificar {state}.")

    @Pyro4.expose
    def fetch_data(self, epoch, offset):
        """
            Responde aos votantes com dados novos ou erro se a solicitação for inconsistente.
        """
        if epoch > self.epoch:
            return {
                "error": True,
                "message": f"Época {epoch} inválida. Maior época: {self.epoch}",
                "max_epoch": self.epoch,
                "max_offset": len(self.uncommited_log)
            }
        elif epoch == self.epoch:
            if offset > len(self.uncommited_log):
                return {
                    "error": True,
                    "message": f"Offset {offset} fora do alcance. Tamanho do log: {len(self.uncommited_log)}",
                    "max_epoch": self.epoch,
                    "max_offset": len(self.uncommited_log)
                }
            return {
                "error": False,
                "data": self.uncommited_log[offset:]
            }
        else:
            return {
                "error": True,
                "message": f"Época {epoch} desatualizada. Maior época: {self.epoch}",
                "max_epoch": self.epoch,
                "max_offset": len(self.uncommited_log)
            }
        
    @Pyro4.expose
    def confirm_entry(self, voter_id, entry_index):
        """
            Recebe confirmação de um votante e verifica o quórum.
        """
        with self.lock:
            self.confirmations[entry_index].add(voter_id)
            print(f"Líder {self.broker_id}: Confirmação recebida de votante {voter_id} para índice {entry_index}")

            # Verifica quórum
            # quorum = len([state for state, _ in self.members if state == "Votante"]) // 2 + 1
            if len(self.confirmations[entry_index]) >= self.maioria_quorum:
                # Move do log não comprometido para o log comprometido
                data_to_commit = self.uncommited_log[entry_index]
                self.commited_log.append(data_to_commit)
                print(f"Líder {self.broker_id}: Entrada {data_to_commit} comprometida e movida para o log comprometido.")
                self.extend_commited_log_member(data_to_commit)
                return True
            return False
    
    @Pyro4.expose
    def extend_commited_log_member(self, data_to_commit):
        """
            Atualiza o log commitado dos membros quando o log commitado do líder for atualizado
        """
        for state, member, _ in self.members:
            if state == "Votante":
                print("Tem um votante!")
                print(self.members)
                try:
                    member.update_data_member(data_to_commit)
                except Pyro4.errors.CommunicationError:
                    print(f"Erro chamar a função para atualizar o log do {state}.")
            
    @Pyro4.expose
    def update_data_member(self, data_to_commit):
        """
            Atualiza o log commitado dos membros quando o log commitado do líder for atualizado
        """
        try:
            self.uncommited_log.extend(data_to_commit)
            print(f"Votante {self.broker_id}: Entrada {data_to_commit} comprometida e movida para o log comprometido")
        except Pyro4.errors.CommunicationError:
            print(f"Erro atualizar o log comprometido do {state}.")

    @Pyro4.expose       # Deu erro colocando como oneway aqui
    def notify(self, epoch):
        """
            Notificação recebida pelos votantes.
        """
        if self.state == "Votante":
            print(f"Votante {self.broker_id} notificado pelo líder.")
            offset = len(self.commited_log)
            response = self.leader.fetch_data(epoch, offset)
            if response.get("error"):
                print(f"Votante {self.broker_id}: Erro ao buscar dados - {response['message']}")
                print(f"Maior época: {response['max_epoch']}, Offset final: {response['max_offset']}")
                self.commited_log = self.commited_log[:response["max_offset"] + 1]
                print(f"Votante {self.broker_id}: Log truncado até o offset {response['max_offset']}")
                self.notify(response["max_epoch"], response["max_offset"])
            else:
                new_data = response["data"]
                self.uncommited_log.extend(new_data)
                print(f"Votante {self.broker_id} atualizou o log nao commitado: {self.uncommited_log}")
                self.leader.confirm_entry(self.broker_id, offset + len(new_data) - 1)

    def send_heartbeat(self):
        if self.state == "Votante":
            while True:
                try:
                    self.leader.receive_heartbeat(self.broker_id)
                except Pyro4.errors.CommunicationError:
                    print(f"Erro ao enviar heartbeat para o líder.")
                sleep(2)

    @Pyro4.expose
    def receive_heartbeat(self, voter_id):
        if self.state == "Líder":
            with self.lock:
                self.heartbeats[voter_id] = time()
            print(f"Líder {self.broker_id}: Heartbeat recebido de {voter_id}.")

    def monitor_heartbeats(self):
        while self.state == "Líder":
            sleep(5)
            current_time = time()
            unavailable_voters = []
            with self.lock:
                for voter_id, last_heartbeat in self.heartbeats.items():
                    if current_time - last_heartbeat > 5:
                        unavailable_voters.append(voter_id)
            if unavailable_voters:
                self.handle_failures(unavailable_voters)
                

    def handle_failures(self, unavailable_voters):
        with self.lock:
            for voter_id in unavailable_voters:
                print(f"Líder {self.broker_id}: Votante {voter_id} indisponível.")
                self.heartbeats.pop(voter_id, None)

                self.members = [element for element in self.members if element[2] != voter_id]

            active_voters = [v for v in self.heartbeats.keys()]
            if len(active_voters) < self.maioria_quorum:
                print(f"Líder {self.broker_id}: Quórum insuficiente. Promovendo observadores.")
                for state, member, broker_id in self.members:
                    if state == "Observador":
                        try:
                            member.promote_to_voter()
                            self.members.remove((state, member, broker_id))
                            self.members.append(("Votante", member, broker_id))
                            self.maioria_quorum = len([s for s, _, _ in self.members if s == "Votante"]) // 2 + 1
                            self.notify_promotion(broker_id)
                            print(f"Líder {self.broker_id}: Observador promovido a votante.")
                            break
                        except Pyro4.errors.CommunicationError:
                            print(f"Erro ao promover observador.")

    def notify_promotion(self, promoted_broker_id):
        """
            Notifica todos os votantes sobre a promoção de um observador.
        """
        for state, member, broker_id in self.members:
            if state == "Votante":
                try:
                    member.response_promotion_leader(promoted_broker_id)
                    print(f"Líder {self.broker_id}: Notificou votante {broker_id} sobre a promoção de {promoted_broker_id}.")
                except Pyro4.errors.CommunicationError:
                    print(f"Erro ao notificar votante {broker_id} sobre a promoção.")

    @Pyro4.expose
    def response_promotion_leader(self, promoted_broker_id):
        """
        Notifica o votante sobre a promoção de um observador.
        """
        print(f"Votante {self.broker_id}: Recebi notificação de que o broker {promoted_broker_id} foi promovido a votante.")


    @Pyro4.expose
    def promote_to_voter(self):
        self.state = "Votante"
        print(f"Broker {self.broker_id} promovido a votante.")
        self.log_sync()
        # Colocar uma thread para ir mandando heartbeat para o lider
        Thread(target=self.send_heartbeat, daemon=True).start()

    def log_sync(self):
        response = self.leader.fetch_data(self.epoch, 0)
        if not response.get("error"):
            self.commited_log = response["data"]
            print(f"Broker {self.broker_id} sincronizou log com o líder.")

    @Pyro4.expose
    def get_data(self):
        """
            Consumidor busca dados do líder.
        """
        if self.state == "Líder":
            return self.commited_log

def start_broker(broker_id, state):
    broker = Broker(broker_id, state)
    daemon = Pyro4.Daemon()  # Inicia um Pyro daemon
    uri = daemon.register(broker)

    if state == "Líder":
        with Pyro4.locateNS() as ns:
            ns.register("Líder-Epoca1", uri)
        print(f"Líder {broker_id} registrado com URI: {uri}")
        # Inicia o monitoramento de heartbeats
        Thread(target=broker.monitor_heartbeats, daemon=True).start()

    elif state in ["Votante", "Observador"]:
        with Pyro4.locateNS() as ns:
            leader_uri = ns.lookup("Líder-Epoca1")
            broker.set_leader(leader_uri)
            broker.leader.register_member(uri, state, broker_id)
        if state == "Votante":
            # Inicia envio de heartbeats
            Thread(target=broker.send_heartbeat, daemon=True).start()

    try:
        daemon.requestLoop()
    except KeyboardInterrupt:
        print(f"\nBroker {broker_id} ({state}) encerrado pelo usuário.")
        daemon.close()
        print("Daemon Pyro encerrado com sucesso.")

# Para execução como script
if __name__ == "__main__":
    import sys
    broker_id = sys.argv[1]
    state = sys.argv[2]
    start_broker(broker_id, state)
