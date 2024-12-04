import Pyro4
from time import sleep, time
from threading import Thread, Lock
from collections import defaultdict

# Teste

@Pyro4.expose
class Broker:
    def __init__(self, broker_id, state):
        self.broker_id = broker_id
        self.state = state
        self.uncommitted_log = []
        self.committed_log = []
        self.epoch = 1
        self.leader = None
        self.members = []
        self.heartbeats = {}
        self.lock = Lock()
        self.quorum_size = 0
        self.confirmations = defaultdict(set)

    def set_leader(self, leader_uri):
        self.leader = Pyro4.Proxy(leader_uri)
        print(f"{self.state} {self.broker_id} registrado ao líder.")

    def register_member(self, member_uri, state):
        member_proxy = Pyro4.Proxy(member_uri)
        self.members.append((state, member_proxy))
        if state == "Votante":
            self.quorum_size = len([s for s, _ in self.members if s == "Votante"]) // 2 + 1
        print(f"{state} registrado no cluster com ID: {member_uri}")

    def publish(self, data):
        if self.state == "Líder":
            self.uncommitted_log.append(data)
            print(f"Líder {self.broker_id} armazenou no log não comprometido: {data}")
            self.notify_members(len(self.uncommitted_log) - 1)

    def notify_members(self, new_entry_index):
        for state, member in self.members:
            if state == "Votante":
                try:
                    member.notify(self.epoch, new_entry_index)
                except Pyro4.errors.CommunicationError:
                    print(f"Erro ao notificar {state}.")

    def fetch_data(self, epoch, offset):
        if epoch > self.epoch:
            return {
                "error": True,
                "message": f"Época {epoch} inválida. Maior época: {self.epoch}",
                "max_epoch": self.epoch,
                "max_offset": len(self.committed_log) - 1
            }
        elif epoch == self.epoch:
            if offset > len(self.uncommitted_log):
                return {
                    "error": True,
                    "message": f"Offset {offset} fora do alcance. Tamanho do log: {len(self.uncommitted_log)}",
                    "max_epoch": self.epoch,
                    "max_offset": len(self.committed_log) - 1
                }
            return {"error": False, "data": self.uncommitted_log[offset:]}
        else:
            return {
                "error": True,
                "message": f"Época {epoch} desatualizada. Maior época: {self.epoch}",
                "max_epoch": self.epoch,
                "max_offset": len(self.committed_log) - 1
            }

    def confirm_entry(self, voter_id, entry_index):
        self.confirmations[entry_index].add(voter_id)
        print(f"Líder {self.broker_id}: Confirmação recebida de votante {voter_id} para índice {entry_index}")

        if len(self.confirmations[entry_index]) >= self.quorum_size:
            data_to_commit = self.uncommitted_log[entry_index]
            self.committed_log.append(data_to_commit)
            print(f"Líder {self.broker_id}: Entrada {data_to_commit} comprometida e movida para o log comprometido.")
            return True
        return False

    def send_heartbeat(self):
        if self.state == "Votante":
            while True:
                try:
                    self.leader.receive_heartbeat(self.broker_id)
                except Pyro4.errors.CommunicationError:
                    print(f"Erro ao enviar heartbeat para o líder.")
                sleep(2)

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
            active_voters = [v for v in self.heartbeats.keys()]
            if len(active_voters) < self.quorum_size:
                print(f"Líder {self.broker_id}: Quórum insuficiente. Promovendo observadores.")
                for state, member in self.members:
                    if state == "Observador":
                        try:
                            member.promote_to_voter()
                            self.members.remove((state, member))
                            self.members.append(("Votante", member))
                            self.quorum_size = len([s for s, _ in self.members if s == "Votante"]) // 2 + 1
                            print(f"Líder {self.broker_id}: Observador promovido a votante.")
                            break
                        except Pyro4.errors.CommunicationError:
                            print(f"Erro ao promover observador.")

    def promote_to_voter(self):
        self.state = "Votante"
        print(f"Broker {self.broker_id} promovido a votante.")
        self.log_sync()

    def log_sync(self):
        response = self.leader.fetch_data(self.epoch, 0)
        if not response.get("error"):
            self.uncommitted_log = response["data"]
            print(f"Broker {self.broker_id} sincronizou log com o líder.")

def start_broker(broker_id, state):
    broker = Broker(broker_id, state)
    daemon = Pyro4.Daemon()
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
            broker.leader.register_member(uri, state)
        if state == "Votante":
            # Inicia envio de heartbeats
            Thread(target=broker.send_heartbeat, daemon=True).start()

    daemon.requestLoop()


if __name__ == "__main__":
    import sys
    broker_id = sys.argv[1]
    state = sys.argv[2]
    start_broker(broker_id, state)
