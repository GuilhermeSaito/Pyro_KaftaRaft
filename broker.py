# broker.py
import Pyro4
from time import sleep

@Pyro4.expose
class Broker:
    def __init__(self, broker_id, state):
        self.broker_id = broker_id
        self.state = state
        self.log = []
        self.leader = None
        self.members = []

    def set_leader(self, leader_uri):
        """Define a referência do líder no broker."""
        self.leader = Pyro4.Proxy(leader_uri)
        print(f"{self.state} {self.broker_id} registrado ao líder.")

    def register_member(self, member_uri, state):
        """Registra um novo membro no líder."""
        member_proxy = Pyro4.Proxy(member_uri)
        self.members.append((state, member_proxy))
        print(f"{state} registrado no cluster com ID: {member_uri}")

    def publish(self, data):
        """Processo para o líder armazenar dados."""
        if self.state == "Líder":
            self.log.append(data)
            print(f"Líder {self.broker_id} armazenou dados: {data}")

    def get_data(self):
        """Consumidor busca dados do líder."""
        if self.state == "Líder":
            return self.log

def start_broker(broker_id, state):
    broker = Broker(broker_id, state)
    daemon = Pyro4.Daemon()  # Inicia um Pyro daemon
    uri = daemon.register(broker)

    if state == "Líder":
        with Pyro4.locateNS() as ns:
            ns.register("Líder-Epoca1", uri)
        print(f"Líder {broker_id} registrado com URI: {uri}")

    elif state in ["Votante", "Observador"]:
        with Pyro4.locateNS() as ns:
            leader_uri = ns.lookup("Líder-Epoca1")
            broker.set_leader(leader_uri)
            broker.leader.register_member(uri, state)

    daemon.requestLoop()

# Para executar como um script
if __name__ == "__main__":
    import sys
    broker_id = sys.argv[1]
    state = sys.argv[2]
    start_broker(broker_id, state)
