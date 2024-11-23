# pyro4-ns

import Pyro4
from time import sleep
from collections import defaultdict

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

    def set_leader(self, leader_uri):
        """
            Define a referência do líder no broker.
        """
        self.leader = Pyro4.Proxy(leader_uri)
        print(f"{self.state} {self.broker_id} registrado ao líder.")

    @Pyro4.expose
    def register_member(self, member_uri, state):
        """
            Registra um novo membro no líder.
        """
        member_proxy = Pyro4.Proxy(member_uri)
        self.members.append((state, member_proxy))
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
        for state, member in self.members:
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
        self.confirmations[entry_index].add(voter_id)
        print(f"Líder {self.broker_id}: Confirmação recebida de votante {voter_id} para índice {entry_index}")

        # Verifica quórum
        quorum = len([state for state, _ in self.members if state == "Votante"]) // 2 + 1
        if len(self.confirmations[entry_index]) >= quorum:
            # Move do log não comprometido para o log comprometido
            data_to_commit = self.uncommited_log[entry_index]
            self.commited_log.append(data_to_commit)
            print(f"Líder {self.broker_id}: Entrada {data_to_commit} comprometida e movida para o log comprometido.")
            return True
        return False

    @Pyro4.expose       # Deu erro colocando como oneway aqui
    def notify(self, epoch):
        """
            Notificação recebida pelos votantes.
        """
        if self.state == "Votante":
            print(f"Votante {self.broker_id} notificado pelo líder.")
            offset = len(self.log)
            response = self.leader.fetch_data(epoch, offset)
            if response.get("error"):
                print(f"Votante {self.broker_id}: Erro ao buscar dados - {response['message']}")
                print(f"Maior época: {response['max_epoch']}, Offset final: {response['max_offset']}")
                self.log = self.log[:response["max_offset"] + 1]
                print(f"Votante {self.broker_id}: Log truncado até o offset {response['max_offset']}")
                self.notify(response["max_epoch"], response["max_offset"])
            else:
                new_data = response["data"]
                self.log.extend(new_data)
                print(f"Votante {self.broker_id} atualizou o log: {self.log}")
                self.leader.confirm_entry(self.broker_id, offset + len(new_data) - 1)

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

    elif state in ["Votante", "Observador"]:
        with Pyro4.locateNS() as ns:
            leader_uri = ns.lookup("Líder-Epoca1")
            broker.set_leader(leader_uri)
            broker.leader.register_member(uri, state)

    daemon.requestLoop()

# Para execução como script
if __name__ == "__main__":
    import sys
    broker_id = sys.argv[1]
    state = sys.argv[2]
    start_broker(broker_id, state)
