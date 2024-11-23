import Pyro4

def consume_data():
    with Pyro4.locateNS() as ns:
        leader_uri = ns.lookup("Líder-Epoca1")
        leader = Pyro4.Proxy(leader_uri)
        data = leader.get_data()
        print(f"Dados consumidos: {data}")

# Para execução como script
if __name__ == "__main__":
    consume_data()
