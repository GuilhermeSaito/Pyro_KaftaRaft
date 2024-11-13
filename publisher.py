# publisher.py
import Pyro4

def publish_data(data):
    with Pyro4.locateNS() as ns:
        leader_uri = ns.lookup("Líder-Epoca1")
        leader = Pyro4.Proxy(leader_uri)
        leader.publish(data)
        print(f"Dados publicados: {data}")

# Para execução como script
if __name__ == "__main__":
    data = input("Insira os dados para publicar: ")
    publish_data(data)
