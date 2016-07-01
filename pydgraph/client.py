from grpc.beta import implementations
import graphresponse_pb2

class Client(object):
    def __init__(self, host, port):
        self.channel = implementations.insecure_channel(host, port)
        self.stub = graphresponse_pb2.beta_create_Dgraph_stub(self.channel)

    def query(self, q, timeout=None):
        request = graphresponse_pb2.Request(query=q)
        return self.stub.Query(request, timeout)

if __name__ == '__main__':
    client = Client('localhost', 8081)
    val = client.query('{me(_xid_: alice) { name _xid_ follows { name _xid_ follows {name _xid_ } } }}')
    print(val)