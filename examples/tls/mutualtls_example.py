#!/usr/bin/python

import pydgraph
import grpc

def create_client(addr='localhost:9080'):
    # Read certs
    with open('./tls/ca.crt', 'rb') as f:
        root_ca_cert = f.read()
    with open('./tls/client.user.key', 'rb') as f:
        client_cert_key = f.read()
    with open('./tls/client.user.crt', 'rb') as f:
        client_cert = f.read()

    # Connect to Dgraph via gRPC with mutual TLS.
    creds = grpc.ssl_channel_credentials(root_certificates=root_ca_cert,
                                         private_key=client_cert_key,
                                         certificate_chain=client_cert)
    client_stub = pydgraph.DgraphClientStub(addr, credentials=creds)
    return pydgraph.DgraphClient(client_stub)

def main():
    client = create_client('localhost:9080')

    # Drop all
    client.alter(pydgraph.Operation(drop_all=True))

    # Update schema
    schema = '''
name: string @index(exact) .
description: string .
url: string .
'''
    op = pydgraph.Operation(schema=schema)
    client.alter(op)

    # Mutate
    dgraph = {
        "name": "Dgraph",
        "description": "Scalable, Distributed, Low Latency Graph Database",
        "url": "https://dgraph.io"
    }
    txn = client.txn()
    try:
        txn.mutate(set_obj=dgraph)
        txn.commit()
    finally:
        txn.discard()
    
    # Query
    res = client.query('''
query dgraph($name: string) {
  data(func: eq(name, $name)) {
    uid
    name
    description
    url
  }
}
''', variables={"$name": "Dgraph"})
    print(res.json);

if __name__ == '__main__':
    main()
