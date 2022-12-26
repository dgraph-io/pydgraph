import datetime
import json

import pydgraph

# Helper function for parsing dgraph's iso strings
def parse_datetime(s):
    try:
        return datetime.datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ")
    except:
        return s

# json decoder object_hook function
def datetime_hook(obj):
    for k, v in obj.items():
        if isinstance(v, list):
            for i in v:
                parse_datetime(i)
        if isinstance(v, str):
            obj[k] = parse_datetime(v)
    return obj


# Create a client stub.
def create_client_stub():
    return pydgraph.DgraphClientStub('localhost:9080')


# Create a client.
def create_client(client_stub):
    return pydgraph.DgraphClient(client_stub)


# Drop All - discard all data and start from a clean slate.
def drop_all(client):
    return client.alter(pydgraph.Operation(drop_all=True))


# Set schema.
def set_schema(client):
    schema = """
    name: string @index(exact) .
    friend: [uid] @reverse .
    married: bool .
    loc: geo .
    dob: datetime .
    distance: float .

    type Person {
        name
        friend
        married 
        loc
        dob
    }
    """
    return client.alter(pydgraph.Operation(schema=schema))


# Create data using JSON.
def create_data(client):
    # Create a new transaction.
    txn = client.txn()
    try:
        # Create data.
        p = {
            'uid': '_:alice',
            'dgraph.type': 'Person',
            'name': 'Alice',
            'dob': datetime.datetime(1980, 1, 1, 23, 0, 0, 0).isoformat(),
            'distance': 12.3,
            'married': True,
            'married|date': datetime.datetime(2005, 6, 1, 10, 10, 0, 0).isoformat(),
            'loc': {
                'type': 'Point',
                'coordinates': [1.1, 2],
            },
            
            'friend': [
                {
                    'uid': '_:bob',
                    'dgraph.type': 'Person',
                    'name': 'Bob',
                    'distance': 55.4,
                    'dob': datetime.datetime(1962, 2, 3, 23, 0, 0, 0).isoformat()
                }
            ],
            'school': [
                {
                    'name': 'Crown Public School',
                }
            ]
        }

        # Run mutation.
        response = txn.mutate(set_obj=p)

        # Commit transaction.
        txn.commit()

        # Get uid of the outermost object (person named "Alice").
        # response.uids returns a map from blank node names to uids.
        print(f'Created person named "Alice" with uid = {response.uids["alice"]}')

    finally:
        # Clean up. Calling this after txn.commit() is a no-op and hence safe.
        txn.discard()



# Query for data.
def query_alice(client):
    # Run query.
    query = """query all($a: string) {
        all(func: eq(name, $a)) {
            uid
            name
            married @facets
            loc
            dob
            distance
            friend {
                name
                dob
                distance
                married
            }
            school {
                name
            }
        }
    }"""

    variables = {'$a': 'Alice'}
    res = client.txn(read_only=True).query(query, variables=variables)
    ppl = json.loads(res.json, object_hook=datetime_hook)
    alice_dob = ppl['all'][0]['dob']
    alice_marriedsince = ppl['all'][0]['married|date']
    bob_dob = ppl['all'][0]['friend'][0]['dob']

    # Print results.
    print(f'{ppl["all"][0]["name"]} is {datetime.date.today().year - alice_dob.year} years old.')
    print(f'She married on {alice_marriedsince.strftime("%B, %d %Y")}.')
    print(f'Bob was born on a {bob_dob.strftime("%A")}.')


def main():
    client_stub = create_client_stub()
    client = create_client(client_stub)
    drop_all(client)
    set_schema(client)
    create_data(client)
    query_alice(client)  # query for Alice and parse datetime information
    # Close the client stub.
    drop_all(client)
    client_stub.close()


if __name__ == '__main__':
    try:
        main()
        print('DONE!')
    except Exception as e:
        print(f'Error: {e}')
