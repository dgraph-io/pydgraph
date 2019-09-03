import datetime
import json

import pydgraph


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
    age: int .
    married: bool .
    loc: geo .
    dob: datetime .
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
            'name': 'Alice',
            'age': 26,
            'married': True,
            'loc': {
                'type': 'Point',
                'coordinates': [1.1, 2],
            },
            'dob': datetime.datetime(1980, 1, 1, 23, 0, 0, 0).isoformat(),
            'friend': [
                {
                    'uid': '_:bob',
                    'name': 'Bob',
                    'age': 24,
                },
                {
                    'uid': '_:charlie',
                    'name': 'Charlie',
                    'age': 29,
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
        print('Created person named "Alice" with uid = {}\n'.format(response.uids['alice']))

        print('All created nodes (map from node names to uids):')
        for uid in response.uids:
            print('{} => {}'.format(uid, response.uids[uid]))
    finally:
        # Clean up. Calling this after txn.commit() is a no-op
        # and hence safe.
        txn.discard()
        print('')


# Deleting a data
def delete_data(client):
    # Create a new transaction.
    txn = client.txn()
    try:
        query1 = """query all($a: string)
{
    all(func: eq(name, $a))
    {
        uid
    }
}"""
        variables1 = {'$a': 'Bob'}
        res1 = client.txn(read_only=True).query(query1, variables=variables1)
        ppl1 = json.loads(res1.json)
        for person in ppl1['all']:
            print('Query to find Uid for Bob:')
            print(query1)
            print('')
            print("Bob's UID:")
            print(person)
            print('')
            print('Bob deleted')
            print('')

        txn.mutate(del_obj= person)
        txn.commit()

    finally:
        txn.discard()


# Query for data.
def query_data(client):
    # Run query.
    query = """query all($a: string)
{
    all(func: eq(name, $a)) {
        uid
        name
        age
        married
        loc
        dob
        friend {
            name
            age
        }
        school {
            name
        }
    }
}"""

    variables = {'$a': 'Alice'}
    res = client.txn(read_only=True).query(query, variables=variables)
    ppl = json.loads(res.json)

    # Print results.
    print('Number of people named "Alice": {}'.format(len(ppl['all'])))
    for person in ppl['all']:
        print('Query for Alice:' +query)
        print('Result:')
        print(person)
        print('')

# Query to check for deleted node
def query_data(client):
    query = """query all($b: string)
{
    all(func: eq(name, $b))
    {
        uid,
        name,
        age
        friend {
            uid,
            name,
            age
        }
        ~friend {
            uid,
            name,
            age
        }
    }
}"""

    variables = {'$b': 'Bob'}
    res = client.txn(read_only=True).query(query, variables=variables)
    ppl = json.loads(res.json)

    print('Number of people named "Bob": {}'.format(len(ppl['all'])))
    for person in ppl['all']:
        print('Query for Bob :\n' + query)
        print('')
        print('Result:')
        print(person)
        print('')


def main():
    client_stub = create_client_stub()
    client = create_client(client_stub)
    drop_all(client)
    set_schema(client)
    create_data(client)
    query_data(client) # query for Alice
    query_data(client) # query for Bob
    delete_data(client) # delete Bob
    query_data(client) # query for Alice
    query_data(client) # query for Bob

    # Close the client stub.
    client_stub.close()


if __name__ == '__main__':
    try:
        main()
        print('\nDONE!')
    except Exception as e:
        print('Error: {}'.format(e))
