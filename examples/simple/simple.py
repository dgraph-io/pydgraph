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
    friend: uid @reverse .
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
                    'name': 'Bob',
                    'age': 24,
                },
                {
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
        assigned = txn.mutate(set_obj=p)

        # Commit transaction.
        txn.commit()

        # Get uid of the outermost object (person named "Alice").
        # assigned.uids returns a map from blank node names to uids.
        # For a json mutation, blank node names "blank-0", "blank-1", ... are used
        # for all the created nodes.
        print('Created person named "Alice" with uid = {}\n'.format(assigned.uids['blank-0']))

        print('All created nodes (map from blank node names to uids):')
        for uid in assigned.uids:
            print('{} => {}'.format(uid, assigned.uids[uid]))
    finally:
        # Clean up. Calling this after txn.commit() is a no-op
        # and hence safe.
        txn.discard()
        print('\n')


#Deleting a data
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
            print('Query to find Uid for Bob :')
            print(query1)
            print('\n')
            print("Bob's UID : ")
            print(person)
            print('\n')
            print('Bob deleted')
            print('\n')


        assigned = txn.mutate(del_obj= person)

        txn.commit()

    finally:
        txn.discard()


# Query for data.
def query_data(client):
    # Run query.
    query = """query all($a: string) {
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
    print('\n')
    for person in ppl['all']:
        print('Query for Alice : \n' +query)
        print('\n')
        print('Result :')
        print(person)
        print('\n')

#Query to check for deleted node
def query_data01(client):
    query01 = """query all($b: string)
        {   all(func: eq(name, $b))
            {   uid,
                name,
                age
                friend
                {
                    uid,
                    name,
                    age
                }
                ~friend
                {
                    uid,
                    name,
                    age
                }
            }
        }"""

    variables01 = {'$b': 'Bob'}
    res01 = client.txn(read_only=True).query(query01, variables=variables01)
    ppl01 = json.loads(res01.json)

    print('Number of people named "Bob": {}'.format(len(ppl01['all'])))
    print('\n')
    for person in ppl01['all']:
        print('Query for Bob :\n' + query01)
        print('\n')
        print('Result :')
        print(person)
        print('\n')


def main():
    client_stub = create_client_stub()
    client = create_client(client_stub)
    drop_all(client)
    set_schema(client)
    create_data(client)
    query_data(client) # query for Alice
    query_data01(client) # query for Bob
    delete_data(client) # delete Bob
    query_data(client) # query for Alice
    query_data01(client) # query for Bob

    # Close the client stub.
    client_stub.close()


if __name__ == '__main__':
    try:
        main()
        print('\nDONE!')
    except Exception as e:
        print('Error: {}'.format(e))
