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
    director.film        : [uid] @reverse @count .
    actor.film           : [uid] @count .
    genre                : [uid] @reverse @count .
    initial_release_date : datetime @index(year) .
    rating               : [uid] @reverse .
    country              : [uid] @reverse .
    loc                  : geo @index(geo) .
    name                 : string @index(hash, term, trigram, fulltext) @lang .
    starring             : [uid] @count .
    performance.character_note : string @lang .
    tagline              : string @lang .
    cut.note             : string @lang .
    rated                : [uid] @reverse .
    email                : string @index(exact) @upsert .
    """
    return client.alter(pydgraph.Operation(schema=schema))

def create_dataTriples(client):
    # Create a new transaction.
    txn = client.txn()
    try:
        data_nquads = """
        <1> <id> "1" .
        <1> <label> "Computer 1" .
        <1> <dgraph.type> "Product" .
        
        <2> <id> "2" .
        <2> <label> "Computer 2" .
        <2> <dgraph.type> "Product" .

        <3> <id> "20" .
        <3> <label> "Dell" .
        <3> <dgraph.type> "AttributeOption" .

        <4> <id> "40" .
        <4> <label> "20GB" .
        <4> <dgraph.type> "AttributeOption" .

        <5> <id> "30" .
        <5> <label> "HP" .
        <5> <dgraph.type> "AttributeOption" .

        <1> <values> <3> .
        <1> <values> <4> .

        <2> <values> <5> .
        <2> <values> <4> .
        """
        # Run mutation.
        response = txn.mutate(set_nquads=data_nquads)

        # Commit transaction.
        txn.commit()
    finally:
        # Clean up. Calling this after txn.commit() is a no-op and hence safe.
        txn.discard()


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
                    'dgraph.type': 'Person',
                    'name': 'Bob',
                    'age': 24,
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
        print('Created person named "Alice" with uid = {}'.format(response.uids['alice']))

    finally:
        # Clean up. Calling this after txn.commit() is a no-op and hence safe.
        txn.discard()



# Deleting a data
def delete_data(client):
    # Create a new transaction.
    txn = client.txn()
    try:
        query1 = """query all($a: string) {
            all(func: eq(name, $a)) {
               uid
            }
        }"""
        variables1 = {'$a': 'Bob'}
        res1 = client.txn(read_only=True).query(query1, variables=variables1)
        ppl1 = json.loads(res1.json)
        for person in ppl1['all']:
            print("Bob's UID: " + person['uid'])
            txn.mutate(del_obj=person)
            print('Bob deleted')
        txn.commit()

    finally:
        txn.discard()


def query_party(client):
    query = """query caro($name1: string){
        q(func: eq(Party, $name1)){
            uid
            Party
        }
    }"""
    variables = {'$name1': "Republican Party"}
    res = client.txn(read_only=True).query(query, variables=variables)
    ppl = json.loads(res.json)
    prettyprint = json.dumps(ppl, indent=2)
    print(prettyprint)

def query_schema(client):
    query = """query{
        schema{}
    }"""
    variables = {'$name1': "Republican Party"}
    res = client.txn(read_only=True).query(query, variables=variables)
    ppl = json.loads(res.json)
    prettyprint = json.dumps(ppl, indent=2)
    print(prettyprint)


#schema {}
def create_slash_stub(url, api_key):
    return pydgraph.SlashGraphQLClient(url, api_key).get_stub()

def main():
    client_stub = create_slash_stub("https://winged-breath.us-west-2.aws.cloud.dgraph.io/graphql","hPJMHG2j8ko58rmUAN2VLxPa7OhmzvIvatNYzsMIIdY=")
    client = create_client(client_stub)
    query_schema(client)



if __name__ == '__main__':
    try:
        main()
        #print('DONE!')
    except Exception as e:
        print('Error: {}'.format(e))