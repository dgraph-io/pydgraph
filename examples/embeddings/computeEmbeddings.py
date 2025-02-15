# !pip install pydgraph pybars3 sentence_transformers mistralai openai
import json
import os
import re
import sys

from mistralai.client import MistralClient
from openai import OpenAI
from pybars import Compiler
from sentence_transformers import SentenceTransformer

import pydgraph

# Example of embeddings.json
# {
# "embeddings" : [
#   {
#     "entityType":"Product",
#     "attribute":"product_embedding",
#     "index":"hnsw(metric: \"euclidean\")",
#     "provider": "huggingface",
#     "model":"sentence-transformers/all-MiniLM-L6-v2",
#     "config" : {
#         "dqlQuery" : "{ title:Product.title }",
#         "template": "{{title}} "
#     },
#     "disabled": false
#   }
# ]
# }
#
#  provider : huggingface or openai or mistral
#  model : model name from the provider doing embeddings

# TODO
# check code for template loops.

compiler = Compiler()
global client  # dgrpah client is a global variable

assert "DGRAPH_GRPC" in os.environ, "DGRAPH_GRPC must be defined"
dgraph_grpc = os.environ["DGRAPH_GRPC"]
if "cloud.dgraph" in dgraph_grpc:
    assert "DGRAPH_ADMIN_KEY" in os.environ, "DGRAPH_ADMIN_KEY must be defined"
    APIAdminKey = os.environ["DGRAPH_ADMIN_KEY"]
else:
    APIAdminKey = None

# TRANSFORMER_API_KEY must be defined in env variables
# client stub for on-prem requires grpc host:port without protocol
# client stub for cloud requires the grpc endpoint of graphql endpoint or base url of the cluster
# to run on a self-hosted env, unset ADMIN_KEY and set DGRAPH_GRPC


def setClient():
    global client
    if APIAdminKey is None:
        client_stub = pydgraph.DgraphClientStub(dgraph_grpc)
    else:
        client_stub = pydgraph.DgraphClientStub.from_cloud(dgraph_grpc, APIAdminKey)
    client = pydgraph.DgraphClient(client_stub)


def clearIndex(predicate):
    print(f"remove index for {predicate}")
    schema = f"{predicate}: float32vector ."
    op = pydgraph.Operation(schema=schema)
    alter = client.alter(op)
    print(alter)


def computeIndex(predicate, index):
    print(f"create index for {predicate} {index}")
    schema = f"{predicate}: float32vector @index({index}) ."
    op = pydgraph.Operation(schema=schema)
    alter = client.alter(op)
    print(alter)


def huggingfaceEmbeddings(model, sentences):
    embeddings = model.encode(sentences)
    return embeddings.tolist()


def computeEmbedding(
    predicate, data, template, provider, modelName, model, llm, dimensions
):
    # data is an array of objects contaiing uid and other predicates
    # create an array of text
    # get the embeddings
    # produce a RDF text
    # data is a list of object having uid and other predicates used in the template

    nquad_list = []
    sentences = [template(e) for e in data]

    if "huggingface" == provider:
        embeddings = huggingfaceEmbeddings(model, sentences)
    elif "openai" == provider:
        if dimensions is not None:
            openaidata = llm.embeddings.create(
                input=sentences,
                model=modelName,
                encoding_format="float",
                dimensions=dimensions,
            )
        else:
            openaidata = llm.embeddings.create(
                input=sentences, model=modelName, encoding_format="float"
            )
        embeddings = [e.embedding for e in openaidata.data]
    elif "mistral" == provider:
        mistraldata = llm.embeddings(model=modelName, input=sentences)
        embeddings = [e.embedding for e in mistraldata.data]

    # embeddings is a list of vectors in the same order as the input data
    try:
        for i in range(0, len(data)):
            uid = data[i]["uid"]
            nquad_list.append(f'<{uid}> <{predicate}> "{embeddings[i]}" .')
    # (prompt="{body[uid]}")
    except Exception:
        print(embeddings)
    return nquad_list


def mutate_rdf(nquads, client):
    ret = {}
    body = "\n".join(nquads)
    if len(nquads) > 0:
        txn = client.txn()
        try:
            res = txn.mutate(set_nquads=body)
            txn.commit()
            ret["nquads"] = (len(nquads),)
            ret["total_ns"] = res.latency.total_ns
        except Exception as inst:
            print(inst)
        finally:
            txn.discard()
    return ret


def buildEmbeddings(embedding_def, only_missing=True, filehandle=sys.stdout):
    global client
    entity = embedding_def["entityType"]
    config = embedding_def["config"]
    provider = embedding_def["provider"]
    modelName = embedding_def["model"]
    dimensions = embedding_def["dimensions"] if "dimensions" in embedding_def else None
    index = embedding_def["index"]

    if "huggingface" == provider:
        model = SentenceTransformer(modelName)
        llmclient = None
    else:
        model = None
        if "openai" == provider:
            llmclient = OpenAI(
                # This is the default and can be omitted
                api_key=os.environ.get("OPENAI_API_KEY"),
            )
        elif "mistral" == provider:
            assert "MISTRAL_API_KEY" in os.environ, "MISTRAL_API_KEY must be defined"
            llmclient = MistralClient(api_key=os.environ.get("MISTRAL_API_KEY"))

    predicate = f"{embedding_def['entityType']}.{embedding_def['attribute']}"

    total = 0

    template = compiler.compile(config["template"])
    # inject uid in the query
    # querypart = re.sub(r'([a-zA-Z_]+)',rf"\1:{entity}.\1",config['query'])
    querypart = config["dqlQuery"]
    querypart = querypart.replace("{", "{ uid ", 1)
    print(querypart)
    # remove index by updating DQL schema
    clearIndex(predicate)
    print(
        f"compute embeddings for {predicate} using  model {modelName} from {provider}"
    )
    if only_missing:
        filter = f"@filter( NOT has({predicate}))"
    else:
        filter = ""
    # Run query.
    after = ""
    while True:
        print(".")
        txn = client.txn(read_only=True)
        query = (
            f"{{list(func: type({entity}),first:100 {after}) {filter}  {querypart} }}"
        )
        try:
            res = txn.query(query)
            data = json.loads(res.json)
        except Exception as inst:
            print(type(inst))  # the exception type
            print(inst.args)  # arguments stored in .args
            print(inst)
            break
        finally:
            txn.discard()

        if len(data["list"]) > 0:
            last_uid = data["list"][-1]["uid"]
            after = f",after:{last_uid}"
        else:
            break

        nquads = computeEmbedding(
            predicate,
            data["list"],
            template,
            provider,
            modelName,
            model,
            llmclient,
            dimensions,
        )
        if filehandle is None:
            mutate_rdf(nquads, client)
        else:
            filehandle.write("\n".join(nquads))
        total += len(data["list"])

    computeIndex(predicate, index)
    return total


def replace_env(matchobj):
    key = matchobj.group(1)
    assert key in os.environ, (
        "config file is using a key not defined as environment variable: " + key
    )
    return os.environ.get(key)


if APIAdminKey is None:
    print("using no API key")
    print(dgraph_grpc)
else:
    print("using cloud API key")
    print(dgraph_grpc)
if len(sys.argv) == 2:
    outputfile = sys.argv[1]
    print(f"Produce RDF file in {outputfile}")
else:
    outputfile = None
    print("Mutate embeddings in cluster.")

confirm = input("Continue (y/n)?")

if confirm == "y":
    q = input("Generate only missing embedding (y/n)?")
    only_missing = q == "y"

    re_env = re.compile(r"{{env.(\w*)}}")
    setClient()

    with open("./embeddings.json") as f:
        data = f.read()
        raw = re_env.sub(replace_env, data)
        embeddings = json.loads(raw)

        definitions = embeddings["embeddings"]

        if outputfile is not None:
            out = open(outputfile, "w")
        else:
            out = None
        for embedding_def in definitions:
            total = buildEmbeddings(embedding_def, only_missing, out)
            print(
                f"{total} embeddings for {embedding_def['entityType']}.{embedding_def['attribute']}"
            )
        if out is not None:
            out.close()
