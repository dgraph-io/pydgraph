This README provides instructions to run the OilRigDemo python notebook.

This demo requires:
**Dgraph Cloud** if you have a dgraph cloud backend already skip to step __
1. go to cloud.dgraph.io, make an account/sign in
2. Click Launch New Backend
3. Choose Free/Shared/Dedicated
4. After the Backend is launched, on the Overview Page under the name of your backend you will see a link to copy 
the graphQL endpoint. Paste this in the **config-local.json** **dgraph_graphql_endpoint** field as a string
5. Click on Settings. Under the General Tab you will see the grpc endpoint. copy this and paste it 
in the **config-local.json** **dgraph_grpc** field as a string
6. Still under settings, click on API Keys. Create a key and paste it in the **config-local.json** **APIAdminKey** field as a string
7. in the **config-local.json** **dgraph_cerebro** field as a string paste "https://cerebro.cloud.dgraph.io/graphql"

**Qdrant Cloud**
1. go to cloud.qdrant.io, make an account/sign in
2. Create a cluster. Free version is fine
3. this will give you a url and an API key. Paste them in the **config-local.json** **qdrnt_endpoint** and **qdrnt_api_key** fields as a string. Also define the name of your collection in the **qdrnt_collection**. in this case "oilrig" works fine

**PostMan** you can interact with your qdrant cluster as you see fit, however here we suggest PostMan

1. Download Postman
2. Create an environment for Qdrant
3. add variables for qdrant endpoint and qdrant api key and paste in the values you got from the previous Qdrant Cloud Setup
4. We need to be able to create, delete and scroll thru our Qdrant oilrig vector db. Create a Collection and add the following Requests
    1. Add Collection: Use this when we want to create a collection.
        PUT {{qdrant-endpoint}}/collections/oilrig
            click on Headers and add a key called api-key with a value {{qdrant-key}} <---this will pull the qdrant-key from the environment you created
            click on body and add this:
                {
                    "vectors": {
                    "size": 1536,
                    "distance": "Dot"
                    }
                }
    2. Delete Collection: Use this to delete your collection, usually to restart/refresh the demo. Send Add Collection after to bring up a fresh Collection
        DELETE {{qdrant-endpoint}}/collections/oilrig
            click on Headers and add a key called api-key with a value {{qdrant-key}} <---this will pull the qdrant-key from the environment 



**OpenAI API**
go to openai.com, create an account and generate an API key
Paste this in the **config-local.json** **OpenAIKey** field as a string


**Node.js----This is only needed for visualization, not necessary**
https://medium.com/@muesingb/how-to-install-update-node-js-on-macos-using-homebrew-22fc921312c9



