## TLS Example Project

Project demonstrating the use of [pydgraph][] and Dgraph set up with client-server
mutual TLS. The following guide shows how to set up a single-group six-node
cluster (3 Dgraph Zero and 3 Dgraph Alpha) configured with mutual TLS.

### Install Dgraph

You will need to [install Dgraph v1.1.0 or
above](https://github.com/dgraph-io/dgraph/releases) and run it.

A quick-start installation script is available for Linux and Mac:

```sh
curl -sSf https://get.dgraph.io | bash
```

### Create TLS certificates

Dgraph provides a `dgraph cert` tool to create and manage self-signed
server and client certificates using a generated Dgraph Root CA. See the [TLS
documentation](https://docs.dgraph.io/deploy/#tls-configuration) for more
information.

Create the root CA. All certificates and keys are created in the `tls` directory.

```sh
dgraph cert
```

Now create the Alpha server certificate (node.crt) and key (node.key) and client
certificate (client.user.crt) key (client.user.key).

```sh
dgraph cert -n localhost
```

```sh
dgraph cert -c user
```

The following files should now be in the `tls` directory:

```sh
$ ls tls
ca.crt  ca.key  client.user.crt  client.user.key  node.crt  node.key
```

Using `dgraph cert ls` provides more details about each file. For instance, it
shows that the `node.crt` is valid only for the host named `localhost` and the
corresponding file permissions.

```sh
$ dgraph cert ls
-rw-r--r-- ca.crt - Dgraph Root CA certificate
    Issuer: Dgraph Labs, Inc.
       S/N: 3dfb9c54929d703b
Expiration: 19 Feb 29 00:57 UTC
  MD5 hash: C82CF5D4C344668E34A61D590D6A4B77

-r-------- ca.key - Dgraph Root CA key
  MD5 hash: C82CF5D4C344668E34A61D590D6A4B77

-rw-r--r-- client.user.crt - Dgraph client certificate: user
    Issuer: Dgraph Labs, Inc.
 CA Verify: PASSED
       S/N: 5991417e75ba14c7
Expiration: 21 Feb 24 01:04 UTC
  MD5 hash: BA35D4ABD8DFF1ED137E8D8E5D921D06

-rw------- client.user.key - Dgraph Client key
  MD5 hash: BA35D4ABD8DFF1ED137E8D8E5D921D06

-rw-r--r-- node.crt - Dgraph Node certificate
    Issuer: Dgraph Labs, Inc.
 CA Verify: PASSED
       S/N: 51d53048b6845d8c
Expiration: 21 Feb 24 01:00 UTC
     Hosts: localhost
  MD5 hash: 5D71F59AAEE294F1CFDA9E3232761018

-rw------- node.key - Dgraph Node key
  MD5 hash: 5D71F59AAEE294F1CFDA9E3232761018
```

### Run Dgraph Cluster
```
docker-compose up
```

### Run TLS Python Example

#### Install Dependencies
```
pip3 install -r requirements.txt
```

#### Run Example Code
```
python3 tls_example.py
```
