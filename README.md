# trino-lb

A highly available load balancer with support for queueing, routing and auto-scaling of multiple [Trino](https://trino.io) clusters.

* [Design](./docs/design.md)
* [Example setups](./docs/example-setups.md)
* [Routing](./docs/routing/index.md)
  * [TrinoRoutingGroupHeaderRouter](./docs/routing/TrinoRoutingGroupHeaderRouter.md)
  * [PythonScriptRouter](./docs/routing/PythonScriptRouter.md)
  * [ExplainCostsRouter](./docs/routing/ExplainCostsRouter.md)
  * [ClientTagsRouter](./docs/routing/ClientTagsRouter.md)
* [Persistence](./docs/persistence/index.md)
  * [In-memory](./docs/persistence/in-memory.md)
  * [Redis](./docs/persistence/redis.md)
  * [Postgres](./docs/persistence/postgres.md)
* [Scaling](./docs/scaling/index.md)
  * [Stackable](./docs/scaling/stackable.md)

## Try it out locally
The easiest way to use trino-lb is by using the available container image.
Make sure you clone this repo and run the following command from the root directory of it.

### Without Trino clusters
In case you don't have any Trino cluster at hand you can start trino-lb without any Trino cluster as follows:

```bash
docker run -p 8080:8080 -v ./example-configs/simple-no-trino.yaml:/etc/trino-lb-config.yaml --rm oci.stackable.tech/stackable/trino-lb:0.1.0
```

This starts trino-lb listening on http://127.0.0.1:8080.

You can submit a test-query using [trino-cli](https://trino.io/docs/current/client/cli.html) by calling the following command and entering `select 42;`:

```bash
java -jar ~/Downloads/trino-cli-435-executable.jar --server http://127.0.0.1:8080
trino> select 42;

Query trino_lb_20240111_194610_ZI6zmb1d, QUEUED_IN_TRINO_LB, 0 nodes, 0 splits
```

As you can see by `QUEUED_IN_TRINO_LB`, the query is queued in trino-lb indefinitely, as no Trino clusters are configured - hence trino-lb has no Trino cluster to hand the query to.

### With Trino clusters

Let's assume your Trino cluster is accessible at `https://127.0.0.1:8443` using the username `admin` and password `admin`.

First check the config file `example-configs/simple-single-trino.yaml` and swap out the hostname and credentials corresponding to your Trino cluster.
Afterwards start trino-lb using the following command.
We are using self-signed certificates for testing purpose here.

```bash
docker run -p 443:8443 -v ./example-configs/simple-single-trino.yaml:/etc/trino-lb-config.yaml -v ./example-configs/self-signed-certs/:/self-signed-certs/ --rm oci.stackable.tech/stackable/trino-lb:0.1.0
```

> [!NOTE]
> We map the trino-lb port 8443 to 443 on our host, as the port 8443 is already taken by Trino. Keep that in mind when accessing trino-lb

We can send queries to trino-lb and it will forward them to the configured Trino cluster.

```bash
java -jar ~/Downloads/trino-cli-435-executable.jar --server https://127.0.0.1:443 --insecure --user admin --password
Password:
trino> select 42;
 _col0
-------
    42
(1 row)
```

While this example seems a bit silly, keep in mind that trino-lb already keeps track of the number of queries running on the underlying Trino cluster and queues every query that would exceed the limit of allowed queries (a single query in this case).
You can test this out by executing multiple long running queries in parallel.
Also this is just the starting point without any routing, load-balancing or autoscaling. Read on the [design guide](./docs/design.md) on what trino-lb can do.

## Example configs
Please have a look at the `example-configs` folder to get an inspiration on what you can configure.
