# Routing

Routing is implemented in a generic fashion by exposing the trait `trino_lb::routing::RouterImplementationTrait` (think of like an interface).
Different routing engines can be implemented easily using this trait, please feel free to open an issue or pull request!

E.g. a router based on [client tags](https://trino.io/docs/current/develop/client-protocol.html?highlight=client+tag#client-request-headers) would be trivial, but also currently possible via the `PythonScriptRouter`.

Currently the following routers are implemented:

1. TrinoRoutingGroupHeaderRouter
2. [PythonScriptRouter](./PythonScriptRouter.md)
3. ExplainCostsRouter
4. [ClientTagsRouter](./ClientTagsRouter.md)
