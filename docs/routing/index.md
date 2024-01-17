# Routing

Routing is implemented in a generic fashion by exposing the trait `trino_lb::routing::RouterImplementationTrait` (think of like an interface).
Different routing engines can be implemented easily using this trait, please feel free to open an issue or pull request!

Currently the following routers are implemented:

1. [TrinoRoutingGroupHeaderRouter](./TrinoRoutingGroupHeaderRouter.md)
2. [PythonScriptRouter](./PythonScriptRouter.md)
3. [ExplainCostsRouter](./ExplainCostsRouter.md)
4. [ClientTagsRouter](./ClientTagsRouter.md)
