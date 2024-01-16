# ClientTagsRouter

This router routes queries based on client tags send in the `X-Trino-Client-Tags` header.
It supports routing a query based on the presence of one tag from a given list OR on the presence of all tags in the list

## One of a list of tags

Let's imagine you want all queries with the tag `etl`, `etl-airflow` **or** `etc-special` to end up the the cluster group `etl`.

You can achieve this with the following config:

```yaml
routers:
  - clientTags:
      oneOf: ["etl", "etl-airflow", "etl-special"]
      trinoClusterGroup: etl
```

## All of a list of tags

A different scenario is that you want to route all queries that have all the required tags, let's say they need the tag `etl` and `system=foo`, as this system executes very very large queries.

You can achieve this with the following config:

```yaml
routers:
  - clientTags:
      allOf: ["etl", "system=foo"]
      trinoClusterGroup: etl-foo
  - clientTags:
      oneOf: ["etl", "etl-airflow", "etl-special"]
      trinoClusterGroup: etl
```

## More flexible routing

If the `oneOf` and `allOf` do not fulfill your routing needs please have a look at the [PythonScriptRouter](./PythonScriptRouter.md), which allows you to execute arbitrary Python script with the most flexibility.
