# PythonScriptRouter

This router interprets arbitrary Python code.
A python function is called and gets the query and headers passed, which  in turn needs to return an optional string.

It enables arbitrary rules to be executed and performs reasonably well (have a look at tracing statistics for performance numbers).

The signature of the called function needs to look like:

```python
def targetClusterGroup(query: str, headers: dict[str, str]) -> Optional[str]:
```

## Configuration

Enable the router as follows:

```yaml
routers:
  - pythonScript:
      script: |
        # Tested using Python 3.11
        from typing import Optional

        def targetClusterGroup(query: str, headers: dict[str, str]) -> Optional[str]:
          return headers.get('x-trino-client-tags') # Will return None in case the header is not set
```

## Route Airflow queries based on client tags

The following script puts all queries scheduled via Airflow in the group `etl` or `etl-special` depended on the client tag `label=special` being present.

```python
from typing import Optional

def targetClusterGroup(query: str, headers: dict[str, str]) -> Optional[str]:
    # If query from airflow, route to etl group
    if headers.get('x-trino-source') == 'airflow':
        # If query from airflow with special label, route to etl-special group
        if 'x-trino-client-tags' in headers and 'label=special' in headers.get('x-trino-client-tags'):
            return 'etl-special'
        else:
            return 'etl'
```

This is equivalent to the following [getting-started rules from trino-gateway](https://github.com/trinodb/trino-gateway/blob/main/docs/routing-rules.md#defining-your-routing-rules):

```
---
name: "airflow"
description: "if query from airflow, route to etl group"
condition: 'request.getHeader("X-Trino-Source") == "airflow"'
actions:
  - 'result.put("routingGroup", "etl")'
---
name: "airflow special"
description: "if query from airflow with special label, route to etl-special group"
condition: 'request.getHeader("X-Trino-Source") == "airflow" && request.getHeader("X-Trino-Client-Tags") contains "label=special"'
actions:
  - 'result.put("routingGroup", "etl-special")'
  ```

## Defining custom functions

As you can pass a arbitrary Python scripts you can define custom helper functions

```python
from typing import Optional

def targetClusterGroup(query: str, headers: dict[str, str]) -> Optional[str]:
    client_tags = get_client_tags(headers)

    if get_source(headers) == "airflow":
        if client_tags.get("label") == "foo":
            return 'etl-foo'
        elif client_tags.get("label") == "bar":
            return 'etl-bar'
        else:
            return 'etl'

def get_source(headers: dict[str, str]) -> Optional[str]:
    return headers.get("x-trino-source")

def get_client_tags(headers: dict[str, str]) -> dict[str, str]:
    tags = {}
    header_value = headers.get("x-trino-client-tags")
    if header_value is not None:
        for pair in header_value.split(","):
            pair = pair.split("=", 1)
            tags[pair[0]] = pair[1]

    return tags
```

## Matching with regex

You can also pull in additional packages as you would do in normal Python scripts.
The following example uses the `re` package to use an regex expression to detect table compactions:

```python
from typing import Optional
import re

def targetClusterGroup(query: str, headers: dict[str, str]) -> Optional[str]:
    # Compactions have to run on "l" clusters
    if re.search("^alter table .* execute optimize", query.lower()):
        return "l"
```
