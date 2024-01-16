# In-memory persistence

This persistence stores the data in the RAM of the trino-lb server.
This implies all queued and running queries will be lost when trino-lb is restarted.
Additionally, as multiple trino-lb instances do not share their memory, you can not run multiple trino-lb instances for scalability or availability reasons.

Because of these restrictions the in-memory persistence is only recommended for testing purpose.

## Configuration

The configuration of the in-memory persistence is the most simple, as it does not require any configurations and can be configured as follows:

```yaml
trinoLb:
  persistence:
    inMemory: {}
```
