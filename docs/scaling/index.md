# Autoscaling

The concept trino-lb takes is that the user needs to provision all Trino clusters beforehand, they will **not** be created on-demand.
The reason is this would make things pretty complicated: Wow should a cluster look like, how many workers, where to request the new cluster, is there even a way to create the clusters programmatically or do they need to be created via a UI, are the clusters created by a different department after raising support tickets?

Instead, to avoid all this complexity and allow for a more easy implementation of new scaling engines, trino-lb takes a different approach:
All Trino clusters are known beforehand and are turned on or off by trino-lb on demand.

Scaling implementation therefore only need to implement functions to turn clusters on or off and functions to determine if a Trino cluster is turned on or ready.
Routing is implemented in a generic fashion by exposing the trait `trino_lb::scaling::ScalerImplementation` (think of like an interface).
Different scaling engines can be implemented using this trait, please feel free to open an issue or pull request!

You can additionally configure the minimum number of clusters per cluster group that should be running per any given time interval.
This allows you to e.g. have a higher minimum number of clusters during work days.
An alternative use-case is to scale up the `etl` cluster group just before 02:00 at night, as a client will submit many queries at this given timestamp and scale down at 03:00 again.

Currently the following autoscalers are implemented:

1. [Stackable](./stackable.md)
