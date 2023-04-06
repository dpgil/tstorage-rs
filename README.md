# rstorage

Local time series storage in Rust. Modeled after nakabonne/tstorage.

## TODO

- ~~Basic interface~~
- ~~Store in-order data points in memory~~
- ~~Support basic querying~~
- ~~Support in-memory partitions~~
- ~~Support out-of-order data points~~
- Support WAL
- ~~Support benchmarks~~
- Support disk partitions
- Support Gorilla compression in disk partitions

## Design decisions

### Out of order writes

Out of order data points is a very realistic scenario due to network latency, application latency, or other factors, so it's unreasonable to expect every single data point to be strictly ordered. When supporting out of order writes, one option is to allow them to be inserted (and immediately queryable) by inserting them in sorted order into the data point list. This can slow insert performance as out of order insertion is linear but it allows the out of order points to be immediately queryable. Additionally, the performance hit can be capped with an insert window (detailed below). Another option is to store the out of order points in a separate list to maintain constant time insert performance, but then sort and merge before flushing to a disk partition.

The second option is more performant, however time series data is most typically needed for recent, near real-time values, rather than historical data. Waiting until the data points flush to disk to be queryable might mean they're not available for an hour or more. As a result, sacrificing performance for data points to be immediately queryable seems to be the most appropriate option. I'll add benchmarks to see exactly how bad the performance hit is.

### Global insert window vs per-metric insert window

The insert window serves to allow data points to be written out of order for some period of time, capping the limit to protect insert performance. For example, inserting at the beginning of a large memory partition would be linear performance and could slow down the system. The insert window puts a cap on this window so that delayed data points can still be written and immediately queried.

The main question is whether the insert window should be globally applied, or applied per-metric. For example, say I have a memory partition with two metrics and the following data point timestamps:

```
metricA | 2 | 3 | 4 | 5
metricB | 2
```

I may not want to allow a data point with timestamp=1 to be inserted for metricA, however it may not be as much of a performance problem to insert it for metricB. Supporting the insert window per metric could allow for more data points to be inserted out of order. However, this could mean two data points with the same timestamps could be sent to the database, with only one of them being written. This makes the inserts harder to reason about. In addition, we'd have to remove the partition boundary optimization and look into each metric entry to determine whether a data point could be inserted. A global limit seems like the easiest path forward even though it is stricter.

## Drawbacks

### Point far into the future

If rstorage receives an anomalous data point far into the future, it'll create a new partition for it, and will make the old partitions unwritable, depending on the number of writable partitions. Ideally th system would provide an option to reject data points more than some time in the future, just like there's an insert window for old data points.
