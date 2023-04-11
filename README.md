# tstorage-rs

Embedded time-series database in Rust.

## About

This project is modeled after [nakabonne/tstorage](github.com/nakabonne/tstorage). Most of the implementation ideas were taken from  that project, as well as their well-written blog post [Write a time series database from scratch](https://nakabonne.dev/posts/write-tsdb-from-scratch).

I built this because I wanted to understand more about database implementation and to improve my knowledge of Rust. This project has not been tested in a production setting.

## Usage

The following is an example of how to insert and select data from the database:

```rust
fn main() {
    let mut storage = Storage::new(Config {
        partition: PartitionConfig {
            duration: 100,
            hot_partitions: 2,
            max_partitions: 2,
        },
        ..Default::default()
    }).unwrap();

    storage.insert(&Row {
        metric: "metric1",
        data_point: DataPoint {
            timestamp: 1600000000,
            value: 0.1,
        },
    }).unwrap();

    let points = storage.select("metric1", 1600000000, 1600000001).unwrap();
    for p in points {
        println!("timestamp: {}, value: {}", p.timestamp, p.value);
        // => timestamp: 1600000000, value: 0.1
    }
}
```

## Configuration

The database stores points in "partitions," which are time-based chunks responsible for all data points within a specific time range. Partitions can be "hot" (in memory) or "cold" (on disk). When flushed to disk, all points in a partition are stored in the same file.

The main settings that can be configured in the database are the partition size, the maximum number of parititons, and the number of partitions to keep in hot storage. When the number of hot partitions is equal to the maximum number of partitions, the database will operate completely in memory. Otherwise, partitions will be flushed to disk in the given data path and memory mapped according to the specified encoding strategy.

## TODO

- ~~Basic interface~~
- ~~Store in-order data points in memory~~
- ~~Support basic querying~~
- ~~Support in-memory partitions~~
- ~~Support out-of-order data points~~
- ~~Support benchmarks~~
- ~~Support disk partitions~~
- ~~Support Gorilla compression in disk partitions~~
- Support WAL
- Support metric labels
- Support timestamp precision

## Implementation notes

### Design decisions

#### Out of order writes

Out of order data points is a very realistic scenario due to network latency, application latency, or other factors, so it's unreasonable to expect every single data point to be strictly ordered. When supporting out of order writes, one option is to allow them to be inserted (and immediately queryable) by inserting them in sorted order into the data point list. This can slow insert performance as out of order insertion is linear but it allows the out of order points to be immediately queryable. Additionally, the performance hit can be capped with an insert window (detailed below). Another option is to store the out of order points in a separate list to maintain constant time insert performance, but then sort and merge before flushing to a disk partition.

The second option is more performant, however time series data is most typically needed for recent, near real-time values, rather than historical data. Waiting until the data points flush to disk to be queryable might mean they're not available for an hour or more. As a result, sacrificing performance for data points to be immediately queryable seems to be the most appropriate option.

#### Global insert window vs per-metric insert window

The insert window serves to allow data points to be written out of order for some period of time, capping the limit to protect insert performance. For example, inserting at the beginning of a large memory partition would be linear performance and could slow down the system. The insert window puts a cap on this window so that delayed data points can still be written and immediately queried.

The main question is whether the insert window should be globally applied, or applied per-metric. For example, say I have a memory partition with two metrics and the following data point timestamps:

```
metricA | 2 | 3 | 4 | 5
metricB | 2
```

I may not want to allow a data point with timestamp=1 to be inserted for metricA, however it may not be as much of a performance problem to insert it for metricB. Supporting the insert window per metric could allow for more data points to be inserted out of order. However, this could mean two data points with the same timestamps could be sent to the database, with only one of them being written. This makes the inserts harder to reason about. In addition, we'd have to remove the partition boundary optimization and look into each metric entry to determine whether a data point could be inserted. A global limit seems like the easiest path forward even though it is stricter.

### Drawbacks

#### Point far into the future

If rstorage receives an anomalous data point far into the future, it'll create a new partition for it, and will make the old partitions unwritable, depending on the number of writable partitions. Ideally th system would provide an option to reject data points more than some time in the future, just like there's an insert window for old data points.
