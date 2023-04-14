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

    storage.close().unwrap();
}
```

See the `examples/` directory for more examples using Storage.

## Architecture

The database stores points in "partitions," which are time-based chunks responsible for all data points within a specific time range. Partitions can be "hot" (in memory, writable) or "cold" (on disk, read-only). When flushed to disk, all points in a partition are stored in the same file.

The database maintains a list of partitions stored in ascending time range. When a data point is attempted to be inserted in the database, the responsible partition is found and the data point is inserted. If the data point is in the future relative to all parititons, a new partition (or multiple new partitions) are created. For example, suppose we have a storage instance that allows for 2 "hot" partitions, illustrated by the following:

┌────────────────┐   ┌────────────────┐   ┌────────────────┐   ┌────────────────┐   ┌────────────────┐
│                │   │                │   │                │   │                │   │                │
│ Disk partition │   │ Disk partition │   │ Disk partition │   │  Mem partition │   │  Mem partition │
│   t=0 to 100   ├──►│  t=100 to 200  ├──►│  t=200 to 300  ├──►│  t=300 to 400  ├──►│  t=400 to 500  │
│                │   │                │   │                │   │                │   │                │
└────────────────┘   └────────────────┘   └────────────────┘   └────────────────┘   └────────────────┘
                                                                        ▲                     ▲
                                                                        │                     │
                                                                        └─────────────────────┤
                                                                                              │
                                                                                           Writable

Data points from t=300 to 500 would be inserted into one of the two memory partitions. An insert window can be set to further restrict tolerance for data points. If a data point were inserted with a timestamp beyond t=500, a new partition would be created to support that data point, and the partition responsible for points t=300 to 400 would no longer be writable and would soon be flushed (on the next `sweep_interval`).

If a new partition were to be inserted with a timestamp t=750, for example, three new partitions would be created: one empty one for t=500 to 600, another empty one for t=600 to 700, and then one for t=700 to 800, where the point would be inserted. This can cause problems if a data point is inserted far into the future, so the database supports setting a limit on how far into the future a point can be inserted.

The asynchronous loop that flushes partitions also handles partition expiry to avoid running out of memory or disk space. 
Originally, partitions were flushed and expired synchronously when new partitions were created, but that ended up slowing insertion since the insert wouldn't complete until the new partition was created, a partition was flushed, and the oldest partition was deleted. Flushing partitions involves writing all data points in a partition to a file on disk which can be CPU intensive.

### Concurrency

An RwLock instead of a Mutex is used to minimize contention on the partition list. When flushing partitions, for example, the flushing process holds a read lock on the partition list and creates all disk partition equivalents. During this time, select operations still work because they only require a read lock as well, and insert operations still work because the system restricts the insert window to "hot" partitions, i.e. partitions that are not being flushed. Once the disk partitions are generated, the write lock is briefly held to swap the references between the memory partitions and the newly flushed disk partitions. This blocks all other operations on the partition list but is a cheap operation so it shouldn't hold up the system.

## Configuration

The main settings that can be configured in the database are the partition size, the maximum number of parititons, and the number of partitions to keep in hot storage. When the number of hot partitions is equal to the maximum number of partitions, the database will operate completely in memory. Otherwise, partitions will be flushed to disk in the given data path and memory mapped according to the specified encoding strategy.

### Encoding

The encoding strategies currently supported are CSV format (for easy debugging) and Gorilla format (double-delta compression). A flushed partition lives in a directory `p-{start_timestamp}-{end_timestamp}` which contains a meta file and a data file. The data file stores the encoded data points, whereas the meta file has information about the metric in the partition, such as the metric names, the number of data points, as well as the seek offsets for each metric's data points in the data file. The `Disk Partition` section in [nakabonne's article](https://nakabonne.dev/posts/write-tsdb-from-scratch/) describes this schema in more detail.

Running the example in `examples/simple.rs`, each partition (100 points) was compressed to 1500 bytes in CSV format, and 49 bytes in Gorilla format.

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

If rstorage receives an anomalous data point far into the future, it'll create a new partition for it, and will make the old partitions unwritable, depending on the number of writable partitions. This means one bad data point far into the future could cause the system to expire all existing data points and reject other data points back at a more reasonable timestamp. For example, given a stream of data points with the following timestamps:
```
0 1 2 99 4 5 6 7
```
and a retention of 10, as soon as the data point at t=99 is received, then 4 5 6 7 will be rejected.

To protect from this scenario, a restriction can be set on how far into the future a data point can be from the most recently written data point. An issue with this, however, is if the system doesn't receive points for a while (for legitimate reasons), and starts receiving points again, the system might think all valid points are too far into the future. For example, say the system receives the following data points:
```
0 1 2 <no metrics for a while> 20 21 22 23 ...
```
With a restriction on future inserts, it's possible every timestamp going forward gets rejected, completely locking up the system.
