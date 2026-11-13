• defineView(name, keyExtractor, filterFn) (Materialized Slices): Let users declare pre-filtered sub-databases. When a user record is written or updated, the primitive checks filterFn(oldUser) and filterFn(newUser) to automatically add, update, or remove the record from that view's dedicated LMDB sub-database. Reading the view becomes a pure, unfiltered getRange() stream.

• defineCounter(name, groupByFn) & defineReducer(name, groupByFn, reduceFn) (Incremental Aggregates): Provide primitives that maintain running totals, counts, min/max, or HyperLogLog cardinality sketches. By passing both the previous state and new state of a record on every write (delta = newVal - oldVal), aggregates update in O(1) time inside the same LMDB transaction.

• defineBucket(name, timestampFn, interval) (Time-Series Rollups): Automatically route incoming events or user updates into pre-aggregated time windows (e.g., hourly, daily, monthly keys). Queries never scan raw timestamps; they simply read the exact bucket keys needed.

• defineTopK(name, groupByFn, scoreFn, k) (Bounded Sorted Sets): Instead of indexing a billion scores just to find the top 100 leaders per country, maintain a fixed-size sorted array or mini B+ tree capped at K items per group. Every write checks if the user beats the K-th score and updates the tiny list in microseconds.
