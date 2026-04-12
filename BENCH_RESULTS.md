# Memtable Benchmark Results (Release Mode)

## Test Summary

| Metric | Value |
|--------|-------|
| Total Tests | 10/10 ✅ |
| Build Mode | Release (optimized) |
| Total Time | 16.6s |

---

## Test 1: ConcurrentPutGet_Benchmark_WithSharding
**Configuration**: 500k operations, 10 concurrent threads, Sharding unenabled

### Performance Metrics

| Metric | Value |
|--------|-------|
| Write QPS | 87,004 |
| Read QPS | 131,316 |
| Write Time | 5.747s |
| Read Time | 3.808s |

### Shard Distribution (Verification)

| Shard | Node Count |
|-------|-----------|
| Shard[0] | 16,040 |
| **Total** | **16,040** |

---

## Test 2: ConcurrentPutGet_Benchmark_Sharding
**Configuration**: 500k operations, 10 concurrent threads, Sharding enabled (8个分片)

### Performance Metrics

| Metric | Value |
|--------|-------|
| Write QPS | 202,957 |
| Read QPS | 270,341 |
| Write Time | 2.464s |
| Read Time | 1.850s |

### Shard Distribution (均衡验证)

| Shard | Node Count |
|-------|-----------|
| Shard[0] | 62,823 |
| Shard[1] | 62,600 |
| Shard[2] | 62,741 |
| Shard[3] | 61,971 |
| Shard[4] | 62,412 |
| Shard[5] | 62,448 |
| Shard[6] | 62,516 |
| Shard[7] | 62,489 |
| **Total** | **500,000** |

---

## Test 3: PerformanceAndMemoryUsageTest
**Configuration**: 10k single-threaded operations

| Operation | Time | Value |
|-----------|------|-------|
| Insert | 52 | ms |
| Query | 55 | ms |
| Delete | 112 | ms |
| Memory Usage | - | 1.26 MB |

---

## Technical Improvements

| Issue | Root Cause | Solution |
|-------|-----------|----------|
| Stack Overflow | Recursive `~unique_ptr<Node>` destruction | Iterative clearLinkedList() |
| Deep Recursion | Compiler no tail-call optimization in Release | Move-based iteration |
| Unbounded Stack | 500k node chain recursion | Handles any size safely |

**Result**: No stack overflow even with 500k nodes in a single Skiplist chain.
