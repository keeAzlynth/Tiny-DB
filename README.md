# Tiny-DB

一个基于 `C++23` 的 LSM-Tree 存储引擎实现，聚焦写入路径、恢复机制、SSTable 组织和可测性。

[GitHub](https://github.com/keeAzlynth/Tiny-DB) · [DeepWiki](https://deepwiki.com/keeAzlynth/Tiny-DB) · [LICENSE](LICENSE)

## 项目简介

Tiny-DB 以 LSM Tree 为核心，覆盖了从内存表、WAL、SSTable、Manifest 到分层 Compaction 的完整存储路径。仓库同时包含模块化单元测试、基准测试入口和结果图表，适合作为存储引擎学习、实验和工程迭代的基础代码。

## 核心能力

- `MemTable + Skiplist` 写入路径，当前采用 8 分片设计
- `WAL` 持久化与恢复，包含 CRC32C 校验和批量写入路径
- `SSTable / Block / BlockMeta / BloomFilter` 读路径实现
- `Manifest` 维护 live SST 元数据
- `BlockCache` 缓存层与多层迭代器合并
- `LSM` 对外提供 `put`、`get`、`put_batch`、`get_batch`、`remove`、`remove_batch`、范围/前缀查询接口
- 提供独立测试目标和 benchmark 入口，便于定位模块问题

## 架构概览

| 模块 | 说明 |
| --- | --- |
| `MemTable` / `Skiplist` | 内存写入与查询路径，支持分片与冻结刷盘 |
| `WAL` | 负责写前日志、校验、恢复与批量持久化 |
| `SSTable` | 磁盘有序表，包含数据块、索引元数据和布隆过滤器 |
| `Manifest` | 记录 SST 生命周期，支持恢复 live-set |
| `BlockCache` | 基于 LRU-K 思路缓存热点 Block |
| `Iterator` | 为范围扫描、合并读和 Compaction 提供统一迭代接口 |
| `Compaction` | 负责多层 SST 合并、清理 tombstone 与空间回收 |
| `Transaction` | 事务 ID 参与可见性控制与恢复过程 |

## 仓库结构

```text
.
├── include/                 # 公共头文件
├── src/                     # 核心实现
│   ├── core/                # MemTable / Skiplist / Record / Global
│   ├── storage/             # SSTable / Block / BlockMeta / WAL / Cache
│   ├── iterator/            # 各类迭代器
│   ├── compaction/          # Manifest 与分层压缩
│   └── transaction/         # 事务相关逻辑
├── test/                    # 模块化单元测试
├── tools/                   # 报告生成与调试工具
├── bench_output/            # 基准测试图表与报告
├── thirdparty/FlameGraph/   # 性能分析工具
├── main.cpp                 # Benchmark 入口
└── BENCH_RESULTS.md         # 现有测试结果记录
```

## 环境与依赖

### 推荐环境

- 操作系统：Linux x86-64（推荐 Ubuntu 24.04+）
- 编译器：GCC 14.2+ 或 Clang 18+
- CMake：`4.0+`
- C++ 标准：`C++23`

### 构建依赖

- `spdlog`
- `fmt`
- `GTest`
- `pthread`

说明：

- 顶层 `CMakeLists.txt` 当前通过 `find_package(spdlog REQUIRED)`、`find_package(GTest REQUIRED)` 查找依赖。
- 多个测试子工程直接链接 `fmt::fmt`，因此建议将 `fmt` 作为显式依赖安装。
- 冷读 benchmark 依赖 Linux 的 `/proc/sys/vm/drop_caches`；非 root 运行时只能得到近似结果。

### 依赖安装

Ubuntu / Debian：

```bash
sudo apt-get update
sudo apt-get install -y build-essential cmake ninja-build libspdlog-dev libfmt-dev libgtest-dev
```

macOS（如需自行尝试编译）：

```bash
brew install cmake ninja spdlog fmt googletest
```

## 构建

### 克隆仓库

```bash
git clone https://github.com/keeAzlynth/Tiny-DB.git
cd Tiny-DB
```

### 默认构建

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
```

默认会生成：

- `lsm_lib`：核心静态库
- `lsm_main`：基于 `main.cpp` 的 benchmark / test 入口

运行 benchmark：

```bash
./build/lsm_main
```

补充说明：

- `lsm_main` 会执行 `main.cpp` 中的 GoogleTest benchmark 用例。
- benchmark 运行过程中会创建本地测试目录，例如 `./bench_db`，并输出 `benchmark_results.csv`。

## 测试

### 测试开关

顶层工程支持以下开关：

| CMake 选项 | 对应目标 |
| --- | --- |
| `ENABLE_BLOCK_TEST=ON` | `block_test` |
| `ENABLE_BLOCKMETA_TEST=ON` | `blockmeta_test` |
| `ENABLE_MEMTABLE_TEST=ON` | `memtable_test` |
| `ENABLE_SKIPLIST_TEST=ON` | `skiplist_test` |
| `ENABLE_SSTABLE_TEST=ON` | `sstable_test` |
| `ENABLE_LSM_TEST=ON` | `lsm_test` |
| `ENABLE_ALL_TESTS=ON` | 启用全部测试 |

### 构建全部测试

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DENABLE_ALL_TESTS=ON
cmake --build build -j --target block_test blockmeta_test memtable_test skiplist_test sstable_test lsm_test
```

### 运行测试

当前顶层工程没有在根构建目录统一注册 `ctest` 用例，因此推荐直接运行各测试可执行文件：

```bash
./build/test/Block_test/block_test
./build/test/BlockMeta_test/blockmeta_test
./build/test/Memtable_test/memtable_test
./build/test/Skiplist_test/skiplist_test
./build/test/Sstable_test/sstable_test
./build/test/LSM_test/lsm_test
```

补充说明：

- `Block_test`、`Memtable_test`、`Skiplist_test`、`Sstable_test`、`LSM_test` 中提供了 `ENABLE_ASAN`、`ENABLE_TSAN`、`ENABLE_VALGRIND` 等可选项，具体以各自 `test/*/CMakeLists.txt` 为准。
- 如果只想验证某个模块，建议只开启对应的 `ENABLE_*_TEST` 开关，减少编译范围。

## 使用示例

```cpp
#include "LSM.h"
#include <iostream>

int main() {
    LSM db("./data");

    db.put("user:1", "alice");
    db.put_batch({{"user:2", "bob"}, {"user:3", "carol"}});

    if (auto value = db.get("user:1")) {
        std::cout << *value << '\n';
    }

    db.remove("user:3");
    db.flush_all();
}
```

对外接口以 `include/LSM.h` 中的 `LSM` 类为主，适合直接作为嵌入式 KV 存储接口使用。

## Benchmark

以下数据沿用仓库当前 README 中的现有结果，仅调整展示方式，不改动数值。

### 测试环境与数据规模

- 数据量级：800,000 Keys（每个 Value 512 Bytes，逻辑总容量约 403 MB）
- 核心优势：极致的并发读取扩展性、微秒级尾延迟
- 测试环境：Ubuntu 24.04 · GCC 14.04 · 12GB RAM

### 性能亮点

- 极限并发读取：突破 **6.18 Million ops/s**（3 线程并发）
- 单核随机写入：高达 **123.4k ops/s**
- 极低响应延迟：缓存命中状态下 **p50 延迟仅 4.9 μs**

### 1. 核心操作吞吐量（单线程）

| 操作类型 | 吞吐量 (ops/s) |
| --- | ---: |
| 随机写入（已有数据） | 123,467 |
| 热随机读取（缓存） | 94,968 |
| 顺序填充（Bulk Load） | 86,837 |

### 2. 并发读取扩展性（百万级 ops）

| 线程数 | 吞吐量 (ops/s) |
| ---: | ---: |
| 1 | 267,031 |
| 2 | 4,138,560 |
| 3 | **6,184,783** |
| 8 | 5,872,271 |

注：8 线程时因硬件核心数限制或 CPU 缓存颠簸，吞吐量略微回落，但仍稳定在 580 万 ops/s 以上。

### 3. 混合读写负载表现

| 读写比例 (Read : Write) | 吞吐量 (ops/s) | 表现分析 |
| --- | ---: | --- |
| R 95 : W 5 | 85,411 | 绝佳的读密集型表现 |
| R 75 : W 25 | 80,393 | 读写混合依然保持高吞吐 |
| R 50 : W 50 | 69,121 | 读写对半时，引擎调度均衡 |
| R 25 : W 75 | 47,097 | 写密集时受限于后台 Compaction |
| R 10 : W 100 | 117,396 | 纯写场景迅速回升，MemTable 吸收力极强 |

### 4. 尾延迟分析（Latency Percentiles）

| 百分位 (Percentile) | 延迟 (μs) | 状态 |
| --- | ---: | --- |
| p50 | 4.9 | 极速响应 |
| p95 | < 17.1 | 稳定可靠 |
| p99 | 17.1 | 波动极小 |
| p999 | 40.9 | 轻微毛刺 |
| p9999 | 151.2 | 正常长尾现象 |

平均延迟：35.6 μs

### 测试备注

冷读取测试因操作系统权限限制未能完全清空 Page Cache（`drop_caches`）；同时在极限压力测试（边读边写并发）下触发了操作系统的 OOM Killer 操作。这表明引擎的读写上限极高，未来版本将进一步优化内存水位控制与 WAL 校验机制。

### 图表与报告

- [并发扩展性图表](bench_output/bench_concurrency_scaling.png)
- [延迟累积分布图 (CDF)](bench_output/bench_latency_cdf.png)
- [延迟百分位图](bench_output/bench_latency_percentiles.png)
- [综合性能图表](bench_output/benchmark_charts.png)
- [详细基准测试报告](bench_output/benchmark_report.md)
- [额外结果记录](BENCH_RESULTS.md)

## 开发说明

建议在提交前至少完成以下检查：

- 构建核心目标或相关测试目标
- 运行本次修改影响到的测试可执行文件
- 如涉及读写路径或并发行为，优先补充对应模块测试

代码风格约定：

- 使用 4 空格缩进
- 类名使用 `PascalCase`
- 函数名使用 `snake_case`
- 常量使用 `UPPER_CASE`

## 项目链接

- 仓库主页：https://github.com/keeAzlynth/Tiny-DB
- 问题反馈：https://github.com/keeAzlynth/Tiny-DB/issues
- DeepWiki：https://deepwiki.com/keeAzlynth/Tiny-DB
- 邮箱 1：trongoneadam@gmail.com
- 邮箱 2：1976909647@qq.com

## 致谢

- [LevelDB](https://github.com/google/leveldb)
- [RocksDB](https://github.com/facebook/rocksdb)
- [GoogleTest](https://github.com/google/googletest)
- [FlameGraph](https://github.com/brendangregg/FlameGraph)

## 许可证

本项目采用 [MIT License](LICENSE)。
