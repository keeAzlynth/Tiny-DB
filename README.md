[TOC]
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/keeAzlynth/Tiny-DB)

# LSM 存储引擎 · Tiny-DB

[![Build Status](https://img.shields.io/badge/build-passing-brightgreen.svg)](https://github.com/your-repo/lsm)
[![C++](https://img.shields.io/badge/C%2B%2B-20-blue.svg)](https://en.cppreference.com/w/cpp/20)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Tests](https://img.shields.io/badge/tests-100%25-success.svg)](https://github.com/your-repo/lsm)

## 概述

本项目是一个基于 C++23 的高性能 LSM（Log-Structured Merge Tree）存储引擎实现。该引擎采用测试驱动开发（TDD）方法，使用 GoogleTest 框架确保代码质量和可靠性。LSM 树是现代 NoSQL 数据库（如 LevelDB、RocksDB）的核心数据结构，特别适用于写密集型应用场景。

### 核心特性

- **高性能写入**：基于 LSM 树的追加写入模式，优化写入性能
- **内存友好**：采用跳表数据结构，提供 O(log n) 的查找效率
- **模块化设计**：清晰的架构分层，便于维护和扩展
- **并发安全**：支持多线程并发操作
- **完整测试**：100% 测试覆盖率，确保系统稳定性


## 环境要求

### 系统要求
- **操作系统**：Linux（推荐 Ubuntu 24.04+）、macOS 10.14+
- **编译器**：GCC 14.2+ 或 Clang 20.0+
- **C++ 标准**：C++23 或更高版本

### 依赖项
- **CMake**：4.0 或更高版本
- **GoogleTest**：1.14.0 或更高版本
- **其他**：pthread（多线程支持）

### 安装依赖

**Ubuntu/Debian：**
```bash
sudo apt-get update
sudo apt-get install -y build-essential cmake libgtest-dev
cd /usr/src/gtest
sudo cmake CMakeLists.txt
sudo make
sudo cp lib/*.a /usr/lib
```

**CentOS/RHEL：**
```bash
sudo yum groupinstall "Development Tools"
sudo yum install cmake gtest-devel
```

**macOS：**
```bash
brew install cmake googletest
```

## 构建与安装

### 快速开始

```bash
# 1. 克隆仓库
git clone https://github.com/your-repo/lsm-engine.git
cd lsm-engine

# 2. 创建构建目录
mkdir build && cd build

# 3. 配置构建
cmake .. -DCMAKE_BUILD_TYPE=Release

# 4. 编译
make -j$(nproc)

# 5. 运行测试
ctest --output-on-failure
```

### 详细构建选项

```bash
# Debug 构建（包含调试信息）
cmake .. -DCMAKE_BUILD_TYPE=Debug

# Release 构建（性能优化）
cmake .. -DCMAKE_BUILD_TYPE=Release

# 启用 ASAN（Address Sanitizer）
cmake .. -DENABLE_ASAN=ON
```

### 运行测试

**运行单元测试：**
```bash
cmake -G Ninja -B build
./build/...test
```

> **测试备注**  
> 冷读取测试因操作系统权限限制未能完全清空 Page Cache (`drop_caches`)；同时在极限压力测试 (边读边写并发) 下触发了操作系统的 OOM Killer 操作。这表明引擎的读写上限极高，未来版本将进一步优化内存水位控制与 WAL 校验机制。

- **测试环境**：Ubuntu 24.04 · GCC 14.04 · 12GB RAM

---

# Tiny-DB 引擎性能基准测试 (Benchmark)

本测试展示了 Tiny-DB 存储引擎在不同负载下的卓越吞吐量与极低延迟表现。

**测试环境与数据规模：**
- **数据量级**：800,000 Keys（每个 Value 512 Bytes，逻辑总容量约 403 MB）
- **核心优势**：极致的并发读取扩展性、微秒级尾延迟

---

## 性能亮点 (Highlights)

- **极限并发读取**：突破 **6.18 Million ops/s**（3 线程并发）
- **单核随机写入**：高达 **123.4k ops/s**
- **极低响应延迟**：缓存命中状态下 **p50 延迟仅 4.9 μs**

---

## 1. 核心操作吞吐量（单线程）

引擎在单线程下的表现依然强劲，尤其是在随机写入和热数据读取场景中，得益于高效的 MemTable 结构和 Block Cache 机制。

| 操作类型               | 吞吐量 (ops/s) |
|------------------------|----------------|
| 随机写入（已有数据）   | 123,467        |
| 热随机读取（缓存）     | 94,968         |
| 顺序填充（Bulk Load）  | 86,837         |

---

## 2. 并发读取扩展性（百万级 ops）

在多核环境下，引擎展现出了惊人的线性扩展能力。从单线程到 3 线程，吞吐量实现了超过 20 倍的非线性飞跃，最高逼近 **618 万 ops/s**。

| 线程数 | 吞吐量 (ops/s) |
|--------|----------------|
| 1      | 267,031        |
| 2      | 4,138,560      |
| 3      | **6,184,783**  |
| 8      | 5,872,271      |

> 注：8 线程时因硬件核心数限制或 CPU 缓存颠簸，吞吐量略微回落，但仍稳定在 580 万 ops/s 以上。

---

## 3. 混合读写负载表现

在真实的业务场景（混合读写）中，引擎能够平滑处理各种 R:W 比例的请求，性能下降曲线非常平缓。

| 读写比例 (Read : Write) | 吞吐量 (ops/s) | 表现分析                                 |
|-------------------------|----------------|------------------------------------------|
| R 95 : W 5              | 85,411         | 绝佳的读密集型表现                       |
| R 75 : W 25             | 80,393         | 读写混合依然保持高吞吐                   |
| R 50 : W 50             | 69,121         | 读写对半时，引擎调度均衡                 |
| R 25 : W 75             | 47,097         | 写密集时受限于后台 Compaction            |
| R 10 : W 100            | 117,396        | 纯写场景迅速回升，MemTable 吸收力极强    |

---

## 4. 尾延迟分析 (Latency Percentiles)

在热随机读取（Warm Random Read）场景下，引擎展现出了工业级存储所需的稳定性，p99 延迟严格控制在 20 微秒以内。

| 百分位 (Percentile) | 延迟 (μs) | 状态         |
|---------------------|-----------|--------------|
| p50                 | 4.9       | 极速响应     |
| p95                 | < 17.1    | 稳定可靠     |
| p99                 | 17.1      | 波动极小     |
| p999                | 40.9      | 轻微毛刺     |
| p9999               | 151.2     | 正常长尾现象 |

**平均延迟：35.6 μs**

---

### 附加测试图表

> 以下测试均为内存测试，详细数据请参考上方表格。

- [并发扩展性图表](bench_output/bench_concurrency_scaling.png)
- [延迟累积分布图 (CDF)](bench_output/bench_latency_cdf.png)
- [延迟百分位图](bench_output/bench_latency_percentiles.png)
- [综合性能图表](bench_output/benchmark_charts.png)
- [详细基准测试报告](bench_output/benchmark_report.md)

---

## 贡献指南

我们欢迎社区贡献！请遵循以下步骤：

1. **Fork 项目**并创建特性分支
2. **编写测试**确保新功能的可靠性
3. **遵循代码规范**（详见 `.clang-format`）
4. **提交 Pull Request**并描述变更内容

### 代码规范

- 使用 4 空格缩进
- 类名使用 PascalCase
- 函数名使用 snake_case
- 常量使用 UPPER_CASE
- 每行不超过 100 字符

## 许可证

本项目采用 MIT 许可证。详情请见 [LICENSE](LICENSE) 文件。

## 联系我们

- **项目主页**：https://github.com/your-repo/lsm-engine
- **问题反馈**：https://github.com/your-repo/lsm-engine/issues
- **邮件联系 1**：trongoneadam@gmail.com
- **邮件联系 2**：1976909647@qq.com

## 致谢

感谢以下开源项目的启发：

- [LevelDB](https://github.com/google/leveldb)
- [RocksDB](https://github.com/facebook/rocksdb)
- [GoogleTest](https://github.com/google/googletest)
```