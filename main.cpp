/**
 * LSMBenchmark.cpp
 *
 * Industrial-grade LSM-Tree benchmark (C++23 Edition).
 * Data volume: ~2.6GB logical to force disk I/O and multi-level compaction.
 *
 * Compile requirement: C++23 (GCC 14+ / Clang 18+)
 * Run requirement: sudo (for drop_caches in cold read test)
 */

#include "LSM.h"
#include <gtest/gtest.h>
#include <chrono>
#include <fstream>
#include <thread>
#include <atomic>
#include <random>
#include <algorithm>
#include <ranges>
#include <print>
#include <locale>
#include <unistd.h>  // sync()

// ============================================================
//  Benchmark configuration (Industrial Scale)
// ============================================================
namespace BenchConfig {
    // FILL_SIZE × 512B ≈ 400MB logical data
    // L1 触发阈值 120MB，L2 触发阈值 1200MB
    // 数据压到 L2 但不触发 L2→L3，三层 SST 结构真实有效
    constexpr int FILL_SIZE           = 800'000;

    constexpr int VAL_SMALL           = 100;
    constexpr int VAL_MEDIUM          = 256;
    constexpr int VAL_DEFAULT         = 512;
    constexpr int VAL_LARGE           = 1024;

    // 冷读很慢（全走磁盘），50K 足够采到稳定的 p99
    // 多了只是在等磁盘 I/O，不增加统计可信度
    constexpr int WARMUP_OPS          = 100'000;
    constexpr int RAND_WRITE_OPS      = 300'000;
    constexpr int COLD_READ_OPS       = 50'000;

    // 热读走 block cache（256MB），1M ops 能充分体现 cache 命中路径的吞吐上限
    constexpr int WARM_READ_OPS       = 1'000'000;

    // LATENCY_SAMPLE_OPS 需要 ≤ min(COLD_READ_OPS, WARM_READ_OPS)
    // Bench03 里 SAMPLE = min(COLD_READ_OPS, LATENCY_SAMPLE_OPS) = 50'000，所以这个值
    // 对冷读没有实际效果，主要给 Bench04 热读采样用
    constexpr int LATENCY_SAMPLE_OPS  = 200'000;

    constexpr int MIXED_OPS           = 500'000;
    constexpr int RWW_READ_OPS        = 1'000'000;
    constexpr int TIME_WINDOW_OPS     = 200'000;
    constexpr int TIME_WINDOWS        = 10;

    // 2M / 线程数，单线程 500K，压力够但不会让 VM OOM
    constexpr int CONCURRENCY_TOTAL   = 2'000'000;
} // namespace BenchConfig

// ============================================================
//  Helpers
// ============================================================
using Clock    = std::chrono::steady_clock;
using Duration = std::chrono::duration<double>;
auto loc = std::locale("en_US.UTF-8");
struct BenchResult {
    std::string benchmark;
    std::string variant;
    double      ops_per_sec;
    double      p50_us;
    double      p95_us;
    double      p99_us;
    double      p999_us;
};

static std::vector<BenchResult> g_results;
static std::string              g_csv_path = "benchmark_results.csv";

inline std::string make_key(std::string_view prefix, int idx) {
    return std::format("{}{:08d}", prefix, idx);
}

inline std::string make_value(int size, int seed = 0) {
    std::string v(size, '\0');
    for (int i = 0; i < size; ++i) {
        v[i] = static_cast<char>('a' + ((seed + i) % 26));
    }
    return v;
}

struct Percentiles {
    double p50, p95, p99, p999, p9999;
};

Percentiles compute_percentiles(std::vector<int64_t>& ns_samples) {
    if (ns_samples.empty()) return {0, 0, 0, 0, 0};
    std::ranges::sort(ns_samples);
    auto at = [&](double pct) -> double {
        size_t idx = static_cast<size_t>(pct * (ns_samples.size() - 1));
        return ns_samples[idx] / 1000.0; // ns → µs
    };
    return {at(0.50), at(0.95), at(0.99), at(0.999), at(0.9999)};
}

void drop_os_cache() {
    sync();
    std::ofstream f("/proc/sys/vm/drop_caches");
    if (f.is_open()) {
        f << "3";
        f.close();
        std::println("    [OS] Page cache, dentries, inodes dropped.");
    } else {
        std::println("    [OS] WARNING: Cannot drop caches (need root). Cold-read results inaccurate.");
    }
}

void sync_and_settle(int settle_ms = 500) {
    sync();
    std::this_thread::sleep_for(std::chrono::milliseconds(settle_ms));
}

void save_csv() {
    std::ofstream f(g_csv_path);
    f << "benchmark,variant,ops_per_sec,p50_us,p95_us,p99_us,p999_us\n";
    for (auto& r : g_results) {
        f << std::format("{},{},{:.0f},{:.2f},{:.2f},{:.2f},{:.2f}\n",
                         r.benchmark, r.variant,
                         r.ops_per_sec, r.p50_us, r.p95_us, r.p99_us, r.p999_us);
    }
    std::println("\n[BENCHMARK] Results saved to: {}", g_csv_path);
}

void print_result(const BenchResult& r) {
    std::println("  {:.<55} {:>10.0f} ops/s   p50={:>7.1f}µs  p99={:>7.1f}µs",
                 r.benchmark + "/" + r.variant, r.ops_per_sec, r.p50_us, r.p99_us);
}

// ============================================================
//  Test fixture
// ============================================================
class LSMBenchmark : public ::testing::Test {
protected:
    std::unique_ptr<LSM> lsm;

    void SetUp() override {
        lsm = std::make_unique<LSM>("./bench_db");
    }
    void TearDown() override {
        lsm.reset();
    }

    void fill_db(int count, int val_size = BenchConfig::VAL_DEFAULT,
                 std::string_view prefix = "db_") {
        std::print("    Filling {} keys ({}B values)...\n", 
           std::format(loc, "{:L}", count), 
           val_size);
        std::cout.flush();
        auto t0 = Clock::now();
        for (int i = 0; i < count; ++i) {
            lsm->put(make_key(prefix, i), make_value(val_size, i));
        }
        lsm->flush_all();
        sync_and_settle();
        double s = Duration(Clock::now() - t0).count();
        double data_mb = count * (16.0 + val_size) / (1024.0 * 1024.0);
        std::println(" done in {:.1f}s ({:.0f} MB)", s, data_mb);
    }
};

// ============================================================
//  1. Sequential Fill (Bulk Load)
// ============================================================
TEST_F(LSMBenchmark, Bench01_FillSequential) {
    std::println("\n[1/8] Sequential Fill (Bulk Load)");
    const int N = BenchConfig::FILL_SIZE;
    const int V = BenchConfig::VAL_DEFAULT;

    auto t0 = Clock::now();
    for (int i = 0; i < N; ++i) {
        lsm->put(make_key("db_", i), make_value(V, i));
    }
    lsm->flush_all();
    double elapsed = Duration(Clock::now() - t0).count();
    double qps     = N / elapsed;
    double data_mb = N * (16.0 + V) / (1024.0 * 1024.0);

    BenchResult r{"fill_sequential", std::format("{}M_{}B", N / 1'000'000, V), qps, 0, 0, 0, 0};
    g_results.push_back(r);
    print_result(r);
    std::println("    {:.0f} MB written in {:.2f}s ({:.1f} MB/s)", data_mb, elapsed, data_mb / elapsed);
}

// ============================================================
//  2. Random Write on Populated DB
// ============================================================
TEST_F(LSMBenchmark, Bench02_RandomWrite) {
    std::println("\n[2/8] Random Write (on populated DB)");
    const int FILL = BenchConfig::FILL_SIZE;
    const int OPS  = BenchConfig::RAND_WRITE_OPS;
    const int V    = BenchConfig::VAL_DEFAULT;

    fill_db(FILL, V, "rw_");

    std::mt19937 rng(42);
    std::uniform_int_distribution<int> dist(0, FILL - 1);

    for (int i = 0; i < BenchConfig::WARMUP_OPS; ++i) {
        lsm->put(make_key("rw_", dist(rng)), make_value(V, i));
    }

    auto t0 = Clock::now();
    for (int i = 0; i < OPS; ++i) {
        lsm->put(make_key("rw_", dist(rng)), make_value(V, i));
    }
    double elapsed = Duration(Clock::now() - t0).count();
    double qps     = OPS / elapsed;

    g_results.push_back({"random_write", "populated_db", qps, 0, 0, 0, 0});
    print_result(g_results.back());
}

// ============================================================
//  3. Cold Random Read (Disk-Bound)
// ============================================================
TEST_F(LSMBenchmark, Bench03_ColdRandomRead) {
    std::println("\n[3/8] Cold Random Read (disk-bound)");
    const int FILL = BenchConfig::FILL_SIZE;
    const int OPS  = BenchConfig::COLD_READ_OPS;
    const int V    = BenchConfig::VAL_DEFAULT;

    fill_db(FILL, V, "cr_");
    drop_os_cache();

    std::mt19937 rng(1337);
    std::uniform_int_distribution<int> dist(0, FILL - 1);

    const int SAMPLE = std::min(OPS, BenchConfig::LATENCY_SAMPLE_OPS);
    std::vector<int64_t> latencies;
    latencies.reserve(SAMPLE);

    int hits = 0;
    auto t0 = Clock::now();
    for (int i = 0; i < OPS; ++i) {
        auto t_start = Clock::now();
        if (lsm->get(make_key("cr_", dist(rng))).has_value()) ++hits;
        if (i < SAMPLE) {
            latencies.push_back(
                std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - t_start).count());
        }
    }
    double elapsed = Duration(Clock::now() - t0).count();
    double qps     = OPS / elapsed;

    auto pct = compute_percentiles(latencies);
    g_results.push_back({"cold_random_read", "disk_bound", qps, pct.p50, pct.p95, pct.p99, pct.p999});
    print_result(g_results.back());
    std::println("    hit rate: {:.1f}%  p9999={:.1f}µs", 100.0 * hits / OPS, pct.p9999);
}

// ============================================================
//  4. Warm Random Read + Latency CDF (using C++23 ranges)
// ============================================================
TEST_F(LSMBenchmark, Bench04_WarmRandomRead_Latency) {
    std::println("\n[4/8] Warm Random Read + Latency Percentiles");
    const int FILL   = BenchConfig::FILL_SIZE;
    const int OPS    = BenchConfig::WARM_READ_OPS;
    const int SAMPLE = BenchConfig::LATENCY_SAMPLE_OPS;
    const int V      = BenchConfig::VAL_DEFAULT;

    fill_db(FILL, V, "wr_");

    std::mt19937 rng(1337);
    std::uniform_int_distribution<int> dist(0, FILL - 1);

    std::print("    Warming up block cache...");
    std::cout.flush();
    for (int i = 0; i < BenchConfig::WARMUP_OPS; ++i) {
        lsm->get(make_key("wr_", dist(rng)));
    }
    std::println(" done");

    auto t0 = Clock::now();
    for (int i = 0; i < OPS; ++i) {
        lsm->get(make_key("wr_", dist(rng)));
    }
    double elapsed = Duration(Clock::now() - t0).count();
    double qps     = OPS / elapsed;

    std::vector<int64_t> latencies;
    latencies.reserve(SAMPLE);
    for (int i = 0; i < SAMPLE; ++i) {
        auto t_start = Clock::now();
        lsm->get(make_key("wr_", dist(rng)));
        latencies.push_back(
            std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - t_start).count());
    }
    auto pct = compute_percentiles(latencies);

    // C++23 Ranges: Elegantly emit CDF rows for Python plotting (5%, 10%, ... 100%)
    std::ranges::sort(latencies);
    for (int p : std::views::iota(5, 101) | std::views::stride(5)) {
        size_t idx = std::min(static_cast<size_t>(p * SAMPLE / 100), latencies.size() - 1);
        double us = latencies[idx] / 1000.0;
        g_results.push_back({"latency_cdf_warm_read", std::format("p{}", p), us, us, us, us, us});
    }

    g_results.push_back({"warm_random_read", "cache_hit_path", qps, pct.p50, pct.p95, pct.p99, pct.p999});
    print_result(g_results.back());
    std::println("    Latency — p50={:.1f}µs  p99={:.1f}µs  p999={:.1f}µs  p9999={:.1f}µs",
                 pct.p50, pct.p99, pct.p999, pct.p9999);
}

// ============================================================
//  5. Mixed Read/Write Workload
// ============================================================
TEST_F(LSMBenchmark, Bench05_MixedWorkload) {
    std::println("\n[5/8] Mixed Read/Write Workload");
    const int FILL = BenchConfig::FILL_SIZE;
    const int OPS  = BenchConfig::MIXED_OPS;
    const int V    = BenchConfig::VAL_DEFAULT;

    fill_db(FILL, V, "mx_");

    std::vector<std::pair<int,int>> ratios = {
        {95, 5}, {75,25}, {50,50}, {25,75}, {0,100}
    };

    for (auto [r_pct, w_pct] : ratios) {
        std::mt19937 rng(42);
        std::uniform_int_distribution<int> key_dist(0, FILL - 1);
        std::uniform_int_distribution<int> op_dist(0, 99);

        auto t0 = Clock::now();
        for (int i = 0; i < OPS; ++i) {
            if (op_dist(rng) < r_pct) {
                lsm->get(make_key("mx_", key_dist(rng)));
            } else {
                lsm->put(make_key("mx_", key_dist(rng)), make_value(V, i));
            }
        }
        double elapsed = Duration(Clock::now() - t0).count();
        double qps     = OPS / elapsed;
        
        g_results.push_back({"mixed_workload", std::format("R{}:W{}", r_pct, w_pct), qps, 0, 0, 0, 0});
        std::println("    R{:>2}:W{:<3}  →  {:.0f} ops/s", r_pct, w_pct, qps);
    }
}

// ============================================================
//  6. Read Concurrency Scaling (using C++23 ranges for thread config)
// ============================================================
TEST_F(LSMBenchmark, Bench06_ReadConcurrencyScaling) {
    std::println("\n[6/8] Read Concurrency Scaling");
    const int FILL      = BenchConfig::FILL_SIZE;
    const int OPS_TOTAL = BenchConfig::CONCURRENCY_TOTAL;
    const int V         = BenchConfig::VAL_DEFAULT;

    fill_db(FILL, V, "cs_");

    // C++23 Ranges: Generate {1, 2, 4, 8} elegantly
    auto thread_counts = std::views::iota(0, 4) 
                       | std::views::transform([](int i) { return 1 << i; });

    for (int num_threads : thread_counts) {
        std::atomic<bool> go{false};
        std::vector<std::jthread> workers; // C++20 jthread for RAII join
        int ops_per_thread = OPS_TOTAL / num_threads;

        auto t0 = Clock::now();
        for (int t = 0; t < num_threads; ++t) {
            workers.emplace_back([&, t]() {
                std::mt19937 rng(t * 1000 + 7);
                std::uniform_int_distribution<int> dist(0, FILL - 1);
                while (!go.load(std::memory_order_acquire)) {}
                for (int i = 0; i < ops_per_thread; ++i) {
                    lsm->get(make_key("cs_", dist(rng)));
                }
            });
        }
        go.store(true, std::memory_order_release);
        workers.clear(); // jthreads automatically join here

        double elapsed = Duration(Clock::now() - t0).count();
        double qps     = OPS_TOTAL / elapsed;
        
        g_results.push_back({"concurrency_scaling", std::format("threads={}", num_threads), qps, 0, 0, 0, 0});
        std::println("    {:>2} threads  →  {:.0f} ops/s", num_threads, qps);
    }
}

// ============================================================
//  7. Read-While-Writing (Compaction Interference)
// ============================================================
#ifndef SKIP_RWW_TEST
TEST_F(LSMBenchmark, Bench07_ReadWhileWriting) {
    std::println("\n[7/8] Read-While-Writing (compaction interference)");
    const int FILL     = BenchConfig::FILL_SIZE;
    const int READ_OPS = BenchConfig::RWW_READ_OPS;
    const int V        = BenchConfig::VAL_DEFAULT;

    fill_db(FILL, V, "rww_");

    std::atomic<bool> stop_writer{false};
    std::atomic<int>  writer_ops{0};

    std::jthread writer([&](std::stop_token st) {
        std::mt19937 rng(777);
        std::uniform_int_distribution<int> dist(0, FILL - 1);
        int i = 0;
        while (!st.stop_requested()) {
            lsm->put(make_key("rww_", dist(rng)), make_value(V, i++));
            writer_ops.fetch_add(1, std::memory_order_relaxed);
        }
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    std::mt19937 rng(1337);
    std::uniform_int_distribution<int> dist(0, FILL - 1);

    const int SAMPLE = std::min(READ_OPS, BenchConfig::LATENCY_SAMPLE_OPS);
    std::vector<int64_t> latencies;
    latencies.reserve(SAMPLE);

    int hits = 0;
    auto t0 = Clock::now();
    for (int i = 0; i < READ_OPS; ++i) {
        auto t_start = Clock::now();
        if (lsm->get(make_key("rww_", dist(rng))).has_value()) ++hits;
        if (i < SAMPLE) {
            latencies.push_back(
                std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - t_start).count());
        }
    }
    double elapsed = Duration(Clock::now() - t0).count();
    double qps     = READ_OPS / elapsed;

    writer.request_stop(); // Signal jthread to stop
    // jthread destructor joins automatically

    auto pct = compute_percentiles(latencies);
    g_results.push_back({"read_while_writing", "bg_writer", qps, pct.p50, pct.p95, pct.p99, pct.p999});
    print_result(g_results.back());
    std::println("    Background writer did {} ops during test", 
             std::format(loc, "{:L}", writer_ops.load()));
    std::println("    Read p999={:.1f}µs  p9999={:.1f}µs", pct.p999, pct.p9999);
}
#endif

// ============================================================
//  8. Sustained Write Throughput Over Time
// ============================================================
TEST_F(LSMBenchmark, Bench08_SustainedWriteOverTime) {
    std::println("\n[8/8] Sustained Write Throughput Over Time");
    const int FILL   = BenchConfig::FILL_SIZE;
    const int V      = BenchConfig::VAL_DEFAULT;
    const int W_OPS  = BenchConfig::TIME_WINDOW_OPS;
    const int WIN    = BenchConfig::TIME_WINDOWS;

    fill_db(FILL, V, "tw_");

    std::mt19937 rng(42);
    std::uniform_int_distribution<int> dist(0, FILL + W_OPS * WIN);

    double first_qps = 0, last_qps = 0;
    double min_qps = std::numeric_limits<double>::max();
    double max_qps = 0;

    for (int w = 0; w < WIN; ++w) {
        auto t0 = Clock::now();
        for (int i = 0; i < W_OPS; ++i) {
            int k = dist(rng);
            lsm->put(make_key("tw_", k), make_value(V, k));
        }
        double elapsed = Duration(Clock::now() - t0).count();
        double qps     = W_OPS / elapsed;

        g_results.push_back({"sustained_write", std::format("window={:02d}", w + 1), qps, 0, 0, 0, 0});
        std::println("    window {:>2}: {:.0f} ops/s", w + 1, qps);

        if (w == 0) first_qps = qps;
        last_qps = qps;
        min_qps  = std::min(min_qps, qps);
        max_qps  = std::max(max_qps, qps);
    }

    double degradation = (1.0 - last_qps / first_qps) * 100.0;
    std::println("    Throughput degradation: {:.1f}%  Min={:.0f}  Max={:.0f}", degradation, min_qps, max_qps);
}

// ============================================================
//  CSV writer
// ============================================================
class LSMBenchmarkCSVWriter : public ::testing::EmptyTestEventListener {
    void OnTestProgramEnd(const ::testing::UnitTest&) override {
        save_csv();
    }
};

int main(int argc, char** argv) {
    std::remove(g_csv_path.c_str()); 
    ::testing::InitGoogleTest(&argc, argv);
    ::testing::UnitTest::GetInstance()->listeners().Append(new LSMBenchmarkCSVWriter());
    return RUN_ALL_TESTS();
}