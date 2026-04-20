/**
 * LSMBenchmark.cpp
 *
 * A comprehensive benchmark suite for LSM-Tree, inspired by RocksDB/db_bench.
 * Outputs results to benchmark_results.csv for chart generation.
 *
 * Benchmarks included:
 *   1. Sequential Write Throughput
 *   2. Random Write Throughput
 *   3. Sequential Read Throughput (after fill)
 *   4. Random Read Throughput (point lookup)
 *   5. Mixed Workload (varying read/write ratios)
 *   6. Read Concurrency Scaling (1/2/4/8 threads)
 *   7. Latency Percentile Distribution (p50/p95/p99/p999)
 *   8. Sustained Write Throughput Over Time (windowed)
 */
#include "LSM.h" 
#include <gtest/gtest.h>
#include <chrono>
#include <fstream>
#include <locale>
// ── Adjust this include to match your project layout ──────────────────────
 // or wherever your LSM entry point lives
// ──────────────────────────────────────────────────────────────────────────

// ============================================================
//  Benchmark configuration
//  Tuned for a system doing ~800K read QPS / ~100K write QPS
// ============================================================
namespace BenchConfig {
constexpr int FILL_SIZE          = 200'000;   // keys pre-loaded before read tests
constexpr int SEQ_WRITE_OPS      = 150'000;   // sequential write benchmark ops
constexpr int RAND_WRITE_OPS     = 100'000;   // random write benchmark ops
constexpr int SEQ_READ_OPS       = 200'000;   // sequential read benchmark ops
constexpr int RAND_READ_OPS      = 200'000;   // random point lookup ops
constexpr int LATENCY_SAMPLE_OPS = 50'000;    // ops for latency CDF
constexpr int MIXED_OPS_PER_SIDE = 80'000;    // ops per side in mixed workload
constexpr int TIME_WINDOW_OPS    = 20'000;    // ops per time window slice
constexpr int TIME_WINDOWS       = 8;         // number of windows to measure
} // namespace BenchConfig

// ============================================================
//  Helpers
// ============================================================
using Clock    = std::chrono::steady_clock;
using Duration = std::chrono::duration<double>;

struct BenchResult {
    std::string benchmark;
    std::string variant;    // e.g. "threads=4", "rw_ratio=70:30"
    double      ops_per_sec;
    double      p50_us;     // microseconds, 0 if not measured
    double      p95_us;
    double      p99_us;
    double      p999_us;
};

// Central result store
static std::vector<BenchResult> g_results;
static std::string              g_csv_path = "benchmark_results.csv";

inline std::string make_key(std::string_view prefix, int idx) {
    return std::format("{}{:08d}", prefix, idx);
}

// Compute percentiles from a sorted latency vector (nanoseconds → microseconds)
struct Percentiles {
    double p50, p95, p99, p999;
};
Percentiles compute_percentiles(std::vector<int64_t>& ns_samples) {
    std::sort(ns_samples.begin(), ns_samples.end());
    auto at = [&](double pct) -> double {
        if (ns_samples.empty()) return 0.0;
        size_t idx = static_cast<size_t>(pct * ns_samples.size());
        if (idx >= ns_samples.size()) idx = ns_samples.size() - 1;
        return ns_samples[idx] / 1000.0; // ns → µs
    };
    return {at(0.50), at(0.95), at(0.99), at(0.999)};
}

void save_csv() {
    std::ofstream f(g_csv_path);
    f << "benchmark,variant,ops_per_sec,p50_us,p95_us,p99_us,p999_us\n";
    for (auto& r : g_results) {
        f << std::format("{},{},{:.0f},{:.2f},{:.2f},{:.2f},{:.2f}\n",
                         r.benchmark, r.variant,
                         r.ops_per_sec, r.p50_us, r.p95_us, r.p99_us, r.p999_us);
    }
    std::cout << "\n[BENCHMARK] Results saved to: " << g_csv_path << "\n";
}

void print_result(const BenchResult& r) {
    std::cout << std::format(
        "  {:.<45} {:>10.0f} ops/s   p50={:>6.1f}µs  p99={:>6.1f}µs\n",
        r.benchmark + "/" + r.variant,
        r.ops_per_sec, r.p50_us, r.p99_us);
}

// ============================================================
//  Test fixture
// ============================================================
class LSMBenchmark : public ::testing::Test {
protected:
    std::unique_ptr<LSM> lsm;  // replace LSM with your actual class name

    void SetUp() override {
        // Create a fresh LSM instance for each benchmark group.
        // Adjust constructor args to match your implementation.
        lsm = std::make_unique<LSM>("./bench_db");
    }
    void TearDown() override {
        lsm.reset();
        // Optionally: std::filesystem::remove_all("./bench_db");
    }
};

// ============================================================
//  1. Sequential Write
// ============================================================
TEST_F(LSMBenchmark, Bench01_SequentialWrite) {
    std::cout << "\n[1/8] Sequential Write Throughput\n";
    const int N = BenchConfig::SEQ_WRITE_OPS;

    auto t0 = Clock::now();
    for (int i = 0; i < N; ++i) {
        lsm->put(make_key("seqw_", i), make_key("val_", i));
    }
    double elapsed = Duration(Clock::now() - t0).count();
    double qps     = N / elapsed;

    BenchResult r{"sequential_write", "single_thread", qps, 0, 0, 0, 0};
    g_results.push_back(r);
    print_result(r);
std::cout << std::format(std::locale("en_US.UTF-8"), "{:L} writes in {:.3f}s  →  {:.0f} ops/s\n", N, elapsed, qps);
    EXPECT_GT(qps, 1000);
}

// ============================================================
//  2. Random Write
// ============================================================
TEST_F(LSMBenchmark, Bench02_RandomWrite) {
    std::cout << "\n[2/8] Random Write Throughput\n";
    const int N = BenchConfig::RAND_WRITE_OPS;

    std::mt19937 rng(42);
    std::uniform_int_distribution<int> dist(0, N * 3);

    auto t0 = Clock::now();
    for (int i = 0; i < N; ++i) {
        int k = dist(rng);
        lsm->put(make_key("rw_", k), make_key("val_", k));
    }
    double elapsed = Duration(Clock::now() - t0).count();
    double qps     = N / elapsed;

    BenchResult r{"random_write", "single_thread", qps, 0, 0, 0, 0};
    g_results.push_back(r);
    print_result(r);
}

// ============================================================
//  3. Sequential Read (after fill)
// ============================================================
TEST_F(LSMBenchmark, Bench03_SequentialRead) {
    std::cout << "\n[3/8] Sequential Read Throughput\n";
    const int FILL = BenchConfig::FILL_SIZE;
    const int READ = BenchConfig::SEQ_READ_OPS;

    // Fill
    for (int i = 0; i < FILL; ++i) {
        lsm->put(make_key("sr_", i), make_key("val_", i));
    }
    lsm->flush_all();

    auto t0 = Clock::now();
    int hits = 0;
    for (int i = 0; i < READ; ++i) {
        auto v = lsm->get(make_key("sr_", i % FILL));
        if (v.has_value()) ++hits;
    }
    double elapsed = Duration(Clock::now() - t0).count();
    double qps     = READ / elapsed;

    BenchResult r{"sequential_read", "single_thread", qps, 0, 0, 0, 0};
    g_results.push_back(r);
    print_result(r);
    std::cout << std::format("    hit rate: {:.1f}%\n", 100.0 * hits / READ);
}

// ============================================================
//  4. Random Read with Latency Percentiles
// ============================================================
TEST_F(LSMBenchmark, Bench04_RandomRead_Latency) {
    std::cout << "\n[4/8] Random Read + Latency Percentiles\n";
    const int FILL    = BenchConfig::FILL_SIZE;
    const int SAMPLE  = BenchConfig::LATENCY_SAMPLE_OPS;
    const int MEASURE = BenchConfig::RAND_READ_OPS;

    for (int i = 0; i < FILL; ++i) {
        lsm->put(make_key("rr_", i), make_key("val_", i));
    }
    lsm->flush_all();

    std::mt19937 rng(1337);
    std::uniform_int_distribution<int> dist(0, FILL - 1);

    // -- Throughput pass --
    auto t0 = Clock::now();
    for (int i = 0; i < MEASURE; ++i) {
        lsm->get(make_key("rr_", dist(rng)));
    }
    double elapsed = Duration(Clock::now() - t0).count();
    double qps     = MEASURE / elapsed;

    // -- Latency sampling pass (smaller N, record each op) --
    std::vector<int64_t> latencies;
    latencies.reserve(SAMPLE);
    for (int i = 0; i < SAMPLE; ++i) {
        auto t_start = Clock::now();
        lsm->get(make_key("rr_", dist(rng)));
        auto t_end = Clock::now();
        latencies.push_back(
            std::chrono::duration_cast<std::chrono::nanoseconds>(t_end - t_start).count());
    }
    auto pct = compute_percentiles(latencies);

    BenchResult r{"random_read", "single_thread", qps,
                  pct.p50, pct.p95, pct.p99, pct.p999};
    g_results.push_back(r);
    print_result(r);
    std::cout << std::format("    Latency — p50={:.1f}µs  p95={:.1f}µs  p99={:.1f}µs  p999={:.1f}µs\n",
                             pct.p50, pct.p95, pct.p99, pct.p999);
}

// ============================================================
//  5. Mixed Workload (varying read/write ratios)
// ============================================================
TEST_F(LSMBenchmark, Bench05_MixedWorkload) {
    std::cout << "\n[5/8] Mixed Read/Write Workload\n";
    const int FILL = BenchConfig::FILL_SIZE;
    const int OPS  = BenchConfig::MIXED_OPS_PER_SIDE;

    for (int i = 0; i < FILL; ++i) {
        lsm->put(make_key("mx_", i), make_key("val_", i));
    }
    lsm->flush_all();

    // Test ratios: read% / write%
    std::vector<std::pair<int,int>> ratios = {{100,0},{90,10},{70,30},{50,50},{30,70},{0,100}};

    for (auto [r_pct, w_pct] : ratios) {
        std::mt19937 rng(42);
        std::uniform_int_distribution<int> key_dist(0, FILL - 1);
        std::uniform_int_distribution<int> op_dist(0, 99);

        int total_ops = OPS;
        auto t0 = Clock::now();
        int write_ctr = 0;
        for (int i = 0; i < total_ops; ++i) {
            if (op_dist(rng) < r_pct) {
                lsm->get(make_key("mx_", key_dist(rng)));
            } else {
                lsm->put(make_key("mx_", key_dist(rng)), make_key("val_", i));
                ++write_ctr;
            }
        }
        double elapsed = Duration(Clock::now() - t0).count();
        double qps     = total_ops / elapsed;
        std::string variant = std::format("read{}w{}", r_pct, w_pct);

        BenchResult res{"mixed_workload", variant, qps, 0, 0, 0, 0};
        g_results.push_back(res);
        std::cout << std::format("    R{}:W{:<3}  →  {:.0f} ops/s\n",
                                 r_pct, w_pct, qps);
    }
}

// ============================================================
//  6. Read Concurrency Scaling
// ============================================================
TEST_F(LSMBenchmark, Bench06_ReadConcurrencyScaling) {
    std::cout << "\n[6/8] Read Concurrency Scaling\n";
    const int FILL      = BenchConfig::FILL_SIZE;
    const int OPS_TOTAL = 400'000; // total ops shared across threads

    for (int i = 0; i < FILL; ++i) {
        lsm->put(make_key("cs_", i), make_key("val_", i));
    }
    lsm->flush_all();

    for (int num_threads : {1, 2, 4, 8}) {
        std::atomic<bool> go{false};
        std::vector<std::thread> workers;
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
        for (auto& w : workers) w.join();

        double elapsed = Duration(Clock::now() - t0).count();
        double qps     = OPS_TOTAL / elapsed;
        std::string variant = std::format("threads={}", num_threads);

        BenchResult r{"concurrency_scaling", variant, qps, 0, 0, 0, 0};
        g_results.push_back(r);
        std::cout << std::format("    {:>2} threads  →  {:.0f} ops/s\n", num_threads, qps);
    }
}

// ============================================================
//  7. Latency CDF  (writes + reads side by side)
// ============================================================
TEST_F(LSMBenchmark, Bench07_LatencyCDF) {
    std::cout << "\n[7/8] Latency CDF (write vs read)\n";
    const int FILL   = BenchConfig::FILL_SIZE;
    const int SAMPLE = BenchConfig::LATENCY_SAMPLE_OPS;

    for (int i = 0; i < FILL; ++i) {
        lsm->put(make_key("cdf_", i), make_key("val_", i));
    }
    lsm->flush_all();

    std::mt19937 rng(99);
    std::uniform_int_distribution<int> dist(0, FILL - 1);

    // Read latency samples
    std::vector<int64_t> read_lat, write_lat;
    read_lat.reserve(SAMPLE);
    write_lat.reserve(SAMPLE);

    for (int i = 0; i < SAMPLE; ++i) {
        {
            auto t0 = Clock::now();
            lsm->get(make_key("cdf_", dist(rng)));
            read_lat.push_back(
                std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - t0).count());
        }
        {
            auto t0 = Clock::now();
            lsm->put(make_key("cdf_", dist(rng)), make_key("v_", i));
            write_lat.push_back(
                std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - t0).count());
        }
    }

    auto rp = compute_percentiles(read_lat);
    auto wp = compute_percentiles(write_lat);

    // Store percentile rows for each decile so Python can draw a CDF
    std::sort(read_lat.begin(), read_lat.end());
    std::sort(write_lat.begin(), write_lat.end());

    // Emit one row per 5th-percentile step
    for (int pct = 5; pct <= 100; pct += 5) {
        size_t idx_r = std::min(static_cast<size_t>(pct * SAMPLE / 100), read_lat.size() - 1);
        size_t idx_w = std::min(static_cast<size_t>(pct * SAMPLE / 100), write_lat.size() - 1);
        double r_us  = read_lat[idx_r]  / 1000.0;
        double w_us  = write_lat[idx_w] / 1000.0;
        g_results.push_back({"latency_cdf_read",  std::format("p{}", pct), r_us, r_us, r_us, r_us, r_us});
        g_results.push_back({"latency_cdf_write", std::format("p{}", pct), w_us, w_us, w_us, w_us, w_us});
    }

    std::cout << std::format("    Read  — p50={:.1f}µs  p99={:.1f}µs  p999={:.1f}µs\n",
                             rp.p50, rp.p99, rp.p999);
    std::cout << std::format("    Write — p50={:.1f}µs  p99={:.1f}µs  p999={:.1f}µs\n",
                             wp.p50, wp.p99, wp.p999);
}

// ============================================================
//  8. Sustained Write Throughput Over Time (time-window slices)
// ============================================================
TEST_F(LSMBenchmark, Bench08_ThroughputOverTime) {
    std::cout << "\n[8/8] Sustained Throughput Over Time\n";
    const int WINDOW_OPS = BenchConfig::TIME_WINDOW_OPS;
    const int WINDOWS    = BenchConfig::TIME_WINDOWS;

    for (int w = 0; w < WINDOWS; ++w) {
        int base = w * WINDOW_OPS;
        auto t0  = Clock::now();
        for (int i = 0; i < WINDOW_OPS; ++i) {
            lsm->put(make_key("tw_", base + i), make_key("val_", base + i));
        }
        double elapsed = Duration(Clock::now() - t0).count();
        double qps     = WINDOW_OPS / elapsed;
        std::string variant = std::format("window={}", w + 1);

        g_results.push_back({"throughput_over_time", variant, qps, 0, 0, 0, 0});
        std::cout << std::format("    window {:>2}: {:.0f} ops/s\n", w + 1, qps);
    }
}

// ============================================================
//  Fixture that saves CSV after all tests
// ============================================================
class LSMBenchmarkCSVWriter : public ::testing::EmptyTestEventListener {
    void OnTestProgramEnd(const ::testing::UnitTest&) override {
        save_csv();
    }
};

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    ::testing::UnitTest::GetInstance()->listeners().Append(new LSMBenchmarkCSVWriter());
    return RUN_ALL_TESTS();
}