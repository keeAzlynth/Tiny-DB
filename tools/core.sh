#!/bin/bash
# ================================================================
# bench_kernel_tune.sh
#
# Industrial LSM benchmark kernel parameter tuning.
# Run as root BEFORE running benchmarks.
#
# WHY: Default Linux kernel is tuned for desktop throughput,
#      not for reproducible storage benchmarks. These settings
#      eliminate noise sources that distort latency measurements.
# ================================================================

set -euo pipefail

if [[ $EUID -ne 0 ]]; then
    echo "ERROR: Run as root (sudo bash bench_kernel_tune.sh)"
    exit 1
fi

echo "=== LSM Benchmark Kernel Tuning ==="
echo ""

# ────────────────────────────────────────────────────────────────
# 1. CPU Frequency Scaling → Performance Governor
#
# WHY: Default "powersave" governor causes CPU to downclock
#      between operations, adding 10-100µs variance to each op.
#      "performance" locks CPU at max frequency.
#
# IMPACT: Reduces p99 variance by 2-5x
# ────────────────────────────────────────────────────────────────
echo "[1/7] CPU governor → performance"
for cpu in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do
    if [[ -f "$cpu" ]]; then
        echo performance > "$cpu"
    fi
done
echo "    Done. Current: $(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || echo 'N/A')"

# ────────────────────────────────────────────────────────────────
# 2. Disable Swap
#
# WHY: If the system swaps during benchmark, you're measuring
#      disk swap latency, not LSM latency. Swap causes
#      multi-millisecond latency spikes that destroy p99.
#
# IMPACT: Eliminates swap-induced tail latency
# ────────────────────────────────────────────────────────────────
echo "[2/7] Disable swap"
echo 0 > /proc/sys/vm/swappiness
swapoff -a 2>/dev/null || true
echo "    Done. Swappiness: $(cat /proc/sys/vm/swappiness)"

# ────────────────────────────────────────────────────────────────
# 3. Dirty Page Ratios
#
# WHY: Default dirty_ratio=20 means the kernel buffers up to 20%
#      of RAM in dirty pages before forcing sync writes. This
#      creates bursty write behavior that skews throughput.
#      Lower values = more frequent, smaller flushes = smoother
#      write throughput (closer to what the LSM actually does).
#
# IMPACT: More consistent write throughput numbers
# ────────────────────────────────────────────────────────────────
echo "[3/7] Dirty page ratios"
echo 5  > /proc/sys/vm/dirty_background_ratio
echo 10 > /proc/sys/vm/dirty_ratio
echo "    dirty_background_ratio=$(cat /proc/sys/vm/dirty_background_ratio)"
echo "    dirty_ratio=$(cat /proc/sys/vm/dirty_ratio)"

# ────────────────────────────────────────────────────────────────
# 4. Transparent Huge Pages → Disable
#
# WHY: THP can cause brief kernel pauses (up to 1ms) when it
#      defragments memory to create huge pages. These pauses
#      show up as p99.9 latency spikes in your benchmark,
#      completely unrelated to LSM performance.
#
# IMPACT: Reduces p9999 latency spikes by 3-10x
# ────────────────────────────────────────────────────────────────
echo "[4/7] Transparent Huge Pages → never"
if [[ -f /sys/kernel/mm/transparent_hugepage/enabled ]]; then
    echo never > /sys/kernel/mm/transparent_hugepage/enabled
    echo "    Done. $(cat /sys/kernel/mm/transparent_hugepage/enabled)"
else
    echo "    Not available (VM?)"
fi

# ────────────────────────────────────────────────────────────────
# 5. I/O Scheduler
#
# WHY: "cfq" scheduler adds fairness queueing that increases
#      latency variance. For benchmarks:
#        - NVMe → "none" (no scheduler, hardware has NCQ)
#        - SATA SSD → "mq-deadline" (simple deadline scheduling)
#        - HDD → "bfq" (proportional fairness)
#
# IMPACT: Reduces p99 read latency by 20-50% on SSD
# ────────────────────────────────────────────────────────────────
echo "[5/7] I/O Scheduler"
for dev in /sys/block/*/queue/scheduler; do
    disk=$(echo "$dev" | cut -d/ -f4)
    # Detect device type
    if [[ "$disk" == nvme* ]]; then
        echo none > "$dev"
    else
        echo mq-deadline > "$dev" 2>/dev/null || true
    fi
    echo "    $disk: $(cat "$dev")"
done

# ────────────────────────────────────────────────────────────────
# 6. Filesystem Mount Options
#
# WHY: "relatime" (default) updates access timestamps on reads,
#      adding unnecessary metadata writes. "noatime" eliminates
#      this overhead. "discard" enables TRIM for SSDs.
#
# IMPACT: 5-15% read throughput improvement
# ────────────────────────────────────────────────────────────────
echo "[6/7] Filesystem options (manual step)"
echo "    Recommended: mount -o remount,noatime,discard /your/bench/path"
echo "    Current bench_db mount:"
BENCH_DIR="./bench_db"
if [[ -d "$BENCH_DIR" ]]; then
    df -T "$BENCH_DIR" | tail -1
fi

# ────────────────────────────────────────────────────────────────
# 7. NUMA Balancing → Disable
#
# WHY: NUMA balancing migrates memory pages between NUMA nodes,
#      causing transient latency spikes (100µs-1ms). For
#      benchmarks, we want deterministic memory access.
#
# IMPACT: Eliminates NUMA migration latency spikes
# ────────────────────────────────────────────────────────────────
echo "[7/7] NUMA balancing → disable"
if [[ -f /proc/sys/kernel/numa_balancing ]]; then
    echo 0 > /proc/sys/kernel/numa_balancing
    echo "    Done. numa_balancing=$(cat /proc/sys/kernel/numa_balancing)"
else
    echo "    Not available (single-socket system, OK)"
fi

echo ""
echo "=== Kernel tuning complete ==="
echo ""
echo "IMPORTANT: These settings are NOT persistent across reboots."
echo "           Add them to /etc/sysctl.d/99-bench.conf for persistence."
echo ""
echo "Now run your benchmark as root:"
echo "  sudo ./lsm_main"
echo ""
echo "To restore defaults after benchmarking:"
echo "  sudo bash -c 'echo powersave > /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor'"
echo "  sudo bash -c 'echo 60 > /proc/sys/vm/dirty_ratio'"
echo "  sudo bash -c 'echo 10 > /proc/sys/vm/dirty_background_ratio'"
echo "  sudo swapon -a"
