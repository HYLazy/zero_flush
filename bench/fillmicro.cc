// flushmicro -- compute & sync-latency references for the flush
// characterization campaign (bench/FLUSHCHAR_PLAN.md C1).
//
// Two probes, both single-threaded:
//
//   1. stream:  mimic the inner copy+checksum loop of a flush
//       (memtable iteration -> block build): copy 16B key + 1KB value
//       entries out of a 64MB source region into an output buffer and hash
//       each value with XXH3 (same hash family RocksDB uses for SST
//       checksums).  Reports GB/s per core -- the "pure streaming" rate that
//       flush's measured CPU intensity (s/GB) is compared against.
//
//   2. fdatasync: write `bytes` of buffered data to a file on the device
//       under test in 1MB chunks (WritableFileWriter's default buffer size),
//       then time one fdatasync.  Repeat `iters` times.  This is the floor
//       cost of the synchronous sync at the end of each flush job.
//
// Build: g++ -O3 -march=native -I source/rocksdb bench/fillmicro.cc -o bench/tools/fillmicro
// Usage: fillmicro stream | fdatasync <path> <bytes> <iters>

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <string>
#include <unistd.h>
#include <vector>

#include "util/xxhash.h"

static double NowSec() {
  return std::chrono::duration<double>(std::chrono::steady_clock::now().time_since_epoch()).count();
}

static int RunStream() {
  const size_t kVal = 1024, kKey = 16, kEntry = kVal + kKey;
  const size_t src_bytes = 64ull << 20;                 // one memtable payload
  size_t n = src_bytes / kEntry;
  std::vector<char> src(src_bytes), dst(src_bytes);
  // fill source with something non-zero
  uint64_t x = 0x243F6A8885A308D3ull;
  for (size_t i = 0; i < src_bytes; i += 8) {
    x ^= x << 13; x ^= x >> 7; x ^= x << 17;
    memcpy(src.data() + i, &x, 8);
  }
  // warm both regions
  memcpy(dst.data(), src.data(), src_bytes);

  uint64_t hash = 0;
  double best = 0;
  for (int rep = 0; rep < 5; rep++) {
    double t0 = NowSec();
    for (size_t i = 0; i < n; i++) {
      const char* e = src.data() + i * kEntry;
      char* o = dst.data() + i * kEntry;
      memcpy(o, e, kEntry);                              // key+value copy
      hash ^= XXH3_64bits(e + kKey, kVal);               // value checksum
    }
    double dt = NowSec() - t0;
    double gbps = (double)src_bytes / dt / 1e9;
    if (!best || gbps > best) best = gbps;
    printf("stream rep%d: %.3f s  ->  %.2f GB/s  (hash=%llx)\n", rep, dt, gbps,
           (unsigned long long)hash);
    fflush(stdout);
  }
  printf("STREAM_BEST_GB_S=%.3f\n", best);
  return 0;
}

static int RunFdatasync(const std::string& path, size_t bytes, int iters) {
  int fd = ::open(path.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0644);
  if (fd < 0) { perror("open"); return 1; }
  std::vector<char> buf(1ull << 20);
  memset(buf.data(), 1, buf.size());
  std::vector<double> ms;
  double wr_total = 0;
  for (int it = 0; it < iters; it++) {
    double w0 = NowSec();
    for (size_t off = 0; off < bytes; off += buf.size()) {
      size_t n = std::min(buf.size(), bytes - off);
      ssize_t w = ::write(fd, buf.data(), n);
      if (w != (ssize_t)n) { perror("write"); return 1; }
    }
    double w1 = NowSec();
    double s0 = NowSec();
    if (fdatasync(fd) != 0) { perror("fdatasync"); return 1; }
    double s1 = NowSec();
    wr_total += w1 - w0;
    ms.push_back((s1 - s0) * 1e3);
    printf("fdatasync iter%d: wrote %.1f MB in %.1f ms, sync %.1f ms "
           "(implied %.0f MB/s during sync)\n",
           it, bytes / 1048576.0, (w1 - w0) * 1e3, ms.back(),
           bytes / 1048576.0 / ((s1 - s0) ? (s1 - s0) : 1e-9));
    fflush(stdout);
  }
  std::vector<double> s = ms;
  std::sort(s.begin(), s.end());
  double med = s[s.size() / 2], p90 = s[(size_t)(s.size() * 0.9)];
  printf("FDATASYNC_BYTES=%.0f ITERS=%d MEDIAN_MS=%.1f P90_MS=%.1f MAX_MS=%.1f "
         "WRITE_MS_PER_ITER=%.1f\n",
         (double)bytes, iters, med, p90, s.back(), wr_total / iters * 1e3);
  ::close(fd);
  ::unlink(path.c_str());
  return 0;
}

int main(int argc, char** argv) {
  if (argc < 2) {
    fprintf(stderr, "usage: %s stream | fdatasync <path> <bytes> <iters>\n", argv[0]);
    return 2;
  }
  if (std::string(argv[1]) == "stream") return RunStream();
  if (std::string(argv[1]) == "fdatasync" && argc == 5) {
    size_t bytes = strtoull(argv[3], nullptr, 10);
    return RunFdatasync(argv[2], bytes, atoi(argv[4]));
  }
  fprintf(stderr, "bad args\n");
  return 2;
}
