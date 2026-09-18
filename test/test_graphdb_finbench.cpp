#include "richgraph/graph_db.h"
#include "core/flags.h"
#include "preprocessed_chunk_loader.h"

#include <gflags/gflags.h>

#include <algorithm>
#include <array>
#include <cassert>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cctype>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <future>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <memory_resource>
#include <mutex>
#include <optional>
#include <queue>
#include <random>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <sys/mman.h>
#include <unistd.h>

#ifdef _OPENMP
#include <omp.h>
#endif

using vertex_t = lsmgraph::VertexId_t;

DEFINE_string(finbench_dataset_root,
              "./datasets/finbench",
              "FinBench dataset root. It should contain snapshot/, "
              "incremental/, params/ and optionally sf30.zip.");
DEFINE_string(finbench_db_path,
              "./richgraph-finbench-db",
              "GraphDb path used by the FinBench test.");
DEFINE_bool(finbench_reset_db,
            true,
            "Whether to delete and recreate finbench_db_path before import.");
DEFINE_bool(finbench_use_csr_disk,
            false,
            "Whether the single edge shard should use CSR disk mode.");
DEFINE_bool(finbench_skip_queries,
            false,
            "If true, only import data and skip the 12 complex-read queries.");
DEFINE_bool(finbench_mix_enable_queries,
            true,
            "If true, the mixed workload interleaves sampled query tasks with "
            "sampled incremental writes. If false, the mixed workload writes "
            "only sampled incremental rows.");
DEFINE_bool(finbench_skip_updates,
            false,
            "If true, skip the final node and edge property update benchmark stages.");
DEFINE_bool(finbench_enable_hot_edge_csr,
            false,
            "If true, use CSR-like compaction thresholds for the hot edge "
            "shard and force compact remaining L0 files after the mixed "
            "workload.");
DEFINE_uint32(finbench_hot_edge_csr_l0_max_sst_num,
              8,
              "L0 auto-compaction limit for FinBench hot edge CSR shard.");
DEFINE_uint32(finbench_hot_edge_csr_l1_max_sst_num,
              10000,
              "L1 auto-compaction limit for FinBench hot edge CSR shard.");
DEFINE_uint32(finbench_property_length,
              20,
              "Fixed property length used by this temporary FinBench schema.");
DEFINE_uint32(finbench_system_threads,
              16,
              "Thread count passed to GraphDb schema.system_threads.");
DEFINE_uint32(finbench_write_threads,
              8,
              "Front-end import thread count used after all dataset files are loaded.");
DEFINE_bool(finbench_write_dynamic,
            false,
            "If true, FinBench import loops use OpenMP dynamic scheduling.");
DEFINE_uint32(finbench_write_dynamic_chunk,
              1024,
              "OpenMP dynamic scheduling chunk size for FinBench import loops.");
DEFINE_uint32(finbench_read_threads,
              16,
              "Front-end query thread count used after all params files are loaded.");
DEFINE_uint32(finbench_memtable_num,
              3,
              "Memtable count used by the underlying stores.");
DEFINE_uint32(finbench_memproperty_num,
              2,
              "Compatibility name for the engine property-buffer count.");
DEFINE_bool(finbench_enable_memproperty,
            false,
            "Deprecated compatibility flag. GraphDb owns property buffering.");
DEFINE_uint32(finbench_memtable_size,
              3050403,
              "Memtable size used by the underlying stores.");
DEFINE_uint32(finbench_node_memtable_size,
              0,
              "Memtable size for node_Db. 0 means finbench_memtable_size.");
DEFINE_string(finbench_edge_memtable_sizes,
              "",
              "Comma-separated memtable sizes for edge_Db0,edge_Db1,... "
              "Empty entries or 0 use finbench_memtable_size.");
DEFINE_uint32(finbench_max_subcompactions,
              4,
              "max_subcompactions used by the underlying stores.");
DEFINE_uint64(finbench_max_vertex_num,
              0,
              "Optional override for schema.max_vertex_num. If 0, derive a "
              "safe upper bound from the dataset files.");
DEFINE_uint32(finbench_param_limit_per_query,
              0,
              "If > 0, only run this many parameter rows for each query type.");
DEFINE_uint32(finbench_workload_sample_mod,
              20,
              "Sample update/query workload rows by zero-based data row index. "
              "Rows with row_index % mod == remainder are used in mixed and "
              "final query phases. 1 disables sampling.");
DEFINE_uint32(finbench_workload_sample_remainder,
              0,
              "Remainder used with finbench_workload_sample_mod.");
DEFINE_uint64(finbench_single_edge_read_ops,
              1000000,
              "Number of random single-edge property reads to run after import. "
              "0 disables the benchmark block.");
DEFINE_bool(finbench_skip_single_edge_read,
            false,
            "If true, skip the single-edge read benchmark block.");
DEFINE_uint64(finbench_single_edge_candidate_cap,
              2000000,
              "Maximum retained candidate edge-property pairs for the single-edge "
              "read benchmark.");
DEFINE_uint64(finbench_single_edge_seed,
              20260624,
              "Deterministic seed for single-edge read candidate sampling and "
              "workload generation.");
DEFINE_uint32(finbench_single_edge_hot_weight,
              10,
              "Hot-property selection weight when generating single-edge reads. "
              "The default hot:cold ratio is 10:1.");
DEFINE_uint32(finbench_single_edge_cold_weight,
              1,
              "Cold-property selection weight when generating single-edge reads. "
              "The default hot:cold ratio is 10:1.");
DEFINE_uint64(finbench_write_latency_sample_target,
              1000000,
              "Target number of logical edge writes sampled for write latency "
              "percentiles. 0 disables write latency sampling.");
DEFINE_uint64(finbench_write_latency_sample_seed,
              20260624,
              "Deterministic seed used to mark logical edge writes for latency "
              "sampling.");
DEFINE_bool(finbench_run_mixed_workload,
            true,
            "If true, execute incremental inserts interleaved with the complex "
            "read workload before the final ordered query run.");
DEFINE_uint64(finbench_mixed_update_interleave,
              1619,
              "FinBench mixed workload update interleave in milliseconds. Query "
              "interleaves are query_frequency * this value.");
DEFINE_uint32(finbench_mixed_threads,
              16,
              "Total worker threads used by the concurrent mixed workload.");
DEFINE_uint32(finbench_batch_size,
              100000,
              "Buffered node/edge writes per FinBench import batch.");
DEFINE_uint64(finbench_load_arena_gb,
              128,
              "Resident anonymous memory reserved for the dataset load buffer. "
              "The arena is mmap'ed and page-touched before LOAD_PREPARE so "
              "unused arena space cannot be used by DB page cache. 0 disables it.");
DEFINE_bool(finbench_enable_preprocessed_loader,
            false,
            "If true, read the preprocessed binary chunk dataset instead of the "
            "legacy CSV path. The current hook validates the loader/queue path "
            "and is the entry point for the new chunk pipeline.");
DEFINE_string(finbench_preprocessed_root,
              "",
              "Preprocessed FinBench chunk root. Empty means use "
              "finbench_dataset_root.");
DEFINE_uint32(finbench_loader_threads,
              16,
              "Loader thread count for preprocessed chunks.");
DEFINE_uint32(finbench_loader_cpu_base,
              16,
              "First CPU used by preprocessed loader threads.");
DEFINE_uint32(finbench_db_cpu_base,
              0,
              "First CPU used by DB/consumer threads in preprocessed mode.");
DEFINE_uint32(finbench_loader_queue_blocks,
              64,
              "Maximum number of preprocessed chunk blocks in the loader queue.");
DEFINE_uint32(finbench_loader_prefill_blocks,
              64,
              "Number of prepared chunk blocks the loader should enqueue before "
              "the DB consumer starts. 0 disables prefill; values larger than "
              "finbench_loader_queue_blocks are clamped.");
DEFINE_uint32(finbench_loader_block_records,
              200000,
              "Maximum record count expected per preprocessed chunk block.");
DEFINE_string(finbench_update_workload_path,
              "",
              "Optional custom single-property update workload. Supported rows: "
              "N|vid|property|value and E|src|dst|edgeType|isOut|property|value. "
              "Comma delimiters are also accepted.");
DEFINE_string(finbench_update_delta_dir,
              "",
              "Deprecated compatibility flag. Engine-owned deltas are stored "
              "under each shard's property-delta directory.");
DEFINE_uint64(finbench_update_node_memproperty_cap,
              1920000,
              "Compatibility input for the engine PropertyBuffer record cap.");
DEFINE_uint64(finbench_update_edge_memproperty_cap,
              2400000,
              "Compatibility input for the engine PropertyBuffer record cap.");
DEFINE_uint64(finbench_property_buffer_bytes,
              256ULL * 1024ULL * 1024ULL,
              "Maximum estimated bytes in each engine PropertyBuffer.");
DEFINE_uint32(finbench_delta_merge_threshold,
              4,
              "Merge a property delta chain when it reaches this file count.");
DEFINE_bool(finbench_delta_crash_safe,
            true,
            "Synchronize delta files and manifest edits before publication.");

namespace {

constexpr uint8_t kNodeEdgeType = 0;

enum class NodeKind : uint8_t {
  kPerson = 1,
  kCompany = 2,
  kAccount = 3,
  kLoan = 4,
  kMedium = 5,
};

enum class RelationWriteDirection : uint8_t {
  kForwardOnly = 0,
  kReverseOnly = 1,
  kBidirectional = 2,
};

RelationWriteDirection WriteDirectionForRelation(std::string_view name) {
  if (name == "AccountTransferAccount" ||
      name == "AccountWithdrawAccount" ||
      name == "LoanDepositAccount") {
    return RelationWriteDirection::kBidirectional;
  }
  if (name == "CompanyOwnAccount" || name == "MediumSignInAccount") {
    return RelationWriteDirection::kReverseOnly;
  }
  return RelationWriteDirection::kForwardOnly;
}

struct TypedEntityKey {
  NodeKind kind = NodeKind::kPerson;
  std::string raw_id;

  bool operator==(const TypedEntityKey& other) const {
    return kind == other.kind && raw_id == other.raw_id;
  }
};

struct TypedEntityKeyHash {
  size_t operator()(const TypedEntityKey& key) const {
    const size_t h1 = std::hash<int>{}(static_cast<int>(key.kind));
    const size_t h2 = std::hash<std::string>{}(key.raw_id);
    return h1 ^ (h2 + 0x9e3779b97f4a7c15ULL + (h1 << 6U) + (h1 >> 2U));
  }
};

struct CsvHeader {
  std::vector<std::string> columns;
  std::unordered_map<std::string, size_t> name_to_idx;

  static constexpr size_t kMissingIndex = std::numeric_limits<size_t>::max();

  bool Has(std::string_view name) const {
    return name_to_idx.find(std::string(name)) != name_to_idx.end();
  }

  bool Has(const std::string& name) const {
    return name_to_idx.find(name) != name_to_idx.end();
  }

  size_t Index(std::string_view name) const {
    const auto it = name_to_idx.find(std::string(name));
    return it == name_to_idx.end() ? kMissingIndex : it->second;
  }

  size_t Index(const std::string& name) const {
    const auto it = name_to_idx.find(name);
    return it == name_to_idx.end() ? kMissingIndex : it->second;
  }

  size_t Index(const char* name) const {
    if (name == nullptr) {
      return kMissingIndex;
    }
    const auto it = name_to_idx.find(name);
    return it == name_to_idx.end() ? kMissingIndex : it->second;
  }
};

struct CsvRow {
  const CsvHeader* header = nullptr;
  std::vector<std::string> fields;

  const std::string& Get(std::string_view name) const {
    static const std::string kEmpty;
    if (header == nullptr) {
      return kEmpty;
    }
    const size_t idx = header->Index(name);
    if (idx == CsvHeader::kMissingIndex || idx >= fields.size()) {
      return kEmpty;
    }
    return fields[idx];
  }

  const std::string& Get(const std::string& name) const {
    static const std::string kEmpty;
    if (header == nullptr) {
      return kEmpty;
    }
    const size_t idx = header->Index(name);
    if (idx == CsvHeader::kMissingIndex || idx >= fields.size()) {
      return kEmpty;
    }
    return fields[idx];
  }

  const std::string& Get(const char* name) const {
    static const std::string kEmpty;
    if (header == nullptr) {
      return kEmpty;
    }
    const size_t idx = header->Index(name);
    if (idx == CsvHeader::kMissingIndex || idx >= fields.size()) {
      return kEmpty;
    }
    return fields[idx];
  }

  const std::string& GetByIndex(size_t idx) const {
    static const std::string kEmpty;
    return idx < fields.size() ? fields[idx] : kEmpty;
  }
};

struct LoadedCsvFile {
  std::shared_ptr<CsvHeader> header;
  std::vector<CsvRow> rows;
};

struct NodeTableSpec {
  const char* file_name = "";
  NodeKind kind = NodeKind::kPerson;
  const char* id_column = "";
  std::vector<std::string> property_columns;
};

struct RelationSpec {
  std::string name;
  NodeKind src_kind = NodeKind::kPerson;
  NodeKind dst_kind = NodeKind::kPerson;
  std::string source_column;
  std::string dest_column;
  std::vector<std::string> property_columns;
  bool temporal = false;
  uint8_t edge_type = 0;
};

struct LoadedParamFile {
  int query_id = 0;
  LoadedCsvFile data;
};

struct TraversedEdge {
  vertex_t other_id = lsmgraph::INVALID_VERTEX_ID;
  std::unordered_map<std::string, std::string> properties;
};

struct QueryRunResult {
  uint64_t rows = 0;
  uint64_t checksum = 0;
};

struct QueryMetrics {
  int query_id = 0;
  uint64_t param_rows = 0;
  uint64_t result_rows = 0;
  uint64_t checksum = 0;
  double sec = 0.0;
};

struct ImportStats {
  uint64_t logical_rows = 0;
  uint64_t node_writes = 0;
  uint64_t edge_writes = 0;
  uint64_t entity_nodes = 0;
  double sec = 0.0;
  double background_wait_sec = 0.0;
  double wall_sec = 0.0;
};

struct UpdateWorkloadStats {
  uint64_t logical_rows = 0;
  uint64_t node_updates = 0;
  uint64_t edge_updates = 0;
  uint64_t missing_targets = 0;
  uint64_t cold_blob_rewrites = 0;
  uint64_t flushes = 0;
  uint64_t delta_files = 0;
  uint64_t value_bytes = 0;
  uint64_t blob_payload_bytes = 0;
  uint64_t engine_submitted_bytes = 0;
  double sec = 0.0;
  double locate_sec = 0.0;
  double cold_rewrite_sec = 0.0;
  double write_sec = 0.0;
};

struct MixedWorkloadStats {
  uint64_t node_writes = 0;
  uint64_t edge_writes = 0;
  uint64_t query_ops = 0;
  uint64_t result_rows = 0;
  uint64_t checksum = 0;
  double total_sec = 0.0;
  double write_sec = 0.0;
  double query_sec = 0.0;
  double background_wait_sec = 0.0;
  double wall_sec = 0.0;
  std::array<QueryMetrics, 13> query_metrics;
};

struct MixedQueryTask {
  int query_id = 0;
  const CsvRow* row = nullptr;
  uint64_t ordinal = 0;
};

struct MixedQueryStream {
  int query_id = 0;
  const LoadedParamFile* file = nullptr;
  std::vector<size_t> row_indices;
  size_t next_row = 0;
};

struct SingleEdgeReadCandidate {
  vertex_t src = lsmgraph::INVALID_VERTEX_ID;
  vertex_t dst = lsmgraph::INVALID_VERTEX_ID;
  uint16_t property_id = 0;
  uint16_t cold_slot = 0;
  uint8_t edge_type = 0;
  bool is_out = true;
};

struct SingleEdgePropertyDef {
  std::string name;
  bool cold = false;
};

struct SingleEdgeReadMetrics {
  uint64_t ops = 0;
  uint64_t hot_ops = 0;
  uint64_t cold_ops = 0;
  uint64_t found = 0;
  uint64_t missed = 0;
  uint64_t checksum = 0;
  double sec = 0.0;
};

uint64_t WriteLatencyHashMix(uint64_t x) {
  x += 0x9e3779b97f4a7c15ULL;
  x = (x ^ (x >> 30U)) * 0xbf58476d1ce4e5b9ULL;
  x = (x ^ (x >> 27U)) * 0x94d049bb133111ebULL;
  return x ^ (x >> 31U);
}

class WriteLatencySampler {
 public:
  void Reset(uint64_t target_samples,
             uint64_t estimated_total_writes,
             uint64_t seed,
             uint32_t thread_count) {
    target_samples_ = target_samples;
    estimated_total_writes_ = estimated_total_writes;
    seed_ = seed;
    next_mark_id_ = 0;
    actual_marked_ = 0;
    samples_by_thread_.clear();
    stride_ = 0;
    if (target_samples_ == 0 || estimated_total_writes_ == 0) {
      return;
    }
    if (estimated_total_writes_ <= target_samples_) {
      stride_ = 1;
    } else {
      stride_ =
          std::max<uint64_t>(1ULL,
                             (estimated_total_writes_ + target_samples_ / 2ULL) /
                                 target_samples_);
    }
    const uint32_t threads = std::max<uint32_t>(1U, thread_count);
    samples_by_thread_.resize(threads);
    const uint64_t expected_samples =
        stride_ == 0 ? 0 : estimated_total_writes_ / stride_ + threads;
    const size_t reserve_per_thread = static_cast<size_t>(
        std::min<uint64_t>(expected_samples / threads + 1024ULL,
                           static_cast<uint64_t>(std::numeric_limits<size_t>::max())));
    for (auto& samples : samples_by_thread_) {
      samples.reserve(reserve_per_thread);
    }
  }

  bool MarkNext() {
    const uint64_t id = next_mark_id_++;
    if (stride_ == 0) {
      return false;
    }
    const bool sample = stride_ == 1 ||
                        (WriteLatencyHashMix(id ^ seed_) % stride_ == 0);
    if (sample) {
      ++actual_marked_;
    }
    return sample;
  }

  void Record(uint32_t thread_id, uint64_t latency_ns) {
    if (samples_by_thread_.empty()) {
      return;
    }
    if (thread_id >= samples_by_thread_.size()) {
      thread_id = static_cast<uint32_t>(samples_by_thread_.size() - 1U);
    }
    samples_by_thread_[thread_id].push_back(latency_ns);
  }

  void Print(const char* phase) const {
    uint64_t actual_samples = 0;
    for (const auto& samples : samples_by_thread_) {
      actual_samples += samples.size();
    }
    std::cout << "[" << phase << "] target_samples: " << target_samples_
              << std::endl;
    std::cout << "[" << phase << "] estimated_total_writes: "
              << estimated_total_writes_ << std::endl;
    std::cout << "[" << phase << "] stride: " << stride_ << std::endl;
    std::cout << "[" << phase << "] marked_samples: " << actual_marked_
              << std::endl;
    std::cout << "[" << phase << "] actual_samples: " << actual_samples
              << std::endl;
    if (actual_samples == 0) {
      return;
    }

    std::vector<uint64_t> merged;
    merged.reserve(static_cast<size_t>(actual_samples));
    for (const auto& samples : samples_by_thread_) {
      merged.insert(merged.end(), samples.begin(), samples.end());
    }
    std::sort(merged.begin(), merged.end());
    long double sum_ns = 0.0L;
    for (uint64_t ns : merged) {
      sum_ns += static_cast<long double>(ns);
    }
    const auto percentile = [&](uint32_t pct) -> uint64_t {
      size_t idx = static_cast<size_t>(
          (static_cast<unsigned long long>(merged.size()) * pct + 99ULL) /
          100ULL);
      if (idx == 0) {
        idx = 1;
      }
      --idx;
      if (idx >= merged.size()) {
        idx = merged.size() - 1U;
      }
      return merged[idx];
    };
    const auto ns_to_us = [](uint64_t ns) {
      return static_cast<double>(ns) / 1000.0;
    };
    const double avg_us =
        static_cast<double>(sum_ns / static_cast<long double>(merged.size())) /
        1000.0;
    std::cout << "[" << phase << "] avg_us: " << avg_us << std::endl;
    std::cout << "[" << phase << "] p90_us: " << ns_to_us(percentile(90))
              << std::endl;
    std::cout << "[" << phase << "] p95_us: " << ns_to_us(percentile(95))
              << std::endl;
    std::cout << "[" << phase << "] p99_us: " << ns_to_us(percentile(99))
              << std::endl;
    std::cout << "[" << phase << "] max_us: " << ns_to_us(merged.back())
              << std::endl;
  }

 private:
  uint64_t target_samples_ = 0;
  uint64_t estimated_total_writes_ = 0;
  uint64_t seed_ = 0;
  uint64_t stride_ = 0;
  uint64_t next_mark_id_ = 0;
  uint64_t actual_marked_ = 0;
  std::vector<std::vector<uint64_t>> samples_by_thread_;
};

class SingleEdgeReadSampler {
 public:
  void Reset(uint64_t total_cap,
             uint32_t hot_weight,
             uint32_t cold_weight,
             uint64_t seed) {
    properties_.clear();
    property_to_id_.clear();
    hot_candidates_.clear();
    cold_candidates_.clear();
    hot_indices_by_property_.clear();
    cold_indices_by_property_.clear();
    active_hot_properties_.clear();
    active_cold_properties_.clear();
    hot_seen_ = 0;
    cold_seen_ = 0;
    const uint64_t hot = std::max<uint32_t>(1U, hot_weight);
    const uint64_t cold = std::max<uint32_t>(1U, cold_weight);
    const uint64_t denom = hot + cold;
    hot_cap_ = denom == 0 ? total_cap : (total_cap * hot) / denom;
    if (total_cap > 0 && hot_cap_ == 0 && hot > 0) {
      hot_cap_ = 1;
    }
    cold_cap_ = total_cap > hot_cap_ ? total_cap - hot_cap_ : 0;
    rng_.seed(seed ^ 0x9e3779b97f4a7c15ULL);
  }

  uint16_t RegisterProperty(const std::string& name, bool cold) {
    const auto it = property_to_id_.find(name);
    if (it != property_to_id_.end()) {
      return it->second;
    }
    const uint16_t id = static_cast<uint16_t>(properties_.size());
    properties_.push_back(SingleEdgePropertyDef{name, cold});
    property_to_id_.emplace(name, id);
    return id;
  }

  void AddCandidate(SingleEdgeReadCandidate candidate) {
    if (candidate.property_id >= properties_.size()) {
      return;
    }
    if (properties_[candidate.property_id].cold) {
      AddReservoirCandidate(&cold_candidates_, cold_cap_, &cold_seen_, candidate);
    } else {
      AddReservoirCandidate(&hot_candidates_, hot_cap_, &hot_seen_, candidate);
    }
  }

  void Finalize() {
    hot_indices_by_property_.assign(properties_.size(), {});
    cold_indices_by_property_.assign(properties_.size(), {});
    active_hot_properties_.clear();
    active_cold_properties_.clear();
    for (uint32_t i = 0; i < hot_candidates_.size(); ++i) {
      const uint16_t prop = hot_candidates_[i].property_id;
      hot_indices_by_property_[prop].push_back(i);
    }
    for (uint32_t i = 0; i < cold_candidates_.size(); ++i) {
      const uint16_t prop = cold_candidates_[i].property_id;
      cold_indices_by_property_[prop].push_back(i);
    }
    for (uint16_t prop = 0; prop < properties_.size(); ++prop) {
      if (!hot_indices_by_property_[prop].empty()) {
        active_hot_properties_.push_back(prop);
      }
      if (!cold_indices_by_property_[prop].empty()) {
        active_cold_properties_.push_back(prop);
      }
    }
    std::sort(active_hot_properties_.begin(), active_hot_properties_.end(),
              [&](uint16_t a, uint16_t b) {
                return properties_[a].name < properties_[b].name;
              });
    std::sort(active_cold_properties_.begin(), active_cold_properties_.end(),
              [&](uint16_t a, uint16_t b) {
                return properties_[a].name < properties_[b].name;
              });
  }

  std::vector<SingleEdgeReadCandidate> BuildRequests(
      uint64_t ops,
      uint32_t hot_weight,
      uint32_t cold_weight,
      uint64_t seed) const {
    std::vector<SingleEdgeReadCandidate> requests;
    requests.reserve(static_cast<size_t>(
        std::min<uint64_t>(ops, static_cast<uint64_t>(std::numeric_limits<size_t>::max()))));
    if (ops == 0 || (active_hot_properties_.empty() &&
                     active_cold_properties_.empty())) {
      return requests;
    }

    std::mt19937_64 rng(seed ^ 0xbf58476d1ce4e5b9ULL);
    const uint32_t hot = std::max<uint32_t>(1U, hot_weight);
    const uint32_t cold = std::max<uint32_t>(1U, cold_weight);
    for (uint64_t i = 0; i < ops; ++i) {
      bool choose_cold = false;
      if (!active_hot_properties_.empty() && !active_cold_properties_.empty()) {
        std::uniform_int_distribution<uint32_t> group_dist(1, hot + cold);
        choose_cold = group_dist(rng) > hot;
      } else {
        choose_cold = active_hot_properties_.empty();
      }

      const auto& active_props =
          choose_cold ? active_cold_properties_ : active_hot_properties_;
      std::uniform_int_distribution<size_t> prop_dist(0, active_props.size() - 1);
      const uint16_t prop = active_props[prop_dist(rng)];
      const auto& indices = choose_cold ? cold_indices_by_property_[prop]
                                        : hot_indices_by_property_[prop];
      std::uniform_int_distribution<size_t> cand_dist(0, indices.size() - 1);
      const uint32_t idx = indices[cand_dist(rng)];
      requests.push_back(choose_cold ? cold_candidates_[idx]
                                     : hot_candidates_[idx]);
    }
    return requests;
  }

  const SingleEdgePropertyDef& Property(uint16_t id) const {
    return properties_[id];
  }

  uint64_t retained_candidates() const {
    return static_cast<uint64_t>(hot_candidates_.size()) + cold_candidates_.size();
  }
  uint64_t retained_hot_candidates() const { return hot_candidates_.size(); }
  uint64_t retained_cold_candidates() const { return cold_candidates_.size(); }
  uint64_t seen_hot_candidates() const { return hot_seen_; }
  uint64_t seen_cold_candidates() const { return cold_seen_; }
  size_t active_hot_property_count() const { return active_hot_properties_.size(); }
  size_t active_cold_property_count() const { return active_cold_properties_.size(); }

 private:
  void AddReservoirCandidate(std::vector<SingleEdgeReadCandidate>* candidates,
                             uint64_t cap,
                             uint64_t* seen,
                             const SingleEdgeReadCandidate& candidate) {
    if (candidates == nullptr || seen == nullptr || cap == 0) {
      return;
    }
    ++(*seen);
    if (candidates->size() < cap) {
      candidates->push_back(candidate);
      return;
    }
    std::uniform_int_distribution<uint64_t> dist(0, *seen - 1);
    const uint64_t pos = dist(rng_);
    if (pos < cap) {
      (*candidates)[static_cast<size_t>(pos)] = candidate;
    }
  }

  std::vector<SingleEdgePropertyDef> properties_;
  std::unordered_map<std::string, uint16_t> property_to_id_;
  std::vector<SingleEdgeReadCandidate> hot_candidates_;
  std::vector<SingleEdgeReadCandidate> cold_candidates_;
  std::vector<std::vector<uint32_t>> hot_indices_by_property_;
  std::vector<std::vector<uint32_t>> cold_indices_by_property_;
  std::vector<uint16_t> active_hot_properties_;
  std::vector<uint16_t> active_cold_properties_;
  uint64_t hot_seen_ = 0;
  uint64_t cold_seen_ = 0;
  uint64_t hot_cap_ = 0;
  uint64_t cold_cap_ = 0;
  std::mt19937_64 rng_;
};

using PmrString = std::pmr::string;

struct PreparedNodeWrite {
  vertex_t id = lsmgraph::INVALID_VERTEX_ID;
  PmrString payload;
  PmrString node_cold_payload;
  bool has_node_cold_payload = false;
  bool sample_write_latency = false;
  uint64_t scheduled_time = 0;

  explicit PreparedNodeWrite(
      std::pmr::memory_resource* mr = std::pmr::get_default_resource())
      : payload(mr), node_cold_payload(mr) {}

  PreparedNodeWrite(vertex_t id_in,
                    PmrString payload_in,
                    uint64_t scheduled_time_in)
      : id(id_in),
        payload(std::move(payload_in)),
        scheduled_time(scheduled_time_in) {}

  PreparedNodeWrite(vertex_t id_in,
                    PmrString payload_in,
                    PmrString node_cold_payload_in,
                    bool has_node_cold_payload_in,
                    uint64_t scheduled_time_in)
      : id(id_in),
        payload(std::move(payload_in)),
        node_cold_payload(std::move(node_cold_payload_in)),
        has_node_cold_payload(has_node_cold_payload_in),
        scheduled_time(scheduled_time_in) {}
};

struct PreparedRelationWrite {
  uint8_t edge_type = 0;
  vertex_t src = lsmgraph::INVALID_VERTEX_ID;
  vertex_t dst = lsmgraph::INVALID_VERTEX_ID;
  RelationWriteDirection write_direction = RelationWriteDirection::kForwardOnly;
  const char* rel_name = "";
  PmrString edge_shard0_payload;  // hot fixed payload for Shard 0
  PmrString cold_payload;         // variable cold payload, written to blob at import time
  bool has_cold_payload = false;
  bool sample_write_latency = false;
  uint64_t scheduled_time = 0;

  explicit PreparedRelationWrite(
      std::pmr::memory_resource* mr = std::pmr::get_default_resource())
      : edge_shard0_payload(mr), cold_payload(mr) {}
};

struct PreparedImportBatch {
  PmrString name;
  uint64_t logical_rows = 0;
  uint64_t new_entity_nodes = 0;
  std::pmr::vector<PreparedNodeWrite> node_writes;
  std::pmr::vector<PreparedRelationWrite> relation_writes;

  explicit PreparedImportBatch(
      std::pmr::memory_resource* mr = std::pmr::get_default_resource())
      : name(mr), node_writes(mr), relation_writes(mr) {}
};

struct LightNodeUpdate {
  vertex_t vid = lsmgraph::INVALID_VERTEX_ID;
  uint32_t logical_property_id = 0;
  uint32_t storage_property_id = 0;
  uint16_t cold_slot = 0;
  std::string storage_property;
  std::string value;
  bool cold = false;
};

struct LightEdgeUpdate {
  vertex_t src = lsmgraph::INVALID_VERTEX_ID;
  vertex_t dst = lsmgraph::INVALID_VERTEX_ID;
  uint8_t edge_type = 0;
  bool is_out = true;
  uint32_t logical_property_id = 0;
  uint32_t storage_property_id = 0;
  uint16_t cold_slot = 0;
  std::string storage_property;
  std::string value;
  bool cold = false;
};

struct EngineUpdateRunState {
  UpdateWorkloadStats stats;
  lsmgraph::PropertyUpdateManagerStats initial_engine_stats;
  std::string phase;
  std::chrono::steady_clock::time_point start_time;
  std::chrono::steady_clock::time_point last_node_report_time;
  std::chrono::steady_clock::time_point last_edge_report_time;
  uint64_t next_node_report = 1000000;
  uint64_t next_edge_report = 1000000;
  uint64_t last_node_report_updates = 0;
  uint64_t last_edge_report_updates = 0;
  bool started = false;
};

std::string Trim(std::string s) {
  while (!s.empty() && std::isspace(static_cast<unsigned char>(s.front()))) {
    s.erase(s.begin());
  }
  while (!s.empty() && std::isspace(static_cast<unsigned char>(s.back()))) {
    s.pop_back();
  }
  return s;
}

bool EndsWith(const std::string& s, const std::string& suffix) {
  return s.size() >= suffix.size() &&
         s.compare(s.size() - suffix.size(), suffix.size(), suffix) == 0;
}

bool IsDigitsOnly(const std::string& s) {
  if (s.empty()) {
    return false;
  }
  for (char c : s) {
    if (!std::isdigit(static_cast<unsigned char>(c))) {
      return false;
    }
  }
  return true;
}

std::vector<std::string> SplitPipe(const std::string& line) {
  std::vector<std::string> out;
  size_t begin = 0;
  while (begin <= line.size()) {
    const size_t sep = line.find('|', begin);
    if (sep == std::string::npos) {
      out.push_back(line.substr(begin));
      break;
    }
    out.push_back(line.substr(begin, sep - begin));
    begin = sep + 1;
  }
  if (!line.empty() && line.back() == '|') {
    out.emplace_back();
  }
  return out;
}

std::vector<std::string> SplitByDelimiter(const std::string& line, char delimiter) {
  std::vector<std::string> out;
  size_t begin = 0;
  while (begin <= line.size()) {
    const size_t sep = line.find(delimiter, begin);
    if (sep == std::string::npos) {
      out.push_back(line.substr(begin));
      break;
    }
    out.push_back(line.substr(begin, sep - begin));
    begin = sep + 1;
  }
  if (!line.empty() && line.back() == delimiter) {
    out.emplace_back();
  }
  return out;
}

uint64_t HashMix(uint64_t x) {
  x ^= x >> 33U;
  x *= 0xff51afd7ed558ccdULL;
  x ^= x >> 33U;
  x *= 0xc4ceb9fe1a85ec53ULL;
  x ^= x >> 33U;
  return x;
}

uint64_t HashStrings(const std::vector<std::string>& values) {
  uint64_t seed = 0x9e3779b97f4a7c15ULL;
  for (const auto& v : values) {
    seed ^= HashMix(std::hash<std::string>{}(v))
            + 0x9e3779b97f4a7c15ULL + (seed << 6U) + (seed >> 2U);
  }
  return seed;
}

double ToDouble(const std::string& s) {
  if (s.empty()) {
    return 0.0;
  }
  try {
    return std::stod(s);
  } catch (...) {
    return 0.0;
  }
}

uint64_t ToUint64(const std::string& s) {
  if (s.empty()) {
    return 0;
  }
  try {
    return static_cast<uint64_t>(std::stoull(s));
  } catch (...) {
    return 0;
  }
}

// FinBench mixes two time formats:
// 1) snapshot/*.csv: "2020-06-13 14:37:13.843"
// 2) incremental/params: epoch milliseconds in decimal form
// To keep a fixed 20-byte property schema while still supporting comparisons,
// the importer normalizes the stored time values to epoch milliseconds.
uint64_t ParseTimeToMillis(const std::string& raw) {
  const std::string value = Trim(raw);
  if (value.empty()) {
    return 0;
  }
  if (IsDigitsOnly(value)) {
    return ToUint64(value);
  }

  std::string datetime_part = value;
  int millis = 0;
  const size_t dot = value.find('.');
  if (dot != std::string::npos) {
    datetime_part = value.substr(0, dot);
    std::string ms_part = value.substr(dot + 1);
    while (ms_part.size() < 3U) {
      ms_part.push_back('0');
    }
    if (ms_part.size() > 3U) {
      ms_part = ms_part.substr(0, 3);
    }
    millis = static_cast<int>(ToUint64(ms_part));
  }

  std::tm tm = {};
  std::istringstream iss(datetime_part);
  if (datetime_part.find(' ') != std::string::npos) {
    iss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");
  } else {
    iss >> std::get_time(&tm, "%Y-%m-%d");
  }
  if (iss.fail()) {
    return 0;
  }

  const time_t seconds = timegm(&tm);
  if (seconds < 0) {
    return 0;
  }
  return static_cast<uint64_t>(seconds) * 1000ULL
       + static_cast<uint64_t>(millis);
}

bool IsTimeLikeProperty(const std::string& name) {
  return name == "createTime" ||
         name == "dependencyTime" ||
         name == "lastLoginTime" ||
         name == "deleteTime" ||
         name == "dependentDate";
}

std::string NormalizePropertyValue(const std::string& name,
                                   const std::string& value) {
  if (value.empty()) {
    return value;
  }
  if (IsTimeLikeProperty(name)) {
    return std::to_string(ParseTimeToMillis(value));
  }
  return value;
}

std::string NodeKindToString(NodeKind kind) {
  switch (kind) {
    case NodeKind::kPerson:
      return "Person";
    case NodeKind::kCompany:
      return "Company";
    case NodeKind::kAccount:
      return "Account";
    case NodeKind::kLoan:
      return "Loan";
    case NodeKind::kMedium:
      return "Medium";
  }
  return "Unknown";
}

const std::vector<NodeTableSpec>& SnapshotNodeTables() {
  static const std::vector<NodeTableSpec> specs = {
      {"Person.csv",
       NodeKind::kPerson,
       "personId",
       {"personName", "isBlocked", "createTime", "gender",
        "birthday", "country", "city"}},
      {"Company.csv",
       NodeKind::kCompany,
       "companyId",
       {"companyName", "isBlocked", "createTime", "country",
        "city", "business", "description", "url"}},
      {"Account.csv",
       NodeKind::kAccount,
       "accountId",
       {"createTime", "isBlocked", "accountType", "nickname", "phonenum",
        "email", "freqLoginType", "lastLoginTime", "accountLevel"}},
      {"Loan.csv",
       NodeKind::kLoan,
       "loanId",
       {"loanAmount", "balance", "createTime", "loanUsage", "interestRate"}},
      {"Medium.csv",
       NodeKind::kMedium,
       "mediumId",
       {"mediumType", "isBlocked", "createTime", "lastLoginTime",
        "riskLevel"}},
  };
  return specs;
}

const std::vector<std::string>& AllNodeProperties() {
  static const std::vector<std::string> props = {
      "nodeLabel",
      "rawId",
      "personName",
      "isBlocked",
      "createTime",
      "gender",
      "birthday",
      "country",
      "city",
      "companyName",
      "business",
      "description",
      "url",
      "accountType",
      "nickname",
      "phonenum",
      "email",
      "freqLoginType",
      "lastLoginTime",
      "accountLevel",
      "loanAmount",
      "balance",
      "loanUsage",
      "interestRate",
      "mediumType",
      "riskLevel",
  };
  return props;
}

const std::vector<std::string>& AllEdgeProperties() {
  static const std::vector<std::string> props = {
      "createTime",
      "amount",
      "cold_property",
  };
  return props;
}

const std::vector<std::string>& ColdEdgeProperties() {
  static const std::vector<std::string> props = {
      "dependencyTime",
      "comment",
      "orderNum",
      "payType",
      "goodsType",
      "fromType",
      "toType",
      "loanAmount",
      "org",
      "relation",
      "ratio",
      "location",
  };
  return props;
}

constexpr size_t kColdEdgePropertyCount = 12;
constexpr std::string_view kGeneratedColdPrefix = "cold_extra_";
constexpr size_t kBlobBufferBytes = 4ULL * 1024ULL * 1024ULL;
constexpr size_t kNodeColdBlobBufferBytes = kBlobBufferBytes;

const std::vector<std::string>& NodeDbProperties() {
  static const std::vector<std::string> props = {
      "rawId",
      "isBlocked",
      "loanAmount",
      "balance",
      "node_cold_property",
  };
  return props;
}

const std::vector<std::string>& ColdNodeProperties() {
  static const std::vector<std::string> props = {
      "nodeLabel",
      "personName",
      "createTime",
      "gender",
      "birthday",
      "country",
      "city",
      "companyName",
      "business",
      "description",
      "url",
      "accountType",
      "nickname",
      "phonenum",
      "email",
      "freqLoginType",
      "lastLoginTime",
      "accountLevel",
      "loanUsage",
      "interestRate",
      "mediumType",
      "riskLevel",
  };
  return props;
}

const std::unordered_map<std::string, size_t>& NodePropertyIndexByName() {
  static const std::unordered_map<std::string, size_t> map = [] {
    std::unordered_map<std::string, size_t> out;
    const auto& props = NodeDbProperties();
    for (size_t i = 0; i < props.size(); ++i) {
      out.emplace(props[i], i);
    }
    return out;
  }();
  return map;
}

const std::unordered_map<std::string, size_t>& ColdNodePropertyIndex() {
  static const std::unordered_map<std::string, size_t> map = [] {
    std::unordered_map<std::string, size_t> out;
    const auto& props = ColdNodeProperties();
    for (size_t i = 0; i < props.size(); ++i) {
      out.emplace(props[i], i);
    }
    return out;
  }();
  return map;
}

bool IsColdNodeProperty(const std::string& name) {
  return ColdNodePropertyIndex().find(name) != ColdNodePropertyIndex().end();
}

uint16_t NodeColdRefSlot() {
  const auto& index = NodePropertyIndexByName();
  const auto it = index.find("node_cold_property");
  assert(it != index.end());
  return static_cast<uint16_t>(it->second);
}

const std::unordered_map<std::string, size_t>& EdgePropertyIndexByName() {
  static const std::unordered_map<std::string, size_t> map = [] {
    std::unordered_map<std::string, size_t> out;
    const auto& props = AllEdgeProperties();
    for (size_t i = 0; i < props.size(); ++i) {
      out.emplace(props[i], i);
    }
    return out;
  }();
  return map;
}

// ---- 2-Shard Edge Property Split ----
// Shard 0 (Hot): attributes used by the current 12 complex-read queries.
const std::vector<std::string>& EdgeShard0Properties() {
  static const std::vector<std::string> props = {
      "createTime",
      "amount",
  };
  return props;
}

// Shard 1 (Cold): fixed-size reference to an append-only cold-property blob.
const std::vector<std::string>& EdgeShard1Properties() {
  static const std::vector<std::string> props = {
      "cold_property",
  };
  return props;
}

const std::unordered_map<std::string, size_t>& EdgeShard0PropertyIndex() {
  static const std::unordered_map<std::string, size_t> map = [] {
    std::unordered_map<std::string, size_t> out;
    const auto& props = EdgeShard0Properties();
    for (size_t i = 0; i < props.size(); ++i) {
      out.emplace(props[i], i);
    }
    return out;
  }();
  return map;
}

const std::unordered_map<std::string, size_t>& EdgeShard1PropertyIndex() {
  static const std::unordered_map<std::string, size_t> map = [] {
    std::unordered_map<std::string, size_t> out;
    const auto& props = EdgeShard1Properties();
    for (size_t i = 0; i < props.size(); ++i) {
      out.emplace(props[i], i);
    }
    return out;
  }();
  return map;
}

const std::unordered_map<std::string, size_t>& ColdEdgePropertyIndex() {
  static const std::unordered_map<std::string, size_t> map = [] {
    std::unordered_map<std::string, size_t> out;
    const auto& props = ColdEdgeProperties();
    for (size_t i = 0; i < props.size(); ++i) {
      out.emplace(props[i], i);
    }
    return out;
  }();
  return map;
}

int ColdEdgePropertySlot(const std::string& name) {
  if (name == "dependencyTime") return 0;
  if (name == "comment") return 1;
  if (name == "orderNum") return 2;
  if (name == "payType") return 3;
  if (name == "goodsType") return 4;
  if (name == "fromType") return 5;
  if (name == "toType") return 6;
  if (name == "loanAmount") return 7;
  if (name == "org") return 8;
  if (name == "relation") return 9;
  if (name == "ratio") return 10;
  if (name == "location") return 11;
  return -1;
}

bool IsGeneratedColdProperty(std::string_view name) {
  return name.size() > kGeneratedColdPrefix.size() &&
         name.substr(0, kGeneratedColdPrefix.size()) == kGeneratedColdPrefix;
}

uint32_t GeneratedColdPropertyOrdinal(std::string_view name) {
  if (!IsGeneratedColdProperty(name)) {
    return 0;
  }
  const std::string suffix(name.substr(kGeneratedColdPrefix.size()));
  if (!IsDigitsOnly(suffix)) {
    return 0;
  }
  return static_cast<uint32_t>(ToUint64(suffix));
}

bool ResolveNodeUpdateProperty(const std::string& token,
                               uint32_t* logical_property_id,
                               uint32_t* storage_property_id,
                               uint16_t* cold_slot,
                               std::string* storage_property,
                               std::string* logical_name,
                               bool* cold) {
  if (logical_property_id == nullptr || storage_property_id == nullptr ||
      cold_slot == nullptr || storage_property == nullptr ||
      logical_name == nullptr || cold == nullptr) {
    return false;
  }
  *cold = false;
  *cold_slot = 0;
  if (IsDigitsOnly(token)) {
    const uint32_t id = static_cast<uint32_t>(ToUint64(token));
    const auto& props = AllNodeProperties();
    if (id >= props.size()) {
      return false;
    }
    *logical_property_id = id;
    *logical_name = props[id];
  } else {
    const auto& props = AllNodeProperties();
    const auto it = std::find(props.begin(), props.end(), token);
    if (it == props.end()) {
      return false;
    }
    *logical_property_id =
        static_cast<uint32_t>(std::distance(props.begin(), it));
    *logical_name = token;
  }

  const auto node_it = NodePropertyIndexByName().find(*logical_name);
  if (node_it != NodePropertyIndexByName().end()) {
    *storage_property = *logical_name;
    *storage_property_id = static_cast<uint32_t>(node_it->second);
    return true;
  }
  const auto cold_it = ColdNodePropertyIndex().find(*logical_name);
  if (cold_it != ColdNodePropertyIndex().end()) {
    *storage_property = "node_cold_property";
    *storage_property_id = NodeColdRefSlot();
    *cold_slot = static_cast<uint16_t>(cold_it->second);
    *cold = true;
    return true;
  }
  return false;
}

int EdgeColdSlotByName(const std::string& name) {
  const int slot = ColdEdgePropertySlot(name);
  if (slot >= 0) {
    return slot;
  }
  if (!IsGeneratedColdProperty(name)) {
    return -1;
  }
  const uint32_t ordinal = GeneratedColdPropertyOrdinal(name);
  if (ordinal == 0) {
    return -1;
  }
  return static_cast<int>(kColdEdgePropertyCount + ordinal - 1U);
}

bool ResolveEdgeUpdateProperty(const std::string& token,
                               uint32_t* logical_property_id,
                               uint32_t* storage_property_id,
                               uint16_t* cold_slot,
                               std::string* storage_property,
                               std::string* logical_name,
                               bool* cold) {
  if (logical_property_id == nullptr || storage_property_id == nullptr ||
      cold_slot == nullptr || storage_property == nullptr ||
      logical_name == nullptr || cold == nullptr) {
    return false;
  }
  *cold = false;
  *cold_slot = 0;
  if (IsDigitsOnly(token)) {
    const uint32_t id = static_cast<uint32_t>(ToUint64(token));
    const auto& hot = AllEdgeProperties();
    if (id < hot.size()) {
      *logical_property_id = id;
      *logical_name = hot[id];
    } else {
      const uint32_t cold_base = static_cast<uint32_t>(hot.size());
      const uint32_t cold_id = id - cold_base;
      const auto& cold = ColdEdgeProperties();
      if (cold_id < cold.size()) {
        *logical_property_id = id;
        *logical_name = cold[cold_id];
      } else {
        *logical_property_id = id;
        *logical_name = "cold_extra_" + std::to_string(cold_id - cold.size() + 1U);
      }
    }
  } else {
    const auto& hot = AllEdgeProperties();
    const auto hot_it = std::find(hot.begin(), hot.end(), token);
    if (hot_it != hot.end()) {
      *logical_property_id =
          static_cast<uint32_t>(std::distance(hot.begin(), hot_it));
      *logical_name = token;
    } else {
      const auto& cold = ColdEdgeProperties();
      const auto cold_it = std::find(cold.begin(), cold.end(), token);
      if (cold_it != cold.end()) {
        *logical_property_id =
            static_cast<uint32_t>(hot.size() +
                                  std::distance(cold.begin(), cold_it));
        *logical_name = token;
      } else if (IsGeneratedColdProperty(token)) {
        *logical_property_id =
            static_cast<uint32_t>(hot.size() + cold.size() +
                                  GeneratedColdPropertyOrdinal(token));
        *logical_name = token;
      } else {
        return false;
      }
    }
  }

  const auto hot_it = EdgeShard0PropertyIndex().find(*logical_name);
  if (hot_it != EdgeShard0PropertyIndex().end()) {
    *storage_property = *logical_name;
    *storage_property_id = static_cast<uint32_t>(hot_it->second);
    return true;
  }
  *storage_property = "cold_property";
  const auto cold_ref_it = EdgeShard1PropertyIndex().find(*storage_property);
  if (cold_ref_it == EdgeShard1PropertyIndex().end()) {
    return false;
  }
  const int slot = EdgeColdSlotByName(*logical_name);
  if (slot < 0 || slot > std::numeric_limits<uint16_t>::max()) {
    return false;
  }
  *storage_property_id = static_cast<uint32_t>(cold_ref_it->second);
  *cold_slot = static_cast<uint16_t>(slot);
  *cold = true;
  return true;
}

// Query-optimized minimal property sets.
// All properties in these sets are in Shard 0, so queries only touch Shard 0.
const std::vector<std::string>& EdgeQueryTemporalProps() {
  static const std::vector<std::string> props = {"createTime"};
  return props;
}

const std::vector<std::string>& EdgeQueryAmountProps() {
  static const std::vector<std::string> props = {"createTime", "amount"};
  return props;
}

const std::vector<RelationSpec>& AllRelations() {
  static const std::vector<RelationSpec> specs = {
      {"AccountTransferAccount", NodeKind::kAccount,
       NodeKind::kAccount, "fromId", "toId",
       {"createTime", "amount", "orderNum", "comment", "payType",
        "goodsType", "dependencyTime"},
       true, 1},
      {"AccountWithdrawAccount", NodeKind::kAccount,
       NodeKind::kAccount, "fromId", "toId",
       {"createTime", "amount", "fromType", "toType", "comment",
        "dependencyTime"},
       true, 3},
      {"AccountRepayLoan", NodeKind::kAccount,
       NodeKind::kLoan, "accountId", "loanId",
       {"createTime", "amount", "comment", "dependencyTime"},
       true, 5},
      {"CompanyApplyLoan", NodeKind::kCompany,
       NodeKind::kLoan, "companyId", "loanId",
       {"createTime", "loanAmount", "org", "comment"},
       false, 7},
      {"CompanyGuaranteeCompany", NodeKind::kCompany,
       NodeKind::kCompany, "fromId", "toId",
       {"createTime", "relation", "comment"},
       false, 9},
      {"CompanyInvestCompany", NodeKind::kCompany,
       NodeKind::kCompany, "investorId", "companyId",
       {"ratio", "createTime", "comment"},
       false, 11},
      {"CompanyOwnAccount", NodeKind::kCompany,
       NodeKind::kAccount, "companyId", "accountId",
       {"createTime", "comment"},
       false, 13},
      {"LoanDepositAccount", NodeKind::kLoan,
       NodeKind::kAccount, "loanId", "accountId",
       {"createTime", "amount", "comment", "dependencyTime"},
       true, 15},
      {"MediumSignInAccount", NodeKind::kMedium,
       NodeKind::kAccount, "mediumId", "accountId",
       {"createTime", "location", "comment", "dependencyTime"},
       true, 17},
      {"PersonApplyLoan", NodeKind::kPerson,
       NodeKind::kLoan, "personId", "loanId",
       {"createTime", "loanAmount", "org", "comment"},
       false, 19},
      {"PersonGuaranteePerson", NodeKind::kPerson,
       NodeKind::kPerson, "fromId", "toId",
       {"createTime", "relation", "comment"},
       false, 21},
      {"PersonInvestCompany", NodeKind::kPerson,
       NodeKind::kCompany, "investorId", "companyId",
       {"ratio", "createTime", "comment"},
       false, 23},
      {"PersonOwnAccount", NodeKind::kPerson,
       NodeKind::kAccount, "personId", "accountId",
       {"createTime", "comment"},
       false, 25},
  };
  return specs;
}

const std::unordered_map<std::string, const RelationSpec*>& RelationByName() {
  static const std::unordered_map<std::string, const RelationSpec*> map = [] {
    std::unordered_map<std::string, const RelationSpec*> out;
    for (const auto& spec : AllRelations()) {
      out.emplace(spec.name, &spec);
    }
    return out;
  }();
  return map;
}

const RelationSpec& MustGetRelation(const std::string& name) {
  const auto& map = RelationByName();
  const auto it = map.find(name);
  assert(it != map.end());
  return *(it->second);
}

std::string GetSnapshotDir() {
  return FLAGS_finbench_dataset_root + "/snapshot";
}

std::string GetIncrementalDir() {
  return FLAGS_finbench_dataset_root + "/incremental";
}

std::string GetParamsDir() {
  return FLAGS_finbench_dataset_root + "/params";
}

bool FileExists(const std::string& path) {
  std::error_code ec;
  return std::filesystem::exists(path, ec) && !ec;
}

bool OpenCsvInput(const std::string& path,
                  std::ifstream* in,
                  std::vector<char>* buffer) {
  if (in == nullptr || buffer == nullptr) {
    return false;
  }
  static constexpr size_t kCsvInputBufferBytes = 4ULL * 1024ULL * 1024ULL;
  buffer->resize(kCsvInputBufferBytes);
  in->rdbuf()->pubsetbuf(buffer->data(),
                         static_cast<std::streamsize>(buffer->size()));
  in->open(path, std::ios::in | std::ios::binary);
  return in->is_open();
}

uint64_t CountDataLines(const std::string& path) {
  std::ifstream in;
  std::vector<char> buffer;
  if (!OpenCsvInput(path, &in, &buffer)) {
    return 0;
  }
  uint64_t line_count = 0;
  std::string line;
  bool first = true;
  while (std::getline(in, line)) {
    if (first) {
      first = false;
      continue;
    }
    if (!line.empty()) {
      ++line_count;
    }
  }
  return line_count;
}

size_t EstimateCsvRowsBySize(const std::string& path, uint64_t avg_row_bytes) {
  if (avg_row_bytes == 0) {
    return 0;
  }
  std::error_code ec;
  const uint64_t bytes = std::filesystem::file_size(path, ec);
  if (ec || bytes == 0) {
    return 0;
  }
  const uint64_t estimate = bytes / avg_row_bytes + 1ULL;
  const uint64_t max_size_t =
      static_cast<uint64_t>(std::numeric_limits<size_t>::max());
  return static_cast<size_t>(std::min<uint64_t>(estimate, max_size_t));
}

uint64_t ExtractTrailingNumber(const std::string& file_name) {
  uint64_t value = 0;
  uint64_t place = 1;
  bool found = false;
  for (int i = static_cast<int>(file_name.size()) - 1; i >= 0; --i) {
    const char c = file_name[static_cast<size_t>(i)];
    if (std::isdigit(static_cast<unsigned char>(c))) {
      value = static_cast<uint64_t>(c - '0') * place + value;
      place *= 10;
      found = true;
      continue;
    }
    if (found) {
      break;
    }
  }
  return value;
}

bool ParseCsvHeader(std::ifstream* in, CsvHeader* out_header) {
  if (in == nullptr || out_header == nullptr || !in->is_open()) {
    return false;
  }
  std::string header_line;
  if (!std::getline(*in, header_line)) {
    return false;
  }
  out_header->columns = SplitPipe(header_line);
  out_header->name_to_idx.clear();
  for (size_t i = 0; i < out_header->columns.size(); ++i) {
    out_header->name_to_idx[out_header->columns[i]] = i;
  }
  return true;
}

bool LoadCsvFile(const std::string& path, LoadedCsvFile* out) {
  if (out == nullptr) {
    return false;
  }
  std::ifstream in;
  std::vector<char> buffer;
  if (!OpenCsvInput(path, &in, &buffer)) {
    return false;
  }
  out->header = std::make_shared<CsvHeader>();
  if (!ParseCsvHeader(&in, out->header.get())) {
    return false;
  }
  out->rows.clear();
  std::string line;
  while (std::getline(in, line)) {
    if (line.empty()) {
      continue;
    }
    CsvRow row;
    row.header = out->header.get();
    row.fields = SplitPipe(line);
    if (row.fields.size() < out->header->columns.size()) {
      row.fields.resize(out->header->columns.size());
    }
    out->rows.push_back(std::move(row));
  }
  return true;
}

void SetPayloadSlot(std::vector<std::string>* slots,
                    const std::unordered_map<std::string, size_t>& index_by_name,
                    const std::string& name,
                    const std::string& value) {
  if (slots == nullptr || value.empty()) {
    return;
  }
  const auto it = index_by_name.find(name);
  if (it == index_by_name.end() || it->second >= slots->size()) {
    return;
  }
  (*slots)[it->second] = value;
}

std::string EncodePayloadSlots(const std::vector<std::string>& slots) {
  std::string payload;
  for (size_t i = 0; i < slots.size(); ++i) {
    if (i > 0) {
      payload.push_back('|');
    }
    payload += slots[i];
  }
  return payload;
}

size_t PipeFieldCount(const std::string& payload) {
  if (payload.empty()) {
    return 0;
  }
  size_t count = 1;
  for (char c : payload) {
    if (c == '|') {
      ++count;
    }
  }
  return count;
}

std::vector<std::string> DecodePayloadSlots(const std::string& payload,
                                            size_t min_slots) {
  const size_t slot_count = std::max(min_slots, PipeFieldCount(payload));
  std::vector<std::string> slots(slot_count);
  if (payload.empty() || slots.empty()) {
    return slots;
  }
  size_t start = 0;
  size_t slot = 0;
  while (slot < slots.size()) {
    const size_t end = payload.find('|', start);
    if (end == std::string::npos) {
      slots[slot] = payload.substr(start);
      break;
    }
    slots[slot] = payload.substr(start, end - start);
    start = end + 1;
    ++slot;
  }
  return slots;
}

PmrString EncodePayloadSlotsPmr(
    const std::vector<std::string>& slots,
    std::pmr::memory_resource* mr = std::pmr::get_default_resource()) {
  PmrString payload{mr};
  for (size_t i = 0; i < slots.size(); ++i) {
    if (i > 0) {
      payload.push_back('|');
    }
    payload += slots[i];
  }
  return payload;
}

bool GetPipeFieldBySlot(const std::string& payload,
                        uint16_t slot,
                        std::string* out) {
  if (out == nullptr) {
    return false;
  }
  size_t start = 0;
  for (uint16_t i = 0; i < slot; ++i) {
    const size_t sep = payload.find('|', start);
    if (sep == std::string::npos) {
      out->clear();
      return false;
    }
    start = sep + 1;
  }
  const size_t end = payload.find('|', start);
  *out = end == std::string::npos ? payload.substr(start)
                                  : payload.substr(start, end - start);
  return true;
}

bool SetPipeFieldBySlot(std::string* payload,
                        uint16_t slot,
                        const std::string& value) {
  if (payload == nullptr) {
    return false;
  }
  size_t start = 0;
  for (uint16_t i = 0; i < slot; ++i) {
    const size_t sep = payload->find('|', start);
    if (sep == std::string::npos) {
      return false;
    }
    start = sep + 1;
  }
  const size_t end = payload->find('|', start);
  payload->replace(start,
                   end == std::string::npos ? std::string::npos
                                             : end - start,
                   value);
  return true;
}

// Build the hot fixed payload for Shard 0.
// Shard 0 payload order: createTime|amount
PmrString BuildEdgeShard0Payload(
    const RelationSpec& rel,
    const CsvRow& row,
    std::pmr::memory_resource* mr = std::pmr::get_default_resource()) {
  const std::string* amount_value = nullptr;
  std::string normalized_create_time;
  bool has_create_time = false;
  for (const auto& column : rel.property_columns) {
    if (column == "createTime") {
      const std::string& value = row.Get(column);
      if (!value.empty()) {
        normalized_create_time = NormalizePropertyValue(column, value);
        has_create_time = true;
      }
      continue;
    }
    if (column == "amount") {
      const std::string& value = row.Get(column);
      if (!value.empty()) {
        amount_value = &value;
      }
    }
  }

  PmrString payload{mr};
  payload.reserve((has_create_time ? normalized_create_time.size() : 0U) + 1U +
                  (amount_value == nullptr ? 0U : amount_value->size()));
  if (has_create_time) {
    payload += normalized_create_time;
  }
  payload.push_back('|');
  if (amount_value != nullptr) {
    payload += *amount_value;
  }
  return payload;
}

// Build the variable cold payload persisted to the external blob at import time.
// Cold payload order:
// dependencyTime|comment|orderNum|payType|goodsType|fromType|toType|loanAmount|org|relation|ratio|location|cold_extra_01|...
PmrString BuildColdPayload(
    const RelationSpec& rel,
    const CsvRow& row,
    std::pmr::memory_resource* mr = std::pmr::get_default_resource()) {
  std::array<const std::string*, kColdEdgePropertyCount> slots = {};
  std::array<std::string, kColdEdgePropertyCount> normalized_values;
  std::vector<const std::string*> generated_slots;
  size_t normalized_count = 0;
  bool has_any = false;
  for (const auto& column : rel.property_columns) {
    const int slot = ColdEdgePropertySlot(column);
    if (slot < 0) {
      continue;
    }
    const std::string& value = row.Get(column);
    if (value.empty()) {
      continue;
    }
    if (IsTimeLikeProperty(column)) {
      assert(normalized_count < normalized_values.size());
      normalized_values[normalized_count] = NormalizePropertyValue(column, value);
      slots[static_cast<size_t>(slot)] = &normalized_values[normalized_count];
      ++normalized_count;
    } else {
      slots[static_cast<size_t>(slot)] = &value;
    }
    has_any = true;
  }
  if (row.header != nullptr) {
    for (size_t i = 0; i < row.header->columns.size(); ++i) {
      const std::string& column = row.header->columns[i];
      if (!IsGeneratedColdProperty(column)) {
        continue;
      }
      const std::string& value = row.Get(column);
      generated_slots.push_back(value.empty() ? nullptr : &value);
      if (!value.empty()) {
        has_any = true;
      }
    }
  }
  if (!has_any) {
    return PmrString{mr};  // empty -> no shard1 ref write needed
  }

  size_t reserve_bytes =
      kColdEdgePropertyCount + generated_slots.size() - 1U;
  for (const std::string* value : slots) {
    if (value != nullptr) {
      reserve_bytes += value->size();
    }
  }
  for (const std::string* value : generated_slots) {
    if (value != nullptr) {
      reserve_bytes += value->size();
    }
  }
  PmrString payload{mr};
  payload.reserve(reserve_bytes);
  for (size_t i = 0; i < slots.size(); ++i) {
    if (i > 0) {
      payload.push_back('|');
    }
    if (slots[i] != nullptr) {
      payload += *slots[i];
    }
  }
  for (const std::string* value : generated_slots) {
    payload.push_back('|');
    if (value != nullptr) {
      payload += *value;
    }
  }
  return payload;
}

// Per-property fixed lengths for GraphDB-stored properties. Cold properties are
// stored in the external blob without truncation; their historical fixed-slot
// lengths are kept below only as documentation.
uint32_t GetEdgePropertyLength(const std::string& name) {
  static const std::unordered_map<std::string, uint32_t> lengths = {
      // Node hot fixed properties.
      {"rawId",           32},
      {"isBlocked",       5},
      {"loanAmount",      20},
      {"balance",         20},
      {"node_cold_property", 32},
      // Shard 0
      {"createTime",      13},
      {"comment",         20},
      {"amount",          11},
      {"dependencyTime",  13},
      {"orderNum",        15},
      {"payType",         22},
      {"goodsType",       22},
      {"fromType",        22},
      {"toType",          10},
      // Cold blob ref stored in Shard 1
      {"cold_property",   32},
      // Original cold property lengths kept here as documentation for the
      // external blob payload fields.
      {"org",             25},
      {"relation",        18},
      {"ratio",           22},
      {"location",        20},
  };
  auto it = lengths.find(name);
  if (it != lengths.end()) {
    return it->second;
  }
  return FLAGS_finbench_property_length;  // default for node properties
}

template <typename Fn>
bool ForEachCsvRow(const std::string& path, Fn&& fn) {
  std::ifstream in;
  std::vector<char> buffer;
  if (!OpenCsvInput(path, &in, &buffer)) {
    return false;
  }
  CsvHeader header;
  if (!ParseCsvHeader(&in, &header)) {
    return false;
  }
  std::string line;
  while (std::getline(in, line)) {
    if (line.empty()) {
      continue;
    }
    CsvRow row;
    row.header = &header;
    row.fields = SplitPipe(line);
    if (row.fields.size() < header.columns.size()) {
      row.fields.resize(header.columns.size());
    }
    fn(row);
  }
  return true;
}

template <typename Fn>
void ParallelForIndex(size_t count, uint32_t thread_count, Fn&& fn) {
  if (thread_count <= 1 || count <= 1) {
    for (size_t i = 0; i < count; ++i) {
      fn(i);
    }
    return;
  }

#pragma omp parallel for num_threads(static_cast<int>(thread_count)) schedule(static)
  for (long long i = 0; i < static_cast<long long>(count); ++i) {
    fn(static_cast<size_t>(i));
  }
}

template <typename Fn>
void ParallelForIndexDynamic(size_t count, uint32_t thread_count, Fn&& fn) {
  if (thread_count <= 1 || count <= 1) {
    for (size_t i = 0; i < count; ++i) {
      fn(i);
    }
    return;
  }

#pragma omp parallel for num_threads(static_cast<int>(thread_count)) schedule(dynamic, 1)
  for (long long i = 0; i < static_cast<long long>(count); ++i) {
    fn(static_cast<size_t>(i));
  }
}

template <typename Fn>
void ParallelForIndexDynamicChunk(size_t count,
                                  uint32_t thread_count,
                                  uint32_t chunk_size,
                                  Fn&& fn) {
  if (thread_count <= 1 || count <= 1) {
    for (size_t i = 0; i < count; ++i) {
      fn(i);
    }
    return;
  }

  const int omp_chunk =
      static_cast<int>(std::max<uint32_t>(1U, chunk_size));
#pragma omp parallel for num_threads(static_cast<int>(thread_count)) schedule(dynamic, omp_chunk)
  for (long long i = 0; i < static_cast<long long>(count); ++i) {
    fn(static_cast<size_t>(i));
  }
}

uint32_t CurrentOpenMpThreadId(uint32_t writer_count) {
  if (writer_count <= 1U) {
    return 0;
  }
#ifdef _OPENMP
  const int tid = omp_in_parallel() ? omp_get_thread_num() : 0;
  if (tid < 0) {
    return 0;
  }
  const uint32_t id = static_cast<uint32_t>(tid);
  return id < writer_count ? id : writer_count - 1U;
#else
  (void)writer_count;
  return 0;
#endif
}

std::vector<uint32_t> ParseMemtableSizeCsv(const std::string& csv,
                                           size_t expected_count,
                                           uint32_t fallback,
                                           const char* flag_name) {
  std::vector<uint32_t> sizes(expected_count, std::max<uint32_t>(1U, fallback));
  if (csv.empty()) {
    return sizes;
  }
  std::stringstream ss(csv);
  std::string token;
  size_t index = 0;
  while (std::getline(ss, token, ',')) {
    if (index >= expected_count) {
      std::cerr << "ignore extra " << flag_name << " item: " << token
                << std::endl;
      continue;
    }
    token.erase(std::remove_if(token.begin(), token.end(), [](unsigned char c) {
                  return std::isspace(c) != 0;
                }),
                token.end());
    if (!token.empty()) {
      const uint64_t parsed = std::stoull(token);
      if (parsed > 0) {
        sizes[index] = static_cast<uint32_t>(
            std::min<uint64_t>(parsed,
                               std::numeric_limits<uint32_t>::max()));
      }
    }
    ++index;
  }
  return sizes;
}

class ColdBlobWriter {
 public:
  static constexpr size_t kDefaultBufferBytes = kBlobBufferBytes;

  bool Open(uint32_t file_id,
            const std::string& path,
            size_t buffer_bytes = kDefaultBufferBytes) {
    file_id_ = file_id;
    path_ = path;
    buffer_limit_bytes_ = std::max<size_t>(1U, buffer_bytes);
    std::filesystem::path blob_path(path);
    std::error_code ec;
    if (blob_path.has_parent_path()) {
      std::filesystem::create_directories(blob_path.parent_path(), ec);
      if (ec) {
        std::cerr << "create cold blob directory failed: "
                  << blob_path.parent_path().string()
                  << ", ec=" << ec.message() << std::endl;
        return false;
      }
    }
    out_.open(path, std::ios::binary | std::ios::trunc);
    if (!out_.is_open()) {
      std::cerr << "open cold blob failed: " << path << std::endl;
      return false;
    }
    buffer_.clear();
    buffer_.reserve(buffer_limit_bytes_);
    next_offset_ = 0;
    record_count_ = 0;
    payload_bytes_ = 0;
    file_bytes_ = 0;
    return true;
  }

  std::string Append(const std::string& payload) {
    if (payload.empty()) {
      return {};
    }
    if (!out_.is_open()) {
      std::cerr << "cold blob writer is not open" << std::endl;
      std::exit(1);
    }
    const uint64_t offset = next_offset_;
    const uint64_t length = static_cast<uint64_t>(payload.size());
    next_offset_ += length + 1ULL;  // one trailing '\n' per record
    buffer_.append(payload);
    buffer_.push_back('\n');
    ++record_count_;
    payload_bytes_ += length;
    if (buffer_.size() >= buffer_limit_bytes_ && !FlushLocked()) {
      std::exit(1);
    }
    return std::to_string(file_id_) + ":" + std::to_string(offset) + ":" +
           std::to_string(length);
  }

  bool Flush() {
    return FlushLocked();
  }

  bool Close() {
    const bool ok = FlushLocked();
    if (out_.is_open()) {
      out_.close();
    }
    return ok;
  }

  uint64_t record_count() const { return record_count_; }
  uint64_t payload_bytes() const { return payload_bytes_; }
  uint64_t file_bytes() const { return file_bytes_; }
  const std::string& path() const { return path_; }
  uint32_t file_id() const { return file_id_; }

 private:
  bool FlushLocked() {
    if (buffer_.empty()) {
      return true;
    }
    out_.write(buffer_.data(), static_cast<std::streamsize>(buffer_.size()));
    if (!out_) {
      std::cerr << "write cold blob failed: " << path_ << std::endl;
      return false;
    }
    file_bytes_ += static_cast<uint64_t>(buffer_.size());
    buffer_.clear();
    return true;
  }

  std::ofstream out_;
  std::string path_;
  std::string buffer_;
  size_t buffer_limit_bytes_ = kDefaultBufferBytes;
  uint32_t file_id_ = 0;
  uint64_t next_offset_ = 0;
  uint64_t record_count_ = 0;
  uint64_t payload_bytes_ = 0;
  uint64_t file_bytes_ = 0;
};

class ColdBlobWriterSet {
 public:
  bool Open(const std::string& prefix,
            uint32_t writer_count,
            size_t buffer_bytes = ColdBlobWriter::kDefaultBufferBytes) {
    prefix_ = prefix;
    const uint32_t count = std::max<uint32_t>(1U, writer_count);
    writers_.clear();
    writers_.reserve(count);
    for (uint32_t i = 0; i < count; ++i) {
      auto writer = std::make_unique<ColdBlobWriter>();
      const std::string path = prefix + "_" + std::to_string(i) + ".blob";
      if (!writer->Open(i, path, buffer_bytes)) {
        return false;
      }
      writers_.push_back(std::move(writer));
    }
    return true;
  }

  std::string Append(const std::string& payload) {
    if (payload.empty()) {
      return {};
    }
    const uint32_t id = CurrentOpenMpThreadId(static_cast<uint32_t>(writers_.size()));
    return writers_[id]->Append(payload);
  }

  bool Flush() {
    for (const auto& writer : writers_) {
      if (!writer->Flush()) {
        return false;
      }
    }
    return true;
  }

  bool Close() {
    bool ok = true;
    for (const auto& writer : writers_) {
      ok = writer->Close() && ok;
    }
    return ok;
  }

  uint64_t record_count() const {
    uint64_t total = 0;
    for (const auto& writer : writers_) {
      total += writer->record_count();
    }
    return total;
  }

  uint64_t payload_bytes() const {
    uint64_t total = 0;
    for (const auto& writer : writers_) {
      total += writer->payload_bytes();
    }
    return total;
  }

  uint64_t file_bytes() const {
    uint64_t total = 0;
    for (const auto& writer : writers_) {
      total += writer->file_bytes();
    }
    return total;
  }

  size_t file_count() const { return writers_.size(); }
  const std::string& prefix() const { return prefix_; }

 private:
  std::string prefix_;
  std::vector<std::unique_ptr<ColdBlobWriter>> writers_;
};

class FixedResidentArena {
 public:
  class Resource final : public std::pmr::memory_resource {
   public:
    Resource(void* base, uint64_t size)
        : base_(static_cast<std::byte*>(base)), size_(size) {}

    void Release() {
      used_ = 0;
    }

    uint64_t used_bytes() const { return used_; }
    uint64_t max_used_bytes() const { return max_used_; }
    uint64_t size_bytes() const { return size_; }

   private:
    static uint64_t AlignUp(uint64_t value, uint64_t alignment) {
      if (alignment == 0) {
        return value;
      }
      const uint64_t remainder = value % alignment;
      return remainder == 0 ? value : value + alignment - remainder;
    }

    void* do_allocate(size_t bytes, size_t alignment) override {
      const uint64_t aligned =
          AlignUp(used_, static_cast<uint64_t>(alignment));
      if (aligned > size_ || bytes > size_ - aligned) {
        throw std::bad_alloc();
      }
      void* ptr = base_ + aligned;
      used_ = aligned + static_cast<uint64_t>(bytes);
      if (used_ > max_used_) {
        max_used_ = used_;
      }
      return ptr;
    }

    void do_deallocate(void*, size_t, size_t) override {
      // Whole-batch arena: memory is reclaimed by Release().
    }

    bool do_is_equal(const std::pmr::memory_resource& other) const noexcept override {
      return this == &other;
    }

    std::byte* base_ = nullptr;
    uint64_t size_ = 0;
    uint64_t used_ = 0;
    uint64_t max_used_ = 0;
  };

  FixedResidentArena() = default;
  FixedResidentArena(const FixedResidentArena&) = delete;
  FixedResidentArena& operator=(const FixedResidentArena&) = delete;

  ~FixedResidentArena() {
    Close();
  }

  bool Open(uint64_t gib, const std::string& name) {
    name_ = name;
    if (gib == 0) {
      std::cout << "[LOAD_ARENA] name: " << name_ << std::endl;
      std::cout << "[LOAD_ARENA] enabled: false" << std::endl;
      return true;
    }

    constexpr uint64_t kGiB = 1024ULL * 1024ULL * 1024ULL;
    if (gib > std::numeric_limits<uint64_t>::max() / kGiB) {
      std::cerr << "load arena size overflow: " << gib << " GiB" << std::endl;
      return false;
    }
    size_ = gib * kGiB;
    base_ = mmap(nullptr,
                 static_cast<size_t>(size_),
                 PROT_READ | PROT_WRITE,
                 MAP_PRIVATE | MAP_ANONYMOUS,
                 -1,
                 0);
    if (base_ == MAP_FAILED) {
      base_ = nullptr;
      std::cerr << "mmap load arena failed, size_gb=" << gib
                << ", errno=" << errno << " (" << std::strerror(errno)
                << ")" << std::endl;
      return false;
    }

    const long page = sysconf(_SC_PAGESIZE);
    const size_t page_size = page > 0 ? static_cast<size_t>(page) : 4096U;
    const auto t1 = std::chrono::steady_clock::now();
    volatile char* p = static_cast<volatile char*>(base_);
    for (uint64_t offset = 0; offset < size_; offset += page_size) {
      p[offset] = 0;
    }
    p[size_ - 1] = 0;
    const auto t2 = std::chrono::steady_clock::now();
    const double sec =
        std::chrono::duration_cast<std::chrono::duration<double>>(t2 - t1)
            .count();

    std::cout << "[LOAD_ARENA] name: " << name_ << std::endl;
    std::cout << "[LOAD_ARENA] enabled: true" << std::endl;
    std::cout << "[LOAD_ARENA] size_gb: " << gib << std::endl;
    std::cout << "[LOAD_ARENA] size_bytes: " << size_ << std::endl;
    std::cout << "[LOAD_ARENA] page_size: " << page_size << std::endl;
    std::cout << "[LOAD_ARENA] touch_time(s): " << sec << std::endl;
    resource_ = std::make_unique<Resource>(base_, size_);
    return true;
  }

  void Close() {
    resource_.reset();
    if (base_ != nullptr) {
      munmap(base_, static_cast<size_t>(size_));
      base_ = nullptr;
      size_ = 0;
    }
  }

  std::pmr::memory_resource* resource() {
    return resource_ == nullptr ? std::pmr::get_default_resource()
                                : resource_.get();
  }

  void Release(const char* phase) {
    if (resource_ == nullptr) {
      return;
    }
    std::cout << "[LOAD_ARENA_BATCH] phase: " << (phase == nullptr ? "" : phase)
              << ", used_bytes: " << resource_->used_bytes()
              << ", max_used_bytes: " << resource_->max_used_bytes()
              << std::endl;
    resource_->Release();
  }

  uint64_t size_bytes() const {
    return size_;
  }

  uint64_t used_bytes() const {
    return resource_ == nullptr ? 0 : resource_->used_bytes();
  }

 private:
  void* base_ = nullptr;
  uint64_t size_ = 0;
  std::string name_;
  std::unique_ptr<Resource> resource_;
};

class FinBenchGraphDbTest {
 public:
  int Run() {
    if (FLAGS_finbench_enable_preprocessed_loader) {
      const int rc = RunPreprocessed();
      google::ShutDownCommandLineFlags();
      return rc;
    }
    if (!ValidateDatasetLayout()) {
      return 1;
    }
    std::cout << "=== test_graphdb_finbench ===" << std::endl;
    std::cout << "dataset_root: " << FLAGS_finbench_dataset_root << std::endl;
    std::cout << "db_path: " << FLAGS_finbench_db_path << std::endl;
    if (!load_arena_.Open(FLAGS_finbench_load_arena_gb,
                          "finbench_load_buffer")) {
      return 1;
    }
    std::cout << "[LOAD_PREPARE] start" << std::endl;
    if (!LoadAllDataset()) {
      return 1;
    }
    if (!PrepareDbDirectory()) {
      return 1;
    }

    ConfigureLowLevelFlags();
    const uint64_t max_vertex_num = DeriveMaxVertexNum();
    const std::string schema_path = FLAGS_finbench_db_path + "/finbench_schema.yaml";
    WriteSchemaFile(schema_path, max_vertex_num);

    if (!OpenDb(schema_path)) {
      return 1;
    }
    db_->InitVerticesUpTo(next_vertex_id_);
    if (!cold_blob_writers_.Open(
            ColdBlobPathPrefix(),
            std::max(GetWriteThreadCount(), GetMixedThreadCount()),
            kBlobBufferBytes)) {
      return 1;
    }
    if (!node_cold_blob_writers_.Open(
            NodeColdBlobPathPrefix(),
            std::max(GetWriteThreadCount(), GetMixedThreadCount()),
            kNodeColdBlobBufferBytes)) {
      return 1;
    }
    ConfigureWriteLatencySampling();

    std::cout << "schema_path: " << schema_path << std::endl;
    std::cout << "cold_blob_prefix: " << ColdBlobPathPrefix() << std::endl;
    std::cout << "cold_blob_files: " << cold_blob_writers_.file_count()
              << std::endl;
    std::cout << "node_cold_blob_prefix: " << NodeColdBlobPathPrefix()
              << std::endl;
    std::cout << "node_cold_blob_files: "
              << node_cold_blob_writers_.file_count() << std::endl;
    std::cout << "max_vertex_num: " << max_vertex_num << std::endl;
    std::cout << "use_csr_disk: " << db_->schema().use_csr_disk << std::endl;
    std::cout << "system_threads: " << FLAGS_finbench_system_threads << std::endl;
    std::cout << "param_limit_per_query: " << FLAGS_finbench_param_limit_per_query
              << std::endl;
    std::cout << "workload_sample_mod: " << WorkloadSampleMod() << std::endl;
    std::cout << "workload_sample_remainder: " << WorkloadSampleRemainder()
              << std::endl;
    std::cout << "legacy_memproperty_flag: "
              << (FLAGS_finbench_enable_memproperty ? "true" : "false")
              << std::endl;
    std::cout << "engine_property_updates: true" << std::endl;
    std::cout << "write_schedule: "
              << (FLAGS_finbench_write_dynamic ? "dynamic" : "static");
    if (FLAGS_finbench_write_dynamic) {
      std::cout << ", chunk=" << FLAGS_finbench_write_dynamic_chunk;
    }
    std::cout << std::endl;
    std::cout << "import_batch_rows: " << ImportBatchRows() << std::endl;
    std::cout << "load_arena_gb: " << FLAGS_finbench_load_arena_gb
              << std::endl;
    std::cout << "memtable_size: " << FLAGS_memtable_size << std::endl;
    std::cout << "node_memtable_size: " << NodeMemtableSize() << std::endl;
    PrintMemtableSizes("edge_memtable_sizes", EdgeMemtableSizes());
    std::cout << "blob_buffer_bytes: " << kBlobBufferBytes << std::endl;
    std::cout << "property_length: " << FLAGS_finbench_property_length << std::endl;
    std::cout << "single_edge_read_ops: "
              << FLAGS_finbench_single_edge_read_ops << std::endl;
    std::cout << "single_edge_candidate_cap: "
              << FLAGS_finbench_single_edge_candidate_cap << std::endl;
    std::cout << "single_edge_hot_cold_ratio: "
              << FLAGS_finbench_single_edge_hot_weight << ":"
              << FLAGS_finbench_single_edge_cold_weight << std::endl;
    std::cout << "single_edge_seed: " << FLAGS_finbench_single_edge_seed
              << std::endl;
    std::cout << "write_latency_sample_target: "
              << FLAGS_finbench_write_latency_sample_target << std::endl;
	    std::cout << "write_latency_sample_seed: "
	              << FLAGS_finbench_write_latency_sample_seed << std::endl;
	    std::cout << "run_mixed_workload: "
		              << (FLAGS_finbench_run_mixed_workload ? "true" : "false")
		              << std::endl;
		    std::cout << "mix_enable_queries: "
		              << (FLAGS_finbench_mix_enable_queries ? "true" : "false")
		              << std::endl;
		    std::cout << "mixed_update_interleave: "
		              << FLAGS_finbench_mixed_update_interleave << std::endl;
	    std::cout << "mixed_threads: " << FLAGS_finbench_mixed_threads
	              << std::endl;
    std::cout << "skip_single_edge_read: "
	              << (FLAGS_finbench_skip_single_edge_read ? "true" : "false")
	              << std::endl;
    std::cout << "update_workload_path: "
              << FLAGS_finbench_update_workload_path << std::endl;
    std::cout << "property_delta_dir: "
              << FLAGS_finbench_db_path << "/<shard>/property-delta"
              << std::endl;
    std::cout << "update_node_memproperty_cap: "
              << FLAGS_finbench_update_node_memproperty_cap << std::endl;
    std::cout << "update_edge_memproperty_cap: "
              << FLAGS_finbench_update_edge_memproperty_cap << std::endl;
    std::cout << "property_buffer_bytes: "
              << FLAGS_finbench_property_buffer_bytes << std::endl;
    std::cout << "delta_merge_threshold: "
              << FLAGS_finbench_delta_merge_threshold << std::endl;
    std::cout << "delta_crash_safe: "
              << (FLAGS_finbench_delta_crash_safe ? "true" : "false")
              << std::endl;

    const ImportStats snapshot_nodes = ImportSnapshotNodes();
    PrintImportStats("SNAPSHOT_NODE_IMPORT", snapshot_nodes);

    const ImportStats snapshot_edges = ImportSnapshotRelations();
    PrintImportStats("SNAPSHOT_EDGE_IMPORT", snapshot_edges);
    PrintCombinedImportStats("SNAPSHOT_IMPORT", snapshot_nodes, snapshot_edges);

	    const bool run_mixed =
	        FLAGS_finbench_run_mixed_workload &&
	        (!FLAGS_finbench_skip_queries || !FLAGS_finbench_mix_enable_queries);
	    MixedWorkloadStats mixed_stats;
	    ImportStats final_import_stats;
	    if (run_mixed) {
		      final_import_stats = ImportRemainingIncremental();
		      PrintImportStats("REMAINING_INCREMENTAL_IMPORT", final_import_stats);
		      mixed_stats = RunMixedWorkload();
          if (!ForceHotEdgeCsrCompactionIfNeeded()) {
            return 1;
          }
		    } else {
		      final_import_stats = ImportIncremental();
		      PrintImportStats("INCREMENTAL_IMPORT", final_import_stats);
		    }
	    PrintFullGraphWriteStats(snapshot_nodes,
	                             snapshot_edges,
                               ImportStats{},
	                             final_import_stats);
        node_write_latency_sampler_.Print("NODE_WRITE_LATENCY_SAMPLE");
	    write_latency_sampler_.Print("WRITE_LATENCY_SAMPLE");

    if (!FLAGS_finbench_update_workload_path.empty()) {
      const UpdateWorkloadStats update_stats = RunLightUpdateWorkload();
      PrintUpdateWorkloadStats(update_stats);
    }

		    if (!cold_blob_writers_.Close() || !node_cold_blob_writers_.Close()) {
		      return 1;
		    }
	    PrintColdBlobStats();

	    if (!FLAGS_finbench_skip_single_edge_read) {
	      RunSingleEdgeReadBenchmark();
	    }

    if (!FLAGS_finbench_skip_queries) {
      RunAllQueries();
    }

    std::cout << "test_graphdb_finbench passed" << std::endl;
    google::ShutDownCommandLineFlags();
    return 0;
  }

 private:
  struct PreprocessedIndexStats {
    uint64_t node_records = 0;
    uint64_t edge_records = 0;
    uint64_t snapshot_edge_records = 0;
    uint64_t single_node_read_records = 0;
    uint64_t single_edge_read_records = 0;
  };

  struct PreprocessedReadStats {
    uint64_t ops = 0;
    uint64_t hot_ops = 0;
    uint64_t cold_ops = 0;
    uint64_t found = 0;
    uint64_t missed = 0;
    uint64_t checksum = 0;
    double sec = 0.0;
    std::unordered_map<std::string, uint64_t> miss_reasons;

    void Add(const PreprocessedReadStats& other) {
      const uint64_t old_ops = ops;
      ops += other.ops;
      hot_ops += other.hot_ops;
      cold_ops += other.cold_ops;
      found += other.found;
      missed += other.missed;
      checksum ^= HashMix(other.checksum + old_ops + 1);
      sec += other.sec;
      for (const auto& item : other.miss_reasons) {
        miss_reasons[item.first] += item.second;
      }
    }
  };

  struct PreprocessedQueryStats {
    std::array<QueryMetrics, 13> query_metrics;
    double wall_sec = 0.0;

    PreprocessedQueryStats() {
      for (int qid = 1; qid <= 12; ++qid) {
        query_metrics[static_cast<size_t>(qid)].query_id = qid;
      }
    }

    void Add(const PreprocessedQueryStats& other) {
      for (int qid = 1; qid <= 12; ++qid) {
        QueryMetrics& dst = query_metrics[static_cast<size_t>(qid)];
        const QueryMetrics& src = other.query_metrics[static_cast<size_t>(qid)];
        dst.param_rows += src.param_rows;
        dst.result_rows += src.result_rows;
        dst.checksum ^= src.checksum;
        dst.sec += src.sec;
      }
      wall_sec += other.wall_sec;
    }
  };

  struct ScopedLoadArenaResource {
    explicit ScopedLoadArenaResource(std::pmr::memory_resource* resource)
        : previous(FinBenchGraphDbTest::tls_load_resource_override_) {
      FinBenchGraphDbTest::tls_load_resource_override_ = resource;
    }

    ~ScopedLoadArenaResource() {
      FinBenchGraphDbTest::tls_load_resource_override_ = previous;
    }

    std::pmr::memory_resource* previous = nullptr;
  };

  prechunk::LoaderOptions BuildPreprocessedLoaderOptions() const {
    prechunk::LoaderOptions options;
    options.root = FLAGS_finbench_preprocessed_root.empty()
                       ? FLAGS_finbench_dataset_root
                       : FLAGS_finbench_preprocessed_root;
    options.threads = FLAGS_finbench_loader_threads;
    options.loader_cpu_base = FLAGS_finbench_loader_cpu_base;
    options.db_cpu_base = FLAGS_finbench_db_cpu_base;
    options.arena_gb = FLAGS_finbench_load_arena_gb;
    options.queue_blocks = FLAGS_finbench_loader_queue_blocks;
    options.prefill_blocks = FLAGS_finbench_loader_prefill_blocks;
    options.block_records = FLAGS_finbench_loader_block_records;
    options.skip_node_update = FLAGS_finbench_skip_updates;
    options.skip_edge_update = FLAGS_finbench_skip_updates;
    return options;
  }

  static CsvHeader BuildPreprocessedHeader(
      const prechunk::SchemaInfo& schema) {
    CsvHeader header;
    header.columns = schema.columns;
    for (size_t i = 0; i < header.columns.size(); ++i) {
      header.name_to_idx[header.columns[i]] = i;
    }
    return header;
  }

  static CsvRow BuildPreprocessedRow(const prechunk::SchemaInfo& schema,
                                     const prechunk::Record& record,
                                     CsvHeader* header) {
    *header = BuildPreprocessedHeader(schema);
    CsvRow row;
    row.header = header;
    row.fields = prechunk::CopyFieldsToStd(record);
    return row;
  }

  static std::optional<NodeKind> NodeKindFromStringForPreprocessed(
      const std::string& name) {
    if (name == "Person") return NodeKind::kPerson;
    if (name == "Company") return NodeKind::kCompany;
    if (name == "Account") return NodeKind::kAccount;
    if (name == "Loan") return NodeKind::kLoan;
    if (name == "Medium") return NodeKind::kMedium;
    return std::nullopt;
  }

  static std::string PreprocessedEdgeSlotKey(std::string_view edge_type,
                                             std::string_view src_type,
                                             std::string_view src_id,
                                             std::string_view dst_type,
                                             std::string_view dst_id,
                                             std::string_view property) {
    std::string key;
    key.reserve(edge_type.size() + src_type.size() + src_id.size() +
                dst_type.size() + dst_id.size() + property.size() + 8);
    key.append(edge_type).push_back('\x1f');
    key.append(src_type).push_back('\x1f');
    key.append(src_id).push_back('\x1f');
    key.append(dst_type).push_back('\x1f');
    key.append(dst_id).push_back('\x1f');
    key.append(property);
    return key;
  }

  vertex_t LookupPreprocessedEntityId(NodeKind kind,
                                      const std::string& raw_id) const {
    if (raw_id.empty()) {
      return lsmgraph::INVALID_VERTEX_ID;
    }
    const auto it = entity_to_vid_.find(TypedEntityKey{kind, raw_id});
    return it == entity_to_vid_.end() ? lsmgraph::INVALID_VERTEX_ID
                                      : it->second;
  }

  void ResetPreprocessedState() {
    prepared_snapshot_node_batches_.clear();
    prepared_snapshot_relation_batches_.clear();
    prepared_incremental_batches_.clear();
    loaded_param_files_.clear();
    entity_to_vid_.clear();
    materialized_vertices_.clear();
    next_vertex_id_ = 0;
    snapshot_entity_total_ = 0;
    incremental_entity_total_ = 0;
    preprocessed_edge_slot_need_.clear();
    preprocessed_edge_slot_.clear();
    single_edge_sampler_.Reset(FLAGS_finbench_single_edge_candidate_cap,
                               FLAGS_finbench_single_edge_hot_weight,
                               FLAGS_finbench_single_edge_cold_weight,
                               FLAGS_finbench_single_edge_seed);
  }

  void IndexPreprocessedNodeRecord(const prechunk::SchemaInfo& schema,
                                   const prechunk::Record& record) {
    const auto kind = NodeKindFromStringForPreprocessed(
        schema.Metadata("node_type"));
    if (!kind.has_value()) {
      return;
    }
    CsvHeader header;
    CsvRow row = BuildPreprocessedRow(schema, record, &header);
    bool created = false;
    ResolveOrCreateEntityId(*kind, row.Get(schema.Metadata("id_column")),
                            &created);
  }

  void IndexPreprocessedEdgeRecord(const prechunk::SchemaInfo& schema,
                                   const prechunk::Record& record) {
    const auto src_kind = NodeKindFromStringForPreprocessed(
        schema.Metadata("src_type"));
    const auto dst_kind = NodeKindFromStringForPreprocessed(
        schema.Metadata("dst_type"));
    if (!src_kind.has_value() || !dst_kind.has_value()) {
      return;
    }
    CsvHeader header;
    CsvRow row = BuildPreprocessedRow(schema, record, &header);
    bool created = false;
    ResolveOrCreateEntityId(*src_kind, row.Get(schema.Metadata("src_column")),
                            &created);
    ResolveOrCreateEntityId(*dst_kind, row.Get(schema.Metadata("dst_column")),
                            &created);
  }

  void IndexPreprocessedSingleEdgeRequest(const prechunk::Record& record) {
    if (record.fields.size() < 8) {
      return;
    }
    const std::string_view property = prechunk::FieldView(record, 6);
    if (!IsGeneratedColdProperty(property)) {
      return;
    }
    preprocessed_edge_slot_need_.insert(
        PreprocessedEdgeSlotKey(prechunk::FieldView(record, 1),
                                prechunk::FieldView(record, 2),
                                prechunk::FieldView(record, 3),
                                prechunk::FieldView(record, 4),
                                prechunk::FieldView(record, 5),
                                property));
  }

  void IndexPreprocessedEdgeUpdateRequest(const prechunk::Record& record) {
    if (record.fields.size() < 9) {
      return;
    }
    const std::string_view property = prechunk::FieldView(record, 6);
    if (!IsGeneratedColdProperty(property)) {
      return;
    }
    preprocessed_edge_slot_need_.insert(
        PreprocessedEdgeSlotKey(prechunk::FieldView(record, 1),
                                prechunk::FieldView(record, 2),
                                prechunk::FieldView(record, 3),
                                prechunk::FieldView(record, 4),
                                prechunk::FieldView(record, 5),
                                property));
  }

  bool IndexPreprocessedDataset(const prechunk::SchemaCatalog& catalog,
                                const std::string& root,
                                PreprocessedIndexStats* stats) {
    ResetPreprocessedState();
    const auto files = prechunk::ListChunkFiles(root);
    for (const auto& file : files) {
      prechunk::Chunk chunk;
      std::string error;
      if (!prechunk::ReadChunkFile(file, &chunk, &error)) {
        std::cerr << error << std::endl;
        return false;
      }
      for (const auto& record : chunk.records) {
        const prechunk::SchemaInfo* schema = catalog.Find(record.schema_id);
        if (schema == nullptr) {
          continue;
        }
        if (record.op_code == 1 && schema->kind == "node") {
          IndexPreprocessedNodeRecord(*schema, record);
          ++stats->node_records;
        } else if (record.op_code == 2 && schema->kind == "edge") {
          IndexPreprocessedEdgeRecord(*schema, record);
          ++stats->edge_records;
          if (chunk.stage == prechunk::Stage::kSnapshotEdges) {
            ++stats->snapshot_edge_records;
          }
        } else if (record.op_code == 4) {
          ++stats->single_node_read_records;
        } else if (record.op_code == 5) {
          ++stats->single_edge_read_records;
          IndexPreprocessedSingleEdgeRequest(record);
        } else if (record.op_code == 7) {
          IndexPreprocessedEdgeUpdateRequest(record);
        }
      }
    }
    snapshot_entity_total_ = next_vertex_id_;
    incremental_entity_total_ = next_vertex_id_;
    return true;
  }

  static uint32_t PreprocessedQueryId(const prechunk::SchemaInfo& schema) {
    return static_cast<uint32_t>(ToUint64(schema.Metadata("query_id")));
  }

  struct PreprocessedQueryTaskStorage {
    std::vector<CsvHeader> headers;
    std::vector<CsvRow> rows;
    std::vector<MixedQueryTask> tasks;
  };

  struct PreparedPreprocessedChunk {
    std::filesystem::path path;
    prechunk::Stage stage = prechunk::Stage::kUnknown;
    uint64_t record_count = 0;
    uint32_t arena_block_id = std::numeric_limits<uint32_t>::max();
    PreparedImportBatch write_batch;
    PreprocessedQueryTaskStorage query_storage;
    std::vector<std::vector<std::string>> single_node_read_fields;
    std::vector<std::vector<std::string>> single_edge_read_fields;
    std::vector<LightNodeUpdate> node_updates;
    std::vector<LightEdgeUpdate> edge_updates;
    UpdateWorkloadStats update_prepare_stats;
    prechunk::Chunk raw_chunk;
    bool has_write_batch = false;
    bool has_query_storage = false;
    bool has_single_node_reads = false;
    bool has_single_edge_reads = false;
    bool has_updates = false;

    explicit PreparedPreprocessedChunk(
        std::pmr::memory_resource* mr = std::pmr::get_default_resource())
        : write_batch(mr), raw_chunk(mr) {}
  };

  PreprocessedQueryTaskStorage BuildPreprocessedQueryTasks(
      const prechunk::Chunk& chunk,
      const prechunk::SchemaCatalog& catalog) {
    PreprocessedQueryTaskStorage storage;
    size_t query_records = 0;
    for (const auto& record : chunk.records) {
      if (record.op_code == 3) {
        ++query_records;
      }
    }
    storage.headers.reserve(query_records);
    storage.rows.reserve(query_records);
    storage.tasks.reserve(query_records);
    for (const auto& record : chunk.records) {
      if (record.op_code != 3) {
        continue;
      }
      const prechunk::SchemaInfo* schema = catalog.Find(record.schema_id);
      if (schema == nullptr || schema->kind != "query") {
        continue;
      }
      const uint32_t query_id = PreprocessedQueryId(*schema);
      if (query_id == 0 || query_id > 12) {
        continue;
      }
      storage.headers.push_back(BuildPreprocessedHeader(*schema));
      CsvRow row;
      row.header = &storage.headers.back();
      row.fields = prechunk::CopyFieldsToStd(record);
      storage.rows.push_back(std::move(row));
      storage.tasks.push_back(MixedQueryTask{
          static_cast<int>(query_id),
          &storage.rows.back(),
          record.row_index + 1});
    }
    return storage;
  }

  void AppendPreprocessedNodeRecord(PreparedImportBatch* batch,
                                    const prechunk::SchemaInfo& schema,
                                    const prechunk::Record& record,
                                    bool track_materialized = true,
                                    bool mark_latency = true) {
    const auto kind = NodeKindFromStringForPreprocessed(
        schema.Metadata("node_type"));
    if (!kind.has_value()) {
      return;
    }
    CsvHeader header;
    CsvRow row = BuildPreprocessedRow(schema, record, &header);
    const std::string id_column = schema.Metadata("id_column");
    const std::string& raw_id = row.Get(id_column);
    const vertex_t id = LookupPreprocessedEntityId(*kind, raw_id);
    if (id == lsmgraph::INVALID_VERTEX_ID) {
      return;
    }
    PushPreparedNodeWrite(
        *batch,
        BuildPreparedNodeWriteFromRow(id,
                                      *kind,
                                      raw_id,
                                      row,
                                      schema.columns,
                                      record.event_time_ms,
                                      LoadArenaResource()),
        mark_latency);
    if (track_materialized && MarkMaterializedVertex(id)) {
      ++batch->new_entity_nodes;
    }
    ++batch->logical_rows;
  }

  void CapturePreprocessedGeneratedColdSlots(
      const prechunk::SchemaInfo& schema,
      const CsvRow& row) {
    if (row.header == nullptr || preprocessed_edge_slot_need_.empty()) {
      return;
    }
    const std::string edge_type = schema.Metadata("edge_type");
    const std::string src_type = schema.Metadata("src_type");
    const std::string dst_type = schema.Metadata("dst_type");
    const std::string src_id = row.Get(schema.Metadata("src_column"));
    const std::string dst_id = row.Get(schema.Metadata("dst_column"));
    uint16_t slot = static_cast<uint16_t>(kColdEdgePropertyCount);
    for (const auto& column : row.header->columns) {
      if (!IsGeneratedColdProperty(column)) {
        continue;
      }
      if (!row.Get(column).empty()) {
        const std::string key = PreprocessedEdgeSlotKey(
            edge_type, src_type, src_id, dst_type, dst_id, column);
        if (preprocessed_edge_slot_need_.find(key) !=
            preprocessed_edge_slot_need_.end()) {
          std::lock_guard<std::mutex> lock(preprocessed_edge_slot_mu_);
          preprocessed_edge_slot_[key] = slot;
        }
      }
      ++slot;
    }
  }

  void AppendPreprocessedEdgeRecord(PreparedImportBatch* batch,
                                    const prechunk::SchemaInfo& schema,
                                    const prechunk::Record& record,
                                    bool sample_write_latency,
                                    bool materialize_missing_endpoints = true) {
    const std::string edge_type = schema.Metadata("edge_type");
    if (edge_type.empty()) {
      return;
    }
    RelationSpec rel = MustGetRelation(edge_type);
    const std::string src_col = schema.Metadata("src_column");
    const std::string dst_col = schema.Metadata("dst_column");
    if (!src_col.empty()) {
      rel.source_column = src_col;
    }
    if (!dst_col.empty()) {
      rel.dest_column = dst_col;
    }
    if (const auto src_kind =
            NodeKindFromStringForPreprocessed(schema.Metadata("src_type"))) {
      rel.src_kind = *src_kind;
    }
    if (const auto dst_kind =
            NodeKindFromStringForPreprocessed(schema.Metadata("dst_type"))) {
      rel.dst_kind = *dst_kind;
    }
    CsvHeader header;
    CsvRow row = BuildPreprocessedRow(schema, record, &header);
    const std::string& src_raw = row.Get(rel.source_column);
    const std::string& dst_raw = row.Get(rel.dest_column);
    const uint64_t scheduled_time =
        record.event_time_ms != 0 ? record.event_time_ms
                                  : ParseTimeToMillis(row.Get("createTime"));
    const vertex_t src = LookupPreprocessedEntityId(rel.src_kind, src_raw);
    if (materialize_missing_endpoints &&
        src != lsmgraph::INVALID_VERTEX_ID &&
        MarkMaterializedVertex(src)) {
      AppendNodeWrite(batch, src, rel.src_kind, src_raw, {}, true, scheduled_time);
    }
    const vertex_t dst = LookupPreprocessedEntityId(rel.dst_kind, dst_raw);
    if (materialize_missing_endpoints &&
        dst != lsmgraph::INVALID_VERTEX_ID &&
        MarkMaterializedVertex(dst)) {
      AppendNodeWrite(batch, dst, rel.dst_kind, dst_raw, {}, true, scheduled_time);
    }
    if (src == lsmgraph::INVALID_VERTEX_ID || dst == lsmgraph::INVALID_VERTEX_ID) {
      return;
    }
    PreparedRelationWrite write = BuildPreparedRelationWrite(rel, src, dst, row);
    write.scheduled_time = scheduled_time;
    if (sample_write_latency) {
      write.sample_write_latency = write_latency_sampler_.MarkNext();
    }
    CapturePreprocessedGeneratedColdSlots(schema, row);
    batch->relation_writes.push_back(std::move(write));
    ++batch->logical_rows;
  }

  bool BuildPreparedPreprocessedChunk(const prechunk::SchemaCatalog& catalog,
                                      prechunk::Chunk* raw,
                                      std::pmr::memory_resource* mr,
                                      PreparedPreprocessedChunk* out,
                                      std::string* error) {
    if (raw == nullptr || out == nullptr) {
      if (error != nullptr) {
        *error = "null preprocessed chunk";
      }
      return false;
    }
    out->path = raw->path;
    out->stage = raw->stage;
    out->record_count = raw->record_count;
    out->arena_block_id = raw->arena_block_id;
    const bool write_stage =
        raw->stage == prechunk::Stage::kSnapshotNodes ||
        raw->stage == prechunk::Stage::kSnapshotEdges ||
        raw->stage == prechunk::Stage::kRemainingNodes ||
        raw->stage == prechunk::Stage::kRemainingEdges ||
        raw->stage == prechunk::Stage::kMixedOps;
    if (raw->stage == prechunk::Stage::kFinalQuery) {
      out->query_storage = BuildPreprocessedQueryTasks(*raw, catalog);
      out->has_query_storage = true;
      return true;
    }
    if (raw->stage == prechunk::Stage::kSingleNodeRead) {
      out->single_node_read_fields.reserve(raw->records.size());
      for (const auto& record : raw->records) {
        out->single_node_read_fields.push_back(prechunk::CopyFieldsToStd(record));
      }
      out->has_single_node_reads = true;
      return true;
    }
    if (raw->stage == prechunk::Stage::kSingleEdgeRead) {
      out->single_edge_read_fields.reserve(raw->records.size());
      for (const auto& record : raw->records) {
        out->single_edge_read_fields.push_back(prechunk::CopyFieldsToStd(record));
      }
      out->has_single_edge_reads = true;
      return true;
    }
    if (raw->stage == prechunk::Stage::kFinbenchNodeUpdate ||
        raw->stage == prechunk::Stage::kFinbenchEdgeUpdate) {
      out->node_updates.reserve(raw->records.size());
      out->edge_updates.reserve(raw->records.size());
      for (const auto& record : raw->records) {
        if (record.op_code == 6) {
          ++out->update_prepare_stats.logical_rows;
          LightNodeUpdate update;
          const std::vector<std::string> fields =
              prechunk::CopyFieldsToStd(record);
          if (ResolvePreprocessedNodeUpdate(
                  fields, &update, &out->update_prepare_stats)) {
            out->node_updates.push_back(std::move(update));
          }
        } else if (record.op_code == 7) {
          ++out->update_prepare_stats.logical_rows;
          LightEdgeUpdate update;
          const std::vector<std::string> fields =
              prechunk::CopyFieldsToStd(record);
          if (ResolvePreprocessedEdgeUpdate(
                  fields, &update, &out->update_prepare_stats)) {
            out->edge_updates.push_back(std::move(update));
          }
        }
      }
      out->has_updates = true;
      return true;
    }
    if (!write_stage) {
      out->raw_chunk = std::move(*raw);
      return true;
    }

    ScopedLoadArenaResource scoped(mr);
    out->write_batch.name.assign(raw->path.filename().string());
    out->write_batch.node_writes.reserve(raw->records.size());
    out->write_batch.relation_writes.reserve(raw->records.size());
    for (const auto& record : raw->records) {
      const prechunk::SchemaInfo* schema = catalog.Find(record.schema_id);
      if (schema == nullptr) {
        continue;
      }
      if (record.op_code == 1 && schema->kind == "node") {
        AppendPreprocessedNodeRecord(&out->write_batch,
                                     *schema,
                                     record,
                                     false,
                                     false);
      } else if (record.op_code == 2 && schema->kind == "edge") {
        AppendPreprocessedEdgeRecord(&out->write_batch,
                                     *schema,
                                     record,
                                     false,
                                     false);
      }
    }
    out->has_write_batch = true;
    if (raw->stage == prechunk::Stage::kMixedOps &&
        FLAGS_finbench_mix_enable_queries) {
      out->query_storage = BuildPreprocessedQueryTasks(*raw, catalog);
      out->has_query_storage = true;
    }
    return true;
  }

  ImportStats ExecutePreparedPreprocessedWriteChunk(
      PreparedPreprocessedChunk* chunk,
      bool sample_nodes,
      bool sample_edges) {
    ImportStats stats;
    if (chunk == nullptr || !chunk->has_write_batch) {
      return stats;
    }
    if (sample_nodes) {
      for (auto& write : chunk->write_batch.node_writes) {
        write.sample_write_latency = node_write_latency_sampler_.MarkNext();
      }
    }
    if (sample_edges) {
      for (auto& write : chunk->write_batch.relation_writes) {
        write.sample_write_latency = write_latency_sampler_.MarkNext();
      }
    }
    FlushPreparedImportBatch(&chunk->write_batch, &stats);
    AddColdBlobFlushTime(&stats);
    return stats;
  }

  MixedWorkloadStats ExecutePreparedPreprocessedMixedChunk(
      PreparedPreprocessedChunk* chunk) {
    MixedWorkloadStats stats;
    if (chunk == nullptr) {
      return stats;
    }
    for (int qid = 1; qid <= 12; ++qid) {
      stats.query_metrics[static_cast<size_t>(qid)].query_id = qid;
    }
    ImportStats import_stats;
    const std::vector<MixedQueryTask> empty_tasks;
    const std::vector<MixedQueryTask>& tasks =
        chunk->has_query_storage ? chunk->query_storage.tasks : empty_tasks;
    ExecuteMixedConcurrentBatch(&chunk->write_batch,
                                tasks,
                                &import_stats,
                                &stats);
    stats.node_writes += import_stats.node_writes;
    stats.edge_writes += import_stats.edge_writes;
    stats.write_sec += import_stats.sec;
    return stats;
  }

  ImportStats ImportPreprocessedWriteChunk(
      const prechunk::Chunk& chunk,
      const prechunk::SchemaCatalog& catalog) {
    ImportStats stats;
    auto batch = NewPreparedBatch(chunk.path.filename().string(),
                                  ImportBatchRows(),
                                  ImportBatchRows());
    for (const auto& record : chunk.records) {
      const prechunk::SchemaInfo* schema = catalog.Find(record.schema_id);
      if (schema == nullptr) {
        continue;
      }
      if (record.op_code == 1 && schema->kind == "node") {
        AppendPreprocessedNodeRecord(batch.get(), *schema, record);
      } else if (record.op_code == 2 && schema->kind == "edge") {
        AppendPreprocessedEdgeRecord(
            batch.get(),
            *schema,
            record,
            chunk.stage == prechunk::Stage::kSnapshotEdges);
      } else {
        continue;
      }
      if (batch->logical_rows >= ImportBatchRows() || LoadArenaNearFull()) {
        FlushPreparedImportBatch(batch.get(), &stats);
        ReleasePreparedBatch(&batch, "preprocessed_write");
        batch = NewPreparedBatch(chunk.path.filename().string(),
                                 ImportBatchRows(),
                                 ImportBatchRows());
      }
    }
    FlushPreparedImportBatch(batch.get(), &stats);
    AddColdBlobFlushTime(&stats);
    ReleasePreparedBatch(&batch, "preprocessed_write");
    return stats;
  }

  MixedWorkloadStats RunPreprocessedMixedChunk(
      const prechunk::Chunk& chunk,
      const prechunk::SchemaCatalog& catalog) {
    MixedWorkloadStats stats;
    for (int qid = 1; qid <= 12; ++qid) {
      stats.query_metrics[static_cast<size_t>(qid)].query_id = qid;
    }
    auto batch = NewPreparedBatch(chunk.path.filename().string(),
                                  ImportBatchRows(),
                                  ImportBatchRows());
    for (const auto& record : chunk.records) {
      const prechunk::SchemaInfo* schema = catalog.Find(record.schema_id);
      if (schema == nullptr) {
        continue;
      }
      if (record.op_code == 1 && schema->kind == "node") {
        AppendPreprocessedNodeRecord(batch.get(), *schema, record);
      } else if (record.op_code == 2 && schema->kind == "edge") {
        AppendPreprocessedEdgeRecord(batch.get(), *schema, record, false);
      }
    }
    PreprocessedQueryTaskStorage query_storage =
        FLAGS_finbench_mix_enable_queries
            ? BuildPreprocessedQueryTasks(chunk, catalog)
            : PreprocessedQueryTaskStorage{};
    ImportStats import_stats;
    ExecuteMixedConcurrentBatch(
        batch.get(), query_storage.tasks, &import_stats, &stats);
    ReleasePreparedBatch(&batch, "preprocessed_mixed");
    stats.node_writes += import_stats.node_writes;
    stats.edge_writes += import_stats.edge_writes;
    stats.write_sec += import_stats.sec;
    return stats;
  }

  static void AccumulateMixedStats(MixedWorkloadStats* total,
                                   const MixedWorkloadStats& delta) {
    total->node_writes += delta.node_writes;
    total->edge_writes += delta.edge_writes;
    total->query_ops += delta.query_ops;
    total->result_rows += delta.result_rows;
    total->checksum ^= delta.checksum;
    total->total_sec += delta.total_sec;
    total->write_sec += delta.write_sec;
    total->query_sec += delta.query_sec;
    total->background_wait_sec += delta.background_wait_sec;
    total->wall_sec += delta.wall_sec;
    for (int qid = 1; qid <= 12; ++qid) {
      QueryMetrics& dst = total->query_metrics[static_cast<size_t>(qid)];
      const QueryMetrics& src = delta.query_metrics[static_cast<size_t>(qid)];
      dst.query_id = qid;
      dst.param_rows += src.param_rows;
      dst.result_rows += src.result_rows;
      dst.checksum ^= src.checksum;
      dst.sec += src.sec;
    }
  }

  PreprocessedQueryStats RunPreprocessedQueryChunk(
      const prechunk::Chunk& chunk,
      const prechunk::SchemaCatalog& catalog) {
    PreprocessedQueryTaskStorage storage =
        BuildPreprocessedQueryTasks(chunk, catalog);
    return RunPreparedPreprocessedQueryTasks(storage);
  }

  PreprocessedQueryStats RunPreparedPreprocessedQueryTasks(
      const PreprocessedQueryTaskStorage& storage) {
    PreprocessedQueryStats stats;
    std::vector<QueryRunResult> results(storage.tasks.size());
    std::vector<double> secs(storage.tasks.size(), 0.0);
    const auto t1 = std::chrono::steady_clock::now();
    ParallelForIndexDynamic(storage.tasks.size(), GetReadThreadCount(), [&](size_t i) {
      const auto q1 = std::chrono::steady_clock::now();
      results[i] = RunQuery(storage.tasks[i].query_id, *storage.tasks[i].row);
      const auto q2 = std::chrono::steady_clock::now();
      secs[i] = std::chrono::duration_cast<std::chrono::duration<double>>(
                    q2 - q1)
                    .count();
    });
    (void)t1;
    for (size_t i = 0; i < storage.tasks.size(); ++i) {
      const int qid = storage.tasks[i].query_id;
      QueryMetrics& metric = stats.query_metrics[static_cast<size_t>(qid)];
      ++metric.param_rows;
      metric.result_rows += results[i].rows;
      metric.checksum ^= results[i].checksum;
      metric.sec += secs[i];
    }
    return stats;
  }

  static void PrintPreprocessedQueryStats(
      const PreprocessedQueryStats& stats) {
    uint64_t total_params = 0;
    uint64_t total_rows = 0;
    uint64_t total_checksum = 0;
    double total_sec = 0.0;
    for (int qid = 1; qid <= 12; ++qid) {
      const QueryMetrics& metric =
          stats.query_metrics[static_cast<size_t>(qid)];
      total_params += metric.param_rows;
      total_rows += metric.result_rows;
      total_checksum ^= metric.checksum;
      total_sec += metric.sec;
    }
    const double elapsed_sec = stats.wall_sec > 0.0 ? stats.wall_sec : total_sec;
    const double total_qps =
        elapsed_sec <= 0.0 || total_params == 0
            ? 0.0
            : static_cast<double>(total_params) / elapsed_sec;
    std::cout << "[QUERY_TOTAL] time(s): " << elapsed_sec << std::endl;
    std::cout << "[QUERY_TOTAL] foreground_query_time(s): " << total_sec
              << std::endl;
    std::cout << "[QUERY_TOTAL] total_params: " << total_params << std::endl;
    std::cout << "[QUERY_TOTAL] qps(param/s): " << total_qps << std::endl;
    std::cout << "[QUERY_TOTAL] total_result_rows: " << total_rows
              << std::endl;
    std::cout << "[QUERY_TOTAL] checksum: " << total_checksum << std::endl;
    for (int qid = 1; qid <= 12; ++qid) {
      const QueryMetrics& metric =
          stats.query_metrics[static_cast<size_t>(qid)];
      const double qps =
          metric.sec <= 0.0 || metric.param_rows == 0
              ? 0.0
              : static_cast<double>(metric.param_rows) / metric.sec;
      std::cout << "[QUERY_" << qid << "] time(s): " << metric.sec
                << std::endl;
      std::cout << "[QUERY_" << qid << "] params: " << metric.param_rows
                << std::endl;
      std::cout << "[QUERY_" << qid << "] qps(param/s): " << qps
                << std::endl;
      std::cout << "[QUERY_" << qid << "] result_rows: "
                << metric.result_rows << std::endl;
      std::cout << "[QUERY_" << qid << "] checksum: " << metric.checksum
                << std::endl;
    }
  }

  static void AccumulateImportStats(ImportStats* total,
                                    const ImportStats& delta) {
    total->logical_rows += delta.logical_rows;
    total->node_writes += delta.node_writes;
    total->edge_writes += delta.edge_writes;
    total->entity_nodes += delta.entity_nodes;
    total->sec += delta.sec;
    total->background_wait_sec += delta.background_wait_sec;
    total->wall_sec += delta.wall_sec;
  }

  static bool IsFinBenchNodeIdAlias(NodeKind kind,
                                    const std::string& property) {
    return property == "rawId" ||
           (kind == NodeKind::kPerson && property == "personId") ||
           (kind == NodeKind::kCompany && property == "companyId") ||
           (kind == NodeKind::kAccount && property == "accountId") ||
           (kind == NodeKind::kLoan && property == "loanId") ||
           (kind == NodeKind::kMedium && property == "mediumId");
  }

  bool ResolvePreprocessedNodeUpdate(
      const std::vector<std::string>& fields,
      LightNodeUpdate* update,
      UpdateWorkloadStats* stats) {
    if (fields.size() < 6 || update == nullptr || stats == nullptr) {
      return false;
    }
    const auto kind = NodeKindFromStringForPreprocessed(fields[1]);
    if (!kind.has_value()) {
      ++stats->missing_targets;
      return false;
    }
    update->vid = RequireExistingEntityId(*kind, fields[2],
                                          "finbench_node_update");
    std::string logical_name;
    const std::string prop_token =
        IsFinBenchNodeIdAlias(*kind, fields[3]) ? "rawId" : fields[3];
    if (!ResolveNodeUpdateProperty(prop_token,
                                   &update->logical_property_id,
                                   &update->storage_property_id,
                                   &update->cold_slot,
                                   &update->storage_property,
                                   &logical_name,
                                   &update->cold)) {
      ++stats->missing_targets;
      return false;
    }
    update->value = NormalizePropertyValue(logical_name, fields[5]);
    return true;
  }

  bool IsPreprocessedTopologyEdgeProperty(const RelationSpec& rel,
                                          const std::string& property) const {
    return property == rel.source_column || property == rel.dest_column ||
           property == "src_id" || property == "dst_id" ||
           property == "edgeExists";
  }

  bool ResolvePreprocessedEdgeUpdate(
      const std::vector<std::string>& fields,
      LightEdgeUpdate* update,
      UpdateWorkloadStats* stats) {
    if (fields.size() < 9 || update == nullptr || stats == nullptr) {
      return false;
    }
    const std::string& edge_type_name = fields[1];
    const std::string& property = fields[6];
    const RelationSpec& rel = MustGetRelation(edge_type_name);
    if (IsPreprocessedTopologyEdgeProperty(rel, property)) {
      ++stats->missing_targets;
      return false;
    }
    const auto src_kind = NodeKindFromStringForPreprocessed(fields[2]);
    const auto dst_kind = NodeKindFromStringForPreprocessed(fields[4]);
    if (!src_kind.has_value() || !dst_kind.has_value()) {
      ++stats->missing_targets;
      return false;
    }
    vertex_t src = RequireExistingEntityId(*src_kind, fields[3],
                                           "finbench_edge_update");
    vertex_t dst = RequireExistingEntityId(*dst_kind, fields[5],
                                           "finbench_edge_update");
    bool is_out = true;
    switch (WriteDirectionForRelation(edge_type_name)) {
      case RelationWriteDirection::kReverseOnly:
        std::swap(src, dst);
        is_out = false;
        break;
      case RelationWriteDirection::kForwardOnly:
      case RelationWriteDirection::kBidirectional:
        is_out = true;
        break;
    }
    update->src = src;
    update->dst = dst;
    update->edge_type = rel.edge_type;
    update->is_out = is_out;
    std::string logical_name;
    if (!ResolveEdgeUpdateProperty(property,
                                   &update->logical_property_id,
                                   &update->storage_property_id,
                                   &update->cold_slot,
                                   &update->storage_property,
                                   &logical_name,
                                   &update->cold)) {
      ++stats->missing_targets;
      return false;
    }
    if (update->cold && IsGeneratedColdProperty(property)) {
      const std::string key = PreprocessedEdgeSlotKey(edge_type_name,
                                                      fields[2],
                                                      fields[3],
                                                      fields[4],
                                                      fields[5],
                                                      property);
      const auto slot_it = preprocessed_edge_slot_.find(key);
      if (slot_it != preprocessed_edge_slot_.end()) {
        update->cold_slot = slot_it->second;
      }
    }
    update->value = NormalizePropertyValue(logical_name, fields[8]);
    return true;
  }

  void InitEngineUpdateState(EngineUpdateRunState* state,
                             const std::string& phase) {
    if (state == nullptr || state->started) {
      return;
    }
    state->phase = phase;
    state->initial_engine_stats = db_->GetPropertyUpdateStats();
    // Cold properties are stored in append-only blob files. Make the import
    // payload visible before the update translator performs its first
    // read-modify-append operation. Buffering and delta publication belong to
    // GraphDb and are deliberately not implemented in this benchmark.
    if (!cold_blob_writers_.Flush() || !node_cold_blob_writers_.Flush()) {
      std::cerr << "failed to flush cold-property blobs before updates"
                << std::endl;
      std::exit(1);
    }
    state->start_time = std::chrono::steady_clock::now();
    state->last_node_report_time = state->start_time;
    state->last_edge_report_time = state->start_time;
    state->started = true;
  }

  static double SecondsSince(std::chrono::steady_clock::time_point begin,
                             std::chrono::steady_clock::time_point end) {
    return std::chrono::duration_cast<std::chrono::duration<double>>(end -
                                                                     begin)
        .count();
  }

  void PrintEngineUpdateProgress(EngineUpdateRunState* state,
                                 bool node) const {
    if (state == nullptr) {
      return;
    }
    auto& next_report =
        node ? state->next_node_report : state->next_edge_report;
    auto& last_report_updates =
        node ? state->last_node_report_updates
             : state->last_edge_report_updates;
    auto& last_report_time =
        node ? state->last_node_report_time : state->last_edge_report_time;
    const uint64_t updates =
        node ? state->stats.node_updates : state->stats.edge_updates;
    while (updates >= next_report) {
      const auto now = std::chrono::steady_clock::now();
      const uint64_t interval_updates = next_report - last_report_updates;
      const double interval_sec = SecondsSince(last_report_time, now);
      const double cumulative_sec = SecondsSince(state->start_time, now);
      const double interval_qps =
          interval_sec <= 0.0
              ? 0.0
              : static_cast<double>(interval_updates) / interval_sec;
      const double cumulative_qps =
          cumulative_sec <= 0.0
              ? 0.0
              : static_cast<double>(next_report) / cumulative_sec;
      std::cout << "[" << state->phase << "_"
                << (node ? "NODE" : "EDGE") << "_PROGRESS] updates: "
                << next_report << std::endl;
      std::cout << "[" << state->phase << "_"
                << (node ? "NODE" : "EDGE")
                << "_PROGRESS] interval_time(s): " << interval_sec
                << std::endl;
      std::cout << "[" << state->phase << "_"
                << (node ? "NODE" : "EDGE")
                << "_PROGRESS] interval_qps(update/s): " << interval_qps
                << std::endl;
      std::cout << "[" << state->phase << "_"
                << (node ? "NODE" : "EDGE")
                << "_PROGRESS] cumulative_time(s): " << cumulative_sec
                << std::endl;
      std::cout << "[" << state->phase << "_"
                << (node ? "NODE" : "EDGE")
                << "_PROGRESS] cumulative_qps(update/s): "
                << cumulative_qps << std::endl;
      last_report_updates = next_report;
      last_report_time = now;
      next_report += 1000000ULL;
    }
  }

  void ApplyNodeUpdate(EngineUpdateRunState* state,
                       LightNodeUpdate update) {
    InitEngineUpdateState(state, "FINBENCH_UPDATE");
    ++state->stats.node_updates;
    const auto cold_begin = std::chrono::steady_clock::now();
    if (!RewriteNodeColdUpdateValue(&update, &state->stats)) {
      ++state->stats.missing_targets;
      PrintEngineUpdateProgress(state, true);
      return;
    }
    const auto cold_end = std::chrono::steady_clock::now();
    state->stats.cold_rewrite_sec += SecondsSince(cold_begin, cold_end);

    const auto write_begin = std::chrono::steady_clock::now();
    const auto status = db_->UpdateNode(
        update.vid,
        {{update.storage_property, update.value}},
        true,
        kNodeEdgeType);
    const auto write_end = std::chrono::steady_clock::now();
    state->stats.write_sec += SecondsSince(write_begin, write_end);
    state->stats.value_bytes += update.value.size();
    if (status != lsmgraph::Status::kOk) {
      ++state->stats.missing_targets;
    }
    PrintEngineUpdateProgress(state, true);
  }

  void ApplyEdgeUpdate(EngineUpdateRunState* state,
                       LightEdgeUpdate update) {
    InitEngineUpdateState(state, "FINBENCH_UPDATE");
    ++state->stats.edge_updates;
    const auto cold_begin = std::chrono::steady_clock::now();
    if (!RewriteEdgeColdUpdateValue(&update, &state->stats)) {
      ++state->stats.missing_targets;
      PrintEngineUpdateProgress(state, false);
      return;
    }
    const auto cold_end = std::chrono::steady_clock::now();
    state->stats.cold_rewrite_sec += SecondsSince(cold_begin, cold_end);

    const auto write_begin = std::chrono::steady_clock::now();
    const auto status = db_->UpdateEdge(
        update.src,
        update.dst,
        {{update.storage_property, update.value}},
        update.is_out,
        update.edge_type);
    const auto write_end = std::chrono::steady_clock::now();
    state->stats.write_sec += SecondsSince(write_begin, write_end);
    state->stats.value_bytes += update.value.size();
    if (status != lsmgraph::Status::kOk) {
      ++state->stats.missing_targets;
    }
    PrintEngineUpdateProgress(state, false);
  }

  void RunPreprocessedUpdateChunk(const prechunk::Chunk& chunk,
                                  EngineUpdateRunState* state,
                                  const std::string& phase) {
    InitEngineUpdateState(state, phase);
    for (const auto& record : chunk.records) {
      if (record.op_code == 6) {
        ++state->stats.logical_rows;
        LightNodeUpdate update;
        const std::vector<std::string> fields =
            prechunk::CopyFieldsToStd(record);
        if (ResolvePreprocessedNodeUpdate(fields, &update, &state->stats)) {
          ApplyNodeUpdate(state, std::move(update));
        }
      } else if (record.op_code == 7) {
        ++state->stats.logical_rows;
        LightEdgeUpdate update;
        const std::vector<std::string> fields =
            prechunk::CopyFieldsToStd(record);
        if (ResolvePreprocessedEdgeUpdate(fields, &update, &state->stats)) {
          ApplyEdgeUpdate(state, std::move(update));
        }
      }
    }
  }

  void RunPreparedPreprocessedUpdateChunk(
      const PreparedPreprocessedChunk& chunk,
      EngineUpdateRunState* state,
      const std::string& phase) {
    InitEngineUpdateState(state, phase);
    if (state == nullptr) {
      return;
    }
    state->stats.logical_rows += chunk.update_prepare_stats.logical_rows;
    state->stats.missing_targets += chunk.update_prepare_stats.missing_targets;
    for (const auto& update : chunk.node_updates) {
      ApplyNodeUpdate(state, update);
    }
    for (const auto& update : chunk.edge_updates) {
      ApplyEdgeUpdate(state, update);
    }
  }

  UpdateWorkloadStats FinalizeEngineUpdateState(
      EngineUpdateRunState* state) {
    if (state == nullptr) {
      return UpdateWorkloadStats{};
    }
    if (db_->FlushPropertyUpdates() != lsmgraph::Status::kOk) {
      std::cerr << "failed to flush engine property updates" << std::endl;
      std::exit(1);
    }
    const auto current = db_->GetPropertyUpdateStats();
    state->stats.flushes =
        current.buffers_flushed - state->initial_engine_stats.buffers_flushed;
    state->stats.delta_files = current.delta_batches_published -
                               state->initial_engine_stats.delta_batches_published;
    state->stats.engine_submitted_bytes =
        current.submitted_bytes - state->initial_engine_stats.submitted_bytes;
    state->stats.sec = SecondsSince(state->start_time,
                                    std::chrono::steady_clock::now());
    return state->stats;
  }

  static void PrintUpdateStatsWithPhase(const char* phase,
                                        const UpdateWorkloadStats& stats) {
    const uint64_t updates = stats.node_updates + stats.edge_updates;
    const double qps = (stats.sec <= 0.0 || updates == 0)
                           ? 0.0
                           : static_cast<double>(updates) / stats.sec;
    const double node_qps = (stats.sec <= 0.0 || stats.node_updates == 0)
                                ? 0.0
                                : static_cast<double>(stats.node_updates) /
                                      stats.sec;
    const double edge_qps = (stats.sec <= 0.0 || stats.edge_updates == 0)
                                ? 0.0
                                : static_cast<double>(stats.edge_updates) /
                                      stats.sec;
    std::cout << "[" << phase << "] time(s): " << stats.sec << std::endl;
    std::cout << "[" << phase << "] locate_time(s): " << stats.locate_sec
              << std::endl;
    std::cout << "[" << phase << "] cold_rewrite_time(s): "
              << stats.cold_rewrite_sec << std::endl;
    std::cout << "[" << phase << "] engine_update_time(s): "
              << stats.write_sec << std::endl;
    std::cout << "[" << phase << "] logical_rows: " << stats.logical_rows
              << std::endl;
    std::cout << "[" << phase << "] updates: " << updates << std::endl;
    std::cout << "[" << phase << "] node_updates: " << stats.node_updates
              << std::endl;
    std::cout << "[" << phase << "] edge_updates: " << stats.edge_updates
              << std::endl;
    std::cout << "[" << phase << "] missing_targets: "
              << stats.missing_targets << std::endl;
    std::cout << "[" << phase << "] cold_blob_rewrites: "
              << stats.cold_blob_rewrites << std::endl;
    std::cout << "[" << phase << "] property_buffer_flushes: " << stats.flushes
              << std::endl;
    std::cout << "[" << phase << "] delta_batches: " << stats.delta_files
              << std::endl;
    std::cout << "[" << phase << "] value_bytes: " << stats.value_bytes
              << std::endl;
    std::cout << "[" << phase << "] blob_payload_bytes: "
              << stats.blob_payload_bytes << std::endl;
    std::cout << "[" << phase << "] engine_submitted_bytes: "
              << stats.engine_submitted_bytes
              << std::endl;
    std::cout << "[" << phase << "] qps(update/s): " << qps
              << std::endl;
    std::cout << "[" << phase << "] node_qps(update/s): " << node_qps
              << std::endl;
    std::cout << "[" << phase << "] edge_qps(update/s): " << edge_qps
              << std::endl;
  }

  PreprocessedReadStats RunPreparedSingleNodeReadChunk(
      const std::vector<std::vector<std::string>>& records) {
    PreprocessedReadStats stats;
    if (records.empty()) {
      return stats;
    }
    std::vector<uint64_t> checksums(records.size(), 0);
    std::vector<uint8_t> found(records.size(), 0);
    std::vector<uint8_t> hot(records.size(), 0);
    std::vector<std::string> miss_reasons(records.size());
    const auto t1 = std::chrono::steady_clock::now();
    ParallelForIndexDynamic(records.size(), GetReadThreadCount(), [&](size_t i) {
      const std::vector<std::string>& fields = records[i];
      if (fields.size() < 5) {
        miss_reasons[i] = "bad_single_node_record";
        return;
      }
      hot[i] = (fields[4] == "1" || fields[4] == "true") ? 1 : 0;
      const auto kind = NodeKindFromStringForPreprocessed(fields[1]);
      if (!kind.has_value()) {
        miss_reasons[i] = "unknown_node_type:" + fields[1];
        return;
      }
      const vertex_t vid = RequireExistingEntityId(*kind, fields[2],
                                                   "single_node_read");
      std::string value;
      if (IsFinBenchNodeIdAlias(*kind, fields[3])) {
        value = fields[2];
      } else {
        value = GetNodeProp(vid, fields[3]);
      }
      if (value.empty()) {
        miss_reasons[i] = "unmapped_or_empty_node_property:" +
                          fields[1] + "." + fields[3];
        return;
      }
      found[i] = 1;
      checksums[i] = HashMix(static_cast<uint64_t>(vid)) ^
                     HashMix(std::hash<std::string>{}(fields[3])) ^
                     HashMix(std::hash<std::string>{}(value));
    });
    const auto t2 = std::chrono::steady_clock::now();
    stats.sec =
        std::chrono::duration_cast<std::chrono::duration<double>>(t2 - t1)
            .count();
    for (size_t i = 0; i < records.size(); ++i) {
      ++stats.ops;
      if (hot[i] != 0) {
        ++stats.hot_ops;
      } else {
        ++stats.cold_ops;
      }
      if (found[i] != 0) {
        ++stats.found;
      } else {
        ++stats.missed;
        ++stats.miss_reasons[miss_reasons[i].empty()
                                  ? "unknown_single_node_miss"
                                  : miss_reasons[i]];
      }
      stats.checksum ^= HashMix(checksums[i] + i + 1);
    }
    return stats;
  }

  PreprocessedReadStats RunPreprocessedSingleNodeReadChunk(
      const prechunk::Chunk& chunk) {
    std::vector<std::vector<std::string>> records;
    records.reserve(chunk.records.size());
    for (const auto& record : chunk.records) {
      records.push_back(prechunk::CopyFieldsToStd(record));
    }
    return RunPreparedSingleNodeReadChunk(records);
  }

  bool ReadPreprocessedSingleEdgeProperty(
      const std::vector<std::string>& fields,
      std::string* value,
      std::string* miss_reason) const {
    if (fields.size() < 8 || value == nullptr) {
      if (miss_reason != nullptr) {
        *miss_reason = "bad_single_edge_record";
      }
      return false;
    }
    const std::string& edge_type_name = fields[1];
    const std::string& src_type_name = fields[2];
    const std::string& src_raw = fields[3];
    const std::string& dst_type_name = fields[4];
    const std::string& dst_raw = fields[5];
    const std::string& property = fields[6];
    const bool hot = fields[7] == "1" || fields[7] == "true";
    const RelationSpec& rel = MustGetRelation(edge_type_name);
    const auto src_kind = NodeKindFromStringForPreprocessed(src_type_name);
    const auto dst_kind = NodeKindFromStringForPreprocessed(dst_type_name);
    if (!src_kind.has_value() || !dst_kind.has_value()) {
      if (miss_reason != nullptr) {
        *miss_reason = "unknown_edge_endpoint_type:" + edge_type_name;
      }
      return false;
    }
    if (property == "edgeExists") {
      *value = "1";
      return true;
    }
    if (property == rel.source_column || property == "src_id") {
      *value = src_raw;
      return true;
    }
    if (property == rel.dest_column || property == "dst_id") {
      *value = dst_raw;
      return true;
    }
    vertex_t src = RequireExistingEntityId(*src_kind, src_raw,
                                           "single_edge_read");
    vertex_t dst = RequireExistingEntityId(*dst_kind, dst_raw,
                                           "single_edge_read");
    bool is_out = true;
    switch (WriteDirectionForRelation(edge_type_name)) {
      case RelationWriteDirection::kReverseOnly:
        std::swap(src, dst);
        is_out = false;
        break;
      case RelationWriteDirection::kForwardOnly:
      case RelationWriteDirection::kBidirectional:
        is_out = true;
        break;
    }
    if (hot) {
      std::unordered_map<std::string, std::string> props;
      const auto rs = db_->GetEdge(src, dst, {property}, &props, is_out,
                                   rel.edge_type);
      if (rs != lsmgraph::Status::kOk) {
        if (miss_reason != nullptr) {
          *miss_reason = "missing_hot_edge:" + edge_type_name + "." + property;
        }
        return false;
      }
      const auto it = props.find(property);
      if (it == props.end() || it->second.empty()) {
        if (miss_reason != nullptr) {
          *miss_reason = "empty_hot_edge_property:" + edge_type_name + "." +
                         property;
        }
        return false;
      }
      *value = it->second;
      return true;
    }
    std::unordered_map<std::string, std::string> ref_props;
    const auto rs = db_->GetEdge(src, dst, {"cold_property"}, &ref_props,
                                 is_out, rel.edge_type);
    if (rs != lsmgraph::Status::kOk) {
      if (miss_reason != nullptr) {
        *miss_reason = "missing_cold_ref_edge:" + edge_type_name;
      }
      return false;
    }
    const auto ref_it = ref_props.find("cold_property");
    if (ref_it == ref_props.end() || ref_it->second.empty()) {
      if (miss_reason != nullptr) {
        *miss_reason = "empty_cold_ref_edge:" + edge_type_name;
      }
      return false;
    }
    const std::string payload = ReadColdBlobPayloadNoCache(ref_it->second);
    if (payload.empty()) {
      if (miss_reason != nullptr) {
        *miss_reason = "empty_cold_blob:" + edge_type_name;
      }
      return false;
    }
    int slot = ColdEdgePropertySlot(property);
    if (slot < 0 && IsGeneratedColdProperty(property)) {
      const std::string key = PreprocessedEdgeSlotKey(edge_type_name,
                                                      src_type_name,
                                                      src_raw,
                                                      dst_type_name,
                                                      dst_raw,
                                                      property);
      const auto it = preprocessed_edge_slot_.find(key);
      if (it != preprocessed_edge_slot_.end()) {
        slot = it->second;
      } else {
        slot = EdgeColdSlotByName(property);
      }
    }
    if (slot < 0) {
      if (miss_reason != nullptr) {
        *miss_reason = "unmapped_cold_edge_property:" + edge_type_name + "." +
                       property;
      }
      return false;
    }
    if (!GetPipeFieldBySlot(payload, static_cast<uint16_t>(slot), value) ||
        value->empty()) {
      if (miss_reason != nullptr) {
        *miss_reason = "empty_cold_edge_property:" + edge_type_name + "." +
                       property;
      }
      return false;
    }
    return true;
  }

  PreprocessedReadStats RunPreparedSingleEdgeReadChunk(
      const std::vector<std::vector<std::string>>& records) {
    PreprocessedReadStats stats;
    if (records.empty()) {
      return stats;
    }
    std::vector<uint64_t> checksums(records.size(), 0);
    std::vector<uint8_t> found(records.size(), 0);
    std::vector<uint8_t> hot(records.size(), 0);
    std::vector<std::string> miss_reasons(records.size());
    const auto t1 = std::chrono::steady_clock::now();
    ParallelForIndexDynamic(records.size(), GetReadThreadCount(), [&](size_t i) {
      const std::vector<std::string>& fields = records[i];
      if (fields.size() < 8) {
        return;
      }
      hot[i] = (fields[7] == "1" || fields[7] == "true") ? 1 : 0;
      std::string value;
      if (!ReadPreprocessedSingleEdgeProperty(fields, &value, &miss_reasons[i])) {
        return;
      }
      found[i] = 1;
      checksums[i] =
          HashMix(std::hash<std::string>{}(fields[1])) ^
          HashMix(std::hash<std::string>{}(fields[3])) ^
          HashMix(std::hash<std::string>{}(fields[5])) ^
          HashMix(std::hash<std::string>{}(fields[6])) ^
          HashMix(std::hash<std::string>{}(value));
    });
    const auto t2 = std::chrono::steady_clock::now();
    stats.sec =
        std::chrono::duration_cast<std::chrono::duration<double>>(t2 - t1)
            .count();
    for (size_t i = 0; i < records.size(); ++i) {
      ++stats.ops;
      if (hot[i] != 0) {
        ++stats.hot_ops;
      } else {
        ++stats.cold_ops;
      }
      if (found[i] != 0) {
        ++stats.found;
      } else {
        ++stats.missed;
        ++stats.miss_reasons[miss_reasons[i].empty()
                                  ? "unknown_single_edge_miss"
                                  : miss_reasons[i]];
      }
      stats.checksum ^= HashMix(checksums[i] + i + 1);
    }
    return stats;
  }

  PreprocessedReadStats RunPreprocessedSingleEdgeReadChunk(
      const prechunk::Chunk& chunk) {
    std::vector<std::vector<std::string>> records;
    records.reserve(chunk.records.size());
    for (const auto& record : chunk.records) {
      records.push_back(prechunk::CopyFieldsToStd(record));
    }
    return RunPreparedSingleEdgeReadChunk(records);
  }

  static void PrintPreprocessedReadStats(const char* phase,
                                         const PreprocessedReadStats& stats) {
    const double qps = stats.sec <= 0.0 ? 0.0
                                        : static_cast<double>(stats.ops) /
                                              stats.sec;
    std::cout << "[" << phase << "] time(s): " << stats.sec << std::endl;
    std::cout << "[" << phase << "] ops: " << stats.ops << std::endl;
    std::cout << "[" << phase << "] qps(op/s): " << qps << std::endl;
    std::cout << "[" << phase << "] hot_ops: " << stats.hot_ops << std::endl;
    std::cout << "[" << phase << "] cold_ops: " << stats.cold_ops << std::endl;
    std::cout << "[" << phase << "] found: " << stats.found << std::endl;
    std::cout << "[" << phase << "] missed: " << stats.missed << std::endl;
    std::cout << "[" << phase << "] checksum: " << stats.checksum
              << std::endl;
    if (!stats.miss_reasons.empty()) {
      std::vector<std::pair<std::string, uint64_t>> reasons(
          stats.miss_reasons.begin(), stats.miss_reasons.end());
      std::sort(reasons.begin(), reasons.end(), [](const auto& a,
                                                   const auto& b) {
        if (a.second != b.second) {
          return a.second > b.second;
        }
        return a.first < b.first;
      });
      const size_t limit = std::min<size_t>(10, reasons.size());
      for (size_t i = 0; i < limit; ++i) {
        std::cout << "[" << phase << "_MISS_REASON] " << reasons[i].first
                  << ": " << reasons[i].second << std::endl;
      }
    }
  }

  static void PrintPreprocessedMixedStats(const MixedWorkloadStats& stats) {
    const uint64_t ops = stats.node_writes + stats.edge_writes +
                         stats.query_ops;
    const double qps = stats.total_sec <= 0.0 ? 0.0
                                              : static_cast<double>(ops) /
                                                    stats.total_sec;
    std::cout << "[MIXED_WORKLOAD] time(s): " << stats.total_sec << std::endl;
    std::cout << "[MIXED_WORKLOAD] ops: " << ops << std::endl;
    std::cout << "[MIXED_WORKLOAD] qps(op/s): " << qps << std::endl;
    std::cout << "[MIXED_WORKLOAD] node_writes: " << stats.node_writes
              << std::endl;
    std::cout << "[MIXED_WORKLOAD] edge_writes: " << stats.edge_writes
              << std::endl;
    std::cout << "[MIXED_WORKLOAD] query_ops: " << stats.query_ops
              << std::endl;
    std::cout << "[MIXED_WORKLOAD] query_checksum: " << stats.checksum
              << std::endl;
  }

  int RunPreprocessed() {
    const prechunk::LoaderOptions options = BuildPreprocessedLoaderOptions();
    std::cout << "=== test_graphdb_finbench preprocessed ===" << std::endl;
    std::cout << "preprocessed_root: " << options.root << std::endl;
    std::cout << "db_path: " << FLAGS_finbench_db_path << std::endl;
    std::cout << "loader_threads: " << options.threads << std::endl;
    std::cout << "loader_cpu_base: " << options.loader_cpu_base << std::endl;
    std::cout << "db_cpu_base: " << options.db_cpu_base << std::endl;
    std::cout << "loader_queue_blocks: " << options.queue_blocks << std::endl;
    std::cout << "loader_prefill_blocks: " << options.prefill_blocks
              << std::endl;
    std::cout << "loader_block_records: " << options.block_records
              << std::endl;

    prechunk::SchemaCatalog catalog;
    std::string error;
    if (!catalog.Load(options.root, &error)) {
      std::cerr << error << std::endl;
      return 1;
    }

    PreprocessedIndexStats index_stats;
    const auto index_t1 = std::chrono::steady_clock::now();
    if (!IndexPreprocessedDataset(catalog, options.root, &index_stats)) {
      return 1;
    }
    const auto index_t2 = std::chrono::steady_clock::now();
    std::cout << "[PREPROCESSED_INDEX] time(s): "
              << std::chrono::duration_cast<std::chrono::duration<double>>(
                     index_t2 - index_t1)
                     .count()
              << std::endl;
    std::cout << "[PREPROCESSED_INDEX] prepared_vertices: "
              << next_vertex_id_ << std::endl;
    std::cout << "[PREPROCESSED_INDEX] node_records: "
              << index_stats.node_records << std::endl;
    std::cout << "[PREPROCESSED_INDEX] edge_records: "
              << index_stats.edge_records << std::endl;

    if (!PrepareDbDirectory()) {
      return 1;
    }
    ConfigureLowLevelFlags();
    const uint64_t max_vertex_num = DeriveMaxVertexNum();
    const std::string schema_path = FLAGS_finbench_db_path +
                                    "/finbench_schema.yaml";
    WriteSchemaFile(schema_path, max_vertex_num);
    if (!OpenDb(schema_path)) {
      return 1;
    }
    db_->InitVerticesUpTo(next_vertex_id_);
    if (!cold_blob_writers_.Open(
            ColdBlobPathPrefix(),
            std::max(GetWriteThreadCount(), GetMixedThreadCount()),
            kBlobBufferBytes)) {
      return 1;
    }
    if (!node_cold_blob_writers_.Open(
            NodeColdBlobPathPrefix(),
            std::max(GetWriteThreadCount(), GetMixedThreadCount()),
            kNodeColdBlobBufferBytes)) {
      return 1;
    }
    write_latency_sampler_.Reset(FLAGS_finbench_write_latency_sample_target,
                                 std::max<uint64_t>(
                                     1ULL, index_stats.snapshot_edge_records),
                                 FLAGS_finbench_write_latency_sample_seed,
                                 std::max(GetWriteThreadCount(),
                                          GetMixedThreadCount()));
    node_write_latency_sampler_.Reset(FLAGS_finbench_write_latency_sample_target,
                                      std::max<uint64_t>(
                                          1ULL, index_stats.node_records),
                                      FLAGS_finbench_write_latency_sample_seed ^
                                          0x9e3779b97f4a7c15ULL,
                                      std::max(GetWriteThreadCount(),
                                               GetMixedThreadCount()));

    ImportStats snapshot_nodes;
    ImportStats snapshot_edges;
    ImportStats remaining_nodes;
    ImportStats remaining_edges;
    MixedWorkloadStats mixed_stats;
    PreprocessedReadStats single_node_stats;
    PreprocessedReadStats single_edge_stats;
    PreprocessedQueryStats final_query_stats;
    EngineUpdateRunState node_update_state;
    EngineUpdateRunState edge_update_state;
    bool blobs_closed = false;
    bool blobs_flushed_for_reads = false;
    struct StageWallClock {
      using Clock = std::chrono::steady_clock;
      bool started = false;
      Clock::time_point start{};
      Clock::time_point end{};

      void BeginIfNeeded() {
        if (!started) {
          started = true;
          start = Clock::now();
          end = start;
        }
      }

      void EndNow() {
        if (started) {
          end = Clock::now();
        }
      }

      double Seconds() const {
        if (!started) {
          return 0.0;
        }
        return std::chrono::duration_cast<std::chrono::duration<double>>(end -
                                                                         start)
            .count();
      }
    };
	    StageWallClock active_stage_wall;
	    prechunk::Stage active_stage = prechunk::Stage::kUnknown;
	    bool has_active_stage = false;

    auto flush_blobs_for_reads = [&]() -> bool {
      if (blobs_closed || blobs_flushed_for_reads) {
        return true;
      }
      if (!cold_blob_writers_.Flush() || !node_cold_blob_writers_.Flush()) {
        return false;
      }
      blobs_flushed_for_reads = true;
      return true;
    };

    auto close_blobs = [&]() -> bool {
      if (blobs_closed) {
        return true;
      }
      if (!cold_blob_writers_.Close() || !node_cold_blob_writers_.Close()) {
        return false;
      }
      blobs_closed = true;
      PrintColdBlobStats();
      return true;
    };

	    auto execute_write_stage = [&](ImportStats* total,
	                                   PreparedPreprocessedChunk* chunk,
	                                   bool sample_nodes,
	                                   bool sample_edges) {
	      AccumulateImportStats(
	          total,
	          ExecutePreparedPreprocessedWriteChunk(chunk, sample_nodes, sample_edges));
	    };

	    auto finish_active_stage = [&](std::chrono::steady_clock::time_point now)
	        -> bool {
	      if (!has_active_stage) {
	        return true;
	      }
	      auto stage_seconds = [&]() {
	        return std::chrono::duration_cast<std::chrono::duration<double>>(
	                   now - active_stage_wall.start)
	            .count();
	      };
	      switch (active_stage) {
	        case prechunk::Stage::kSnapshotNodes:
	          snapshot_nodes.wall_sec = stage_seconds();
	          PrintImportStats("SNAPSHOT_NODE_IMPORT", snapshot_nodes);
	          break;
	        case prechunk::Stage::kSnapshotEdges:
	          snapshot_edges.wall_sec = stage_seconds();
	          PrintImportStats("SNAPSHOT_EDGE_IMPORT", snapshot_edges);
	          PrintCombinedImportStats(
	              "SNAPSHOT_IMPORT", snapshot_nodes, snapshot_edges);
	          break;
	        case prechunk::Stage::kRemainingNodes:
	          remaining_nodes.wall_sec = stage_seconds();
	          PrintImportStats("REMAINING_NODE_IMPORT", remaining_nodes);
	          break;
	        case prechunk::Stage::kRemainingEdges: {
	          remaining_edges.wall_sec = stage_seconds();
	          PrintImportStats("REMAINING_EDGE_IMPORT", remaining_edges);
	          ImportStats remaining_total;
	          AccumulateImportStats(&remaining_total, remaining_nodes);
	          AccumulateImportStats(&remaining_total, remaining_edges);
	          PrintImportStats("REMAINING_INCREMENTAL_IMPORT", remaining_total);
	          PrintFullGraphWriteStats(snapshot_nodes,
	                                   snapshot_edges,
	                                   remaining_nodes,
	                                   remaining_edges);
	          node_write_latency_sampler_.Print("NODE_WRITE_LATENCY_SAMPLE");
	          write_latency_sampler_.Print("WRITE_LATENCY_SAMPLE");
	          break;
	        }
	        case prechunk::Stage::kMixedOps:
	          mixed_stats.wall_sec = stage_seconds();
	          PrintMixedWorkloadStats(mixed_stats);
            if (!ForceHotEdgeCsrCompactionIfNeeded()) {
              return false;
            }
	          break;
	        case prechunk::Stage::kSingleNodeRead:
	          single_node_stats.sec = stage_seconds();
	          PrintPreprocessedReadStats("SINGLE_NODE_READ", single_node_stats);
	          break;
	        case prechunk::Stage::kSingleEdgeRead:
	          single_edge_stats.sec = stage_seconds();
	          PrintPreprocessedReadStats("SINGLE_EDGE_READ", single_edge_stats);
	          break;
	        case prechunk::Stage::kFinalQuery:
	          final_query_stats.wall_sec = stage_seconds();
	          PrintPreprocessedQueryStats(final_query_stats);
	          break;
	        case prechunk::Stage::kFinbenchNodeUpdate: {
	          UpdateWorkloadStats node_update_stats =
	              FinalizeEngineUpdateState(&node_update_state);
	          now = std::chrono::steady_clock::now();
	          node_update_stats.sec =
	              std::chrono::duration_cast<std::chrono::duration<double>>(
	                  now - active_stage_wall.start)
	                  .count();
	          PrintUpdateStatsWithPhase("FINBENCH_NODE_UPDATE",
	                                    node_update_stats);
	          break;
	        }
	        case prechunk::Stage::kFinbenchEdgeUpdate: {
	          UpdateWorkloadStats edge_update_stats =
	              FinalizeEngineUpdateState(&edge_update_state);
	          now = std::chrono::steady_clock::now();
	          edge_update_stats.sec =
	              std::chrono::duration_cast<std::chrono::duration<double>>(
	                  now - active_stage_wall.start)
	                  .count();
	          PrintUpdateStatsWithPhase("FINBENCH_EDGE_UPDATE",
	                                    edge_update_stats);
	          break;
	        }
	        default:
	          break;
	      }
	      return true;
	    };

	    auto enter_stage = [&](prechunk::Stage stage) -> bool {
	      const auto now = std::chrono::steady_clock::now();
	      if (!has_active_stage) {
	        has_active_stage = true;
	        active_stage = stage;
	        active_stage_wall.started = true;
	        active_stage_wall.start = now;
	        active_stage_wall.end = now;
	        return true;
	      }
	      if (stage == active_stage) {
	        return true;
	      }
	      if (!finish_active_stage(now)) {
	        return false;
	      }
	      active_stage = stage;
	      active_stage_wall.started = true;
	      active_stage_wall.start = std::chrono::steady_clock::now();
	      active_stage_wall.end = active_stage_wall.start;
	      return true;
	    };

    prechunk::LoaderStats loader_stats;
    const bool ok = prechunk::RunTransformedPreprocessedLoader<
        PreparedPreprocessedChunk>(
        options,
        [&](prechunk::Chunk* chunk,
            std::pmr::memory_resource* mr,
            PreparedPreprocessedChunk* prepared,
            std::string* error) -> bool {
          return BuildPreparedPreprocessedChunk(
              catalog, chunk, mr, prepared, error);
	        },
	        [&](PreparedPreprocessedChunk* chunk) -> bool {
	          if (chunk->stage == prechunk::Stage::kSingleEdgeRead &&
	              FLAGS_finbench_skip_single_edge_read) {
	            return true;
	          }
	          if (!enter_stage(chunk->stage)) {
	            return false;
	          }
	          switch (chunk->stage) {
	            case prechunk::Stage::kSnapshotNodes:
	              execute_write_stage(&snapshot_nodes, chunk, true, false);
	              break;
	            case prechunk::Stage::kSnapshotEdges:
	              execute_write_stage(&snapshot_edges, chunk, false, true);
	              break;
	            case prechunk::Stage::kRemainingNodes:
	              execute_write_stage(&remaining_nodes, chunk, false, false);
	              break;
	            case prechunk::Stage::kRemainingEdges:
	              execute_write_stage(&remaining_edges, chunk, false, false);
	              break;
	            case prechunk::Stage::kMixedOps: {
	              AccumulateMixedStats(
	                  &mixed_stats,
	                  ExecutePreparedPreprocessedMixedChunk(chunk));
	              break;
	            }
            case prechunk::Stage::kSingleNodeRead:
              if (!flush_blobs_for_reads()) {
                return false;
              }
              single_node_stats.Add(
                  RunPreparedSingleNodeReadChunk(
                      chunk->single_node_read_fields));
              break;
            case prechunk::Stage::kSingleEdgeRead:
              if (!flush_blobs_for_reads()) {
                return false;
              }
              single_edge_stats.Add(
                  RunPreparedSingleEdgeReadChunk(
                      chunk->single_edge_read_fields));
              break;
            case prechunk::Stage::kFinalQuery:
              final_query_stats.Add(
                  RunPreparedPreprocessedQueryTasks(chunk->query_storage));
              break;
            case prechunk::Stage::kFinbenchNodeUpdate:
              RunPreparedPreprocessedUpdateChunk(*chunk,
                                                 &node_update_state,
                                                 "FINBENCH_NODE_UPDATE");
              break;
            case prechunk::Stage::kFinbenchEdgeUpdate:
              RunPreparedPreprocessedUpdateChunk(*chunk,
                                                 &edge_update_state,
                                                 "FINBENCH_EDGE_UPDATE");
              break;
            default:
              break;
          }
          return true;
	        },
	        &loader_stats);
	    if (ok && has_active_stage) {
	      if (!finish_active_stage(std::chrono::steady_clock::now())) {
	        return 1;
	      }
	      has_active_stage = false;
	    }
	    prechunk::PrintLoaderStats(loader_stats);
	    if (!ok) {
	      return 1;
	    }
	    if (!close_blobs()) {
	      return 1;
	    }
	    std::cout << "test_graphdb_finbench preprocessed path passed"
	              << std::endl;
    return 0;
  }

  bool ValidateDatasetLayout() const {
    const std::array<std::string, 3> required_dirs = {
        GetSnapshotDir(),
        GetIncrementalDir(),
        GetParamsDir(),
    };
    for (const auto& path : required_dirs) {
      if (!FileExists(path)) {
        std::cerr << "missing dataset path: " << path << std::endl;
        return false;
      }
    }
    return true;
  }

  bool PrepareDbDirectory() const {
    std::error_code ec;
    if (FLAGS_finbench_reset_db) {
      std::filesystem::remove_all(FLAGS_finbench_db_path, ec);
      if (ec) {
        std::cerr << "remove_all failed for path=" << FLAGS_finbench_db_path
                  << ", ec=" << ec.message() << std::endl;
        return false;
      }
    }
    std::filesystem::create_directories(FLAGS_finbench_db_path, ec);
    if (ec) {
      std::cerr << "create_directories failed for path=" << FLAGS_finbench_db_path
                << ", ec=" << ec.message() << std::endl;
      return false;
    }
    return true;
  }

  void ConfigureLowLevelFlags() const {
    FLAGS_support_mulversion = true;
    FLAGS_LOAD_OLD_DATA = false;
    FLAGS_OPEN_SSTDATA_CACHE = true;
    FLAGS_thread_num = FLAGS_finbench_system_threads;
    FLAGS_memtable_num = FLAGS_finbench_memtable_num;
    // Keep the retired LSMStore MemProperty path off. The similarly named
    // OpenDb configures the engine-owned PropertyUpdateManager explicitly.
    FLAGS_enable_memproperty = false;
    FLAGS_memproperty_num = 0;
    FLAGS_memtable_size = FLAGS_finbench_memtable_size;
    FLAGS_max_property_length = FLAGS_finbench_property_length;
    FLAGS_max_subcompactions = FLAGS_finbench_max_subcompactions;
    FLAGS_db_path = FLAGS_finbench_db_path;
  }

  uint32_t NodeMemtableSize() const {
    return FLAGS_finbench_node_memtable_size == 0
               ? FLAGS_finbench_memtable_size
               : FLAGS_finbench_node_memtable_size;
  }

  std::vector<uint32_t> EdgeMemtableSizes() const {
    return ParseMemtableSizeCsv(FLAGS_finbench_edge_memtable_sizes,
                                2,
                                FLAGS_finbench_memtable_size,
                                "finbench_edge_memtable_sizes");
  }

  void PrintMemtableSizes(const char* label,
                          const std::vector<uint32_t>& sizes) const {
    std::cout << label << ": [";
    for (size_t i = 0; i < sizes.size(); ++i) {
      if (i > 0) {
        std::cout << ",";
      }
      std::cout << sizes[i];
    }
    std::cout << "]" << std::endl;
  }

  uint64_t DeriveMaxVertexNum() const {
    if (FLAGS_finbench_max_vertex_num > 0) {
      return FLAGS_finbench_max_vertex_num;
    }
    return std::max<uint64_t>(next_vertex_id_ + 1024ULL, 4096ULL);
  }

  std::string ColdBlobPathPrefix() const {
    return FLAGS_finbench_db_path + "/finbench_cold_property";
  }

  std::string NodeColdBlobPathPrefix() const {
    return FLAGS_finbench_db_path + "/finbench_node_cold_property";
  }

  void WriteSchemaFile(const std::string& schema_path,
                       uint64_t max_vertex_num) const {
    std::ofstream out(schema_path);
    assert(out.is_open());

    std::unordered_set<std::string> seen;
    std::vector<std::string> property_defs;
    property_defs.reserve(AllNodeProperties().size() + NodeDbProperties().size() +
                          AllEdgeProperties().size());
    for (const auto& name : AllNodeProperties()) {
      if (seen.insert(name).second) {
        property_defs.push_back(name);
      }
    }
    for (const auto& name : NodeDbProperties()) {
      if (seen.insert(name).second) {
        property_defs.push_back(name);
      }
    }
    for (const auto& name : AllEdgeProperties()) {
      if (seen.insert(name).second) {
        property_defs.push_back(name);
      }
    }

    out << "max_vertex_num: " << max_vertex_num << "\n";
    out << "use_csr_disk: " << (FLAGS_finbench_use_csr_disk ? "true" : "false") << "\n";
    out << "max_property_length: " << FLAGS_finbench_property_length << "\n";
    out << "system_threads: " << FLAGS_finbench_system_threads << "\n";
    out << "property_defs:\n";
    for (const auto& name : property_defs) {
      out << "  - name: " << name << "\n";
      out << "    length: " << GetEdgePropertyLength(name) << "\n";
    }
    out << "edge_shards:\n";
    const auto edge_memtable_sizes = EdgeMemtableSizes();
    // Shard 0: query-hot fixed properties.
    out << "  - name: edge_Db0\n";
    out << "    memtable_size: " << edge_memtable_sizes[0] << "\n";
    out << "    is_csr: "
        << (FLAGS_finbench_enable_hot_edge_csr ? "true" : "false") << "\n";
    out << "    csr_l0_max_sst_num: "
        << FLAGS_finbench_hot_edge_csr_l0_max_sst_num << "\n";
    out << "    csr_l1_max_sst_num: "
        << FLAGS_finbench_hot_edge_csr_l1_max_sst_num << "\n";
    out << "    properties: [";
    for (size_t i = 0; i < EdgeShard0Properties().size(); ++i) {
      if (i > 0) {
        out << ", ";
      }
      out << EdgeShard0Properties()[i];
    }
    out << "]\n";
    // Shard 1: fixed-size ref to the external cold-property blob.
    out << "  - name: edge_Db1\n";
    out << "    memtable_size: " << edge_memtable_sizes[1] << "\n";
    out << "    is_csr: false\n";
    out << "    csr_l0_max_sst_num: "
        << FLAGS_finbench_hot_edge_csr_l0_max_sst_num << "\n";
    out << "    csr_l1_max_sst_num: "
        << FLAGS_finbench_hot_edge_csr_l1_max_sst_num << "\n";
    out << "    properties: [";
    for (size_t i = 0; i < EdgeShard1Properties().size(); ++i) {
      if (i > 0) {
        out << ", ";
      }
      out << EdgeShard1Properties()[i];
    }
    out << "]\n";
    out << "node_db:\n";
    out << "  name: node_Db\n";
    out << "  memtable_size: " << NodeMemtableSize() << "\n";
    out << "  is_csr: false\n";
    out << "  csr_l0_max_sst_num: "
        << FLAGS_finbench_hot_edge_csr_l0_max_sst_num << "\n";
    out << "  csr_l1_max_sst_num: "
        << FLAGS_finbench_hot_edge_csr_l1_max_sst_num << "\n";
    out << "  properties: [";
    for (size_t i = 0; i < NodeDbProperties().size(); ++i) {
      if (i > 0) {
        out << ", ";
      }
      out << NodeDbProperties()[i];
    }
    out << "]\n";
  }

  bool OpenDb(const std::string& schema_path) {
    lsmgraph::GraphDbOptions options =
        lsmgraph::GraphDb::LegacyOptionsFromFlags();
    options.property_updates.enabled = true;
    options.property_updates.buffer_count =
        std::max<uint32_t>(2U, FLAGS_finbench_memproperty_num);
    options.property_updates.buffer_capacity_records =
        static_cast<std::size_t>(std::max(
            FLAGS_finbench_update_node_memproperty_cap,
            FLAGS_finbench_update_edge_memproperty_cap));
    options.property_updates.buffer_capacity_bytes =
        static_cast<std::size_t>(FLAGS_finbench_property_buffer_bytes);
    options.property_updates.delta_chain_merge_threshold =
        FLAGS_finbench_delta_merge_threshold;
    options.property_updates.durability = FLAGS_finbench_delta_crash_safe
        ? lsmgraph::DeltaDurability::kProcessCrashSafe
        : lsmgraph::DeltaDurability::kNone;
    const auto rs = lsmgraph::GraphDb::OpenFromYaml(
        FLAGS_finbench_db_path, schema_path, options, &db_);
    if (rs != lsmgraph::Status::kOk || db_ == nullptr) {
      std::cerr << "GraphDb::OpenFromYaml failed" << std::endl;
      return false;
    }
    return true;
  }

  static double ImportStatsElapsedSec(const ImportStats& stats) {
    if (stats.wall_sec > 0.0) {
      return stats.wall_sec;
    }
    return stats.sec + stats.background_wait_sec;
  }

  static double MixedTotalElapsedSec(const MixedWorkloadStats& stats) {
    if (stats.wall_sec > 0.0) {
      return stats.wall_sec;
    }
    return stats.total_sec + stats.background_wait_sec;
  }

	  static void PrintImportStats(const char* phase, const ImportStats& stats) {
    const uint64_t logical_writes = stats.node_writes + stats.edge_writes;
    const double total_sec = ImportStatsElapsedSec(stats);
    const double write_qps = (total_sec <= 0.0 || logical_writes == 0)
                                 ? 0.0
                                 : static_cast<double>(logical_writes) / total_sec;
    const double node_qps = (total_sec <= 0.0 || stats.node_writes == 0)
                                ? 0.0
                                : static_cast<double>(stats.node_writes) / total_sec;
    const double edge_qps = (total_sec <= 0.0 || stats.edge_writes == 0)
                                ? 0.0
                                : static_cast<double>(stats.edge_writes) / total_sec;
    std::cout << "[" << phase << "] time(s): " << total_sec << std::endl;
    std::cout << "[" << phase << "] foreground_write_time(s): " << stats.sec
              << std::endl;
    std::cout << "[" << phase << "] logical_rows: " << stats.logical_rows << std::endl;
    std::cout << "[" << phase << "] node_writes: " << stats.node_writes << std::endl;
    std::cout << "[" << phase << "] edge_writes: " << stats.edge_writes << std::endl;
    std::cout << "[" << phase << "] logical_writes: " << logical_writes << std::endl;
    std::cout << "[" << phase << "] qps(logical_write/s): " << write_qps
              << std::endl;
    std::cout << "[" << phase << "] node_qps(node/s): " << node_qps << std::endl;
    std::cout << "[" << phase << "] edge_qps(edge/s): " << edge_qps << std::endl;
    std::cout << "[" << phase << "] entity_nodes: " << stats.entity_nodes
              << std::endl;
  }

  static void PrintCombinedImportStats(const char* phase,
                                       const ImportStats& lhs,
                                       const ImportStats& rhs) {
    ImportStats total;
    total.logical_rows = lhs.logical_rows + rhs.logical_rows;
    total.node_writes = lhs.node_writes + rhs.node_writes;
	    total.edge_writes = lhs.edge_writes + rhs.edge_writes;
	    total.entity_nodes = lhs.entity_nodes + rhs.entity_nodes;
	    total.sec = lhs.sec + rhs.sec;
	    total.background_wait_sec =
	        lhs.background_wait_sec + rhs.background_wait_sec;
      total.wall_sec = ImportStatsElapsedSec(lhs) + ImportStatsElapsedSec(rhs);
	    PrintImportStats(phase, total);
	  }

  void PrintBlobStats(const char* label, const ColdBlobWriterSet& writers) const {
    std::cout << "[" << label << "] prefix: " << writers.prefix()
              << std::endl;
    std::cout << "[" << label << "] files: " << writers.file_count()
              << std::endl;
    std::cout << "[" << label << "] records: " << writers.record_count()
              << std::endl;
    std::cout << "[" << label << "] payload_bytes: "
              << writers.payload_bytes() << std::endl;
    std::cout << "[" << label << "] file_bytes: " << writers.file_bytes()
              << std::endl;
  }

  void PrintColdBlobStats() const {
    PrintBlobStats("COLD_BLOB", cold_blob_writers_);
    PrintBlobStats("NODE_COLD_BLOB", node_cold_blob_writers_);
  }

  static void PrintUpdateWorkloadStats(const UpdateWorkloadStats& stats) {
    const uint64_t updates = stats.node_updates + stats.edge_updates;
    const double qps = (stats.sec <= 0.0 || updates == 0)
                           ? 0.0
                           : static_cast<double>(updates) / stats.sec;
    const double node_qps = (stats.sec <= 0.0 || stats.node_updates == 0)
                                ? 0.0
                                : static_cast<double>(stats.node_updates) /
                                      stats.sec;
    const double edge_qps = (stats.sec <= 0.0 || stats.edge_updates == 0)
                                ? 0.0
                                : static_cast<double>(stats.edge_updates) /
                                      stats.sec;
    std::cout << "[UPDATE_WORKLOAD] time(s): " << stats.sec << std::endl;
    std::cout << "[UPDATE_WORKLOAD] locate_time(s): " << stats.locate_sec
              << std::endl;
    std::cout << "[UPDATE_WORKLOAD] cold_rewrite_time(s): "
              << stats.cold_rewrite_sec << std::endl;
    std::cout << "[UPDATE_WORKLOAD] engine_update_time(s): " << stats.write_sec
              << std::endl;
    std::cout << "[UPDATE_WORKLOAD] logical_rows: " << stats.logical_rows
              << std::endl;
    std::cout << "[UPDATE_WORKLOAD] node_updates: " << stats.node_updates
              << std::endl;
    std::cout << "[UPDATE_WORKLOAD] edge_updates: " << stats.edge_updates
              << std::endl;
    std::cout << "[UPDATE_WORKLOAD] missing_targets: " << stats.missing_targets
              << std::endl;
    std::cout << "[UPDATE_WORKLOAD] cold_blob_rewrites: "
              << stats.cold_blob_rewrites << std::endl;
    std::cout << "[UPDATE_WORKLOAD] property_buffer_flushes: "
              << stats.flushes << std::endl;
    std::cout << "[UPDATE_WORKLOAD] delta_batches: " << stats.delta_files
              << std::endl;
    std::cout << "[UPDATE_WORKLOAD] value_bytes: " << stats.value_bytes
              << std::endl;
    std::cout << "[UPDATE_WORKLOAD] blob_payload_bytes: "
              << stats.blob_payload_bytes << std::endl;
    std::cout << "[UPDATE_WORKLOAD] engine_submitted_bytes: "
              << stats.engine_submitted_bytes
              << std::endl;
    std::cout << "[UPDATE_WORKLOAD] qps(update/s): " << qps << std::endl;
    std::cout << "[UPDATE_WORKLOAD] node_qps(update/s): " << node_qps
              << std::endl;
    std::cout << "[UPDATE_WORKLOAD] edge_qps(update/s): " << edge_qps
              << std::endl;
  }

  static std::string JoinTail(const std::vector<std::string>& fields,
                              size_t begin,
                              char delimiter) {
    if (begin >= fields.size()) {
      return "";
    }
    std::string out = fields[begin];
    for (size_t i = begin + 1; i < fields.size(); ++i) {
      out.push_back(delimiter);
      out += fields[i];
    }
    return out;
  }

  static bool ParseBoolToken(const std::string& value) {
    const std::string v = Trim(value);
    return v == "1" || v == "true" || v == "TRUE" || v == "True" ||
           v == "out" || v == "OUT";
  }

  std::string GetEdgeDbProp(vertex_t src,
                            vertex_t dst,
                            const std::string& name,
                            bool is_out,
                            uint8_t edge_type) const {
    std::unordered_map<std::string, std::string> props;
    const auto rs = db_->GetEdge(src, dst, {name}, &props, is_out, edge_type);
    if (rs != lsmgraph::Status::kOk) {
      return {};
    }
    const auto it = props.find(name);
    return it == props.end() ? std::string() : it->second;
  }

  bool RewriteNodeColdUpdateValue(LightNodeUpdate* update,
                                  UpdateWorkloadStats* stats) {
    if (update == nullptr || stats == nullptr || !update->cold) {
      return true;
    }
    const std::string old_ref =
        GetNodeDbProp(update->vid, "node_cold_property");
    const std::string old_payload =
        old_ref.empty() ? std::string()
                        : ReadNodeColdBlobPayloadNoCache(old_ref);
    const size_t min_slots = std::max<size_t>(
        ColdNodeProperties().size(), static_cast<size_t>(update->cold_slot) + 1U);
    std::vector<std::string> slots = DecodePayloadSlots(old_payload, min_slots);
    if (update->cold_slot >= slots.size()) {
      return false;
    }
    slots[update->cold_slot] = update->value;
    const std::string new_payload = EncodePayloadSlots(slots);
    const std::string new_ref = node_cold_blob_writers_.Append(new_payload);
    if (new_ref.empty()) {
      return false;
    }
    update->value = new_ref;
    update->storage_property = "node_cold_property";
    update->storage_property_id = NodeColdRefSlot();
    stats->blob_payload_bytes += new_payload.size();
    ++stats->cold_blob_rewrites;
    return true;
  }

  bool RewriteEdgeColdUpdateValue(LightEdgeUpdate* update,
                                  UpdateWorkloadStats* stats) {
    if (update == nullptr || stats == nullptr || !update->cold) {
      return true;
    }
    const std::string old_ref = GetEdgeDbProp(update->src,
                                              update->dst,
                                              "cold_property",
                                              update->is_out,
                                              update->edge_type);
    const std::string old_payload =
        old_ref.empty() ? std::string()
                        : ReadColdBlobPayloadNoCache(old_ref);
    const size_t min_slots = std::max<size_t>(
        kColdEdgePropertyCount, static_cast<size_t>(update->cold_slot) + 1U);
    std::vector<std::string> slots = DecodePayloadSlots(old_payload, min_slots);
    if (update->cold_slot >= slots.size()) {
      return false;
    }
    slots[update->cold_slot] = update->value;
    const std::string new_payload = EncodePayloadSlots(slots);
    const std::string new_ref = cold_blob_writers_.Append(new_payload);
    if (new_ref.empty()) {
      return false;
    }
    update->value = new_ref;
    update->storage_property = "cold_property";
    const auto cold_ref_it = EdgeShard1PropertyIndex().find("cold_property");
    if (cold_ref_it == EdgeShard1PropertyIndex().end()) {
      return false;
    }
    update->storage_property_id = static_cast<uint32_t>(cold_ref_it->second);
    stats->blob_payload_bytes += new_payload.size();
    ++stats->cold_blob_rewrites;
    return true;
  }

  UpdateWorkloadStats RunLightUpdateWorkload() {
    EngineUpdateRunState state;
    InitEngineUpdateState(&state, "FINBENCH_UPDATE");

    std::ifstream in;
    std::vector<char> buffer;
    if (!OpenCsvInput(FLAGS_finbench_update_workload_path, &in, &buffer)) {
      std::cerr << "failed to open update workload: "
                << FLAGS_finbench_update_workload_path << std::endl;
      std::exit(1);
    }

    std::string line;
    while (std::getline(in, line)) {
      if (line.empty()) {
        continue;
      }
      const char delimiter = line.find('|') == std::string::npos ? ',' : '|';
      std::vector<std::string> fields = SplitByDelimiter(line, delimiter);
      if (fields.empty()) {
        continue;
      }
      fields[0] = Trim(fields[0]);
      if (fields[0].empty() || fields[0][0] == '#') {
        continue;
      }
      if (fields[0] == "kind" || fields[0] == "KIND") {
        continue;
      }

      if ((fields[0] == "N" || fields[0] == "n") && fields.size() >= 4) {
        std::string logical_name;
        LightNodeUpdate update;
        update.vid = ToUint64(Trim(fields[1]));
        const std::string prop_token = Trim(fields[2]);
        if (!ResolveNodeUpdateProperty(prop_token,
                                       &update.logical_property_id,
                                       &update.storage_property_id,
                                       &update.cold_slot,
                                       &update.storage_property,
                                       &logical_name,
                                       &update.cold)) {
          std::cerr << "unknown node update property: " << prop_token
                    << std::endl;
          std::exit(1);
        }
        update.value =
            NormalizePropertyValue(logical_name, JoinTail(fields, 3, delimiter));
        ++state.stats.logical_rows;
        ApplyNodeUpdate(&state, std::move(update));
        continue;
      }

      if ((fields[0] == "E" || fields[0] == "e") && fields.size() >= 7) {
        std::string logical_name;
        LightEdgeUpdate update;
        update.src = ToUint64(Trim(fields[1]));
        update.dst = ToUint64(Trim(fields[2]));
        update.edge_type = static_cast<uint8_t>(ToUint64(Trim(fields[3])));
        update.is_out = ParseBoolToken(fields[4]);
        const std::string prop_token = Trim(fields[5]);
        if (!ResolveEdgeUpdateProperty(prop_token,
                                       &update.logical_property_id,
                                       &update.storage_property_id,
                                       &update.cold_slot,
                                       &update.storage_property,
                                       &logical_name,
                                       &update.cold)) {
          std::cerr << "unknown edge update property: " << prop_token
                    << std::endl;
          std::exit(1);
        }
        update.value =
            NormalizePropertyValue(logical_name, JoinTail(fields, 6, delimiter));
        ++state.stats.logical_rows;
        ApplyEdgeUpdate(&state, std::move(update));
        continue;
      }

      std::cerr << "bad update workload row: " << line << std::endl;
      std::exit(1);
    }

    return FinalizeEngineUpdateState(&state);
  }

  uint32_t GetWriteThreadCount() const {
    return std::max<uint32_t>(1U, FLAGS_finbench_write_threads);
  }

  uint32_t GetReadThreadCount() const {
    return std::max<uint32_t>(1U, FLAGS_finbench_read_threads);
  }

  uint32_t GetMixedThreadCount() const {
    return std::max<uint32_t>(1U, FLAGS_finbench_mixed_threads);
  }

  bool SingleEdgeReadEnabled() const {
    return FLAGS_finbench_single_edge_read_ops > 0 &&
           FLAGS_finbench_single_edge_candidate_cap > 0;
  }

  size_t ImportBatchRows() const {
    return std::max<uint32_t>(1U, FLAGS_finbench_batch_size);
  }

  std::pmr::memory_resource* LoadArenaResource() {
    if (tls_load_resource_override_ != nullptr) {
      return tls_load_resource_override_;
    }
    return load_arena_.resource();
  }

  bool LoadArenaNearFull() const {
    const uint64_t size = load_arena_.size_bytes();
    return size > 0 && load_arena_.used_bytes() >= (size / 20ULL) * 19ULL;
  }

  bool MarkMaterializedVertex(vertex_t id) {
    std::lock_guard<std::mutex> lock(materialized_vertices_mu_);
    return materialized_vertices_.insert(id).second;
  }

  std::unique_ptr<PreparedImportBatch> NewPreparedBatch(
      std::string_view name,
      size_t node_reserve,
      size_t relation_reserve) {
    auto batch = std::make_unique<PreparedImportBatch>(LoadArenaResource());
    batch->name.assign(name.begin(), name.end());
    if (node_reserve > 0) {
      batch->node_writes.reserve(node_reserve);
    }
    if (relation_reserve > 0) {
      batch->relation_writes.reserve(relation_reserve);
    }
    return batch;
  }

  void ReleasePreparedBatch(std::unique_ptr<PreparedImportBatch>* batch,
                            const char* phase) {
    if (batch != nullptr) {
      batch->reset();
    }
    load_arena_.Release(phase);
  }

  uint64_t CountNonEmptyCsvRows(const std::string& path, bool has_header) const {
    std::ifstream in;
    std::vector<char> buffer;
    if (!OpenCsvInput(path, &in, &buffer)) {
      return EstimateCsvRowsBySize(path, 1024);
    }
    std::string line;
    if (has_header) {
      std::getline(in, line);
    }
    uint64_t rows = 0;
    while (std::getline(in, line)) {
      if (!line.empty()) {
        ++rows;
      }
    }
    return rows;
  }

  uint64_t EstimateSnapshotRelationWrites() const {
    uint64_t total = 0;
    for (const auto& rel : AllRelations()) {
      total += CountNonEmptyCsvRows(GetSnapshotDir() + "/" + rel.name + ".csv",
                                    true);
    }
    return total;
  }

  uint64_t EstimateSnapshotNodeWrites() const {
    uint64_t total = 0;
    for (const auto& spec : SnapshotNodeTables()) {
      total += CountNonEmptyCsvRows(GetSnapshotDir() + "/" + spec.file_name,
                                    true);
    }
    return total;
  }

  void ConfigureWriteLatencySampling() {
    const uint64_t node_total = EstimateSnapshotNodeWrites();
    const uint64_t total = EstimateSnapshotRelationWrites();
    node_write_latency_sampler_.Reset(
        FLAGS_finbench_write_latency_sample_target,
        node_total,
        FLAGS_finbench_write_latency_sample_seed ^ 0x9e3779b97f4a7c15ULL,
        std::max(GetWriteThreadCount(), GetMixedThreadCount()));
    write_latency_sampler_.Reset(FLAGS_finbench_write_latency_sample_target,
                                 total,
                                 FLAGS_finbench_write_latency_sample_seed,
                                 std::max(GetWriteThreadCount(),
                                          GetMixedThreadCount()));
    std::cout << "node_write_latency_estimated_total_writes: " << node_total
              << std::endl;
    std::cout << "write_latency_estimated_total_writes: " << total
              << std::endl;
  }

  template <typename Fn>
  void ParallelForWriteIndex(size_t count, Fn&& fn) const {
    if (FLAGS_finbench_write_dynamic) {
      ParallelForIndexDynamicChunk(count,
                                   GetWriteThreadCount(),
                                   FLAGS_finbench_write_dynamic_chunk,
                                   fn);
      return;
    }
    ParallelForIndex(count, GetWriteThreadCount(), fn);
  }

  std::vector<std::filesystem::path> SortedIncrementalFiles() const {
    std::vector<std::filesystem::path> files;
    std::error_code ec;
    for (const auto& entry :
         std::filesystem::directory_iterator(GetIncrementalDir(), ec)) {
      if (!ec && entry.is_regular_file()) {
        files.push_back(entry.path());
      }
    }
    std::sort(files.begin(), files.end(),
              [](const std::filesystem::path& a, const std::filesystem::path& b) {
                const uint64_t an = ExtractTrailingNumber(a.filename().string());
                const uint64_t bn = ExtractTrailingNumber(b.filename().string());
                if (an != bn) {
                  return an < bn;
                }
                return a.filename().string() < b.filename().string();
              });
    return files;
  }

  uint64_t CountMixedUpdateRows() const {
    return CountSelectedUpdateRows(true);
  }

  uint32_t WorkloadSampleMod() const {
    return std::max<uint32_t>(1, FLAGS_finbench_workload_sample_mod);
  }

  uint32_t WorkloadSampleRemainder() const {
    return FLAGS_finbench_workload_sample_remainder % WorkloadSampleMod();
  }

  bool IsSampledWorkloadRow(uint64_t row_index) const {
    const uint32_t mod = WorkloadSampleMod();
    return mod <= 1 || row_index % mod == WorkloadSampleRemainder();
  }

  bool IsRemainingWorkloadRow(uint64_t row_index) const {
    const uint32_t mod = WorkloadSampleMod();
    return mod > 1 && row_index % mod != WorkloadSampleRemainder();
  }

  bool SelectWorkloadRow(uint64_t row_index, bool sampled) const {
    return sampled ? IsSampledWorkloadRow(row_index)
                   : IsRemainingWorkloadRow(row_index);
  }

  std::vector<size_t> SelectedParamRowIndices(size_t row_count) const {
    std::vector<size_t> indices;
    const uint32_t mod = WorkloadSampleMod();
    indices.reserve(mod <= 1 ? row_count : row_count / mod + 1);
    for (size_t i = 0; i < row_count; ++i) {
      if (!IsSampledWorkloadRow(i)) {
        continue;
      }
      indices.push_back(i);
      if (FLAGS_finbench_param_limit_per_query > 0 &&
          indices.size() >= FLAGS_finbench_param_limit_per_query) {
        break;
      }
    }
    return indices;
  }

  uint64_t CountSelectedCsvRows(const std::string& path,
                                bool has_header,
                                bool sampled) const {
    std::ifstream in(path);
    if (!in.is_open()) {
      return 0;
    }
    std::string line;
    if (has_header) {
      std::getline(in, line);
    }
    uint64_t row_index = 0;
    uint64_t selected = 0;
    while (std::getline(in, line)) {
      if (line.empty()) {
        continue;
      }
      if (SelectWorkloadRow(row_index, sampled)) {
        ++selected;
      }
      ++row_index;
    }
    return selected;
  }

  uint64_t CountSelectedUpdateRows(bool sampled) const {
    uint64_t total = 0;
    for (const auto& path : SortedIncrementalFiles()) {
      const std::string file_name = path.filename().string();
      const uint64_t op_num = ExtractTrailingNumber(file_name);
      if (op_num == 17 || op_num == 18 || op_num == 19) {
        continue;
      }
      total += CountSelectedCsvRows(path.string(), true, sampled);
    }
    return total;
  }

  bool LoadAllDataset() {
    const auto t1 = std::chrono::steady_clock::now();
    prepared_snapshot_node_batches_.clear();
    prepared_snapshot_relation_batches_.clear();
    prepared_incremental_batches_.clear();
    loaded_param_files_.clear();
    entity_to_vid_.clear();
    materialized_vertices_.clear();
    next_vertex_id_ = 0;
    snapshot_entity_total_ = 0;
    incremental_entity_total_ = 0;
    single_edge_sampler_.Reset(FLAGS_finbench_single_edge_candidate_cap,
                               FLAGS_finbench_single_edge_hot_weight,
                               FLAGS_finbench_single_edge_cold_weight,
                               FLAGS_finbench_single_edge_seed);

    for (const auto& spec : SnapshotNodeTables()) {
      const std::string path = GetSnapshotDir() + "/" + spec.file_name;
      if (!IndexSnapshotNodeIdsFromFile(spec, path)) {
        std::cerr << "failed to index snapshot csv: " << path << std::endl;
        return false;
      }
    }
    snapshot_entity_total_ = next_vertex_id_;

    const std::vector<std::filesystem::path> files = SortedIncrementalFiles();
    for (const auto& path : files) {
      const std::string file_name = path.filename().string();
      const uint64_t op_num = ExtractTrailingNumber(file_name);
      if (!IndexIncrementalBatch(file_name, op_num, path.string())) {
        std::cerr << "failed to index incremental csv: " << path.string()
                  << std::endl;
        return false;
      }
    }
    incremental_entity_total_ = next_vertex_id_;

    if (!FLAGS_finbench_skip_queries) {
      for (int qid = 1; qid <= 12; ++qid) {
        LoadedParamFile file;
        file.query_id = qid;
        const std::string path =
            GetParamsDir() + "/complex_" + std::to_string(qid) + "_param.csv";
        if (!LoadCsvFile(path, &file.data)) {
          std::cerr << "failed to load params csv: " << path << std::endl;
          return false;
        }
        loaded_param_files_.push_back(std::move(file));
      }
    }
    const auto t2 = std::chrono::steady_clock::now();
    const double sec =
        std::chrono::duration_cast<std::chrono::duration<double>>(t2 - t1).count();
    std::cout << "[LOAD_PREPARE] time(s): " << sec << std::endl;
    std::cout << "[LOAD_PREPARE] prepared_vertices: " << next_vertex_id_
              << std::endl;
    return true;
  }

  bool IndexSnapshotNodeIdsFromFile(const NodeTableSpec& spec,
                                    const std::string& path) {
    return ForEachCsvRow(path, [&](const CsvRow& row) {
      bool created = false;
      const vertex_t id =
          ResolveOrCreateEntityId(spec.kind, row.Get(spec.id_column), &created);
      if (id == lsmgraph::INVALID_VERTEX_ID) {
        std::cerr << "failed to allocate snapshot node id from "
                  << spec.file_name << std::endl;
        std::exit(1);
      }
    });
  }

  bool IndexIncrementalNodeIds(const std::string& path,
                               NodeKind kind,
                               const char* id_column) {
    return ForEachCsvRow(path, [&](const CsvRow& row) {
      bool created = false;
      ResolveOrCreateEntityId(kind, row.Get(id_column), &created);
    });
  }

  bool IndexIncrementalOwnAccountIds(const std::string& path,
                                     NodeKind owner_kind,
                                     const char* owner_id_column) {
    return ForEachCsvRow(path, [&](const CsvRow& row) {
      bool created = false;
      ResolveOrCreateEntityId(owner_kind, row.Get(owner_id_column), &created);
      ResolveOrCreateEntityId(NodeKind::kAccount, row.Get("accountId"), &created);
    });
  }

  bool IndexIncrementalApplyLoanIds(const std::string& path,
                                    NodeKind applicant_kind,
                                    const char* applicant_id_column) {
    return ForEachCsvRow(path, [&](const CsvRow& row) {
      bool created = false;
      ResolveOrCreateEntityId(applicant_kind, row.Get(applicant_id_column), &created);
      ResolveOrCreateEntityId(NodeKind::kLoan, row.Get("loanId"), &created);
    });
  }

  bool IndexSimpleRelationIncrementalIds(const RelationSpec& rel,
                                         const std::string& path,
                                         const char* source_column = nullptr,
                                         const char* dest_column = nullptr) {
    const std::string src_col =
        source_column == nullptr ? rel.source_column : source_column;
    const std::string dst_col =
        dest_column == nullptr ? rel.dest_column : dest_column;
    return ForEachCsvRow(path, [&](const CsvRow& row) {
      bool created = false;
      ResolveOrCreateEntityId(rel.src_kind, row.Get(src_col), &created);
      ResolveOrCreateEntityId(rel.dst_kind, row.Get(dst_col), &created);
    });
  }

  bool IndexIncrementalBatch(const std::string& file_name,
                             uint64_t op_num,
                             const std::string& path) {
    if (op_num == 17 || op_num == 18 || op_num == 19) {
      return true;
    }
    if (file_name == "AddPersonWrite1.csv") {
      return IndexIncrementalNodeIds(path, NodeKind::kPerson, "personId");
    }
    if (file_name == "AddCompanyWrite2.csv") {
      return IndexIncrementalNodeIds(path, NodeKind::kCompany, "companyId");
    }
    if (file_name == "AddMediumWrite3.csv") {
      return IndexIncrementalNodeIds(path, NodeKind::kMedium, "mediumId");
    }
    if (file_name == "AddPersonOwnAccountWrite4.csv") {
      return IndexIncrementalOwnAccountIds(path, NodeKind::kPerson, "personId");
    }
    if (file_name == "AddCompanyOwnAccountWrite5.csv") {
      return IndexIncrementalOwnAccountIds(path, NodeKind::kCompany, "companyId");
    }
    if (file_name == "AddPersonApplyLoanWrite6.csv") {
      return IndexIncrementalApplyLoanIds(path, NodeKind::kPerson, "personId");
    }
    if (file_name == "AddCompanyApplyLoanWrite7.csv") {
      return IndexIncrementalApplyLoanIds(path, NodeKind::kCompany, "companyId");
    }
    if (file_name == "AddPersonInvestCompanyWrite8.csv") {
      return IndexSimpleRelationIncrementalIds(MustGetRelation("PersonInvestCompany"),
                                               path);
    }
    if (file_name == "AddCompanyInvestCompanyWrite9.csv") {
      return IndexSimpleRelationIncrementalIds(MustGetRelation("CompanyInvestCompany"),
                                               path);
    }
    if (file_name == "AddPersonGuaranteePersonWrite10.csv" ||
        file_name == "AddPersonGuaranteePersonReadWrite3.csv") {
      return IndexSimpleRelationIncrementalIds(MustGetRelation("PersonGuaranteePerson"),
                                               path);
    }
    if (file_name == "AddCompanyGuaranteeCompanyWrite11.csv") {
      return IndexSimpleRelationIncrementalIds(MustGetRelation("CompanyGuaranteeCompany"),
                                               path);
    }
    if (file_name == "AddAccountTransferAccountWrite12.csv" ||
        file_name == "AddAccountTransferAccountReadWrite1.csv" ||
        file_name == "AddAccountTransferAccountReadWrite2.csv") {
      return IndexSimpleRelationIncrementalIds(MustGetRelation("AccountTransferAccount"),
                                               path);
    }
    if (file_name == "AddAccountWithdrawAccountWrite13.csv") {
      return IndexSimpleRelationIncrementalIds(MustGetRelation("AccountWithdrawAccount"),
                                               path);
    }
    if (file_name == "AddAccountRepayLoanWrite14.csv") {
      return IndexSimpleRelationIncrementalIds(MustGetRelation("AccountRepayLoan"),
                                               path,
                                               "account",
                                               nullptr);
    }
    if (file_name == "AddLoanDepositAccountWrite15.csv") {
      return IndexSimpleRelationIncrementalIds(MustGetRelation("LoanDepositAccount"),
                                               path,
                                               "loanId",
                                               "accountId");
    }
    if (file_name == "AddMediumSigninAccountWrite16.csv") {
      return IndexSimpleRelationIncrementalIds(MustGetRelation("MediumSignInAccount"),
                                               path);
    }
    std::cerr << "unknown incremental file: " << file_name << std::endl;
    std::exit(1);
  }

  vertex_t AllocateVertexId() {
    return next_vertex_id_++;
  }

  vertex_t ResolveOrCreateEntityId(NodeKind kind,
                                   const std::string& raw_id,
                                   bool* created) {
    if (created != nullptr) {
      *created = false;
    }
    if (raw_id.empty()) {
      return lsmgraph::INVALID_VERTEX_ID;
    }
    const TypedEntityKey key{kind, raw_id};
    const auto it = entity_to_vid_.find(key);
    if (it != entity_to_vid_.end()) {
      return it->second;
    }
    const vertex_t id = AllocateVertexId();
    entity_to_vid_.emplace(key, id);
    if (created != nullptr) {
      *created = true;
    }
    return id;
  }

  vertex_t RequireExistingEntityId(NodeKind kind,
                                   const std::string& raw_id,
                                   const std::string& context) const {
    if (raw_id.empty()) {
      return lsmgraph::INVALID_VERTEX_ID;
    }
    const auto it = entity_to_vid_.find(TypedEntityKey{kind, raw_id});
    if (it == entity_to_vid_.end()) {
      std::cerr << "missing entity for relation import, kind="
                << NodeKindToString(kind) << ", raw_id=" << raw_id
                << ", context=" << context << std::endl;
      std::exit(1);
    }
    return it->second;
  }

  PmrString BuildNodePayload(
      NodeKind kind,
      const std::string& raw_id,
      const std::vector<std::pair<std::string, std::string>>& extra_props,
      std::pmr::memory_resource* mr = std::pmr::get_default_resource()) const {
    (void)kind;
    std::vector<std::string> slots(NodeDbProperties().size());
    const auto& index_by_name = NodePropertyIndexByName();
    SetPayloadSlot(&slots, index_by_name, "rawId", raw_id);
    for (const auto& kv : extra_props) {
      if (kv.first.empty() || kv.second.empty()) {
        continue;
      }
      SetPayloadSlot(&slots,
                     index_by_name,
                     kv.first,
                     NormalizePropertyValue(kv.first, kv.second));
    }
    return EncodePayloadSlotsPmr(slots, mr);
  }

  PmrString BuildNodeColdPayload(
      NodeKind kind,
      const std::string& raw_id,
      const std::vector<std::pair<std::string, std::string>>& extra_props,
      std::pmr::memory_resource* mr = std::pmr::get_default_resource()) const {
    (void)raw_id;
    std::vector<std::string> slots(ColdNodeProperties().size());
    const auto& index_by_name = ColdNodePropertyIndex();
    SetPayloadSlot(&slots, index_by_name, "nodeLabel", NodeKindToString(kind));
    for (const auto& kv : extra_props) {
      if (kv.first.empty() || kv.second.empty()) {
        continue;
      }
      SetPayloadSlot(&slots,
                     index_by_name,
                     kv.first,
                     NormalizePropertyValue(kv.first, kv.second));
    }
    return EncodePayloadSlotsPmr(slots, mr);
  }

  PmrString BuildNodePayloadFromRow(
      NodeKind kind,
      const std::string& raw_id,
      const CsvRow& row,
      const std::vector<std::string>& property_columns,
      std::pmr::memory_resource* mr = std::pmr::get_default_resource()) const {
    (void)kind;
    std::vector<std::string> slots(NodeDbProperties().size());
    const auto& index_by_name = NodePropertyIndexByName();
    SetPayloadSlot(&slots, index_by_name, "rawId", raw_id);
    for (const auto& column : property_columns) {
      const std::string& value = row.Get(column);
      if (value.empty()) {
        continue;
      }
      SetPayloadSlot(&slots,
                     index_by_name,
                     column,
                     NormalizePropertyValue(column, value));
    }
    return EncodePayloadSlotsPmr(slots, mr);
  }

  PmrString BuildNodeColdPayloadFromRow(
      NodeKind kind,
      const std::string& raw_id,
      const CsvRow& row,
      const std::vector<std::string>& property_columns,
      std::pmr::memory_resource* mr = std::pmr::get_default_resource()) const {
    (void)raw_id;
    std::vector<std::string> slots(ColdNodeProperties().size());
    const auto& index_by_name = ColdNodePropertyIndex();
    SetPayloadSlot(&slots, index_by_name, "nodeLabel", NodeKindToString(kind));
    for (const auto& column : property_columns) {
      const std::string& value = row.Get(column);
      if (value.empty()) {
        continue;
      }
      SetPayloadSlot(&slots,
                     index_by_name,
                     column,
                     NormalizePropertyValue(column, value));
    }
    return EncodePayloadSlotsPmr(slots, mr);
  }

  PreparedNodeWrite BuildPreparedNodeWrite(
      vertex_t id,
      NodeKind kind,
      const std::string& raw_id,
      const std::vector<std::pair<std::string, std::string>>& extra_props,
      uint64_t scheduled_time,
      std::pmr::memory_resource* mr = std::pmr::get_default_resource()) const {
    PmrString cold = BuildNodeColdPayload(kind, raw_id, extra_props, mr);
    const bool has_cold = !cold.empty();
    return PreparedNodeWrite{id,
                             BuildNodePayload(kind, raw_id, extra_props, mr),
                             std::move(cold),
                             has_cold,
                             scheduled_time};
  }

  PreparedNodeWrite BuildPreparedNodeWriteFromRow(
      vertex_t id,
      NodeKind kind,
      const std::string& raw_id,
      const CsvRow& row,
      const std::vector<std::string>& property_columns,
      uint64_t scheduled_time,
      std::pmr::memory_resource* mr = std::pmr::get_default_resource()) const {
    PmrString cold =
        BuildNodeColdPayloadFromRow(kind, raw_id, row, property_columns, mr);
    const bool has_cold = !cold.empty();
    return PreparedNodeWrite{
        id,
        BuildNodePayloadFromRow(kind, raw_id, row, property_columns, mr),
        std::move(cold),
        has_cold,
        scheduled_time};
  }

  void PushPreparedNodeWrite(PreparedImportBatch* batch,
                             PreparedNodeWrite write,
                             bool mark_latency = true) {
    if (batch == nullptr) {
      return;
    }
    if (mark_latency) {
      write.sample_write_latency = node_write_latency_sampler_.MarkNext();
    }
    batch->node_writes.push_back(std::move(write));
  }

  void PushPreparedNodeWrite(PreparedImportBatch& batch,
                             PreparedNodeWrite write,
                             bool mark_latency = true) {
    if (mark_latency) {
      write.sample_write_latency = node_write_latency_sampler_.MarkNext();
    }
    batch.node_writes.push_back(std::move(write));
  }

  uint16_t RegisterSingleEdgeProperty(const std::string& name, bool cold) {
    return single_edge_sampler_.RegisterProperty(name, cold);
  }

  void AddSingleEdgeCandidate(vertex_t src,
                              vertex_t dst,
                              uint8_t edge_type,
                              bool is_out,
                              uint16_t property_id,
                              uint16_t cold_slot) {
    if (!SingleEdgeReadEnabled()) {
      return;
    }
    SingleEdgeReadCandidate candidate;
    candidate.src = src;
    candidate.dst = dst;
    candidate.edge_type = edge_type;
    candidate.is_out = is_out;
    candidate.property_id = property_id;
    candidate.cold_slot = cold_slot;
    single_edge_sampler_.AddCandidate(candidate);
  }

  void AddSingleEdgeCandidateForStoredDirections(
      const PreparedRelationWrite& write,
      uint16_t property_id,
      uint16_t cold_slot) {
    switch (write.write_direction) {
      case RelationWriteDirection::kBidirectional:
        AddSingleEdgeCandidate(write.src,
                               write.dst,
                               write.edge_type,
                               true,
                               property_id,
                               cold_slot);
        AddSingleEdgeCandidate(write.dst,
                               write.src,
                               write.edge_type,
                               false,
                               property_id,
                               cold_slot);
        break;
      case RelationWriteDirection::kForwardOnly:
        AddSingleEdgeCandidate(write.src,
                               write.dst,
                               write.edge_type,
                               true,
                               property_id,
                               cold_slot);
        break;
      case RelationWriteDirection::kReverseOnly:
        AddSingleEdgeCandidate(write.dst,
                               write.src,
                               write.edge_type,
                               false,
                               property_id,
                               cold_slot);
        break;
    }
  }

  void CollectSingleEdgeCandidates(const RelationSpec& rel,
                                   const PreparedRelationWrite& write,
                                   const CsvRow& row) {
    if (!SingleEdgeReadEnabled()) {
      return;
    }
    for (const auto& column : rel.property_columns) {
      const std::string& value = row.Get(column);
      if (value.empty()) {
        continue;
      }
      if (column == "createTime" || column == "amount") {
        const uint16_t property_id = RegisterSingleEdgeProperty(column, false);
        AddSingleEdgeCandidateForStoredDirections(write, property_id, 0);
        continue;
      }
      const int cold_slot = ColdEdgePropertySlot(column);
      if (cold_slot >= 0) {
        const uint16_t property_id = RegisterSingleEdgeProperty(column, true);
        AddSingleEdgeCandidateForStoredDirections(
            write, property_id, static_cast<uint16_t>(cold_slot));
      }
    }
    if (row.header == nullptr) {
      return;
    }
    uint16_t generated_slot = static_cast<uint16_t>(kColdEdgePropertyCount);
    for (const auto& column : row.header->columns) {
      if (!IsGeneratedColdProperty(column)) {
        continue;
      }
      const std::string& value = row.Get(column);
      if (!value.empty()) {
        const uint16_t property_id = RegisterSingleEdgeProperty(column, true);
        AddSingleEdgeCandidateForStoredDirections(
            write, property_id, generated_slot);
      }
      ++generated_slot;
    }
  }

	  PreparedRelationWrite BuildPreparedRelationWrite(const RelationSpec& rel,
	                                                   vertex_t src,
	                                                   vertex_t dst,
	                                                   const CsvRow& row) {
	    PreparedRelationWrite write(LoadArenaResource());
    write.edge_type = rel.edge_type;
    write.src = src;
    write.dst = dst;
    write.write_direction = WriteDirectionForRelation(rel.name);
    write.rel_name = rel.name.c_str();
	    write.edge_shard0_payload =
        BuildEdgeShard0Payload(rel, row, LoadArenaResource());
	    write.cold_payload = BuildColdPayload(rel, row, LoadArenaResource());
	    write.has_cold_payload = !write.cold_payload.empty();
	    write.scheduled_time = ParseTimeToMillis(row.Get("createTime"));
	    return write;
	  }

	  static void PrintFullGraphWriteStats(const ImportStats& snapshot_nodes,
	                                       const ImportStats& snapshot_edges,
	                                       const ImportStats& incremental_nodes,
	                                       const ImportStats& incremental_edges) {
	    const uint64_t node_writes = snapshot_nodes.node_writes +
	                                 snapshot_edges.node_writes +
	                                 incremental_nodes.node_writes +
                                   incremental_edges.node_writes;
	    const uint64_t edge_writes = snapshot_nodes.edge_writes +
	                                 snapshot_edges.edge_writes +
	                                 incremental_nodes.edge_writes +
                                   incremental_edges.edge_writes;
	    const uint64_t logical_writes = node_writes + edge_writes;
		    const double foreground_sec = snapshot_nodes.sec + snapshot_edges.sec +
		                                  incremental_nodes.sec +
                                      incremental_edges.sec;
      const double node_sec = ((snapshot_nodes.node_writes == 0)
                                   ? 0.0
                                   : ImportStatsElapsedSec(snapshot_nodes)) +
                              ((snapshot_edges.node_writes == 0)
                                   ? 0.0
                                   : ImportStatsElapsedSec(snapshot_edges)) +
                              ((incremental_nodes.node_writes == 0)
                                   ? 0.0
                                   : ImportStatsElapsedSec(incremental_nodes)) +
                              ((incremental_edges.node_writes == 0)
                                   ? 0.0
                                   : ImportStatsElapsedSec(incremental_edges));
      const double edge_sec = ((snapshot_nodes.edge_writes == 0)
                                   ? 0.0
                                   : ImportStatsElapsedSec(snapshot_nodes)) +
                              ((snapshot_edges.edge_writes == 0)
                                   ? 0.0
                                   : ImportStatsElapsedSec(snapshot_edges)) +
                              ((incremental_nodes.edge_writes == 0)
                                   ? 0.0
                                   : ImportStatsElapsedSec(incremental_nodes)) +
                              ((incremental_edges.edge_writes == 0)
                                   ? 0.0
                                   : ImportStatsElapsedSec(incremental_edges));
      const double sec = node_sec + edge_sec;
	    const double total_qps =
	        (sec <= 0.0 || logical_writes == 0)
	            ? 0.0
	            : static_cast<double>(logical_writes) / sec;
	    const double node_qps =
	        (node_sec <= 0.0 || node_writes == 0)
	            ? 0.0
	            : static_cast<double>(node_writes) / node_sec;
	    const double edge_qps =
	        (edge_sec <= 0.0 || edge_writes == 0)
	            ? 0.0
	            : static_cast<double>(edge_writes) / edge_sec;
		    std::cout << "[FULL_GRAPH_WRITE] time(s): " << sec << std::endl;
		    std::cout << "[FULL_GRAPH_WRITE] foreground_write_time(s): "
		              << foreground_sec << std::endl;
	    std::cout << "[FULL_GRAPH_WRITE] node_writes: " << node_writes
	              << std::endl;
	    std::cout << "[FULL_GRAPH_WRITE] edge_writes: " << edge_writes
	              << std::endl;
	    std::cout << "[FULL_GRAPH_WRITE] logical_writes: " << logical_writes
	              << std::endl;
	    std::cout << "[FULL_GRAPH_WRITE] qps(logical_write/s): " << total_qps
	              << std::endl;
      std::cout << "[FULL_GRAPH_WRITE] node_time(s): " << node_sec
                << std::endl;
	    std::cout << "[FULL_GRAPH_WRITE] node_qps(node/s): " << node_qps
	              << std::endl;
      std::cout << "[FULL_GRAPH_WRITE] edge_time(s): " << edge_sec
                << std::endl;
	    std::cout << "[FULL_GRAPH_WRITE] edge_qps(edge/s): " << edge_qps
	              << std::endl;
	  }

  static uint64_t CountPreparedEntityNodes(
      const std::vector<PreparedImportBatch>& batches) {
    uint64_t total = 0;
    for (const auto& batch : batches) {
      total += batch.new_entity_nodes;
    }
    return total;
  }

  void AppendSnapshotNodeRow(PreparedImportBatch* batch,
                             const NodeTableSpec& spec,
                             const CsvRow& row) {
    assert(batch != nullptr);
    const vertex_t id =
        RequireExistingEntityId(spec.kind, row.Get(spec.id_column), spec.file_name);
    PushPreparedNodeWrite(
        *batch,
        BuildPreparedNodeWriteFromRow(id,
                                      spec.kind,
                                      row.Get(spec.id_column),
                                      row,
                                      spec.property_columns,
                                      ParseTimeToMillis(row.Get("createTime")),
                                      LoadArenaResource()));
    if (MarkMaterializedVertex(id)) {
      ++batch->new_entity_nodes;
    }
  }

  void AppendSnapshotRelationRow(PreparedImportBatch* batch,
                                 const RelationSpec& rel,
                                 const CsvRow& row,
                                 bool sample_write_latency) {
    assert(batch != nullptr);
    const vertex_t src =
        RequireExistingEntityId(rel.src_kind, row.Get(rel.source_column), rel.name);
    const vertex_t dst =
        RequireExistingEntityId(rel.dst_kind, row.Get(rel.dest_column), rel.name);
    PreparedRelationWrite write = BuildPreparedRelationWrite(rel, src, dst, row);
    if (sample_write_latency) {
      write.sample_write_latency = write_latency_sampler_.MarkNext();
    }
    CollectSingleEdgeCandidates(rel, write, row);
    batch->relation_writes.push_back(std::move(write));
  }

  void AppendIncrementalNodeRow(PreparedImportBatch* batch,
                                const std::string& name,
                                NodeKind kind,
                                const char* id_column,
                                const std::vector<std::string>& property_columns,
                                const CsvRow& row) {
    assert(batch != nullptr);
    const uint64_t scheduled_time = ParseTimeToMillis(row.Get("createTime"));
    const std::string& raw_id = row.Get(id_column);
    const vertex_t id = RequireExistingEntityId(kind, raw_id, name);
    PushPreparedNodeWrite(
        *batch,
        BuildPreparedNodeWriteFromRow(id,
                                      kind,
                                      raw_id,
                                      row,
                                      property_columns,
                                      scheduled_time,
                                      LoadArenaResource()));
    if (MarkMaterializedVertex(id)) {
      ++batch->new_entity_nodes;
    }
  }

  void AppendIncrementalOwnAccountRow(PreparedImportBatch* batch,
                                      const std::string& name,
                                      const RelationSpec& rel,
                                      NodeKind owner_kind,
                                      const char* owner_id_column,
                                      const CsvRow& row) {
    assert(batch != nullptr);
    const uint64_t scheduled_time = ParseTimeToMillis(row.Get("createTime"));
    const vertex_t owner =
        ResolveIncrementalEndpointId(
            batch, owner_kind, row.Get(owner_id_column), name, scheduled_time);
    const std::string& account_raw_id = row.Get("accountId");
    const vertex_t account =
        RequireExistingEntityId(NodeKind::kAccount, account_raw_id, name);
    std::vector<std::pair<std::string, std::string>> account_props = {
        {"accountType", row.Get("accountType")},
        {"isBlocked", row.Get("accountBlocked")},
        {"nickname", row.Get("nickname")},
        {"phonenum", row.Get("phonenum")},
        {"email", row.Get("email")},
        {"freqLoginType", row.Get("freqLoginType")},
        {"lastLoginTime", row.Get("lastLoginTime")},
        {"accountLevel", row.Get("accountLevel")},
    };
    AppendNodeWrite(batch,
                    account,
                    NodeKind::kAccount,
                    account_raw_id,
                    account_props,
                    MarkMaterializedVertex(account),
                    scheduled_time);
    PreparedRelationWrite write =
        BuildPreparedRelationWrite(rel, owner, account, row);
    CollectSingleEdgeCandidates(rel, write, row);
    batch->relation_writes.push_back(std::move(write));
  }

  void AppendIncrementalApplyLoanRow(PreparedImportBatch* batch,
                                     const std::string& name,
                                     const RelationSpec& rel,
                                     NodeKind applicant_kind,
                                     const char* applicant_id_column,
                                     const CsvRow& row) {
    assert(batch != nullptr);
    const uint64_t scheduled_time = ParseTimeToMillis(row.Get("createTime"));
    const vertex_t applicant = ResolveIncrementalEndpointId(
        batch, applicant_kind, row.Get(applicant_id_column), name, scheduled_time);
    const std::string& loan_raw_id = row.Get("loanId");
    const vertex_t loan =
        RequireExistingEntityId(NodeKind::kLoan, loan_raw_id, name);
    std::vector<std::pair<std::string, std::string>> loan_props = {
        {"loanAmount", row.Get("loanAmount")},
        {"balance", row.Get("balance")},
        {"loanUsage", row.Get("loanUsage")},
        {"interestRate", row.Get("interestRate")},
    };
    AppendNodeWrite(batch,
                    loan,
                    NodeKind::kLoan,
                    loan_raw_id,
                    loan_props,
                    MarkMaterializedVertex(loan),
                    scheduled_time);
    PreparedRelationWrite write =
        BuildPreparedRelationWrite(rel, applicant, loan, row);
    CollectSingleEdgeCandidates(rel, write, row);
    batch->relation_writes.push_back(std::move(write));
  }

  void AppendSimpleRelationIncrementalRow(PreparedImportBatch* batch,
                                          const std::string& name,
                                          const RelationSpec& rel,
                                          const CsvRow& row,
                                          const char* source_column = nullptr,
                                          const char* dest_column = nullptr) {
    assert(batch != nullptr);
    const uint64_t scheduled_time = ParseTimeToMillis(row.Get("createTime"));
    const std::string src_col =
        source_column == nullptr ? rel.source_column : source_column;
    const std::string dst_col =
        dest_column == nullptr ? rel.dest_column : dest_column;
    const vertex_t src =
        ResolveIncrementalEndpointId(
            batch, rel.src_kind, row.Get(src_col), name, scheduled_time);
    const vertex_t dst =
        ResolveIncrementalEndpointId(
            batch, rel.dst_kind, row.Get(dst_col), name, scheduled_time);
    PreparedRelationWrite write = BuildPreparedRelationWrite(rel, src, dst, row);
    CollectSingleEdgeCandidates(rel, write, row);
    batch->relation_writes.push_back(std::move(write));
  }

  bool PrepareSnapshotNodeBatch(const NodeTableSpec& spec,
                                const std::string& path) {
    PreparedImportBatch batch;
    batch.name = spec.file_name;
    batch.node_writes.reserve(EstimateCsvRowsBySize(path, 192));
    const bool ok = ForEachCsvRow(path, [&](const CsvRow& row) {
      ++batch.logical_rows;
      bool created = false;
      const vertex_t id =
          ResolveOrCreateEntityId(spec.kind, row.Get(spec.id_column), &created);
      if (id == lsmgraph::INVALID_VERTEX_ID) {
        std::cerr << "failed to allocate snapshot node id from " << spec.file_name
                  << std::endl;
        std::exit(1);
      }
	      PushPreparedNodeWrite(
	          batch,
	          BuildPreparedNodeWriteFromRow(id,
	                                        spec.kind,
	                                        row.Get(spec.id_column),
	                                        row,
	                                        spec.property_columns,
	                                        ParseTimeToMillis(row.Get("createTime"))));
      if (created) {
        ++batch.new_entity_nodes;
      }
    });
    if (!ok) {
      return false;
    }
    prepared_snapshot_node_batches_.push_back(std::move(batch));
    return true;
  }

  bool PrepareSnapshotRelationBatch(const RelationSpec& rel,
                                    const std::string& path) {
    PreparedImportBatch batch;
    batch.name = rel.name;
    const size_t row_hint = EstimateCsvRowsBySize(path, 1024);
    batch.relation_writes.reserve(row_hint);
    const bool ok = ForEachCsvRow(path, [&](const CsvRow& row) {
      ++batch.logical_rows;
      const vertex_t src =
          RequireExistingEntityId(rel.src_kind, row.Get(rel.source_column), rel.name);
      const vertex_t dst =
          RequireExistingEntityId(rel.dst_kind, row.Get(rel.dest_column), rel.name);
      PreparedRelationWrite write = BuildPreparedRelationWrite(rel, src, dst, row);
      CollectSingleEdgeCandidates(rel, write, row);
      batch.relation_writes.push_back(std::move(write));
    });
    if (!ok) {
      return false;
    }
    prepared_snapshot_relation_batches_.push_back(std::move(batch));
    return true;
  }

	  void AppendNodeWrite(PreparedImportBatch* batch,
	                       vertex_t id,
	                       NodeKind kind,
	                       const std::string& raw_id,
	                       const std::vector<std::pair<std::string, std::string>>& props,
	                       bool created,
	                       uint64_t scheduled_time = 0) {
	    assert(batch != nullptr);
	    PushPreparedNodeWrite(
	        *batch,
	        BuildPreparedNodeWrite(id,
	                               kind,
	                               raw_id,
	                               props,
	                               scheduled_time,
	                               LoadArenaResource()));
	    if (created) {
	      ++batch->new_entity_nodes;
	    }
	  }

  vertex_t ResolveIncrementalEndpointId(PreparedImportBatch* batch,
	                                        NodeKind kind,
	                                        const std::string& raw_id,
	                                        const std::string& context,
	                                        uint64_t scheduled_time) {
    bool created = false;
    const vertex_t id = ResolveOrCreateEntityId(kind, raw_id, &created);
    if (id == lsmgraph::INVALID_VERTEX_ID) {
      std::cerr << "invalid incremental endpoint id, kind=" << NodeKindToString(kind)
                << ", raw_id=" << raw_id << ", context=" << context << std::endl;
      std::exit(1);
    }
	    if (MarkMaterializedVertex(id)) {
	      AppendNodeWrite(batch, id, kind, raw_id, {}, true, scheduled_time);
	    }
	    return id;
	  }

  bool PrepareIncrementalNodeBatch(const std::string& name,
                                   NodeKind kind,
                                   const char* id_column,
                                   const std::vector<std::string>& property_columns,
                                   const std::string& path) {
    PreparedImportBatch batch;
    batch.name = name;
    batch.node_writes.reserve(EstimateCsvRowsBySize(path, 192));
	    const bool ok = ForEachCsvRow(path, [&](const CsvRow& row) {
	      ++batch.logical_rows;
	      const uint64_t scheduled_time = ParseTimeToMillis(row.Get("createTime"));
	      bool created = false;
	      const std::string& raw_id = row.Get(id_column);
	      const vertex_t id = ResolveOrCreateEntityId(kind, raw_id, &created);
	      PushPreparedNodeWrite(
	          batch,
	          BuildPreparedNodeWriteFromRow(id,
	                                        kind,
	                                        raw_id,
	                                        row,
	                                        property_columns,
	                                        scheduled_time));
      if (created) {
        ++batch.new_entity_nodes;
      }
    });
    if (!ok) {
      return false;
    }
    prepared_incremental_batches_.push_back(std::move(batch));
    return true;
  }

  bool PrepareIncrementalOwnAccountBatch(const std::string& name,
                                         const RelationSpec& rel,
                                         NodeKind owner_kind,
                                         const char* owner_id_column,
                                         const std::string& path) {
    PreparedImportBatch batch;
    batch.name = name;
    const size_t row_hint = EstimateCsvRowsBySize(path, 1024);
    batch.node_writes.reserve(row_hint * 2U);
    batch.relation_writes.reserve(row_hint);
	    const bool ok = ForEachCsvRow(path, [&](const CsvRow& row) {
	      ++batch.logical_rows;
	      const uint64_t scheduled_time = ParseTimeToMillis(row.Get("createTime"));
	      const vertex_t owner =
	          ResolveIncrementalEndpointId(
	              &batch, owner_kind, row.Get(owner_id_column), name, scheduled_time);
      bool account_created = false;
      const std::string& account_raw_id = row.Get("accountId");
      const vertex_t account =
          ResolveOrCreateEntityId(NodeKind::kAccount, account_raw_id, &account_created);
      std::vector<std::pair<std::string, std::string>> account_props = {
          {"accountType", row.Get("accountType")},
          {"isBlocked", row.Get("accountBlocked")},
          {"nickname", row.Get("nickname")},
          {"phonenum", row.Get("phonenum")},
          {"email", row.Get("email")},
          {"freqLoginType", row.Get("freqLoginType")},
          {"lastLoginTime", row.Get("lastLoginTime")},
          {"accountLevel", row.Get("accountLevel")},
      };
	      AppendNodeWrite(&batch,
	                      account,
	                      NodeKind::kAccount,
	                      account_raw_id,
	                      account_props,
	                      account_created,
	                      scheduled_time);
      PreparedRelationWrite write =
          BuildPreparedRelationWrite(rel, owner, account, row);
      CollectSingleEdgeCandidates(rel, write, row);
      batch.relation_writes.push_back(std::move(write));
    });
    if (!ok) {
      return false;
    }
    prepared_incremental_batches_.push_back(std::move(batch));
    return true;
  }

  bool PrepareIncrementalApplyLoanBatch(const std::string& name,
                                        const RelationSpec& rel,
                                        NodeKind applicant_kind,
                                        const char* applicant_id_column,
                                        const std::string& path) {
    PreparedImportBatch batch;
    batch.name = name;
    const size_t row_hint = EstimateCsvRowsBySize(path, 1024);
    batch.node_writes.reserve(row_hint * 2U);
    batch.relation_writes.reserve(row_hint);
	    const bool ok = ForEachCsvRow(path, [&](const CsvRow& row) {
	      ++batch.logical_rows;
	      const uint64_t scheduled_time = ParseTimeToMillis(row.Get("createTime"));
	      const vertex_t applicant = ResolveIncrementalEndpointId(
	          &batch, applicant_kind, row.Get(applicant_id_column), name, scheduled_time);
      bool loan_created = false;
      const std::string& loan_raw_id = row.Get("loanId");
      const vertex_t loan =
          ResolveOrCreateEntityId(NodeKind::kLoan, loan_raw_id, &loan_created);
      std::vector<std::pair<std::string, std::string>> loan_props = {
          {"loanAmount", row.Get("loanAmount")},
          {"balance", row.Get("balance")},
          {"loanUsage", row.Get("loanUsage")},
          {"interestRate", row.Get("interestRate")},
      };
	      AppendNodeWrite(&batch,
	                      loan,
	                      NodeKind::kLoan,
	                      loan_raw_id,
	                      loan_props,
	                      loan_created,
	                      scheduled_time);
      PreparedRelationWrite write =
          BuildPreparedRelationWrite(rel, applicant, loan, row);
      CollectSingleEdgeCandidates(rel, write, row);
      batch.relation_writes.push_back(std::move(write));
    });
    if (!ok) {
      return false;
    }
    prepared_incremental_batches_.push_back(std::move(batch));
    return true;
  }

  bool PrepareSimpleRelationIncrementalBatch(const std::string& name,
                                             const RelationSpec& rel,
                                             const std::string& path,
                                             const char* source_column = nullptr,
                                             const char* dest_column = nullptr) {
    PreparedImportBatch batch;
    batch.name = name;
    const size_t row_hint = EstimateCsvRowsBySize(path, 1024);
    batch.node_writes.reserve(row_hint);
    batch.relation_writes.reserve(row_hint);
    const std::string src_col = source_column == nullptr ? rel.source_column : source_column;
    const std::string dst_col = dest_column == nullptr ? rel.dest_column : dest_column;
	    const bool ok = ForEachCsvRow(path, [&](const CsvRow& row) {
	      ++batch.logical_rows;
	      const uint64_t scheduled_time = ParseTimeToMillis(row.Get("createTime"));
	      const vertex_t src =
	          ResolveIncrementalEndpointId(
	              &batch, rel.src_kind, row.Get(src_col), name, scheduled_time);
	      const vertex_t dst =
	          ResolveIncrementalEndpointId(
	              &batch, rel.dst_kind, row.Get(dst_col), name, scheduled_time);
      PreparedRelationWrite write = BuildPreparedRelationWrite(rel, src, dst, row);
      CollectSingleEdgeCandidates(rel, write, row);
      batch.relation_writes.push_back(std::move(write));
    });
    if (!ok) {
      return false;
    }
    prepared_incremental_batches_.push_back(std::move(batch));
    return true;
  }

  bool PrepareIncrementalBatch(const std::string& file_name,
                               uint64_t op_num,
                               const std::string& path) {
    if (op_num == 17 || op_num == 18 || op_num == 19) {
      return true;
    }
    if (file_name == "AddPersonWrite1.csv") {
      return PrepareIncrementalNodeBatch(file_name,
                                         NodeKind::kPerson,
                                         "personId",
                                         {"createTime", "personName", "isBlocked", "gender",
                                          "birthday", "country", "city"},
                                         path);
    }
    if (file_name == "AddCompanyWrite2.csv") {
      return PrepareIncrementalNodeBatch(file_name,
                                         NodeKind::kCompany,
                                         "companyId",
                                         {"createTime", "companyName", "isBlocked", "country",
                                          "city", "business", "description", "url"},
                                         path);
    }
    if (file_name == "AddMediumWrite3.csv") {
      return PrepareIncrementalNodeBatch(file_name,
                                         NodeKind::kMedium,
                                         "mediumId",
                                         {"createTime", "mediumType", "isBlocked",
                                          "lastLoginTime", "riskLevel"},
                                         path);
    }
    if (file_name == "AddPersonOwnAccountWrite4.csv") {
      return PrepareIncrementalOwnAccountBatch(file_name,
                                               MustGetRelation("PersonOwnAccount"),
                                               NodeKind::kPerson,
                                               "personId",
                                               path);
    }
    if (file_name == "AddCompanyOwnAccountWrite5.csv") {
      return PrepareIncrementalOwnAccountBatch(file_name,
                                               MustGetRelation("CompanyOwnAccount"),
                                               NodeKind::kCompany,
                                               "companyId",
                                               path);
    }
    if (file_name == "AddPersonApplyLoanWrite6.csv") {
      return PrepareIncrementalApplyLoanBatch(file_name,
                                              MustGetRelation("PersonApplyLoan"),
                                              NodeKind::kPerson,
                                              "personId",
                                              path);
    }
    if (file_name == "AddCompanyApplyLoanWrite7.csv") {
      return PrepareIncrementalApplyLoanBatch(file_name,
                                              MustGetRelation("CompanyApplyLoan"),
                                              NodeKind::kCompany,
                                              "companyId",
                                              path);
    }
    if (file_name == "AddPersonInvestCompanyWrite8.csv") {
      return PrepareSimpleRelationIncrementalBatch(file_name,
                                                   MustGetRelation("PersonInvestCompany"),
                                                   path);
    }
    if (file_name == "AddCompanyInvestCompanyWrite9.csv") {
      return PrepareSimpleRelationIncrementalBatch(file_name,
                                                   MustGetRelation("CompanyInvestCompany"),
                                                   path);
    }
    if (file_name == "AddPersonGuaranteePersonWrite10.csv" ||
        file_name == "AddPersonGuaranteePersonReadWrite3.csv") {
      return PrepareSimpleRelationIncrementalBatch(file_name,
                                                   MustGetRelation("PersonGuaranteePerson"),
                                                   path);
    }
    if (file_name == "AddCompanyGuaranteeCompanyWrite11.csv") {
      return PrepareSimpleRelationIncrementalBatch(file_name,
                                                   MustGetRelation("CompanyGuaranteeCompany"),
                                                   path);
    }
    if (file_name == "AddAccountTransferAccountWrite12.csv" ||
        file_name == "AddAccountTransferAccountReadWrite1.csv" ||
        file_name == "AddAccountTransferAccountReadWrite2.csv") {
      return PrepareSimpleRelationIncrementalBatch(file_name,
                                                   MustGetRelation("AccountTransferAccount"),
                                                   path);
    }
    if (file_name == "AddAccountWithdrawAccountWrite13.csv") {
      return PrepareSimpleRelationIncrementalBatch(file_name,
                                                   MustGetRelation("AccountWithdrawAccount"),
                                                   path);
    }
    if (file_name == "AddAccountRepayLoanWrite14.csv") {
      return PrepareSimpleRelationIncrementalBatch(file_name,
                                                   MustGetRelation("AccountRepayLoan"),
                                                   path,
                                                   "account",
                                                   nullptr);
    }
    if (file_name == "AddLoanDepositAccountWrite15.csv") {
      return PrepareSimpleRelationIncrementalBatch(file_name,
                                                   MustGetRelation("LoanDepositAccount"),
                                                   path,
                                                   "loanId",
                                                   "accountId");
    }
    if (file_name == "AddMediumSigninAccountWrite16.csv") {
      return PrepareSimpleRelationIncrementalBatch(file_name,
                                                   MustGetRelation("MediumSignInAccount"),
                                                   path);
    }
    std::cerr << "unknown incremental file: " << file_name << std::endl;
    std::exit(1);
  }

  void AppendIncrementalRowToBatch(const std::string& file_name,
                                   uint64_t op_num,
                                   const CsvRow& row,
                                   PreparedImportBatch* batch) {
    if (op_num == 17 || op_num == 18 || op_num == 19) {
      return;
    }
    if (file_name == "AddPersonWrite1.csv") {
      AppendIncrementalNodeRow(batch,
                               file_name,
                               NodeKind::kPerson,
                               "personId",
                               {"createTime", "personName", "isBlocked", "gender",
                                "birthday", "country", "city"},
                               row);
      return;
    }
    if (file_name == "AddCompanyWrite2.csv") {
      AppendIncrementalNodeRow(batch,
                               file_name,
                               NodeKind::kCompany,
                               "companyId",
                               {"createTime", "companyName", "isBlocked", "country",
                                "city", "business", "description", "url"},
                               row);
      return;
    }
    if (file_name == "AddMediumWrite3.csv") {
      AppendIncrementalNodeRow(batch,
                               file_name,
                               NodeKind::kMedium,
                               "mediumId",
                               {"createTime", "mediumType", "isBlocked",
                                "lastLoginTime", "riskLevel"},
                               row);
      return;
    }
    if (file_name == "AddPersonOwnAccountWrite4.csv") {
      AppendIncrementalOwnAccountRow(batch,
                                     file_name,
                                     MustGetRelation("PersonOwnAccount"),
                                     NodeKind::kPerson,
                                     "personId",
                                     row);
      return;
    }
    if (file_name == "AddCompanyOwnAccountWrite5.csv") {
      AppendIncrementalOwnAccountRow(batch,
                                     file_name,
                                     MustGetRelation("CompanyOwnAccount"),
                                     NodeKind::kCompany,
                                     "companyId",
                                     row);
      return;
    }
    if (file_name == "AddPersonApplyLoanWrite6.csv") {
      AppendIncrementalApplyLoanRow(batch,
                                    file_name,
                                    MustGetRelation("PersonApplyLoan"),
                                    NodeKind::kPerson,
                                    "personId",
                                    row);
      return;
    }
    if (file_name == "AddCompanyApplyLoanWrite7.csv") {
      AppendIncrementalApplyLoanRow(batch,
                                    file_name,
                                    MustGetRelation("CompanyApplyLoan"),
                                    NodeKind::kCompany,
                                    "companyId",
                                    row);
      return;
    }
    if (file_name == "AddPersonInvestCompanyWrite8.csv") {
      AppendSimpleRelationIncrementalRow(batch,
                                         file_name,
                                         MustGetRelation("PersonInvestCompany"),
                                         row);
      return;
    }
    if (file_name == "AddCompanyInvestCompanyWrite9.csv") {
      AppendSimpleRelationIncrementalRow(batch,
                                         file_name,
                                         MustGetRelation("CompanyInvestCompany"),
                                         row);
      return;
    }
    if (file_name == "AddPersonGuaranteePersonWrite10.csv" ||
        file_name == "AddPersonGuaranteePersonReadWrite3.csv") {
      AppendSimpleRelationIncrementalRow(batch,
                                         file_name,
                                         MustGetRelation("PersonGuaranteePerson"),
                                         row);
      return;
    }
    if (file_name == "AddCompanyGuaranteeCompanyWrite11.csv") {
      AppendSimpleRelationIncrementalRow(batch,
                                         file_name,
                                         MustGetRelation("CompanyGuaranteeCompany"),
                                         row);
      return;
    }
    if (file_name == "AddAccountTransferAccountWrite12.csv" ||
        file_name == "AddAccountTransferAccountReadWrite1.csv" ||
        file_name == "AddAccountTransferAccountReadWrite2.csv") {
      AppendSimpleRelationIncrementalRow(batch,
                                         file_name,
                                         MustGetRelation("AccountTransferAccount"),
                                         row);
      return;
    }
    if (file_name == "AddAccountWithdrawAccountWrite13.csv") {
      AppendSimpleRelationIncrementalRow(batch,
                                         file_name,
                                         MustGetRelation("AccountWithdrawAccount"),
                                         row);
      return;
    }
    if (file_name == "AddAccountRepayLoanWrite14.csv") {
      AppendSimpleRelationIncrementalRow(batch,
                                         file_name,
                                         MustGetRelation("AccountRepayLoan"),
                                         row,
                                         "account",
                                         nullptr);
      return;
    }
    if (file_name == "AddLoanDepositAccountWrite15.csv") {
      AppendSimpleRelationIncrementalRow(batch,
                                         file_name,
                                         MustGetRelation("LoanDepositAccount"),
                                         row,
                                         "loanId",
                                         "accountId");
      return;
    }
    if (file_name == "AddMediumSigninAccountWrite16.csv") {
      AppendSimpleRelationIncrementalRow(batch,
                                         file_name,
                                         MustGetRelation("MediumSignInAccount"),
                                         row);
      return;
    }
    std::cerr << "unknown incremental file: " << file_name << std::endl;
    std::exit(1);
  }

  void ExecutePreparedNodeWriteOne(const PreparedNodeWrite& write) {
        std::chrono::steady_clock::time_point latency_t1;
        if (write.sample_write_latency) {
          latency_t1 = std::chrono::steady_clock::now();
        }
        std::string payload(write.payload.data(), write.payload.size());
        if (write.has_node_cold_payload) {
          const std::string cold_payload(write.node_cold_payload.data(),
                                         write.node_cold_payload.size());
          const std::string cold_ref =
              node_cold_blob_writers_.Append(cold_payload);
          if (!SetPipeFieldBySlot(&payload, NodeColdRefSlot(), cold_ref)) {
            std::cerr << "failed to set node cold ref for id=" << write.id
                      << std::endl;
            std::exit(1);
          }
        }
	    const auto rs = db_->PutNodePayload(write.id, payload, true, kNodeEdgeType);
	    if (rs != lsmgraph::Status::kOk) {
	      std::cerr << "PutNode failed for prepared node id=" << write.id
	                << std::endl;
	      std::exit(1);
	    }
        if (write.sample_write_latency) {
          const auto latency_t2 = std::chrono::steady_clock::now();
          const uint64_t latency_ns =
              static_cast<uint64_t>(
                  std::chrono::duration_cast<std::chrono::nanoseconds>(
                      latency_t2 - latency_t1)
                      .count());
          node_write_latency_sampler_.Record(CurrentOpenMpThreadId(
                                                std::max(GetWriteThreadCount(),
                                                         GetMixedThreadCount())),
                                            latency_ns);
        }
	  }
	
	  void ExecutePreparedNodeWrites(
          const std::pmr::vector<PreparedNodeWrite>& writes) {
	    ParallelForWriteIndex(writes.size(), [&](size_t i) {
	      ExecutePreparedNodeWriteOne(writes[i]);
	    });
	  }
	
	  void ExecutePreparedNodeWriteRefs(
	      const std::vector<const PreparedNodeWrite*>& writes) {
	    ParallelForWriteIndex(writes.size(), [&](size_t i) {
	      ExecutePreparedNodeWriteOne(*writes[i]);
	    });
	  }
	
		  void ExecutePreparedRelationWriteOne(const PreparedRelationWrite& write) {
	    std::chrono::steady_clock::time_point latency_t1;
	    if (write.sample_write_latency) {
	      latency_t1 = std::chrono::steady_clock::now();
	    }
	    const lsmgraph::SequenceNumber_t sequence = db_->NextSequence();
	
	    auto put_payload = [&](size_t shard_idx,
	                           const std::string& payload) -> lsmgraph::Status {
	      switch (write.write_direction) {
	        case RelationWriteDirection::kBidirectional:
	          return db_->PutEdgePayload(shard_idx,
	                                     write.src,
	                                     write.dst,
	                                     payload,
	                                     lsmgraph::EdgeInsertMode::kBidirectional,
	                                     true,
	                                     write.edge_type,
	                                     sequence);
	        case RelationWriteDirection::kForwardOnly:
	          return db_->PutEdgePayload(shard_idx,
	                                     write.src,
	                                     write.dst,
	                                     payload,
	                                     lsmgraph::EdgeInsertMode::kSingle,
	                                     true,
	                                     write.edge_type,
	                                     sequence);
	        case RelationWriteDirection::kReverseOnly:
	          return db_->PutEdgePayload(shard_idx,
	                                     write.dst,
	                                     write.src,
	                                     payload,
	                                     lsmgraph::EdgeInsertMode::kSingle,
	                                     false,
	                                     write.edge_type,
	                                     sequence);
	      }
	      return lsmgraph::Status::kNotFound;
	    };
	
	    // Shard 0: always written with the query-hot fixed properties.
	    {
          const std::string hot_payload(write.edge_shard0_payload.data(),
                                        write.edge_shard0_payload.size());
	      auto rs = put_payload(0, hot_payload);
	      if (rs != lsmgraph::Status::kOk) {
	        std::cerr << "PutEdge shard0 failed for relation "
	                  << write.rel_name << std::endl;
	        std::exit(1);
	      }
	    }
	
	    // Shard 1: write only a fixed-size ref to the append-only cold blob.
	    if (write.has_cold_payload) {
	      const std::string cold_payload(write.cold_payload.data(),
                                         write.cold_payload.size());
	      const std::string cold_ref = cold_blob_writers_.Append(cold_payload);
	      auto rs = put_payload(1, cold_ref);
	      if (rs != lsmgraph::Status::kOk) {
	        std::cerr << "PutEdge shard1 failed for relation "
	                  << write.rel_name << std::endl;
	        std::exit(1);
	      }
	    }
	
	    if (write.sample_write_latency) {
	      const auto latency_t2 = std::chrono::steady_clock::now();
	      const uint64_t latency_ns =
	          static_cast<uint64_t>(
	              std::chrono::duration_cast<std::chrono::nanoseconds>(
	                  latency_t2 - latency_t1)
	                  .count());
	      write_latency_sampler_.Record(CurrentOpenMpThreadId(
                                            std::max(GetWriteThreadCount(),
                                                     GetMixedThreadCount())),
	                                    latency_ns);
	    }
	  }
	
	  void ExecutePreparedRelationWrites(
	      const std::pmr::vector<PreparedRelationWrite>& writes) {
	    ParallelForWriteIndex(writes.size(), [&](size_t i) {
	      ExecutePreparedRelationWriteOne(writes[i]);
	    });
	  }
	
	  void ExecutePreparedRelationWriteRefs(
	      const std::vector<const PreparedRelationWrite*>& writes) {
	    ParallelForWriteIndex(writes.size(), [&](size_t i) {
	      ExecutePreparedRelationWriteOne(*writes[i]);
	    });
	  }

  static void AccumulatePreparedStats(const PreparedImportBatch& batch,
                                      ImportStats* stats) {
    if (stats == nullptr) {
      return;
    }
    stats->logical_rows += batch.logical_rows;
    stats->node_writes += batch.node_writes.size();
    stats->edge_writes += batch.relation_writes.size();
  }

  void ResetPreparedImportBatch(PreparedImportBatch* batch) const {
    if (batch == nullptr) {
      return;
    }
    batch->logical_rows = 0;
    batch->new_entity_nodes = 0;
    batch->node_writes.clear();
    batch->relation_writes.clear();
  }

  void FlushPreparedImportBatch(PreparedImportBatch* batch,
                                ImportStats* stats) {
    if (batch == nullptr || stats == nullptr ||
        (batch->logical_rows == 0 && batch->node_writes.empty() &&
         batch->relation_writes.empty())) {
      return;
    }
    const bool has_writes =
        !batch->node_writes.empty() || !batch->relation_writes.empty();
    const auto t1 = std::chrono::steady_clock::now();
    if (!batch->node_writes.empty()) {
      ExecutePreparedNodeWrites(batch->node_writes);
    }
    if (!batch->relation_writes.empty()) {
      ExecutePreparedRelationWrites(batch->relation_writes);
    }
    const auto t2 = std::chrono::steady_clock::now();
	    if (has_writes) {
	      stats->sec +=
	          std::chrono::duration_cast<std::chrono::duration<double>>(t2 - t1)
	              .count();
		    }
    AccumulatePreparedStats(*batch, stats);
    ResetPreparedImportBatch(batch);
  }

  void AddColdBlobFlushTime(ImportStats* stats) {
    const auto t1 = std::chrono::steady_clock::now();
    if (!cold_blob_writers_.Flush() || !node_cold_blob_writers_.Flush()) {
      std::exit(1);
    }
    const auto t2 = std::chrono::steady_clock::now();
    if (stats != nullptr) {
      stats->sec +=
          std::chrono::duration_cast<std::chrono::duration<double>>(t2 - t1)
              .count();
    }
  }

  bool StreamSnapshotNodeWritesFromFile(const NodeTableSpec& spec,
                                        const std::string& path,
                                        ImportStats* stats) {
    auto batch = NewPreparedBatch(spec.file_name, ImportBatchRows(), 0);
    const bool ok = ForEachCsvRow(path, [&](const CsvRow& row) {
      ++batch->logical_rows;
      AppendSnapshotNodeRow(batch.get(), spec, row);
      if (batch->logical_rows >= ImportBatchRows() || LoadArenaNearFull()) {
        FlushPreparedImportBatch(batch.get(), stats);
        ReleasePreparedBatch(&batch, "snapshot_nodes");
        batch = NewPreparedBatch(spec.file_name, ImportBatchRows(), 0);
      }
    });
    if (!ok) {
      return false;
    }
    FlushPreparedImportBatch(batch.get(), stats);
    ReleasePreparedBatch(&batch, "snapshot_nodes");
    return true;
  }

  bool StreamSnapshotRelationWritesFromFile(const RelationSpec& rel,
                                            const std::string& path,
                                            ImportStats* stats) {
    auto batch = NewPreparedBatch(rel.name, 0, ImportBatchRows());
    const bool ok = ForEachCsvRow(path, [&](const CsvRow& row) {
      ++batch->logical_rows;
      AppendSnapshotRelationRow(batch.get(), rel, row, true);
      if (batch->logical_rows >= ImportBatchRows() || LoadArenaNearFull()) {
        FlushPreparedImportBatch(batch.get(), stats);
        ReleasePreparedBatch(&batch, "snapshot_edges");
        batch = NewPreparedBatch(rel.name, 0, ImportBatchRows());
      }
    });
    if (!ok) {
      return false;
    }
    FlushPreparedImportBatch(batch.get(), stats);
    ReleasePreparedBatch(&batch, "snapshot_edges");
    return true;
  }

  bool StreamIncrementalWritesFromFile(const std::string& file_name,
                                       uint64_t op_num,
                                       const std::string& path,
                                       ImportStats* stats) {
    if (op_num == 17 || op_num == 18 || op_num == 19) {
      return true;
    }
    auto batch =
        NewPreparedBatch(file_name, ImportBatchRows(), ImportBatchRows());
    const bool ok = ForEachCsvRow(path, [&](const CsvRow& row) {
      ++batch->logical_rows;
      AppendIncrementalRowToBatch(file_name, op_num, row, batch.get());
      if (batch->logical_rows >= ImportBatchRows() || LoadArenaNearFull()) {
        FlushPreparedImportBatch(batch.get(), stats);
        ReleasePreparedBatch(&batch, "incremental");
        batch = NewPreparedBatch(file_name, ImportBatchRows(), ImportBatchRows());
      }
    });
    if (!ok) {
      return false;
    }
    FlushPreparedImportBatch(batch.get(), stats);
    ReleasePreparedBatch(&batch, "incremental");
    return true;
  }

  ImportStats ImportSnapshotNodes() {
    ImportStats stats;
    for (const auto& spec : SnapshotNodeTables()) {
      const std::string path = GetSnapshotDir() + "/" + spec.file_name;
      if (!StreamSnapshotNodeWritesFromFile(spec, path, &stats)) {
        std::cerr << "failed to stream snapshot node csv: " << path
                  << std::endl;
        std::exit(1);
      }
    }
    stats.entity_nodes = snapshot_entity_total_;
    return stats;
  }

  ImportStats ImportSnapshotRelations() {
    ImportStats stats;
    for (const auto& rel : AllRelations()) {
      const std::string path = GetSnapshotDir() + "/" + rel.name + ".csv";
      if (!StreamSnapshotRelationWritesFromFile(rel, path, &stats)) {
        std::cerr << "failed to stream snapshot relation csv: " << path
                  << std::endl;
        std::exit(1);
      }
    }
    AddColdBlobFlushTime(&stats);
    stats.entity_nodes = snapshot_entity_total_;
    return stats;
  }

  ImportStats ImportIncremental() {
    ImportStats stats;
    for (const auto& path : SortedIncrementalFiles()) {
      const std::string file_name = path.filename().string();
      const uint64_t op_num = ExtractTrailingNumber(file_name);
      if (!StreamIncrementalWritesFromFile(file_name,
                                           op_num,
                                           path.string(),
                                           &stats)) {
        std::cerr << "failed to stream incremental csv: " << path.string()
                  << std::endl;
        std::exit(1);
      }
    }
    AddColdBlobFlushTime(&stats);
    stats.entity_nodes = incremental_entity_total_;
    return stats;
  }

  std::optional<vertex_t> LookupEntity(NodeKind kind,
                                       const std::string& raw_id) const {
    const auto it = entity_to_vid_.find(TypedEntityKey{kind, raw_id});
    if (it == entity_to_vid_.end()) {
      return std::nullopt;
    }
    return it->second;
  }

  std::string GetNodeDbProp(vertex_t vid, const std::string& name) const {
    std::unordered_map<std::string, std::string> props;
    const auto rs = db_->GetNode(vid, {name}, &props, true, kNodeEdgeType);
    if (rs != lsmgraph::Status::kOk) {
      return {};
    }
    const auto it = props.find(name);
    return it == props.end() ? std::string() : it->second;
  }

  std::string GetNodeProp(vertex_t vid, const std::string& name) {
    if (!IsColdNodeProperty(name)) {
      return GetNodeDbProp(vid, name);
    }
    const std::string ref = GetNodeDbProp(vid, "node_cold_property");
    if (ref.empty()) {
      return {};
    }
    const std::string payload = ReadNodeColdBlobPayloadNoCache(ref);
    if (payload.empty()) {
      return {};
    }
    const auto& index = ColdNodePropertyIndex();
    const auto it = index.find(name);
    if (it == index.end()) {
      return {};
    }
    std::string value;
    return GetPipeFieldBySlot(payload, static_cast<uint16_t>(it->second), &value)
               ? value
               : std::string();
  }

  bool ParseOrderDescending(const std::string& s) const {
    return s.find("DESC") != std::string::npos;
  }

  bool PassTemporalFilter(const RelationSpec& rel,
                          const std::unordered_map<std::string, std::string>& props,
                          uint64_t start_time,
                          uint64_t end_time) const {
    if (!rel.temporal) {
      return true;
    }
    const auto it = props.find("createTime");
    if (it == props.end()) {
      return false;
    }
    const uint64_t ts = ToUint64(it->second);
    return ts >= start_time && ts <= end_time;
  }

  static uint64_t ExtractSortTime(const TraversedEdge& record) {
    const auto it = record.properties.find("createTime");
    return it == record.properties.end() ? 0 : ToUint64(it->second);
  }

  void ApplyTruncation(std::vector<TraversedEdge>* edges,
                       uint32_t limit,
                       bool descending) const {
    if (edges == nullptr || edges->empty()) {
      return;
    }
    std::sort(edges->begin(), edges->end(),
              [&](const TraversedEdge& a, const TraversedEdge& b) {
                const uint64_t ta = ExtractSortTime(a);
                const uint64_t tb = ExtractSortTime(b);
                if (ta != tb) {
                  return descending ? (ta > tb) : (ta < tb);
                }
                return a.other_id < b.other_id;
              });
    if (limit > 0 && edges->size() > limit) {
      edges->resize(limit);
    }
  }

  std::vector<TraversedEdge> TraverseFromSource(vertex_t src,
                                                const RelationSpec& rel,
                                                uint64_t start_time,
                                                uint64_t end_time,
                                                uint32_t truncation_limit,
                                                bool order_desc,
                                                const std::vector<std::string>& property_names = EdgeQueryAmountProps()) {
    std::vector<TraversedEdge> out;
    std::vector<lsmgraph::GraphDbEdgeScanRecord> records;
    const auto rs =
        db_->ScanEdges(src, property_names, &records, true, rel.edge_type);
    if (rs != lsmgraph::Status::kOk) {
      return out;
    }

    out.reserve(records.size());
    for (auto& rec : records) {
      if (!PassTemporalFilter(rel, rec.properties, start_time, end_time)) {
        continue;
      }
      TraversedEdge item;
      item.other_id = rec.dst;
      item.properties = std::move(rec.properties);
      out.push_back(std::move(item));
    }
    ApplyTruncation(&out, truncation_limit, order_desc);
    return out;
  }

  std::vector<TraversedEdge> TraverseToTarget(vertex_t dst,
                                              const RelationSpec& rel,
                                              uint64_t start_time,
                                               uint64_t end_time,
                                               uint32_t truncation_limit,
                                               bool order_desc,
                                               const std::vector<std::string>& property_names = EdgeQueryAmountProps()) {
    std::vector<TraversedEdge> out;
    std::vector<lsmgraph::GraphDbEdgeScanRecord> records;
    const auto rs =
        db_->ScanEdges(dst, property_names, &records, false, rel.edge_type);
    if (rs != lsmgraph::Status::kOk) {
      return out;
    }

    out.reserve(records.size());
    for (auto& rec : records) {
      if (!PassTemporalFilter(rel, rec.properties, start_time, end_time)) {
        continue;
      }
      TraversedEdge item;
      item.other_id = rec.dst;
      item.properties = std::move(rec.properties);
      out.push_back(std::move(item));
    }
    ApplyTruncation(&out, truncation_limit, order_desc);
    return out;
  }

  bool IsBlockedMedium(vertex_t medium_id) {
    const auto value = GetNodeProp(medium_id, "isBlocked");
    return value == "true" || value == "TRUE" || value == "True" || value == "1";
  }

  bool IsCompanyOwnedAccount(vertex_t account_id) {
    thread_local std::unordered_map<vertex_t, bool> company_account_cache;
    const auto cached = company_account_cache.find(account_id);
    if (cached != company_account_cache.end()) {
      return cached->second;
    }
    const auto& rel = MustGetRelation("CompanyOwnAccount");
    const auto incoming = TraverseToTarget(account_id, rel, 0, 0, 0, false, EdgeQueryTemporalProps());
    const bool result = !incoming.empty();
    company_account_cache[account_id] = result;
    return result;
  }

  bool HasTemporalDirectTransfer(vertex_t src,
                                 vertex_t dst,
                                 uint64_t start_time,
                                 uint64_t end_time,
                                 uint32_t truncation_limit,
                                 bool order_desc) {
    const auto& rel = MustGetRelation("AccountTransferAccount");
    const auto edges =
        TraverseFromSource(src, rel, start_time, end_time, truncation_limit, order_desc,
                           EdgeQueryTemporalProps());
    for (const auto& edge : edges) {
      if (edge.other_id == dst) {
        return true;
      }
    }
    return false;
  }

  QueryRunResult RunQuery1(const CsvRow& row) {
    QueryRunResult result;
    const auto start =
        LookupEntity(NodeKind::kAccount, row.GetByIndex(0));
    if (!start.has_value()) {
      return result;
    }
    const uint64_t start_time = ToUint64(row.GetByIndex(1));
    const uint64_t end_time = ToUint64(row.GetByIndex(2));
    const uint32_t truncation_limit =
        static_cast<uint32_t>(ToUint64(row.GetByIndex(3)));
    const bool order_desc = ParseOrderDescending(row.GetByIndex(4));

    const auto& transfer = MustGetRelation("AccountTransferAccount");
    const auto& sign_in = MustGetRelation("MediumSignInAccount");

    std::unordered_set<vertex_t> visited_accounts;
    std::vector<vertex_t> frontier;
    frontier.push_back(*start);
    visited_accounts.insert(*start);

    std::unordered_set<uint64_t> seen_pairs;
    for (int depth = 0; depth < 3; ++depth) {
      std::vector<vertex_t> next;
      for (vertex_t account_id : frontier) {
        const auto hops =
            TraverseFromSource(account_id, transfer, start_time, end_time,
                               truncation_limit, order_desc, EdgeQueryTemporalProps());
        for (const auto& hop : hops) {
          if (visited_accounts.insert(hop.other_id).second) {
            next.push_back(hop.other_id);
          }
          const auto signins =
              TraverseToTarget(hop.other_id, sign_in, start_time, end_time,
                               truncation_limit, order_desc, EdgeQueryTemporalProps());
          for (const auto& signin : signins) {
            if (!IsBlockedMedium(signin.other_id)) {
              continue;
            }
            const uint64_t pair_hash =
                HashMix(static_cast<uint64_t>(hop.other_id)) ^
                HashMix(static_cast<uint64_t>(signin.other_id));
            if (seen_pairs.insert(pair_hash).second) {
              ++result.rows;
              result.checksum ^= pair_hash;
            }
          }
        }
      }
      frontier.swap(next);
      if (frontier.empty()) {
        break;
      }
    }
    return result;
  }

  QueryRunResult RunQuery2(const CsvRow& row) {
    QueryRunResult result;
    const auto person = LookupEntity(NodeKind::kPerson, row.GetByIndex(0));
    if (!person.has_value()) {
      return result;
    }
    const uint64_t start_time = ToUint64(row.GetByIndex(1));
    const uint64_t end_time = ToUint64(row.GetByIndex(2));
    const uint32_t truncation_limit =
        static_cast<uint32_t>(ToUint64(row.GetByIndex(3)));
    const bool order_desc = ParseOrderDescending(row.GetByIndex(4));

    const auto& own = MustGetRelation("PersonOwnAccount");
    const auto& transfer = MustGetRelation("AccountTransferAccount");
    const auto& deposit = MustGetRelation("LoanDepositAccount");

    std::unordered_set<vertex_t> loan_ids;
    double sum_loan_amount = 0.0;
    double sum_loan_balance = 0.0;

    // Q2: PersonOwnAccount
    const auto accounts =
        TraverseFromSource(*person, own, 0, 0, 0, false, EdgeQueryTemporalProps());
    for (const auto& account_hop : accounts) {
      const auto incoming =
          TraverseToTarget(account_hop.other_id, transfer, start_time, end_time,
                           truncation_limit, order_desc, EdgeQueryTemporalProps());
      for (const auto& gather_hop : incoming) {
        const auto loans =
            TraverseToTarget(gather_hop.other_id, deposit, start_time, end_time,
                             truncation_limit, order_desc, EdgeQueryTemporalProps());
        for (const auto& loan_hop : loans) {
          if (loan_ids.insert(loan_hop.other_id).second) {
            ++result.rows;
            sum_loan_amount += ToDouble(GetNodeProp(loan_hop.other_id, "loanAmount"));
            sum_loan_balance += ToDouble(GetNodeProp(loan_hop.other_id, "balance"));
            result.checksum ^= HashMix(static_cast<uint64_t>(loan_hop.other_id));
          }
        }
      }
    }

    result.checksum ^=
        static_cast<uint64_t>(std::llround(sum_loan_amount * 1000.0));
    result.checksum ^=
        static_cast<uint64_t>(std::llround(sum_loan_balance * 1000.0));
    return result;
  }

  QueryRunResult RunQuery3(const CsvRow& row) {
    QueryRunResult result;
    const auto src = LookupEntity(NodeKind::kAccount, row.GetByIndex(0));
    const auto dst = LookupEntity(NodeKind::kAccount, row.GetByIndex(1));
    if (!src.has_value() || !dst.has_value()) {
      return result;
    }
    const uint64_t start_time = ToUint64(row.GetByIndex(2));
    const uint64_t end_time = ToUint64(row.GetByIndex(3));
    const uint32_t truncation_limit =
        static_cast<uint32_t>(ToUint64(row.GetByIndex(4)));
    const bool order_desc = ParseOrderDescending(row.GetByIndex(5));
    const auto& transfer = MustGetRelation("AccountTransferAccount");

    std::queue<std::pair<vertex_t, uint32_t>> q;
    std::unordered_set<vertex_t> visited;
    q.push({*src, 0});
    visited.insert(*src);

    uint32_t best = std::numeric_limits<uint32_t>::max();
    while (!q.empty()) {
      const auto [curr, depth] = q.front();
      q.pop();
      if (curr == *dst) {
        best = depth;
        break;
      }
      const auto hops =
          TraverseFromSource(curr, transfer, start_time, end_time,
                             truncation_limit, order_desc, EdgeQueryTemporalProps());
      for (const auto& hop : hops) {
        if (visited.insert(hop.other_id).second) {
          q.push({hop.other_id, depth + 1});
        }
      }
    }

    result.rows = 1;
    result.checksum =
        (best == std::numeric_limits<uint32_t>::max()) ? 0ULL : best;
    return result;
  }

  QueryRunResult RunQuery4(const CsvRow& row) {
    QueryRunResult result;
    const auto a = LookupEntity(NodeKind::kAccount, row.GetByIndex(0));
    const auto b = LookupEntity(NodeKind::kAccount, row.GetByIndex(1));
    if (!a.has_value() || !b.has_value()) {
      return result;
    }
    const uint64_t start_time = ToUint64(row.GetByIndex(2));
    const uint64_t end_time = ToUint64(row.GetByIndex(3));
    const uint32_t truncation_limit =
        static_cast<uint32_t>(ToUint64(row.GetByIndex(4)));
    const bool order_desc = ParseOrderDescending(row.GetByIndex(5));

    if (!HasTemporalDirectTransfer(*a, *b, start_time, end_time,
                                   truncation_limit, order_desc)) {
      return result;
    }

    const auto& transfer = MustGetRelation("AccountTransferAccount");
    const auto out_of_b =
        TraverseFromSource(*b, transfer, start_time, end_time,
                           truncation_limit, order_desc, EdgeQueryTemporalProps());
    std::unordered_set<vertex_t> mids;
    for (const auto& hop : out_of_b) {
      const vertex_t mid = hop.other_id;
      if (mid == *a || mid == *b) {
        continue;
      }
      if (HasTemporalDirectTransfer(mid, *a, start_time, end_time,
                                    truncation_limit, order_desc) &&
          mids.insert(mid).second) {
        ++result.rows;
        result.checksum ^= HashMix(static_cast<uint64_t>(mid));
      }
    }
    return result;
  }

  QueryRunResult RunQuery5(const CsvRow& row) {
    QueryRunResult result;
    const auto person = LookupEntity(NodeKind::kPerson, row.GetByIndex(0));
    if (!person.has_value()) {
      return result;
    }
    const uint64_t start_time = ToUint64(row.GetByIndex(1));
    const uint64_t end_time = ToUint64(row.GetByIndex(2));
    const uint32_t truncation_limit =
        static_cast<uint32_t>(ToUint64(row.GetByIndex(3)));
    const bool order_desc = ParseOrderDescending(row.GetByIndex(4));

    const auto& own = MustGetRelation("PersonOwnAccount");
    const auto& transfer = MustGetRelation("AccountTransferAccount");
    const auto accounts =
        TraverseFromSource(*person, own, 0, 0, 0, false, EdgeQueryTemporalProps());

    struct State {
      vertex_t account_id = lsmgraph::INVALID_VERTEX_ID;
      uint64_t last_time = 0;
      uint32_t depth = 0;
    };

    std::queue<State> q;
    std::unordered_set<uint64_t> visited;
    for (const auto& hop : accounts) {
      q.push(State{hop.other_id, 0, 0});
      visited.insert((static_cast<uint64_t>(hop.other_id) << 8U) | 0U);
    }

    std::unordered_set<vertex_t> reached;
    while (!q.empty()) {
      const State state = q.front();
      q.pop();
      if (state.depth >= 3) {
        continue;
      }
      const auto hops =
          TraverseFromSource(state.account_id, transfer, start_time, end_time,
                             truncation_limit, order_desc, EdgeQueryTemporalProps());
      for (const auto& hop : hops) {
        const uint64_t edge_time = ToUint64(hop.properties.at("createTime"));
        if (edge_time < state.last_time) {
          continue;
        }
        reached.insert(hop.other_id);
        const uint64_t key =
            (static_cast<uint64_t>(hop.other_id) << 8U) | (state.depth + 1U);
        if (visited.insert(key).second) {
          q.push(State{hop.other_id, edge_time, state.depth + 1U});
        }
      }
    }

    result.rows = static_cast<uint64_t>(reached.size());
    for (vertex_t vid : reached) {
      result.checksum ^= HashMix(static_cast<uint64_t>(vid));
    }
    return result;
  }

  QueryRunResult RunQuery6(const CsvRow& row) {
    QueryRunResult result;
    const auto target = LookupEntity(NodeKind::kAccount, row.GetByIndex(0));
    if (!target.has_value()) {
      return result;
    }
    const double threshold1 = ToDouble(row.GetByIndex(1));
    const double threshold2 = ToDouble(row.GetByIndex(2));
    const uint64_t start_time = ToUint64(row.GetByIndex(3));
    const uint64_t end_time = ToUint64(row.GetByIndex(4));
    const uint32_t truncation_limit =
        static_cast<uint32_t>(ToUint64(row.GetByIndex(5)));
    const bool order_desc = ParseOrderDescending(row.GetByIndex(6));

    const auto& withdraw = MustGetRelation("AccountWithdrawAccount");
    const auto& transfer = MustGetRelation("AccountTransferAccount");
    const auto incoming_withdraws =
        TraverseToTarget(*target, withdraw, start_time, end_time,
                         truncation_limit, order_desc);
    std::unordered_set<vertex_t> qualified;
    for (const auto& hop : incoming_withdraws) {
      const double withdraw_amount = ToDouble(hop.properties.at("amount"));
      if (withdraw_amount < threshold2) {
        continue;
      }
      const vertex_t middle = hop.other_id;
      const auto many_to_one =
          TraverseToTarget(middle, transfer, start_time, end_time,
                           truncation_limit, order_desc);
      std::unordered_set<vertex_t> unique_sources;
      double gathered = 0.0;
      for (const auto& gather_hop : many_to_one) {
        unique_sources.insert(gather_hop.other_id);
        gathered += ToDouble(gather_hop.properties.at("amount"));
      }
      if (unique_sources.size() >= 2U && gathered >= threshold1 &&
          qualified.insert(middle).second) {
        ++result.rows;
        result.checksum ^= HashMix(static_cast<uint64_t>(middle));
      }
    }
    return result;
  }

  QueryRunResult RunQuery7(const CsvRow& row) {
    QueryRunResult result;
    const auto account = LookupEntity(NodeKind::kAccount, row.GetByIndex(0));
    if (!account.has_value()) {
      return result;
    }
    const double threshold = ToDouble(row.GetByIndex(1));
    const uint64_t start_time = ToUint64(row.GetByIndex(2));
    const uint64_t end_time = ToUint64(row.GetByIndex(3));
    const uint32_t truncation_limit =
        static_cast<uint32_t>(ToUint64(row.GetByIndex(4)));
    const bool order_desc = ParseOrderDescending(row.GetByIndex(5));

    const auto& transfer = MustGetRelation("AccountTransferAccount");
    const auto outgoing =
        TraverseFromSource(*account, transfer, start_time, end_time,
                           truncation_limit, order_desc);
    const auto incoming =
        TraverseToTarget(*account, transfer, start_time, end_time,
                         truncation_limit, order_desc);

    double sum_out = 0.0;
    double sum_in = 0.0;
    std::unordered_set<vertex_t> out_neighbors;
    std::unordered_set<vertex_t> in_neighbors;

    for (const auto& hop : outgoing) {
      const double amount = ToDouble(hop.properties.at("amount"));
      if (amount >= threshold) {
        sum_out += amount;
        out_neighbors.insert(hop.other_id);
      }
    }
    for (const auto& hop : incoming) {
      const double amount = ToDouble(hop.properties.at("amount"));
      if (amount >= threshold) {
        sum_in += amount;
        in_neighbors.insert(hop.other_id);
      }
    }

    const double ratio = (sum_out == 0.0) ? 0.0 : (sum_in / sum_out);
    result.rows = 1;
    result.checksum ^=
        static_cast<uint64_t>(std::llround(sum_in * 1000.0));
    result.checksum ^=
        static_cast<uint64_t>(std::llround(sum_out * 1000.0));
    result.checksum ^= static_cast<uint64_t>(in_neighbors.size() << 16U);
    result.checksum ^= static_cast<uint64_t>(out_neighbors.size() << 24U);
    result.checksum ^=
        static_cast<uint64_t>(std::llround(ratio * 1000000.0));
    return result;
  }

  QueryRunResult RunQuery8(const CsvRow& row) {
    QueryRunResult result;
    const auto loan = LookupEntity(NodeKind::kLoan, row.GetByIndex(0));
    if (!loan.has_value()) {
      return result;
    }
    const double threshold = ToDouble(row.GetByIndex(1));
    const uint64_t start_time = ToUint64(row.GetByIndex(2));
    const uint64_t end_time = ToUint64(row.GetByIndex(3));
    const uint32_t truncation_limit =
        static_cast<uint32_t>(ToUint64(row.GetByIndex(4)));
    const bool order_desc = ParseOrderDescending(row.GetByIndex(5));

    const auto& deposit = MustGetRelation("LoanDepositAccount");
    const auto& transfer = MustGetRelation("AccountTransferAccount");
    const auto& withdraw = MustGetRelation("AccountWithdrawAccount");

    struct State {
      vertex_t account_id = lsmgraph::INVALID_VERTEX_ID;
      uint32_t depth = 0;
    };

    std::queue<State> q;
    std::unordered_set<vertex_t> visited;
    const auto seeds =
        TraverseFromSource(*loan, deposit, start_time, end_time,
                           truncation_limit, order_desc, EdgeQueryTemporalProps());
    for (const auto& seed : seeds) {
      q.push(State{seed.other_id, 0});
      visited.insert(seed.other_id);
    }

    std::unordered_set<vertex_t> reached;
    while (!q.empty()) {
      const State state = q.front();
      q.pop();
      if (state.depth >= 3) {
        continue;
      }
      const auto out_transfer =
          TraverseFromSource(state.account_id, transfer, start_time, end_time,
                             truncation_limit, order_desc);
      const auto out_withdraw =
          TraverseFromSource(state.account_id, withdraw, start_time, end_time,
                             truncation_limit, order_desc);

      auto consume = [&](const std::vector<TraversedEdge>& hops) {
        for (const auto& hop : hops) {
          if (ToDouble(hop.properties.at("amount")) < threshold) {
            continue;
          }
          if (reached.insert(hop.other_id).second) {
            result.checksum ^= HashMix(static_cast<uint64_t>(hop.other_id));
          }
          if (visited.insert(hop.other_id).second) {
            q.push(State{hop.other_id, state.depth + 1});
          }
        }
      };
      consume(out_transfer);
      consume(out_withdraw);
    }

    result.rows = static_cast<uint64_t>(reached.size());
    return result;
  }

  QueryRunResult RunQuery9(const CsvRow& row) {
    QueryRunResult result;
    const auto account = LookupEntity(NodeKind::kAccount, row.GetByIndex(0));
    if (!account.has_value()) {
      return result;
    }
    const double threshold = ToDouble(row.GetByIndex(1));
    const uint64_t start_time = ToUint64(row.GetByIndex(2));
    const uint64_t end_time = ToUint64(row.GetByIndex(3));
    const uint32_t truncation_limit =
        static_cast<uint32_t>(ToUint64(row.GetByIndex(4)));
    const bool order_desc = ParseOrderDescending(row.GetByIndex(5));

    const auto& deposit = MustGetRelation("LoanDepositAccount");
    const auto& transfer = MustGetRelation("AccountTransferAccount");
    const auto& withdraw = MustGetRelation("AccountWithdrawAccount");
    const auto& repay = MustGetRelation("AccountRepayLoan");

    double loan_in = 0.0;
    double transfer_out = 0.0;
    double withdraw_out = 0.0;
    double repay_out = 0.0;

    for (const auto& hop :
         TraverseToTarget(*account, deposit, start_time, end_time,
                          truncation_limit, order_desc)) {
      loan_in += ToDouble(hop.properties.at("amount"));
    }
    for (const auto& hop :
         TraverseFromSource(*account, transfer, start_time, end_time,
                            truncation_limit, order_desc)) {
      const double amount = ToDouble(hop.properties.at("amount"));
      if (amount >= threshold) {
        transfer_out += amount;
      }
    }
    for (const auto& hop :
         TraverseFromSource(*account, withdraw, start_time, end_time,
                            truncation_limit, order_desc)) {
      const double amount = ToDouble(hop.properties.at("amount"));
      if (amount >= threshold) {
        withdraw_out += amount;
      }
    }
    for (const auto& hop :
         TraverseFromSource(*account, repay, start_time, end_time,
                            truncation_limit, order_desc)) {
      repay_out += ToDouble(hop.properties.at("amount"));
    }

    const double total_out = transfer_out + withdraw_out + repay_out;
    const double out_ratio = (loan_in == 0.0) ? 0.0 : (total_out / loan_in);
    result.rows = 1;
    result.checksum ^=
        static_cast<uint64_t>(std::llround(loan_in * 1000.0));
    result.checksum ^=
        static_cast<uint64_t>(std::llround(total_out * 1000.0));
    result.checksum ^=
        static_cast<uint64_t>(std::llround(out_ratio * 1000000.0));
    return result;
  }

  QueryRunResult RunQuery10(const CsvRow& row) {
    QueryRunResult result;
    const auto p1 = LookupEntity(NodeKind::kPerson, row.GetByIndex(0));
    const auto p2 = LookupEntity(NodeKind::kPerson, row.GetByIndex(1));
    if (!p1.has_value() || !p2.has_value()) {
      return result;
    }
    const auto& invest = MustGetRelation("PersonInvestCompany");

    std::unordered_set<vertex_t> c1;
    std::unordered_set<vertex_t> c2;
    for (const auto& hop : TraverseFromSource(*p1, invest, 0, 0, 0, false, EdgeQueryTemporalProps())) {
      c1.insert(hop.other_id);
    }
    for (const auto& hop : TraverseFromSource(*p2, invest, 0, 0, 0, false, EdgeQueryTemporalProps())) {
      c2.insert(hop.other_id);
    }

    uint64_t inter = 0;
    for (vertex_t v : c1) {
      if (c2.find(v) != c2.end()) {
        ++inter;
      }
    }
    const uint64_t uni = c1.size() + c2.size() - inter;
    const double jaccard = (uni == 0) ? 0.0 : (static_cast<double>(inter) / uni);

    result.rows = 1;
    result.checksum = static_cast<uint64_t>(std::llround(jaccard * 1000000.0));
    return result;
  }

  QueryRunResult RunQuery11(const CsvRow& row) {
    QueryRunResult result;
    const auto person = LookupEntity(NodeKind::kPerson, row.GetByIndex(0));
    if (!person.has_value()) {
      return result;
    }
    const auto& guarantee = MustGetRelation("PersonGuaranteePerson");
    const auto& apply = MustGetRelation("PersonApplyLoan");

    std::queue<std::pair<vertex_t, uint32_t>> q;
    std::unordered_set<vertex_t> visited;
    q.push({*person, 0});
    visited.insert(*person);

    std::unordered_set<vertex_t> loans;
    while (!q.empty()) {
      const auto [curr, depth] = q.front();
      q.pop();
      for (const auto& loan_hop : TraverseFromSource(curr, apply, 0, 0, 0, false, EdgeQueryTemporalProps())) {
        if (loans.insert(loan_hop.other_id).second) {
          ++result.rows;
          result.checksum ^= HashMix(static_cast<uint64_t>(loan_hop.other_id));
        }
      }
      if (depth >= 3) {
        continue;
      }
      for (const auto& next_hop : TraverseFromSource(curr, guarantee, 0, 0, 0, false, EdgeQueryTemporalProps())) {
        if (visited.insert(next_hop.other_id).second) {
          q.push({next_hop.other_id, depth + 1});
        }
      }
    }
    return result;
  }

  QueryRunResult RunQuery12(const CsvRow& row) {
    QueryRunResult result;
    const auto person = LookupEntity(NodeKind::kPerson, row.GetByIndex(0));
    if (!person.has_value()) {
      return result;
    }
    const uint64_t start_time = ToUint64(row.GetByIndex(1));
    const uint64_t end_time = ToUint64(row.GetByIndex(2));
    const uint32_t truncation_limit =
        static_cast<uint32_t>(ToUint64(row.GetByIndex(3)));
    const bool order_desc = ParseOrderDescending(row.GetByIndex(4));

    const auto& own = MustGetRelation("PersonOwnAccount");
    const auto& transfer = MustGetRelation("AccountTransferAccount");

    std::unordered_map<vertex_t, double> sum_by_company_account;
    for (const auto& own_hop : TraverseFromSource(*person, own, 0, 0, 0, false, EdgeQueryTemporalProps())) {
      const auto out_transfers =
          TraverseFromSource(own_hop.other_id, transfer, start_time, end_time,
                             truncation_limit, order_desc);
      for (const auto& trans_hop : out_transfers) {
        if (!IsCompanyOwnedAccount(trans_hop.other_id)) {
          continue;
        }
        sum_by_company_account[trans_hop.other_id] +=
            ToDouble(trans_hop.properties.at("amount"));
      }
    }

    result.rows = static_cast<uint64_t>(sum_by_company_account.size());
    for (const auto& kv : sum_by_company_account) {
      result.checksum ^= HashMix(static_cast<uint64_t>(kv.first));
      result.checksum ^=
          static_cast<uint64_t>(std::llround(kv.second * 1000.0));
    }
    return result;
  }

  std::string ReadColdBlobPayloadNoCache(const std::string& ref) const {
    if (ref.empty()) {
      return {};
    }
    const size_t p1 = ref.find(':');
    const size_t p2 = p1 == std::string::npos ? std::string::npos
                                               : ref.find(':', p1 + 1);
    if (p1 == std::string::npos || p2 == std::string::npos) {
      return {};
    }
    const uint64_t file_id = ToUint64(ref.substr(0, p1));
    const uint64_t offset = ToUint64(ref.substr(p1 + 1, p2 - p1 - 1));
    const uint64_t length = ToUint64(ref.substr(p2 + 1));
    if (length == 0) {
      return {};
    }

    thread_local std::unordered_map<uint64_t, std::unique_ptr<std::ifstream>>
        cold_blob_files;
    auto& in = cold_blob_files[file_id];
    if (!in) {
      const std::string path =
          ColdBlobPathPrefix() + "_" + std::to_string(file_id) + ".blob";
      in = std::make_unique<std::ifstream>(path, std::ios::binary);
      if (!in->is_open()) {
        in.reset();
        return {};
      }
    }

    std::string payload(static_cast<size_t>(length), '\0');
    in->clear();
    in->seekg(static_cast<std::streamoff>(offset));
    in->read(payload.data(), static_cast<std::streamsize>(payload.size()));
    if (!(*in)) {
      return {};
    }
    return payload;
  }

  std::string ReadNodeColdBlobPayloadNoCache(const std::string& ref) const {
    if (ref.empty()) {
      return {};
    }
    const size_t p1 = ref.find(':');
    const size_t p2 = p1 == std::string::npos ? std::string::npos
                                               : ref.find(':', p1 + 1);
    if (p1 == std::string::npos || p2 == std::string::npos) {
      return {};
    }
    const uint64_t file_id = ToUint64(ref.substr(0, p1));
    const uint64_t offset = ToUint64(ref.substr(p1 + 1, p2 - p1 - 1));
    const uint64_t length = ToUint64(ref.substr(p2 + 1));
    if (length == 0) {
      return {};
    }

    thread_local std::unordered_map<uint64_t, std::unique_ptr<std::ifstream>>
        node_cold_blob_files;
    auto& in = node_cold_blob_files[file_id];
    if (!in) {
      const std::string path =
          NodeColdBlobPathPrefix() + "_" + std::to_string(file_id) + ".blob";
      in = std::make_unique<std::ifstream>(path, std::ios::binary);
      if (!in->is_open()) {
        in.reset();
        return {};
      }
    }

    std::string payload(static_cast<size_t>(length), '\0');
    in->clear();
    in->seekg(static_cast<std::streamoff>(offset));
    in->read(payload.data(), static_cast<std::streamsize>(payload.size()));
    if (!(*in)) {
      return {};
    }
    return payload;
  }

  bool ReadSingleEdgeProperty(const SingleEdgeReadCandidate& request,
                              std::string* value) const {
    if (value == nullptr || db_ == nullptr) {
      return false;
    }
    const auto& property = single_edge_sampler_.Property(request.property_id);
    if (!property.cold) {
      std::unordered_map<std::string, std::string> props;
      const auto rs = db_->GetEdge(request.src,
                                   request.dst,
                                   {property.name},
                                   &props,
                                   request.is_out,
                                   request.edge_type);
      if (rs != lsmgraph::Status::kOk) {
        return false;
      }
      const auto it = props.find(property.name);
      if (it == props.end()) {
        return false;
      }
      *value = it->second;
      return true;
    }

    std::unordered_map<std::string, std::string> ref_props;
    const auto rs = db_->GetEdge(request.src,
                                 request.dst,
                                 {"cold_property"},
                                 &ref_props,
                                 request.is_out,
                                 request.edge_type);
    if (rs != lsmgraph::Status::kOk) {
      return false;
    }
    const auto ref_it = ref_props.find("cold_property");
    if (ref_it == ref_props.end() || ref_it->second.empty()) {
      return false;
    }
    const std::string payload = ReadColdBlobPayloadNoCache(ref_it->second);
    if (payload.empty()) {
      return false;
    }
    return GetPipeFieldBySlot(payload, request.cold_slot, value) &&
           !value->empty();
  }

  void RunSingleEdgeReadBenchmark() {
    if (!SingleEdgeReadEnabled()) {
      return;
    }
    single_edge_sampler_.Finalize();
    const auto prepare_t1 = std::chrono::steady_clock::now();
    std::vector<SingleEdgeReadCandidate> requests =
        single_edge_sampler_.BuildRequests(FLAGS_finbench_single_edge_read_ops,
                                           FLAGS_finbench_single_edge_hot_weight,
                                           FLAGS_finbench_single_edge_cold_weight,
                                           FLAGS_finbench_single_edge_seed);
    const auto prepare_t2 = std::chrono::steady_clock::now();
    const double prepare_sec =
        std::chrono::duration_cast<std::chrono::duration<double>>(prepare_t2 -
                                                                  prepare_t1)
            .count();

    std::cout << "[SINGLE_EDGE_READ_PREPARE] requested_ops: "
              << FLAGS_finbench_single_edge_read_ops << std::endl;
    std::cout << "[SINGLE_EDGE_READ_PREPARE] generated_ops: "
              << requests.size() << std::endl;
    std::cout << "[SINGLE_EDGE_READ_PREPARE] retained_candidates: "
              << single_edge_sampler_.retained_candidates() << std::endl;
    std::cout << "[SINGLE_EDGE_READ_PREPARE] retained_hot_candidates: "
              << single_edge_sampler_.retained_hot_candidates() << std::endl;
    std::cout << "[SINGLE_EDGE_READ_PREPARE] retained_cold_candidates: "
              << single_edge_sampler_.retained_cold_candidates() << std::endl;
    std::cout << "[SINGLE_EDGE_READ_PREPARE] seen_hot_candidates: "
              << single_edge_sampler_.seen_hot_candidates() << std::endl;
    std::cout << "[SINGLE_EDGE_READ_PREPARE] seen_cold_candidates: "
              << single_edge_sampler_.seen_cold_candidates() << std::endl;
    std::cout << "[SINGLE_EDGE_READ_PREPARE] active_hot_properties: "
              << single_edge_sampler_.active_hot_property_count() << std::endl;
    std::cout << "[SINGLE_EDGE_READ_PREPARE] active_cold_properties: "
              << single_edge_sampler_.active_cold_property_count() << std::endl;
    std::cout << "[SINGLE_EDGE_READ_PREPARE] time(s): " << prepare_sec
              << std::endl;
    if (requests.empty()) {
      std::cout << "[SINGLE_EDGE_READ] ops: 0" << std::endl;
      return;
    }

    std::vector<uint64_t> checksums(requests.size(), 0);
    std::vector<uint8_t> found(requests.size(), 0);
    const auto t1 = std::chrono::steady_clock::now();
    ParallelForIndexDynamic(requests.size(), GetReadThreadCount(), [&](size_t i) {
      std::string value;
      if (ReadSingleEdgeProperty(requests[i], &value)) {
        found[i] = 1;
        const auto& property =
            single_edge_sampler_.Property(requests[i].property_id);
        checksums[i] =
            HashMix(static_cast<uint64_t>(requests[i].src)) ^
            HashMix(static_cast<uint64_t>(requests[i].dst)) ^
            HashMix(static_cast<uint64_t>(requests[i].edge_type)) ^
            HashMix(std::hash<std::string>{}(property.name)) ^
            HashMix(std::hash<std::string>{}(value));
      }
    });
    const auto t2 = std::chrono::steady_clock::now();

    SingleEdgeReadMetrics metrics;
    metrics.ops = requests.size();
    metrics.sec =
        std::chrono::duration_cast<std::chrono::duration<double>>(t2 - t1)
            .count();
    for (size_t i = 0; i < requests.size(); ++i) {
      const auto& property =
          single_edge_sampler_.Property(requests[i].property_id);
      if (property.cold) {
        ++metrics.cold_ops;
      } else {
        ++metrics.hot_ops;
      }
      if (found[i] != 0) {
        ++metrics.found;
      } else {
        ++metrics.missed;
      }
      metrics.checksum ^= HashMix(checksums[i] + i + 1);
    }
    const double qps = metrics.sec <= 0.0
                           ? 0.0
                           : static_cast<double>(metrics.ops) / metrics.sec;
    std::cout << "[SINGLE_EDGE_READ] time(s): " << metrics.sec << std::endl;
    std::cout << "[SINGLE_EDGE_READ] ops: " << metrics.ops << std::endl;
    std::cout << "[SINGLE_EDGE_READ] qps(op/s): " << qps << std::endl;
    std::cout << "[SINGLE_EDGE_READ] hot_ops: " << metrics.hot_ops
              << std::endl;
    std::cout << "[SINGLE_EDGE_READ] cold_ops: " << metrics.cold_ops
              << std::endl;
    std::cout << "[SINGLE_EDGE_READ] found: " << metrics.found << std::endl;
    std::cout << "[SINGLE_EDGE_READ] missed: " << metrics.missed << std::endl;
    std::cout << "[SINGLE_EDGE_READ] checksum: " << metrics.checksum
              << std::endl;
  }

  static uint64_t FinBenchQueryFrequency(int query_id) {
    static constexpr std::array<uint64_t, 13> kFreq = {
        0, 26, 37, 106, 36, 72, 316, 48, 9, 384, 37, 20, 44};
    if (query_id <= 0 || query_id >= static_cast<int>(kFreq.size())) {
      return 1;
    }
    return kFreq[static_cast<size_t>(query_id)];
  }

  const LoadedParamFile* FindLoadedParamFile(int query_id) const {
    for (const auto& item : loaded_param_files_) {
      if (item.query_id == query_id) {
        return &item;
      }
    }
    return nullptr;
  }

  size_t EffectiveParamRows(const LoadedParamFile& file) const {
    size_t row_count = file.data.rows.size();
    if (FLAGS_finbench_param_limit_per_query > 0 &&
        row_count > FLAGS_finbench_param_limit_per_query) {
      row_count = FLAGS_finbench_param_limit_per_query;
    }
    return row_count;
  }

	  void PrintMixedWorkloadStats(const MixedWorkloadStats& stats) const {
	    const uint64_t total_ops =
	        stats.node_writes + stats.edge_writes + stats.query_ops;
	    const double total_sec = MixedTotalElapsedSec(stats);
	    const double write_sec = stats.write_sec + stats.background_wait_sec;
	    const double total_qps = (total_sec <= 0.0 || total_ops == 0)
	                                 ? 0.0
	                                 : static_cast<double>(total_ops) / total_sec;
	    const uint64_t total_writes = stats.node_writes + stats.edge_writes;
	    const double write_qps =
	        (write_sec <= 0.0 || total_writes == 0)
	            ? 0.0
	            : static_cast<double>(total_writes) / write_sec;
    const double query_qps = (stats.query_sec <= 0.0 || stats.query_ops == 0)
                                 ? 0.0
                                 : static_cast<double>(stats.query_ops) /
                                       stats.query_sec;
	    std::cout << "[MIXED_WORKLOAD] time(s): " << total_sec << std::endl;
	    std::cout << "[MIXED_WORKLOAD] write_time(s): " << stats.write_sec
	              << std::endl;
    std::cout << "[MIXED_WORKLOAD] query_time(s): " << stats.query_sec
              << std::endl;
    std::cout << "[MIXED_WORKLOAD] node_writes: " << stats.node_writes
              << std::endl;
    std::cout << "[MIXED_WORKLOAD] edge_writes: " << stats.edge_writes
              << std::endl;
    std::cout << "[MIXED_WORKLOAD] query_ops: " << stats.query_ops
              << std::endl;
    std::cout << "[MIXED_WORKLOAD] qps(op/s): " << total_qps << std::endl;
    std::cout << "[MIXED_WORKLOAD] write_qps(op/s): " << write_qps
              << std::endl;
    std::cout << "[MIXED_WORKLOAD] query_qps(param/s): " << query_qps
              << std::endl;
    std::cout << "[MIXED_WORKLOAD] result_rows: " << stats.result_rows
              << std::endl;
    std::cout << "[MIXED_WORKLOAD] checksum: " << stats.checksum << std::endl;
    for (int qid = 1; qid <= 12; ++qid) {
      const auto& metric = stats.query_metrics[static_cast<size_t>(qid)];
      const double qps =
          (metric.sec <= 0.0 || metric.param_rows == 0)
              ? 0.0
              : static_cast<double>(metric.param_rows) / metric.sec;
      std::cout << "[MIXED_Q" << qid << "] time(s): " << metric.sec
                << std::endl;
      std::cout << "[MIXED_Q" << qid << "] params: " << metric.param_rows
                << std::endl;
      std::cout << "[MIXED_Q" << qid << "] qps(param/s): " << qps
                << std::endl;
      std::cout << "[MIXED_Q" << qid << "] result_rows: "
                << metric.result_rows << std::endl;
      std::cout << "[MIXED_Q" << qid << "] checksum: " << metric.checksum
                << std::endl;
    }
  }

  bool ForceHotEdgeCsrCompactionIfNeeded() {
    if (!FLAGS_finbench_enable_hot_edge_csr) {
      return true;
    }
    if (db_ == nullptr) {
      std::cerr << "GraphDb is null before hot edge CSR compaction"
                << std::endl;
      return false;
    }
    std::cout << "[HOT_EDGE_CSR_COMPACTION] enabled: true" << std::endl;
    return db_->ForceCompactCsrEdgeShards();
  }

  void AppendQueryTasksByProgress(std::vector<MixedQueryStream>* streams,
                                  uint64_t cumulative_updates,
                                  uint64_t total_updates,
                                  std::vector<MixedQueryTask>* tasks) const {
    if (streams == nullptr || tasks == nullptr || total_updates == 0) {
      return;
    }
    for (auto& stream : *streams) {
      const uint64_t target =
          std::min<uint64_t>(
              stream.row_indices.size(),
              (static_cast<unsigned __int128>(cumulative_updates) *
               stream.row_indices.size()) /
                  total_updates);
      while (stream.next_row < target) {
        const size_t row_index = stream.row_indices[stream.next_row];
        tasks->push_back(MixedQueryTask{
            stream.query_id,
            &stream.file->data.rows[row_index],
            static_cast<uint64_t>(row_index + 1)});
        ++stream.next_row;
      }
    }
  }

  void ExecuteMixedConcurrentBatch(PreparedImportBatch* batch,
                                   const std::vector<MixedQueryTask>& tasks,
                                   ImportStats* import_stats,
                                   MixedWorkloadStats* stats) {
    if (batch == nullptr || import_stats == nullptr || stats == nullptr) {
      return;
    }
    const bool has_writes =
        !batch->node_writes.empty() || !batch->relation_writes.empty();
    if (!has_writes && tasks.empty()) {
      ResetPreparedImportBatch(batch);
      return;
    }

    std::vector<QueryRunResult> query_results(tasks.size());
    std::vector<double> query_secs(tasks.size(), 0.0);
    std::atomic<size_t> next_node{0};
    std::atomic<size_t> next_edge{0};
    std::atomic<size_t> next_query{0};
    std::atomic<size_t> remaining_nodes{batch->node_writes.size()};
    std::atomic<bool> nodes_done{batch->node_writes.empty()};

    const auto exec_t1 = std::chrono::steady_clock::now();
#pragma omp parallel num_threads(static_cast<int>(GetMixedThreadCount()))
    {
      const int tid =
#ifdef _OPENMP
          omp_get_thread_num();
#else
          0;
#endif
      uint64_t spin = 0;
      while (true) {
        bool did_work = false;
        const bool prefer_query = ((static_cast<uint64_t>(tid) + spin) & 1ULL) == 0;

        auto try_query = [&]() -> bool {
          const size_t idx = next_query.fetch_add(1);
          if (idx >= tasks.size()) {
            return false;
          }
          const auto q1 = std::chrono::steady_clock::now();
          query_results[idx] = RunQuery(tasks[idx].query_id, *tasks[idx].row);
          const auto q2 = std::chrono::steady_clock::now();
          query_secs[idx] =
              std::chrono::duration_cast<std::chrono::duration<double>>(q2 - q1)
                  .count();
          return true;
        };

        auto try_node = [&]() -> bool {
          if (nodes_done.load(std::memory_order_acquire)) {
            return false;
          }
          const size_t idx = next_node.fetch_add(1);
          if (idx >= batch->node_writes.size()) {
            return false;
          }
          ExecutePreparedNodeWriteOne(batch->node_writes[idx]);
          if (remaining_nodes.fetch_sub(1) == 1) {
            nodes_done.store(true, std::memory_order_release);
          }
          return true;
        };

        auto try_edge = [&]() -> bool {
          if (!nodes_done.load(std::memory_order_acquire)) {
            return false;
          }
          const size_t idx = next_edge.fetch_add(1);
          if (idx >= batch->relation_writes.size()) {
            return false;
          }
          ExecutePreparedRelationWriteOne(batch->relation_writes[idx]);
          return true;
        };

        if (prefer_query) {
          did_work = try_query() || try_node() || try_edge();
        } else {
          did_work = try_node() || try_edge() || try_query();
        }

        if (!did_work) {
          if (next_query.load() >= tasks.size() &&
              next_node.load() >= batch->node_writes.size() &&
              nodes_done.load(std::memory_order_acquire) &&
              next_edge.load() >= batch->relation_writes.size()) {
            break;
          }
        }
        ++spin;
      }
    }
    const auto exec_t2 = std::chrono::steady_clock::now();
    const double exec_sec =
        std::chrono::duration_cast<std::chrono::duration<double>>(exec_t2 -
                                                                  exec_t1)
            .count();

	    double foreground_batch_sec = exec_sec;
	    if (has_writes) {
	      const double write_sec_before = import_stats->sec;
	      import_stats->sec += exec_sec;
	      AddColdBlobFlushTime(import_stats);
	      foreground_batch_sec = import_stats->sec - write_sec_before;
		    }
    AccumulatePreparedStats(*batch, import_stats);

    if (!tasks.empty()) {
      stats->query_sec += exec_sec;
    }
    for (size_t i = 0; i < tasks.size(); ++i) {
      const auto& task = tasks[i];
      const auto& run = query_results[i];
      QueryMetrics& metric =
          stats->query_metrics[static_cast<size_t>(task.query_id)];
      ++metric.param_rows;
      metric.result_rows += run.rows;
      metric.checksum ^= run.checksum;
      metric.sec += query_secs[i];
      ++stats->query_ops;
      stats->result_rows += run.rows;
      stats->checksum ^= run.checksum;
    }
	    stats->total_sec += foreground_batch_sec;
    ResetPreparedImportBatch(batch);
  }

  ImportStats ImportRemainingIncremental() {
    struct UpdateCursor {
      std::string file_name;
      uint64_t op_num = 0;
      std::ifstream in;
      std::vector<char> buffer;
      std::shared_ptr<CsvHeader> header;
      CsvRow row;
      uint64_t time = 0;
      uint64_t rows = 0;
      bool has = false;
      const FinBenchGraphDbTest* owner = nullptr;

      bool Open(const std::string& path) {
        if (!OpenCsvInput(path, &in, &buffer)) {
          return false;
        }
        header = std::make_shared<CsvHeader>();
        return ParseCsvHeader(&in, header.get());
      }

      bool Advance() {
        std::string line;
        while (std::getline(in, line)) {
          if (line.empty()) {
            continue;
          }
          const uint64_t row_index = rows++;
          if (owner != nullptr && !owner->IsRemainingWorkloadRow(row_index)) {
            continue;
          }
          row.header = header.get();
          row.fields = SplitPipe(line);
          if (row.fields.size() < header->columns.size()) {
            row.fields.resize(header->columns.size());
          }
          time = ParseTimeToMillis(row.Get("createTime"));
          has = true;
          return true;
        }
        has = false;
        return false;
      }
    };

    ImportStats stats;
    if (WorkloadSampleMod() <= 1) {
      return stats;
    }

    std::vector<UpdateCursor> updates;
    for (const auto& path : SortedIncrementalFiles()) {
      const std::string file_name = path.filename().string();
      const uint64_t op_num = ExtractTrailingNumber(file_name);
      if (op_num == 17 || op_num == 18 || op_num == 19) {
        continue;
      }
      UpdateCursor cursor;
      cursor.file_name = file_name;
      cursor.op_num = op_num;
      cursor.owner = this;
      if (!cursor.Open(path.string())) {
        std::cerr << "failed to open incremental csv for remaining import: "
                  << path.string() << std::endl;
        std::exit(1);
      }
      cursor.Advance();
      if (cursor.has) {
        updates.push_back(std::move(cursor));
      }
    }

    auto next_update_index = [&]() -> int {
      int best = -1;
      uint64_t best_time = std::numeric_limits<uint64_t>::max();
      for (size_t i = 0; i < updates.size(); ++i) {
        if (updates[i].has && updates[i].time < best_time) {
          best_time = updates[i].time;
          best = static_cast<int>(i);
        }
      }
      return best;
    };

    auto batch = NewPreparedBatch("remaining_incremental_batch",
                                  ImportBatchRows(),
                                  ImportBatchRows());
    while (next_update_index() >= 0) {
      const int update_idx = next_update_index();
      if (update_idx < 0) {
        break;
      }
      UpdateCursor& update = updates[static_cast<size_t>(update_idx)];
      ++batch->logical_rows;
      AppendIncrementalRowToBatch(update.file_name,
                                  update.op_num,
                                  update.row,
                                  batch.get());
      update.Advance();
      if (batch->logical_rows >= ImportBatchRows() || LoadArenaNearFull()) {
        FlushPreparedImportBatch(batch.get(), &stats);
        ReleasePreparedBatch(&batch, "remaining_incremental");
        batch = NewPreparedBatch("remaining_incremental_batch",
                                 ImportBatchRows(),
                                 ImportBatchRows());
      }
    }
    FlushPreparedImportBatch(batch.get(), &stats);
    ReleasePreparedBatch(&batch, "remaining_incremental");
    AddColdBlobFlushTime(&stats);
    stats.entity_nodes = incremental_entity_total_;
    return stats;
  }

  MixedWorkloadStats RunMixedWorkload() {
    struct UpdateCursor {
      std::string file_name;
      uint64_t op_num = 0;
      std::ifstream in;
      std::vector<char> buffer;
      std::shared_ptr<CsvHeader> header;
      CsvRow row;
      uint64_t time = 0;
      uint64_t rows = 0;
      bool has = false;
      const FinBenchGraphDbTest* owner = nullptr;
      bool sampled = true;

      bool Open(const std::string& path) {
        if (!OpenCsvInput(path, &in, &buffer)) {
          return false;
        }
        header = std::make_shared<CsvHeader>();
        return ParseCsvHeader(&in, header.get());
      }

      bool Advance() {
        std::string line;
        while (std::getline(in, line)) {
          if (line.empty()) {
            continue;
          }
          const uint64_t row_index = rows++;
          if (owner != nullptr && !owner->SelectWorkloadRow(row_index, sampled)) {
            continue;
          }
          row.header = header.get();
          row.fields = SplitPipe(line);
          if (row.fields.size() < header->columns.size()) {
            row.fields.resize(header->columns.size());
          }
          time = ParseTimeToMillis(row.Get("createTime"));
          has = true;
          return true;
        }
        has = false;
        return false;
      }
    };

    MixedWorkloadStats stats;
    for (int qid = 1; qid <= 12; ++qid) {
      stats.query_metrics[static_cast<size_t>(qid)].query_id = qid;
    }

    std::vector<UpdateCursor> updates;
    for (const auto& path : SortedIncrementalFiles()) {
      const std::string file_name = path.filename().string();
      const uint64_t op_num = ExtractTrailingNumber(file_name);
      if (op_num == 17 || op_num == 18 || op_num == 19) {
        continue;
      }
      UpdateCursor cursor;
      cursor.file_name = file_name;
      cursor.op_num = op_num;
      cursor.owner = this;
      cursor.sampled = true;
      if (!cursor.Open(path.string())) {
        std::cerr << "failed to open incremental csv for mixed workload: "
                  << path.string() << std::endl;
        std::exit(1);
      }
      cursor.Advance();
      if (cursor.has) {
        updates.push_back(std::move(cursor));
      }
    }

    auto next_update_index = [&]() -> int {
      int best = -1;
      uint64_t best_time = std::numeric_limits<uint64_t>::max();
      for (size_t i = 0; i < updates.size(); ++i) {
        if (updates[i].has && updates[i].time < best_time) {
          best_time = updates[i].time;
          best = static_cast<int>(i);
        }
      }
      return best;
    };

    const int first_update_idx = next_update_index();
    if (first_update_idx < 0) {
      PrintMixedWorkloadStats(stats);
      return stats;
    }

    const uint64_t total_update_rows = CountMixedUpdateRows();
    std::cout << "[MIXED_WORKLOAD] total_update_rows_for_progress: "
              << total_update_rows << std::endl;

	    std::vector<MixedQueryStream> streams;
	    if (FLAGS_finbench_mix_enable_queries) {
	      streams.reserve(12);
	      for (int qid = 1; qid <= 12; ++qid) {
	        const LoadedParamFile* file = FindLoadedParamFile(qid);
	        if (file == nullptr) {
	          continue;
	        }
	        auto indices = SelectedParamRowIndices(file->data.rows.size());
	        if (indices.empty()) {
	          continue;
	        }
	        streams.push_back(MixedQueryStream{qid, file, std::move(indices), 0});
	      }
	    }

    auto batch = NewPreparedBatch("mixed_incremental_batch",
                                  ImportBatchRows(),
                                  ImportBatchRows());
    ImportStats import_stats;
    uint64_t cumulative_updates = 0;

    auto execute_batch = [&]() {
	      std::vector<MixedQueryTask> tasks;
	      if (FLAGS_finbench_mix_enable_queries) {
	        AppendQueryTasksByProgress(&streams,
	                                   cumulative_updates,
	                                   std::max<uint64_t>(1, total_update_rows),
	                                   &tasks);
	      }
      ExecuteMixedConcurrentBatch(batch.get(), tasks, &import_stats, &stats);
      ReleasePreparedBatch(&batch, "mixed_incremental");
      batch = NewPreparedBatch("mixed_incremental_batch",
                               ImportBatchRows(),
                               ImportBatchRows());
    };

    while (next_update_index() >= 0) {
      const int update_idx = next_update_index();
      if (update_idx < 0) {
        break;
      }

      UpdateCursor& update = updates[static_cast<size_t>(update_idx)];
      ++batch->logical_rows;
      AppendIncrementalRowToBatch(update.file_name,
                                  update.op_num,
                                  update.row,
                                  batch.get());
      ++cumulative_updates;
      update.Advance();
      if (batch->logical_rows >= ImportBatchRows() || LoadArenaNearFull()) {
        execute_batch();
      }
    }
    cumulative_updates = std::max(cumulative_updates, total_update_rows);
    execute_batch();
    stats.node_writes = import_stats.node_writes;
    stats.edge_writes = import_stats.edge_writes;
    stats.write_sec = import_stats.sec;
    PrintMixedWorkloadStats(stats);
    return stats;
  }

  void RunAllQueries() {
    std::vector<QueryMetrics> metrics;
    metrics.reserve(12);
    const auto total_t1 = std::chrono::steady_clock::now();
    uint64_t total_param_rows = 0;
    uint64_t total_result_rows = 0;
    uint64_t total_checksum = 0;

    for (int qid = 1; qid <= 12; ++qid) {
      const QueryMetrics metric = RunOneQueryType(qid);
      total_param_rows += metric.param_rows;
      total_result_rows += metric.result_rows;
      total_checksum ^= metric.checksum;
      metrics.push_back(metric);
    }

    const auto total_t2 = std::chrono::steady_clock::now();
    const double total_sec =
        std::chrono::duration_cast<std::chrono::duration<double>>(total_t2 - total_t1).count();
    const double total_qps =
        (total_sec <= 0.0 || total_param_rows == 0)
            ? 0.0
            : static_cast<double>(total_param_rows) / total_sec;

    std::cout << "[QUERY_TOTAL] time(s): " << total_sec << std::endl;
    std::cout << "[QUERY_TOTAL] total_params: " << total_param_rows << std::endl;
    std::cout << "[QUERY_TOTAL] qps(param/s): " << total_qps << std::endl;
    std::cout << "[QUERY_TOTAL] total_result_rows: " << total_result_rows
              << std::endl;
    std::cout << "[QUERY_TOTAL] checksum: " << total_checksum << std::endl;

    for (const auto& metric : metrics) {
      const double qps =
          (metric.sec <= 0.0 || metric.param_rows == 0)
              ? 0.0
              : static_cast<double>(metric.param_rows) / metric.sec;
      std::cout << "[QUERY_" << metric.query_id << "] time(s): "
                << metric.sec << std::endl;
      std::cout << "[QUERY_" << metric.query_id << "] params: "
                << metric.param_rows << std::endl;
      std::cout << "[QUERY_" << metric.query_id << "] qps(param/s): "
                << qps << std::endl;
      std::cout << "[QUERY_" << metric.query_id << "] result_rows: "
                << metric.result_rows << std::endl;
      std::cout << "[QUERY_" << metric.query_id << "] checksum: "
                << metric.checksum << std::endl;
    }
  }

  QueryMetrics RunOneQueryType(int query_id) {
    QueryMetrics metrics;
    metrics.query_id = query_id;

    const LoadedParamFile* file = nullptr;
    for (const auto& item : loaded_param_files_) {
      if (item.query_id == query_id) {
        file = &item;
        break;
      }
    }
    if (file == nullptr) {
      std::cerr << "missing loaded params for query " << query_id << std::endl;
      return metrics;
    }

    const std::vector<size_t> row_indices =
        SelectedParamRowIndices(file->data.rows.size());

    const auto t1 = std::chrono::steady_clock::now();
    std::vector<QueryRunResult> results(row_indices.size());
    ParallelForIndexDynamic(row_indices.size(), GetReadThreadCount(), [&](size_t i) {
      results[i] = RunQuery(query_id, file->data.rows[row_indices[i]]);
    });

    uint64_t result_rows = 0;
    uint64_t checksum = 0;
    for (const auto& run : results) {
      result_rows += run.rows;
      checksum ^= run.checksum;
    }

    const auto t2 = std::chrono::steady_clock::now();
    metrics.param_rows = static_cast<uint64_t>(row_indices.size());
    metrics.result_rows = result_rows;
    metrics.checksum = checksum;
    metrics.sec =
        std::chrono::duration_cast<std::chrono::duration<double>>(t2 - t1).count();
    return metrics;
  }

  QueryRunResult RunQuery(int query_id, const CsvRow& row) {
    switch (query_id) {
      case 1:
        return RunQuery1(row);
      case 2:
        return RunQuery2(row);
      case 3:
        return RunQuery3(row);
      case 4:
        return RunQuery4(row);
      case 5:
        return RunQuery5(row);
      case 6:
        return RunQuery6(row);
      case 7:
        return RunQuery7(row);
      case 8:
        return RunQuery8(row);
      case 9:
        return RunQuery9(row);
      case 10:
        return RunQuery10(row);
      case 11:
        return RunQuery11(row);
      case 12:
        return RunQuery12(row);
      default:
        return QueryRunResult{};
    }
  }

  lsmgraph::GraphDb* db_ = nullptr;
  ColdBlobWriterSet cold_blob_writers_;
  ColdBlobWriterSet node_cold_blob_writers_;
  FixedResidentArena load_arena_;
  SingleEdgeReadSampler single_edge_sampler_;
  WriteLatencySampler node_write_latency_sampler_;
  WriteLatencySampler write_latency_sampler_;

  std::unordered_map<TypedEntityKey, vertex_t, TypedEntityKeyHash> entity_to_vid_;
  std::unordered_set<vertex_t> materialized_vertices_;
  std::mutex materialized_vertices_mu_;
  std::vector<PreparedImportBatch> prepared_snapshot_node_batches_;
  std::vector<PreparedImportBatch> prepared_snapshot_relation_batches_;
  std::vector<PreparedImportBatch> prepared_incremental_batches_;
  std::vector<LoadedParamFile> loaded_param_files_;
  std::unordered_set<std::string> preprocessed_edge_slot_need_;
  std::unordered_map<std::string, uint16_t> preprocessed_edge_slot_;
  std::mutex preprocessed_edge_slot_mu_;
  static thread_local std::pmr::memory_resource* tls_load_resource_override_;

  vertex_t next_vertex_id_ = 0;
  uint64_t snapshot_entity_total_ = 0;
  uint64_t incremental_entity_total_ = 0;
};

thread_local std::pmr::memory_resource*
    FinBenchGraphDbTest::tls_load_resource_override_ = nullptr;

}  // namespace

int main(int argc, char** argv) {
  google::ParseCommandLineFlags(&argc, &argv, true);
  FinBenchGraphDbTest test;
  return test.Run();
}
