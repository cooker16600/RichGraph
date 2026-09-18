#include "core/flags.h"
#include "richgraph/graph_db.h"
#include "preprocessed_chunk_loader.h"

#include <gflags/gflags.h>

#include <algorithm>
#include <array>
#include <cassert>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <ctime>
#include <cctype>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <memory_resource>
#include <mutex>
#include <optional>
#include <set>
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

DEFINE_string(snb_v1_dataset_root,
              "./datasets/snb-v1",
              "SNB Interactive v1 CsvCompositeMergeForeign LongDateFormatter "
              "dataset root. For plus datasets, point this at the "
              "social_network-sfX+-... directory.");
DEFINE_string(snb_v1_params_root,
              "./datasets/snb-v1-params",
              "SNB Interactive v1 substitution parameter directory.");
DEFINE_string(snb_v1_db_path,
              "./richgraph-snb-v1-db",
              "GraphDb path used by the SNB-v1 test.");
DEFINE_bool(snb_v1_reset_db,
            true,
            "Whether to delete and recreate snb_v1_db_path.");
DEFINE_bool(snb_v1_use_csr_disk,
            false,
            "Whether the edge shards use CSR disk mode.");
DEFINE_bool(snb_v1_skip_queries,
            false,
            "If true, only import the initial graph and skip interactive queries.");
DEFINE_bool(snb_v1_mix_enable_queries,
            true,
            "If true, the mixed workload interleaves sampled query tasks with "
            "sampled updateStream writes. If false, the mixed workload writes "
            "only sampled updateStream rows.");
DEFINE_bool(snb_v1_skip_updates,
            false,
            "If true, skip the final node and edge property update benchmark stages.");
DEFINE_bool(snb_v1_enable_hot_edge_csr,
            false,
            "If true, use CSR-like compaction thresholds for hot edge shards "
            "and force compact remaining L0 files after the mixed workload.");
DEFINE_uint32(snb_v1_hot_edge_csr_l0_max_sst_num,
              8,
              "L0 auto-compaction limit for SNB hot edge CSR shards.");
DEFINE_uint32(snb_v1_hot_edge_csr_l1_max_sst_num,
              10000,
              "L1 auto-compaction limit for SNB hot edge CSR shards.");
DEFINE_bool(snb_v1_import_update_stream,
            true,
            "If true, import all updateStream rows after the initial graph and "
            "before running queries. This is a simple correctness mode, not the "
            "official mixed read/write schedule.");
DEFINE_bool(snb_v1_use_plus_params,
            false,
            "Prefer interactive_plus_N_param.txt. The plus20 generator now "
            "copies the original interactive_N_param.txt files unchanged, so "
            "the default query path uses the original parameter schema.");
DEFINE_uint64(snb_v1_import_row_limit_per_table,
              0,
              "If > 0, import at most this many rows from each node/edge table. "
              "This is intended for smoke tests; edges with missing endpoints are skipped.");
DEFINE_uint64(snb_v1_update_row_limit_per_file,
              0,
              "If > 0, import at most this many rows from each updateStream file. "
              "This is intended for smoke tests.");
DEFINE_uint32(snb_v1_param_limit_per_query,
              0,
              "If > 0, run at most this many parameter rows per query type.");
DEFINE_uint32(snb_v1_workload_sample_mod,
              20,
              "Sample update/query workload rows by zero-based data row index. "
              "Rows with row_index % mod == remainder are used in mixed and "
              "final query phases. 1 disables sampling.");
DEFINE_uint32(snb_v1_workload_sample_remainder,
              0,
              "Remainder used with snb_v1_workload_sample_mod.");
DEFINE_uint32(snb_v1_result_limit_per_param,
              0,
              "Maximum result rows hashed per parameter row. 0 means unlimited.");
DEFINE_uint32(snb_v1_frontier_limit,
              0,
              "Maximum traversal frontier/candidate size per parameter row. "
              "0 means unlimited.");
DEFINE_uint32(snb_v1_write_threads,
              8,
              "Front-end import thread count.");
DEFINE_bool(snb_v1_write_dynamic,
            false,
            "If true, SNB-v1 import loops use OpenMP dynamic scheduling.");
DEFINE_uint32(snb_v1_write_dynamic_chunk,
              1024,
              "OpenMP dynamic scheduling chunk size for SNB-v1 import loops.");
DEFINE_uint32(snb_v1_read_threads,
              16,
              "Front-end query thread count.");
DEFINE_uint32(snb_v1_batch_size,
              100000,
              "CSV rows buffered per streaming import batch. The SNB-v1 "
              "loader prepares one batch, writes it, then clears it.");
DEFINE_uint32(snb_v1_system_threads,
              16,
              "Thread count passed to GraphDb schema.system_threads.");
DEFINE_uint32(snb_v1_memtable_num,
              2,
              "Memtable count used by the underlying stores.");
DEFINE_uint32(snb_v1_memproperty_num,
              2,
              "Compatibility name for the engine property-buffer count.");
DEFINE_uint32(snb_v1_memtable_size,
              3050403,
              "Memtable size used by the underlying stores.");
DEFINE_uint32(snb_v1_node_memtable_size,
              0,
              "Memtable size for node_Db. 0 means snb_v1_memtable_size.");
DEFINE_string(snb_v1_edge_memtable_sizes,
              "",
              "Comma-separated memtable sizes for edge_Db0,edge_Db1,... "
              "Empty entries or 0 use snb_v1_memtable_size.");
DEFINE_uint32(snb_v1_max_subcompactions,
              4,
              "max_subcompactions used by the underlying stores.");
DEFINE_uint64(snb_v1_max_vertex_num,
              0,
              "Optional override for schema.max_vertex_num. If 0, derive from "
              "the imported node table row counts.");
DEFINE_uint32(snb_v1_max_property_length,
              64,
              "Fallback fixed property length for properties without an explicit length.");
DEFINE_uint64(snb_v1_single_edge_read_ops,
              1000000,
              "Number of random single-edge property reads to run after import. "
              "0 disables the benchmark block.");
DEFINE_bool(snb_v1_skip_single_edge_read,
            false,
            "If true, skip the single-edge read benchmark block.");
DEFINE_uint64(snb_v1_single_edge_candidate_cap,
              2000000,
              "Maximum retained candidate edge-property pairs for the single-edge "
              "read benchmark.");
DEFINE_uint64(snb_v1_single_edge_seed,
              20260624,
              "Deterministic seed for single-edge read candidate sampling and "
              "workload generation.");
DEFINE_uint32(snb_v1_single_edge_hot_weight,
              10,
              "Hot-property selection weight when generating single-edge reads. "
              "The default hot:cold ratio is 10:1.");
DEFINE_uint32(snb_v1_single_edge_cold_weight,
              1,
              "Cold-property selection weight when generating single-edge reads. "
              "The default hot:cold ratio is 10:1.");
DEFINE_uint64(snb_v1_write_latency_sample_target,
              1000000,
              "Target number of logical edge writes sampled for write latency "
              "percentiles. 0 disables write latency sampling.");
DEFINE_uint64(snb_v1_write_latency_sample_seed,
              20260624,
              "Deterministic seed used to mark logical edge writes for latency "
              "sampling.");
DEFINE_uint64(snb_v1_update_node_memproperty_cap,
              1080000,
              "Compatibility input for the engine PropertyBuffer record cap.");
DEFINE_uint64(snb_v1_update_edge_memproperty_cap,
              1600000,
              "Compatibility input for the engine PropertyBuffer record cap.");
DEFINE_uint64(snb_v1_property_buffer_bytes,
              256ULL * 1024ULL * 1024ULL,
              "Maximum estimated bytes in each engine PropertyBuffer.");
DEFINE_uint32(snb_v1_delta_merge_threshold,
              4,
              "Merge a property delta chain when it reaches this file count.");
DEFINE_bool(snb_v1_delta_crash_safe,
            true,
            "Synchronize delta files and manifest edits before publication.");
DEFINE_bool(snb_v1_run_mixed_workload,
            true,
            "If true, execute updateStream inserts interleaved with SNB long "
            "read queries before the final ordered query run.");
DEFINE_uint64(snb_v1_mixed_update_interleave,
              0,
              "SNB mixed workload update interleave in milliseconds. 0 derives "
              "a default from the dataset scale in snb_v1_dataset_root.");
DEFINE_uint32(snb_v1_mixed_threads,
              16,
              "Total worker threads used by the concurrent mixed workload.");
DEFINE_uint64(snb_v1_load_arena_gb,
              128,
              "Resident anonymous memory reserved for the dataset load buffer. "
              "The arena is mmap'ed and page-touched before LOAD_PREPARE so "
              "unused arena space cannot be used by DB page cache. 0 disables it.");
DEFINE_bool(snb_v1_enable_preprocessed_loader,
            false,
            "If true, read the preprocessed binary chunk dataset instead of the "
            "legacy CSV path. The current hook validates the loader/queue path "
            "and is the entry point for the new chunk pipeline.");
DEFINE_string(snb_v1_preprocessed_root,
              "",
              "Preprocessed SNB-v1 chunk root. Empty means use "
              "snb_v1_dataset_root.");
DEFINE_uint32(snb_v1_loader_threads,
              16,
              "Loader thread count for preprocessed chunks.");
DEFINE_uint32(snb_v1_loader_cpu_base,
              16,
              "First CPU used by preprocessed loader threads.");
DEFINE_uint32(snb_v1_db_cpu_base,
              0,
              "First CPU used by DB/consumer threads in preprocessed mode.");
DEFINE_uint32(snb_v1_loader_queue_blocks,
              64,
              "Maximum number of preprocessed chunk blocks in the loader queue.");
DEFINE_uint32(snb_v1_loader_prefill_blocks,
              64,
              "Number of prepared chunk blocks the loader should enqueue before "
              "the DB consumer starts. 0 disables prefill; values larger than "
              "snb_v1_loader_queue_blocks are clamped.");
DEFINE_uint32(snb_v1_loader_block_records,
              200000,
              "Maximum record count expected per preprocessed chunk block.");

namespace {

constexpr uint8_t kNodeEdgeType = 0;
constexpr uint64_t kMillisPerDay = 24ULL * 60ULL * 60ULL * 1000ULL;
constexpr std::string_view kGeneratedColdPrefix = "cold_extra_";

enum class NodeKind : uint8_t {
  kPlace = 1,
  kOrganisation = 2,
  kTag = 3,
  kTagClass = 4,
  kPerson = 5,
  kForum = 6,
  kPost = 7,
  kComment = 8,
};

enum EdgeType : uint8_t {
  kPersonKnowsPerson = 1,
  kForumHasMemberPerson = 2,
  kForumHasTagTag = 3,
  kPersonHasInterestTag = 4,
  kPersonLikesPost = 5,
  kPersonLikesComment = 6,
  kPersonStudyAtOrganisation = 7,
  kPersonWorkAtOrganisation = 8,
  kPostHasTagTag = 9,
  kCommentHasTagTag = 10,
  kPostCreatorPerson = 11,
  kPostContainerForum = 12,
  kPostLocationPlace = 13,
  kCommentCreatorPerson = 14,
  kCommentLocationPlace = 15,
  kCommentParentPost = 16,
  kCommentParentComment = 17,
  kTagTypeTagClass = 18,
  kTagClassSubclassTagClass = 19,
  kPlacePartOfPlace = 20,
  kOrganisationLocationPlace = 21,
  kForumModeratorPerson = 22,
  kPersonLocationPlace = 23,
};

struct CsvHeader {
  std::vector<std::string> columns;
  std::unordered_map<std::string, size_t> name_to_idx;

  size_t Index(std::string_view name) const {
    const auto it = name_to_idx.find(std::string(name));
    return it == name_to_idx.end() ? std::numeric_limits<size_t>::max()
                                   : it->second;
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
    if (idx == std::numeric_limits<size_t>::max() || idx >= fields.size()) {
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
  const char* area = "";
  const char* file_name = "";
  NodeKind kind = NodeKind::kPerson;
  const char* id_column = "id";
  std::vector<std::string> properties;
};

struct EdgeTableSpec {
  const char* file_name = "";
  NodeKind src_kind = NodeKind::kPerson;
  NodeKind dst_kind = NodeKind::kPerson;
  const char* src_column = "";
  const char* dst_column = "";
  size_t src_index = std::numeric_limits<size_t>::max();
  size_t dst_index = std::numeric_limits<size_t>::max();
  uint8_t edge_type = 0;
  std::vector<std::string> properties;
};

using PmrString = std::pmr::string;

struct PreparedNodeWrite {
  vertex_t id = lsmgraph::INVALID_VERTEX_ID;
  PmrString payload;
  PmrString node_cold_payload;
  bool has_node_cold_payload = false;
  bool sample_write_latency = false;
  uint64_t scheduled_time = 0;

  PreparedNodeWrite(vertex_t id_in,
                    PmrString payload_in,
                    PmrString node_cold_payload_in,
                    bool has_node_cold_payload_in,
                    uint64_t scheduled_time_in = 0)
      : id(id_in),
        payload(std::move(payload_in)),
        node_cold_payload(std::move(node_cold_payload_in)),
        has_node_cold_payload(has_node_cold_payload_in),
        scheduled_time(scheduled_time_in) {}
};

struct PreparedEdgeWrite {
  vertex_t src = lsmgraph::INVALID_VERTEX_ID;
  vertex_t dst = lsmgraph::INVALID_VERTEX_ID;
  uint8_t edge_type = 0;
  lsmgraph::EdgeInsertMode insert_mode = lsmgraph::EdgeInsertMode::kSingle;
  bool is_out = true;
  PmrString shard0_payload;
  PmrString shard1_payload;
  PmrString shard2_payload;
  PmrString shard3_payload;
  bool has_shard1 = false;
  bool has_shard2 = false;
  bool has_shard3 = false;
  bool sample_write_latency = false;
  uint64_t scheduled_time = 0;

  explicit PreparedEdgeWrite(
      std::pmr::memory_resource* mr = std::pmr::get_default_resource())
      : shard0_payload(mr),
        shard1_payload(mr),
        shard2_payload(mr),
        shard3_payload(mr) {}
};

struct PreparedImportBatch {
  PmrString name;
  uint64_t logical_rows = 0;
  uint64_t skipped_edges = 0;
  std::pmr::vector<PreparedNodeWrite> node_writes;
  std::pmr::vector<PreparedEdgeWrite> edge_writes;

  explicit PreparedImportBatch(
      std::pmr::memory_resource* mr = std::pmr::get_default_resource())
      : name(mr), node_writes(mr), edge_writes(mr) {}
};

struct LoadedNodeTable {
  NodeTableSpec spec;
  LoadedCsvFile data;
};

struct LoadedParamFile {
  int query_id = 0;
  LoadedCsvFile data;
};

struct IdPair {
  uint64_t raw_id = 0;
  vertex_t vid = lsmgraph::INVALID_VERTEX_ID;
};

struct ImportStats {
  uint64_t logical_rows = 0;
  uint64_t node_writes = 0;
  uint64_t edge_writes = 0;
  uint64_t skipped_edges = 0;
  double sec = 0.0;
  double background_wait_sec = 0.0;
  double wall_sec = 0.0;
};

struct QueryRunResult {
  uint64_t rows = 0;
  uint64_t checksum = 0;
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
      hot_indices_by_property_[hot_candidates_[i].property_id].push_back(i);
    }
    for (uint32_t i = 0; i < cold_candidates_.size(); ++i) {
      cold_indices_by_property_[cold_candidates_[i].property_id].push_back(i);
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

struct QueryMetrics {
  int query_id = 0;
  uint64_t param_rows = 0;
  uint64_t result_rows = 0;
  uint64_t checksum = 0;
  double sec = 0.0;
};

struct MixedWorkloadStats {
  uint64_t logical_update_rows = 0;
  uint64_t node_writes = 0;
  uint64_t edge_writes = 0;
  uint64_t skipped_edges = 0;
  uint64_t query_ops = 0;
  uint64_t result_rows = 0;
  uint64_t checksum = 0;
  double total_sec = 0.0;
  double write_sec = 0.0;
  double query_sec = 0.0;
  double background_wait_sec = 0.0;
  double wall_sec = 0.0;
  std::array<QueryMetrics, 15> query_metrics;
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

struct MixedQueryTask {
  int query_id = 0;
  const CsvRow* row = nullptr;
  uint64_t ordinal = 0;
};

struct MixedQueryStream {
  int query_id = 0;
  const LoadedCsvFile* file = nullptr;
  std::vector<size_t> row_indices;
  size_t next_row = 0;
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

void HashRowsInOrder(const std::vector<std::vector<std::string>>& rows,
                     QueryRunResult* out) {
  if (out == nullptr) {
    return;
  }
  out->rows = rows.size();
  out->checksum = 0;
  uint64_t row_no = 0;
  for (const auto& row : rows) {
    out->checksum ^= HashMix(HashStrings(row) + (++row_no));
  }
}

std::string JoinStrings(const std::vector<std::string>& values,
                        char sep = ';') {
  std::string out;
  for (size_t i = 0; i < values.size(); ++i) {
    if (i > 0) {
      out.push_back(sep);
    }
    out += values[i];
  }
  return out;
}

std::string FormatPathWeight(double value) {
  std::ostringstream out;
  out << std::fixed << std::setprecision(1) << value;
  return out.str();
}

time_t PortableTimegm(std::tm* tm) {
  return timegm(tm);
}

uint64_t ParseDateTimeToMillis(const std::string& raw) {
  const std::string value = Trim(raw);
  if (value.empty()) {
    return 0;
  }
  if (IsDigitsOnly(value)) {
    return ToUint64(value);
  }

  std::string datetime = value;
  int millis = 0;
  const size_t tz_pos = datetime.find_first_of("+-", 10);
  if (tz_pos != std::string::npos) {
    datetime = datetime.substr(0, tz_pos);
  }
  if (!datetime.empty() && datetime.back() == 'Z') {
    datetime.pop_back();
  }
  std::replace(datetime.begin(), datetime.end(), 'T', ' ');

  const size_t dot = datetime.find('.');
  if (dot != std::string::npos) {
    std::string ms = datetime.substr(dot + 1);
    datetime = datetime.substr(0, dot);
    while (ms.size() < 3U) {
      ms.push_back('0');
    }
    if (ms.size() > 3U) {
      ms = ms.substr(0, 3);
    }
    millis = static_cast<int>(ToUint64(ms));
  }

  std::tm tm = {};
  std::istringstream iss(datetime);
  if (datetime.find(':') != std::string::npos) {
    iss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");
  } else {
    iss >> std::get_time(&tm, "%Y-%m-%d");
  }
  if (iss.fail()) {
    return 0;
  }
  const time_t seconds = PortableTimegm(&tm);
  if (seconds < 0) {
    return 0;
  }
  return static_cast<uint64_t>(seconds) * 1000ULL +
         static_cast<uint64_t>(millis);
}

uint64_t ParamMillis(const CsvRow& row, const char* name) {
  return ParseDateTimeToMillis(row.Get(name));
}

uint64_t LengthBucket(uint64_t len) {
  if (len < 40) {
    return 0;
  }
  if (len < 80) {
    return 1;
  }
  if (len < 160) {
    return 2;
  }
  return 3;
}

std::string EdgeTypeCode(uint8_t edge_type) {
  std::ostringstream out;
  out << std::setw(2) << std::setfill('0') << static_cast<uint32_t>(edge_type);
  return out.str();
}

std::string EventMonth(uint64_t millis) {
  if (millis == 0) {
    return "";
  }
  const time_t seconds = static_cast<time_t>(millis / 1000ULL);
  std::tm tm = {};
  gmtime_r(&seconds, &tm);
  std::ostringstream out;
  out << std::setw(4) << std::setfill('0') << (tm.tm_year + 1900)
      << std::setw(2) << std::setfill('0') << (tm.tm_mon + 1);
  return out.str();
}

std::string EventDow(uint64_t millis) {
  if (millis == 0) {
    return "";
  }
  return std::to_string(((millis / kMillisPerDay) + 4ULL) % 7ULL);
}

std::string FieldAt(const std::vector<std::string>& fields, size_t idx) {
  return idx < fields.size() ? fields[idx] : std::string();
}

std::vector<std::string> SplitSemicolonList(const std::string& value) {
  std::vector<std::string> out;
  size_t begin = 0;
  while (begin <= value.size()) {
    const size_t sep = value.find(';', begin);
    std::string item =
        sep == std::string::npos ? value.substr(begin) : value.substr(begin, sep - begin);
    item = Trim(std::move(item));
    if (!item.empty() && item != "-1") {
      out.push_back(std::move(item));
    }
    if (sep == std::string::npos) {
      break;
    }
    begin = sep + 1;
  }
  return out;
}

std::vector<std::pair<std::string, std::string>> SplitPairList(
    const std::string& value) {
  std::vector<std::pair<std::string, std::string>> out;
  for (const auto& item : SplitSemicolonList(value)) {
    const size_t comma = item.find(',');
    if (comma == std::string::npos) {
      continue;
    }
    const std::string first = Trim(item.substr(0, comma));
    const std::string second = Trim(item.substr(comma + 1));
    if (!first.empty() && !second.empty()) {
      out.emplace_back(first, second);
    }
  }
  return out;
}

std::string NodeKindToString(NodeKind kind) {
  switch (kind) {
    case NodeKind::kPlace:
      return "Place";
    case NodeKind::kOrganisation:
      return "Organisation";
    case NodeKind::kTag:
      return "Tag";
    case NodeKind::kTagClass:
      return "TagClass";
    case NodeKind::kPerson:
      return "Person";
    case NodeKind::kForum:
      return "Forum";
    case NodeKind::kPost:
      return "Post";
    case NodeKind::kComment:
      return "Comment";
  }
  return "Unknown";
}

size_t KindIndex(NodeKind kind) {
  return static_cast<size_t>(kind);
}

const std::vector<std::string>& AllNodeProperties() {
  static const std::vector<std::string> props = {
      "nodeLabel", "rawId", "name", "url", "type", "firstName",
      "lastName", "gender", "birthday", "creationDate", "locationIP",
      "browserUsed", "place", "language", "email", "imageFile", "content",
      "length", "title"};
  return props;
}

constexpr size_t kBlobBufferBytes = 4ULL * 1024ULL * 1024ULL;
constexpr size_t kNodeColdBlobBufferBytes = kBlobBufferBytes;

const std::vector<std::string>& NodeDbProperties() {
  static const std::vector<std::string> props = {
      "rawId",
      "name",
      "type",
      "firstName",
      "lastName",
      "gender",
      "birthday",
      "creationDate",
      "place",
      "node_cold_property",
  };
  return props;
}

const std::vector<std::string>& ColdNodeProperties() {
  static const std::vector<std::string> props = {
      "nodeLabel",
      "url",
      "locationIP",
      "browserUsed",
      "language",
      "email",
      "imageFile",
      "content",
      "length",
      "title",
  };
  return props;
}

struct StringInterner {
  std::unordered_map<std::string, uint32_t> id_by_value;
  std::vector<std::string> value_by_id;

  StringInterner() { value_by_id.emplace_back(); }

  void Clear() {
    id_by_value.clear();
    value_by_id.clear();
    value_by_id.emplace_back();
  }

  uint32_t Intern(std::string value) {
    if (value.empty()) {
      return 0;
    }
    const auto it = id_by_value.find(value);
    if (it != id_by_value.end()) {
      return it->second;
    }
    const uint32_t id = static_cast<uint32_t>(value_by_id.size());
    value_by_id.push_back(std::move(value));
    id_by_value.emplace(value_by_id.back(), id);
    return id;
  }

  const std::string& Get(uint32_t id) const {
    if (id < value_by_id.size()) {
      return value_by_id[id];
    }
    return value_by_id.front();
  }

  void ReleaseIndex() {
    std::unordered_map<std::string, uint32_t>().swap(id_by_value);
  }
};

struct HotNodeColumns {
  static constexpr uint32_t kInvalidVid = std::numeric_limits<uint32_t>::max();

  std::vector<uint64_t> raw_id;
  std::vector<uint64_t> birthday;
  std::vector<uint32_t> place_vid;
  std::vector<uint32_t> first_name_id;
  std::vector<uint32_t> last_name_id;
  std::vector<uint32_t> name_id;
  std::vector<uint8_t> gender_code;
  std::vector<uint8_t> is_country_place;

  StringInterner first_names;
  StringInterner last_names;
  StringInterner names;

  void Clear() {
    raw_id.clear();
    birthday.clear();
    place_vid.clear();
    first_name_id.clear();
    last_name_id.clear();
    name_id.clear();
    gender_code.clear();
    is_country_place.clear();
    first_names.Clear();
    last_names.Clear();
    names.Clear();
  }

  void Ensure(size_t size) {
    if (raw_id.size() >= size) {
      return;
    }
    raw_id.resize(size, 0);
    birthday.resize(size, 0);
    place_vid.resize(size, kInvalidVid);
    first_name_id.resize(size, 0);
    last_name_id.resize(size, 0);
    name_id.resize(size, 0);
    gender_code.resize(size, 0);
    is_country_place.resize(size, 0);
  }

  void ReleaseInternIndexes() {
    first_names.ReleaseIndex();
    last_names.ReleaseIndex();
    names.ReleaseIndex();
  }
};

const std::vector<std::string>& AllEdgeProperties() {
  static const std::vector<std::string> props = {
      "edgeExists", "creationDate", "joinDate", "workFrom", "cold_property"};
  return props;
}

const std::vector<std::string>& EdgeShard0Properties() {
  static const std::vector<std::string> props = {
      "edgeExists", "creationDate"};
  return props;
}

const std::vector<std::string>& EdgeShard1Properties() {
  static const std::vector<std::string> props = {
      "joinDate"};
  return props;
}

const std::vector<std::string>& EdgeShard2Properties() {
  static const std::vector<std::string> props = {
      "workFrom"};
  return props;
}

const std::vector<std::string>& EdgeShard3Properties() {
  static const std::vector<std::string> props = {
      "cold_property"};
  return props;
}

bool IsGeneratedColdProperty(std::string_view name) {
  return name.size() > kGeneratedColdPrefix.size() &&
         name.substr(0, kGeneratedColdPrefix.size()) == kGeneratedColdPrefix;
}

std::unordered_map<std::string, size_t> BuildIndex(
    const std::vector<std::string>& props) {
  std::unordered_map<std::string, size_t> out;
  for (size_t i = 0; i < props.size(); ++i) {
    out.emplace(props[i], i);
  }
  return out;
}

const std::unordered_map<std::string, size_t>& NodePropertyIndexByName() {
  static const auto map = BuildIndex(NodeDbProperties());
  return map;
}

const std::unordered_map<std::string, size_t>& ColdNodePropertyIndex() {
  static const auto map = BuildIndex(ColdNodeProperties());
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

const std::unordered_map<std::string, size_t>& EdgeShard0PropertyIndex() {
  static const auto map = BuildIndex(EdgeShard0Properties());
  return map;
}

const std::unordered_map<std::string, size_t>& EdgeShard1PropertyIndex() {
  static const auto map = BuildIndex(EdgeShard1Properties());
  return map;
}

const std::unordered_map<std::string, size_t>& EdgeShard2PropertyIndex() {
  static const auto map = BuildIndex(EdgeShard2Properties());
  return map;
}

const std::unordered_map<std::string, size_t>& EdgeShard3PropertyIndex() {
  static const auto map = BuildIndex(EdgeShard3Properties());
  return map;
}

bool IsEdgeInlineProperty(const std::string& name) {
  return EdgeShard0PropertyIndex().find(name) != EdgeShard0PropertyIndex().end() ||
         EdgeShard1PropertyIndex().find(name) != EdgeShard1PropertyIndex().end() ||
         EdgeShard2PropertyIndex().find(name) != EdgeShard2PropertyIndex().end();
}

bool IsEdgeColdBlobProperty(const std::string& name) {
  return !name.empty() && name != "cold_property" &&
         !IsEdgeInlineProperty(name);
}

std::vector<std::string> SortedEdgeColdBlobProperties(
    const std::unordered_map<std::string, std::string>& props) {
  std::vector<std::string> out;
  out.reserve(props.size());
  for (const auto& kv : props) {
    if (kv.first == "classYear" || kv.second.empty() ||
        !IsEdgeColdBlobProperty(kv.first)) {
      continue;
    }
    out.push_back(kv.first);
  }
  std::sort(out.begin(), out.end());
  out.erase(std::unique(out.begin(), out.end()), out.end());
  return out;
}

const std::vector<NodeTableSpec>& NodeTables() {
  static const std::vector<NodeTableSpec> specs = {
      {"static", "place_0_0.csv", NodeKind::kPlace, "id",
       {"name", "url", "type"}},
      {"static", "organisation_0_0.csv", NodeKind::kOrganisation, "id",
       {"type", "name", "url"}},
      {"static", "tag_0_0.csv", NodeKind::kTag, "id",
       {"name", "url"}},
      {"static", "tagclass_0_0.csv", NodeKind::kTagClass, "id",
       {"name", "url"}},
      {"dynamic", "person_0_0.csv", NodeKind::kPerson, "id",
       {"firstName", "lastName", "gender", "birthday", "creationDate",
        "locationIP", "browserUsed", "place", "language", "email"}},
      {"dynamic", "forum_0_0.csv", NodeKind::kForum, "id",
       {"title", "creationDate"}},
      {"dynamic", "post_0_0.csv", NodeKind::kPost, "id",
       {"imageFile", "creationDate", "locationIP", "browserUsed", "language",
        "content", "length", "place"}},
      {"dynamic", "comment_0_0.csv", NodeKind::kComment, "id",
       {"creationDate", "locationIP", "browserUsed", "content", "length",
        "place"}},
  };
  return specs;
}

const std::vector<EdgeTableSpec>& EdgeTables() {
  static const std::vector<EdgeTableSpec> specs = {
      {"person_knows_person_0_0.csv", NodeKind::kPerson, NodeKind::kPerson,
       "", "", 0, 1, kPersonKnowsPerson,
       {"creationDate", "edgeTypeCode", "eventMonth", "eventDow",
        "srcCountryId", "srcCityId", "dstCountryId", "dstCityId",
        "srcActivityBucket", "dstActivityBucket", "edgeWeight",
        "interactionCnt", "sameCountry", "sameCity"}},
      {"forum_hasMember_person_0_0.csv", NodeKind::kForum, NodeKind::kPerson,
       "Forum.id", "Person.id", std::numeric_limits<size_t>::max(),
       std::numeric_limits<size_t>::max(), kForumHasMemberPerson,
       {"joinDate", "edgeTypeCode", "eventMonth", "eventDow",
        "srcCountryId", "srcCityId", "dstCountryId", "dstCityId",
        "srcActivityBucket", "dstActivityBucket", "sameCountry",
        "sameCity"}},
      {"forum_hasTag_tag_0_0.csv", NodeKind::kForum, NodeKind::kTag,
       "Forum.id", "Tag.id", std::numeric_limits<size_t>::max(),
       std::numeric_limits<size_t>::max(), kForumHasTagTag,
       {"edgeTypeCode", "srcActivityBucket", "tagClassId",
        "tagPopularityBucket"}},
      {"person_hasInterest_tag_0_0.csv", NodeKind::kPerson, NodeKind::kTag,
       "Person.id", "Tag.id", std::numeric_limits<size_t>::max(),
       std::numeric_limits<size_t>::max(), kPersonHasInterestTag,
       {"edgeTypeCode", "srcCountryId", "srcCityId", "srcActivityBucket",
        "tagClassId", "tagPopularityBucket", "edgeWeight"}},
      {"person_likes_post_0_0.csv", NodeKind::kPerson, NodeKind::kPost,
       "Person.id", "Post.id", std::numeric_limits<size_t>::max(),
       std::numeric_limits<size_t>::max(), kPersonLikesPost,
       {"creationDate", "edgeTypeCode", "eventMonth", "eventDow",
        "srcCountryId", "srcCityId", "dstCountryId", "dstCityId",
        "srcActivityBucket", "dstActivityBucket", "edgeWeight",
        "messageLengthBucket"}},
      {"person_likes_comment_0_0.csv", NodeKind::kPerson, NodeKind::kComment,
       "Person.id", "Comment.id", std::numeric_limits<size_t>::max(),
       std::numeric_limits<size_t>::max(), kPersonLikesComment,
       {"creationDate", "edgeTypeCode", "eventMonth", "eventDow",
        "srcCountryId", "srcCityId", "dstCountryId", "dstCityId",
        "srcActivityBucket", "dstActivityBucket", "edgeWeight",
        "messageLengthBucket"}},
      {"person_studyAt_organisation_0_0.csv", NodeKind::kPerson,
       NodeKind::kOrganisation, "Person.id", "Organisation.id",
       std::numeric_limits<size_t>::max(), std::numeric_limits<size_t>::max(),
       kPersonStudyAtOrganisation,
       {"classYear", "edgeTypeCode", "srcCountryId", "srcCityId",
        "dstCountryId", "srcActivityBucket", "dstActivityBucket"}},
      {"person_workAt_organisation_0_0.csv", NodeKind::kPerson,
       NodeKind::kOrganisation, "Person.id", "Organisation.id",
       std::numeric_limits<size_t>::max(), std::numeric_limits<size_t>::max(),
       kPersonWorkAtOrganisation,
       {"workFrom", "edgeTypeCode", "srcCountryId", "srcCityId",
        "dstCountryId", "srcActivityBucket", "dstActivityBucket"}},
      {"post_hasTag_tag_0_0.csv", NodeKind::kPost, NodeKind::kTag,
       "Post.id", "Tag.id", std::numeric_limits<size_t>::max(),
       std::numeric_limits<size_t>::max(), kPostHasTagTag,
       {"edgeTypeCode", "eventMonth", "eventDow", "srcCountryId",
        "srcCityId", "tagClassId", "tagPopularityBucket",
        "messageLengthBucket"}},
      {"comment_hasTag_tag_0_0.csv", NodeKind::kComment, NodeKind::kTag,
       "Comment.id", "Tag.id", std::numeric_limits<size_t>::max(),
       std::numeric_limits<size_t>::max(), kCommentHasTagTag,
       {"edgeTypeCode", "eventMonth", "eventDow", "srcCountryId",
        "srcCityId", "tagClassId", "tagPopularityBucket",
        "messageLengthBucket"}},
  };
  return specs;
}

bool ShouldWriteBidirectional(uint8_t edge_type) {
  return edge_type == kPersonKnowsPerson ||
         edge_type == kPersonHasInterestTag ||
         edge_type == kPostCreatorPerson ||
         edge_type == kCommentCreatorPerson ||
         edge_type == kCommentParentPost ||
         edge_type == kCommentParentComment;
}

bool ShouldWriteReverseOnly(uint8_t edge_type) {
  switch (edge_type) {
    case kForumHasMemberPerson:
    case kPersonLikesPost:
    case kPersonLikesComment:
      return true;
    default:
      return false;
  }
}

std::string NodeTablePath(const NodeTableSpec& spec) {
  return FLAGS_snb_v1_dataset_root + "/" + spec.area + "/" + spec.file_name;
}

std::string EdgeTablePath(const EdgeTableSpec& spec) {
  return FLAGS_snb_v1_dataset_root + "/dynamic/" + spec.file_name;
}

bool FileOrDirExists(const std::string& path) {
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

bool LoadCsvFile(const std::string& path,
                 uint64_t row_limit,
                 LoadedCsvFile* out) {
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
  const size_t row_hint = row_limit > 0
                              ? static_cast<size_t>(row_limit)
                              : EstimateCsvRowsBySize(path, 192);
  if (row_hint > 0) {
    out->rows.reserve(row_hint);
  }
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
    if (row_limit > 0 && out->rows.size() >= row_limit) {
      break;
    }
  }
  return true;
}

template <typename Fn>
bool ForEachCsvRow(const std::string& path, uint64_t row_limit, Fn&& fn) {
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
  uint64_t rows = 0;
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
    ++rows;
    if (row_limit > 0 && rows >= row_limit) {
      break;
    }
  }
  return true;
}

template <typename T>
void ReleaseVectorMemory(std::vector<T>* values) {
  if (values == nullptr) {
    return;
  }
  std::vector<T>().swap(*values);
}

struct ProcessMemoryUsage {
  uint64_t vm_rss_kb = 0;
  uint64_t vm_hwm_kb = 0;
  uint64_t vm_size_kb = 0;
  bool valid = false;
};

bool ParseProcStatusKbLine(const std::string& line,
                           std::string_view key,
                           uint64_t* out) {
  if (out == nullptr || line.rfind(std::string(key), 0) != 0) {
    return false;
  }
  std::istringstream iss(line.substr(key.size()));
  uint64_t value = 0;
  if (!(iss >> value)) {
    return false;
  }
  *out = value;
  return true;
}

ProcessMemoryUsage ReadProcessMemoryUsage() {
  ProcessMemoryUsage usage;
  std::ifstream in("/proc/self/status");
  if (!in.is_open()) {
    return usage;
  }
  std::string line;
  while (std::getline(in, line)) {
    usage.valid |= ParseProcStatusKbLine(line, "VmRSS:", &usage.vm_rss_kb);
    usage.valid |= ParseProcStatusKbLine(line, "VmHWM:", &usage.vm_hwm_kb);
    usage.valid |= ParseProcStatusKbLine(line, "VmSize:", &usage.vm_size_kb);
  }
  return usage;
}

std::string KbToGiBString(uint64_t kb) {
  std::ostringstream out;
  out << std::fixed << std::setprecision(3)
      << static_cast<double>(kb) / (1024.0 * 1024.0);
  return out.str();
}

void PrintProcessMemoryUsage(const char* phase) {
  const ProcessMemoryUsage usage = ReadProcessMemoryUsage();
  if (!usage.valid) {
    std::cout << "[MEMORY][" << phase << "] unavailable" << std::endl;
    return;
  }
  std::cout << "[MEMORY][" << phase << "] VmRSS(kB): " << usage.vm_rss_kb
            << ", VmRSS(GiB): " << KbToGiBString(usage.vm_rss_kb)
            << ", VmHWM(kB): " << usage.vm_hwm_kb
            << ", VmHWM(GiB): " << KbToGiBString(usage.vm_hwm_kb)
            << ", VmSize(kB): " << usage.vm_size_kb
            << ", VmSize(GiB): " << KbToGiBString(usage.vm_size_kb)
            << std::endl;
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

  [[maybe_unused]] const int omp_chunk =
      static_cast<int>(std::max<uint32_t>(1U, chunk_size));
#pragma omp parallel for num_threads(static_cast<int>(thread_count)) schedule(dynamic, omp_chunk)
  for (long long i = 0; i < static_cast<long long>(count); ++i) {
    fn(static_cast<size_t>(i));
  }
}

void SetSlot(std::vector<std::string>* slots,
             const std::unordered_map<std::string, size_t>& index,
             const std::string& name,
             const std::string& value) {
  if (slots == nullptr || value.empty()) {
    return;
  }
  const auto it = index.find(name);
  if (it == index.end() || it->second >= slots->size()) {
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

std::string ToStdString(const PmrString& value) {
  return std::string(value.data(), value.size());
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

std::vector<std::string> DecodePayloadSlots(const std::string& payload,
                                            size_t min_slots) {
  std::vector<std::string> slots;
  size_t start = 0;
  while (start <= payload.size()) {
    const size_t sep = payload.find('|', start);
    if (sep == std::string::npos) {
      slots.push_back(payload.substr(start));
      break;
    }
    slots.push_back(payload.substr(start, sep - start));
    start = sep + 1;
  }
  if (slots.size() < min_slots) {
    slots.resize(min_slots);
  }
  return slots;
}

uint32_t CurrentOpenMpThreadId(uint32_t fallback_mod) {
  if (fallback_mod == 0) {
    return 0;
  }
#ifdef _OPENMP
  const int tid = omp_get_thread_num();
  if (tid >= 0) {
    return static_cast<uint32_t>(tid) % fallback_mod;
  }
#endif
  return 0;
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
    next_offset_ += length + 1ULL;
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
    const uint32_t id =
        CurrentOpenMpThreadId(static_cast<uint32_t>(writers_.size()));
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

class SnbV1GraphDbTest {
 public:
  int Run() {
    if (FLAGS_snb_v1_enable_preprocessed_loader) {
      const int rc = RunPreprocessed();
      google::ShutDownCommandLineFlags();
      return rc;
    }
    if (!ValidateDatasetLayout()) {
      return 1;
    }
    if (!load_arena_.Open(FLAGS_snb_v1_load_arena_gb,
                          "snb_v1_load_buffer")) {
      return 1;
    }
    if (!LoadAllDataset()) {
      return 1;
    }
    if (!PrepareDbDirectory()) {
      return 1;
    }

    ConfigureLowLevelFlags();
    const uint64_t max_vertex_num = DeriveMaxVertexNum();
    const std::string schema_path = FLAGS_snb_v1_db_path + "/snb_v1_schema.yaml";
    WriteSchemaFile(schema_path);
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
    PrintProcessMemoryUsage("DB_OPEN");

    std::cout << "=== test_graphdb_snb_v1 ===" << std::endl;
    std::cout << "dataset_root: " << FLAGS_snb_v1_dataset_root << std::endl;
    std::cout << "params_root: " << FLAGS_snb_v1_params_root << std::endl;
    std::cout << "db_path: " << FLAGS_snb_v1_db_path << std::endl;
    std::cout << "schema_path: " << schema_path << std::endl;
    std::cout << "cold_blob_prefix: " << ColdBlobPathPrefix() << std::endl;
    std::cout << "cold_blob_files: " << cold_blob_writers_.file_count()
              << std::endl;
    std::cout << "node_cold_blob_prefix: " << NodeColdBlobPathPrefix()
              << std::endl;
    std::cout << "node_cold_blob_files: "
              << node_cold_blob_writers_.file_count() << std::endl;
    std::cout << "max_vertex_num: " << max_vertex_num << std::endl;
    std::cout << "prepared_vertices: " << next_vertex_id_ << std::endl;
    std::cout << "use_csr_disk: " << db_->schema().use_csr_disk << std::endl;
    std::cout << "import_update_stream: "
              << (FLAGS_snb_v1_import_update_stream ? "true" : "false")
              << std::endl;
	    std::cout << "run_mixed_workload: "
	              << (FLAGS_snb_v1_run_mixed_workload ? "true" : "false")
	              << std::endl;
	    std::cout << "mix_enable_queries: "
	              << (FLAGS_snb_v1_mix_enable_queries ? "true" : "false")
	              << std::endl;
    std::cout << "mixed_update_interleave: "
              << FLAGS_snb_v1_mixed_update_interleave << std::endl;
    std::cout << "mixed_threads: " << FLAGS_snb_v1_mixed_threads
              << std::endl;
    std::cout << "row_limit_per_table: " << FLAGS_snb_v1_import_row_limit_per_table
              << std::endl;
    std::cout << "update_row_limit_per_file: "
              << FLAGS_snb_v1_update_row_limit_per_file << std::endl;
    std::cout << "param_limit_per_query: " << FLAGS_snb_v1_param_limit_per_query
              << std::endl;
    std::cout << "workload_sample_mod: " << WorkloadSampleMod() << std::endl;
    std::cout << "workload_sample_remainder: " << WorkloadSampleRemainder()
              << std::endl;
    std::cout << "system_threads: " << FLAGS_snb_v1_system_threads << std::endl;
    std::cout << "engine_property_updates: true" << std::endl;
    std::cout << "property_buffer_records: "
              << std::max(FLAGS_snb_v1_update_node_memproperty_cap,
                          FLAGS_snb_v1_update_edge_memproperty_cap)
              << std::endl;
    std::cout << "property_buffer_bytes: "
              << FLAGS_snb_v1_property_buffer_bytes << std::endl;
    std::cout << "delta_merge_threshold: "
              << FLAGS_snb_v1_delta_merge_threshold << std::endl;
    std::cout << "delta_crash_safe: "
              << (FLAGS_snb_v1_delta_crash_safe ? "true" : "false")
              << std::endl;
    std::cout << "write_schedule: "
              << (FLAGS_snb_v1_write_dynamic ? "dynamic" : "static");
    if (FLAGS_snb_v1_write_dynamic) {
      std::cout << ", chunk=" << FLAGS_snb_v1_write_dynamic_chunk;
    }
    std::cout << std::endl;
    std::cout << "import_batch_rows: " << FLAGS_snb_v1_batch_size
              << std::endl;
    std::cout << "load_arena_gb: " << FLAGS_snb_v1_load_arena_gb
              << std::endl;
    std::cout << "memtable_size: " << FLAGS_snb_v1_memtable_size << std::endl;
    std::cout << "node_memtable_size: " << NodeMemtableSize() << std::endl;
    PrintMemtableSizes("edge_memtable_sizes", EdgeMemtableSizes());
    std::cout << "blob_buffer_bytes: " << kBlobBufferBytes << std::endl;
    std::cout << "single_edge_read_ops: "
              << FLAGS_snb_v1_single_edge_read_ops << std::endl;
    std::cout << "single_edge_candidate_cap: "
              << FLAGS_snb_v1_single_edge_candidate_cap << std::endl;
    std::cout << "single_edge_hot_cold_ratio: "
              << FLAGS_snb_v1_single_edge_hot_weight << ":"
              << FLAGS_snb_v1_single_edge_cold_weight << std::endl;
	    std::cout << "single_edge_seed: " << FLAGS_snb_v1_single_edge_seed
	              << std::endl;
	    std::cout << "skip_single_edge_read: "
	              << (FLAGS_snb_v1_skip_single_edge_read ? "true" : "false")
	              << std::endl;
    std::cout << "write_latency_sample_target: "
              << FLAGS_snb_v1_write_latency_sample_target << std::endl;
    std::cout << "write_latency_sample_seed: "
              << FLAGS_snb_v1_write_latency_sample_seed << std::endl;

    ConfigureWriteLatencySampling();

    const ImportStats initial_nodes = ImportInitialNodes();
    PrintImportStats("INITIAL_NODE_IMPORT", initial_nodes);
    PrintProcessMemoryUsage("INITIAL_NODE_IMPORT");

	    const ImportStats initial_edges = ImportInitialRelations();
	    PrintImportStats("INITIAL_RELATION_IMPORT", initial_edges);
	    PrintProcessMemoryUsage("INITIAL_RELATION_IMPORT");
	    PrintCombinedImportStats("INITIAL_GRAPH_IMPORT", initial_nodes, initial_edges);
	    write_latency_sampling_active_ = false;

	    MixedWorkloadStats mixed_stats;
	    ImportStats update_import_stats;
	    const bool run_mixed =
	        FLAGS_snb_v1_import_update_stream &&
	        FLAGS_snb_v1_run_mixed_workload &&
	        (!FLAGS_snb_v1_skip_queries || !FLAGS_snb_v1_mix_enable_queries);
	    if (FLAGS_snb_v1_import_update_stream) {
	      if (run_mixed) {
		        update_import_stats = ImportRemainingUpdateStream();
		        PrintImportStats("REMAINING_UPDATE_STREAM_IMPORT", update_import_stats);
		        PrintProcessMemoryUsage("REMAINING_UPDATE_STREAM_IMPORT");
		        mixed_stats = RunMixedWorkload();
            if (!ForceHotEdgeCsrCompactionIfNeeded()) {
              return 1;
            }
		        PrintProcessMemoryUsage("MIXED_WORKLOAD");
		      } else {
		        update_import_stats = ImportUpdateStream();
		        PrintImportStats("UPDATE_STREAM_IMPORT", update_import_stats);
		        PrintProcessMemoryUsage("UPDATE_STREAM_IMPORT");
		      }
	    }
	    PrintFullGraphWriteStats(initial_nodes,
	                             initial_edges,
                               ImportStats{},
	                             update_import_stats);

    hot_nodes_.ReleaseInternIndexes();
    PrintProcessMemoryUsage("NODE_HOT_CACHE_READY");

    if (!cold_blob_writers_.Close() || !node_cold_blob_writers_.Close()) {
      return 1;
    }
    PrintColdBlobStats();

    write_latency_sampler_.Print("WRITE_LATENCY_SAMPLE");

	    if (!FLAGS_snb_v1_skip_single_edge_read) {
	      RunSingleEdgeReadBenchmark();
	    }

    if (!FLAGS_snb_v1_skip_queries) {
      RunAllQueries();
    }

    std::cout << "test_graphdb_snb_v1 passed" << std::endl;
    google::ShutDownCommandLineFlags();
    return 0;
  }

 private:
  struct PreprocessedIndexStats {
    uint64_t node_records = 0;
    uint64_t edge_records = 0;
    uint64_t snapshot_edge_records = 0;
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
    std::array<QueryMetrics, 15> query_metrics;
    double wall_sec = 0.0;

    PreprocessedQueryStats() {
      for (int qid = 1; qid <= 14; ++qid) {
        query_metrics[static_cast<size_t>(qid)].query_id = qid;
      }
    }

    void Add(const PreprocessedQueryStats& other) {
      for (int qid = 1; qid <= 14; ++qid) {
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
        : previous(SnbV1GraphDbTest::tls_load_resource_override_) {
      SnbV1GraphDbTest::tls_load_resource_override_ = resource;
    }

    ~ScopedLoadArenaResource() {
      SnbV1GraphDbTest::tls_load_resource_override_ = previous;
    }

    std::pmr::memory_resource* previous = nullptr;
  };

  prechunk::LoaderOptions BuildPreprocessedLoaderOptions() const {
    prechunk::LoaderOptions options;
    options.root = FLAGS_snb_v1_preprocessed_root.empty()
                       ? FLAGS_snb_v1_dataset_root
                       : FLAGS_snb_v1_preprocessed_root;
    options.threads = FLAGS_snb_v1_loader_threads;
    options.loader_cpu_base = FLAGS_snb_v1_loader_cpu_base;
    options.db_cpu_base = FLAGS_snb_v1_db_cpu_base;
    options.arena_gb = FLAGS_snb_v1_load_arena_gb;
    options.queue_blocks = FLAGS_snb_v1_loader_queue_blocks;
    options.prefill_blocks = FLAGS_snb_v1_loader_prefill_blocks;
    options.block_records = FLAGS_snb_v1_loader_block_records;
    options.skip_node_update = FLAGS_snb_v1_skip_updates;
    options.skip_edge_update = FLAGS_snb_v1_skip_updates;
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
    if (name == "Place") return NodeKind::kPlace;
    if (name == "Organisation") return NodeKind::kOrganisation;
    if (name == "Tag") return NodeKind::kTag;
    if (name == "TagClass") return NodeKind::kTagClass;
    if (name == "Person") return NodeKind::kPerson;
    if (name == "Forum") return NodeKind::kForum;
    if (name == "Post") return NodeKind::kPost;
    if (name == "Comment") return NodeKind::kComment;
    return std::nullopt;
  }

  static uint8_t EdgeTypeFromStringForPreprocessed(const std::string& name) {
    static const std::unordered_map<std::string, uint8_t> map = {
        {"PersonKnowsPerson", kPersonKnowsPerson},
        {"ForumHasMemberPerson", kForumHasMemberPerson},
        {"ForumHasTagTag", kForumHasTagTag},
        {"PersonHasInterestTag", kPersonHasInterestTag},
        {"PersonLikesPost", kPersonLikesPost},
        {"PersonLikesComment", kPersonLikesComment},
        {"PersonStudyAtOrganisation", kPersonStudyAtOrganisation},
        {"PersonWorkAtOrganisation", kPersonWorkAtOrganisation},
        {"PostHasTagTag", kPostHasTagTag},
        {"CommentHasTagTag", kCommentHasTagTag},
        {"PostCreatorPerson", kPostCreatorPerson},
        {"PostContainerForum", kPostContainerForum},
        {"PostLocationPlace", kPostLocationPlace},
        {"CommentCreatorPerson", kCommentCreatorPerson},
        {"CommentLocationPlace", kCommentLocationPlace},
        {"CommentParentPost", kCommentParentPost},
        {"CommentParentComment", kCommentParentComment},
        {"TagTypeTagClass", kTagTypeTagClass},
        {"TagClassSubclassTagClass", kTagClassSubclassTagClass},
        {"PlacePartOfPlace", kPlacePartOfPlace},
        {"OrganisationLocationPlace", kOrganisationLocationPlace},
        {"ForumModeratorPerson", kForumModeratorPerson},
        {"PersonLocationPlace", kPersonLocationPlace},
    };
    const auto it = map.find(name);
    return it == map.end() ? 0 : it->second;
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

  vertex_t ResolveOrAllocatePreprocessed(NodeKind kind,
                                         const std::string& raw) {
    if (raw.empty() || raw == "-1") {
      return lsmgraph::INVALID_VERTEX_ID;
    }
    const uint64_t raw_id = ToUint64(raw);
    const size_t idx = KindIndex(kind);
    auto& fast = preprocessed_id_index_[idx];
    const auto found = fast.find(raw_id);
    if (found != fast.end()) {
      return found->second;
    }
    vertex_t vid = lsmgraph::INVALID_VERTEX_ID;
    if (id_maps_sorted_[idx]) {
      vid = AllocateVertexId(kind, raw_id);
    } else {
      vid = next_vertex_id_++;
      entity_to_vid_[idx].push_back(IdPair{raw_id, vid});
      EnsureVertexVectors(vid);
      vid_to_kind_[static_cast<size_t>(vid)] = kind;
      hot_nodes_.raw_id[static_cast<size_t>(vid)] = raw_id;
    }
    fast.emplace(raw_id, vid);
    return vid;
  }

  vertex_t LookupPreprocessedVertexId(NodeKind kind,
                                      const std::string& raw) const {
    if (raw.empty() || raw == "-1") {
      return lsmgraph::INVALID_VERTEX_ID;
    }
    const uint64_t raw_id = ToUint64(raw);
    const size_t idx = KindIndex(kind);
    const auto found = preprocessed_id_index_[idx].find(raw_id);
    return found == preprocessed_id_index_[idx].end()
               ? lsmgraph::INVALID_VERTEX_ID
               : found->second;
  }

  void ResetPreprocessedState() {
    ReleaseVectorMemory(&loaded_node_tables_);
    ReleaseVectorMemory(&loaded_param_files_);
    entity_to_vid_ = {};
    id_maps_sorted_.fill(false);
    vid_to_kind_.clear();
    creation_by_vid_.clear();
    name_to_vertices_.clear();
    hot_nodes_.Clear();
    next_vertex_id_ = 0;
    preprocessed_edge_slot_need_.clear();
    preprocessed_edge_slot_.clear();
    for (auto& index : preprocessed_id_index_) {
      index.clear();
    }
    single_edge_sampler_.Reset(FLAGS_snb_v1_single_edge_candidate_cap,
                               FLAGS_snb_v1_single_edge_hot_weight,
                               FLAGS_snb_v1_single_edge_cold_weight,
                               FLAGS_snb_v1_single_edge_seed);
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
    ResolveOrAllocatePreprocessed(*kind, row.Get(schema.Metadata("id_column")));
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
    ResolveOrAllocatePreprocessed(*src_kind, row.Get(schema.Metadata("src_column")));
    ResolveOrAllocatePreprocessed(*dst_kind, row.Get(schema.Metadata("dst_column")));
  }

  void IndexPreprocessedSingleEdgeRequest(const prechunk::Record& record) {
    if (record.fields.size() < 8) {
      return;
    }
    const std::string_view property = prechunk::FieldView(record, 6);
    if (property == "classYear" ||
        !IsEdgeColdBlobProperty(std::string(property))) {
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
    if (property == "classYear" ||
        !IsEdgeColdBlobProperty(std::string(property))) {
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
        } else if (record.op_code == 5) {
          IndexPreprocessedSingleEdgeRequest(record);
        } else if (record.op_code == 7) {
          IndexPreprocessedEdgeUpdateRequest(record);
        }
      }
    }
    const auto sort_t1 = std::chrono::steady_clock::now();
    SortAllIdMaps();
    const auto sort_t2 = std::chrono::steady_clock::now();
    std::cout << "[PREPROCESSED_INDEX_SORT] time(s): "
              << std::chrono::duration_cast<std::chrono::duration<double>>(
                     sort_t2 - sort_t1)
                     .count()
              << std::endl;
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
      if (query_id == 0 || query_id > 14) {
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

  std::unordered_map<std::string, std::string> PreprocessedPropsFromRow(
      const prechunk::SchemaInfo& schema,
      const CsvRow& row) const {
    std::unordered_map<std::string, std::string> props;
    const std::string src_col = schema.Metadata("src_column");
    const std::string dst_col = schema.Metadata("dst_column");
    for (const auto& col : schema.columns) {
      if (col == src_col || col == dst_col || col == "src_type" ||
          col == "src_id" || col == "dst_type" || col == "dst_id") {
        continue;
      }
      const std::string value = row.Get(col);
      if (!value.empty()) {
        props[col] = value;
      }
    }
    props.emplace("edgeExists", "1");
    return props;
  }

  void AppendPreprocessedNodeRecord(PreparedImportBatch* batch,
                                    const prechunk::SchemaInfo& schema,
                                    const prechunk::Record& record) {
    const auto kind = NodeKindFromStringForPreprocessed(
        schema.Metadata("node_type"));
    if (!kind.has_value()) {
      return;
    }
    CsvHeader header;
    CsvRow row = BuildPreprocessedRow(schema, record, &header);
    const std::string raw_id = row.Get(schema.Metadata("id_column"));
    const vertex_t vid = LookupPreprocessedVertexId(*kind, raw_id);
    if (vid == lsmgraph::INVALID_VERTEX_ID) {
      return;
    }
    std::unordered_map<std::string, std::string> props;
    for (const auto& col : schema.columns) {
      if (col == schema.Metadata("id_column")) {
        continue;
      }
      const std::string value = row.Get(col);
      if (!value.empty()) {
        props[col] = value;
      }
    }
    batch->node_writes.push_back(
        BuildPreparedNodeWriteFromProps(vid,
                                        *kind,
                                        raw_id,
                                        props,
                                        record.event_time_ms,
                                        LoadArenaResource()));
    ++batch->logical_rows;
  }

  void CapturePreprocessedGeneratedColdSlots(
      const prechunk::SchemaInfo& schema,
      const CsvRow& row,
      const std::unordered_map<std::string, std::string>& props) {
    if (preprocessed_edge_slot_need_.empty()) {
      return;
    }
    const std::string edge_type = schema.Metadata("edge_type");
    const std::string src_type = schema.Metadata("src_type");
    const std::string dst_type = schema.Metadata("dst_type");
    const std::string src_id = row.Get(schema.Metadata("src_column"));
    const std::string dst_id = row.Get(schema.Metadata("dst_column"));
    const uint8_t edge_type_code = EdgeTypeFromStringForPreprocessed(edge_type);
    auto effective_props = props;
    if (edge_type_code != 0) {
      effective_props.emplace("edgeTypeCode", EdgeTypeCode(edge_type_code));
    }
    const auto creation_it = effective_props.find("creationDate");
    if (creation_it != effective_props.end() && !creation_it->second.empty()) {
      const uint64_t creation = ToUint64(creation_it->second);
      effective_props.emplace("eventMonth", EventMonth(creation));
      effective_props.emplace("eventDow", EventDow(creation));
    }
    const std::vector<std::string> cold_props =
        SortedEdgeColdBlobProperties(effective_props);
    for (size_t i = 0; i < cold_props.size(); ++i) {
      const std::string key = PreprocessedEdgeSlotKey(edge_type,
                                                      src_type,
                                                      src_id,
                                                      dst_type,
                                                      dst_id,
                                                      cold_props[i]);
      if (preprocessed_edge_slot_need_.find(key) !=
          preprocessed_edge_slot_need_.end()) {
        std::lock_guard<std::mutex> lock(preprocessed_edge_slot_mu_);
        preprocessed_edge_slot_[key] = static_cast<uint16_t>(i + 1);
      }
    }
  }

  void AppendPreprocessedEdgeRecord(PreparedImportBatch* batch,
                                    const prechunk::SchemaInfo& schema,
                                    const prechunk::Record& record) {
    const auto src_kind = NodeKindFromStringForPreprocessed(
        schema.Metadata("src_type"));
    const auto dst_kind = NodeKindFromStringForPreprocessed(
        schema.Metadata("dst_type"));
    const uint8_t edge_type =
        EdgeTypeFromStringForPreprocessed(schema.Metadata("edge_type"));
    if (!src_kind.has_value() || !dst_kind.has_value() || edge_type == 0) {
      return;
    }
    CsvHeader header;
    CsvRow row = BuildPreprocessedRow(schema, record, &header);
    const vertex_t src =
        LookupPreprocessedVertexId(*src_kind, row.Get(schema.Metadata("src_column")));
    const vertex_t dst =
        LookupPreprocessedVertexId(*dst_kind, row.Get(schema.Metadata("dst_column")));
    if (src == lsmgraph::INVALID_VERTEX_ID || dst == lsmgraph::INVALID_VERTEX_ID) {
      return;
    }
    const auto props = PreprocessedPropsFromRow(schema, row);
    batch->edge_writes.push_back(
        BuildEdgeWrite(src,
                       dst,
                       edge_type,
                       props,
                       record.event_time_ms,
                       false,
                       false));
    CapturePreprocessedGeneratedColdSlots(schema, row, props);
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
    out->write_batch.edge_writes.reserve(raw->records.size());
    for (const auto& record : raw->records) {
      const prechunk::SchemaInfo* schema = catalog.Find(record.schema_id);
      if (schema == nullptr) {
        continue;
      }
      if (record.op_code == 1 && schema->kind == "node") {
        AppendPreprocessedNodeRecord(&out->write_batch, *schema, record);
      } else if (record.op_code == 2 && schema->kind == "edge") {
        AppendPreprocessedEdgeRecord(&out->write_batch, *schema, record);
      }
    }
    out->has_write_batch = true;
    if (raw->stage == prechunk::Stage::kMixedOps &&
        FLAGS_snb_v1_mix_enable_queries) {
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
      for (auto& write : chunk->write_batch.edge_writes) {
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
    for (int qid = 1; qid <= 14; ++qid) {
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
    stats.logical_update_rows += import_stats.logical_rows;
    stats.node_writes += import_stats.node_writes;
    stats.edge_writes += import_stats.edge_writes;
    stats.skipped_edges += import_stats.skipped_edges;
    stats.write_sec += import_stats.sec;
    return stats;
  }

  static void AccumulateMixedStats(MixedWorkloadStats* total,
                                   const MixedWorkloadStats& delta) {
    total->logical_update_rows += delta.logical_update_rows;
    total->node_writes += delta.node_writes;
    total->edge_writes += delta.edge_writes;
    total->skipped_edges += delta.skipped_edges;
    total->query_ops += delta.query_ops;
    total->result_rows += delta.result_rows;
    total->checksum ^= delta.checksum;
    total->total_sec += delta.total_sec;
    total->write_sec += delta.write_sec;
    total->query_sec += delta.query_sec;
    total->background_wait_sec += delta.background_wait_sec;
    total->wall_sec += delta.wall_sec;
    for (int qid = 1; qid <= 14; ++qid) {
      QueryMetrics& dst = total->query_metrics[static_cast<size_t>(qid)];
      const QueryMetrics& src = delta.query_metrics[static_cast<size_t>(qid)];
      dst.query_id = qid;
      dst.param_rows += src.param_rows;
      dst.result_rows += src.result_rows;
      dst.checksum ^= src.checksum;
      dst.sec += src.sec;
    }
  }

  PreprocessedQueryStats RunPreparedPreprocessedQueryTasks(
      const PreprocessedQueryTaskStorage& storage) {
    PreprocessedQueryStats stats;
    std::vector<QueryRunResult> results(storage.tasks.size());
    std::vector<double> secs(storage.tasks.size(), 0.0);
    ParallelForIndexDynamic(storage.tasks.size(), GetReadThreadCount(), [&](size_t i) {
      const auto q1 = std::chrono::steady_clock::now();
      results[i] = RunQuery(storage.tasks[i].query_id, *storage.tasks[i].row);
      const auto q2 = std::chrono::steady_clock::now();
      secs[i] = std::chrono::duration_cast<std::chrono::duration<double>>(
                    q2 - q1)
                    .count();
    });
    for (size_t i = 0; i < storage.tasks.size(); ++i) {
      const int qid = storage.tasks[i].query_id;
      QueryMetrics& metric = stats.query_metrics[static_cast<size_t>(qid)];
      ++metric.param_rows;
      metric.result_rows += results[i].rows;
      metric.checksum ^= HashMix(results[i].checksum + storage.tasks[i].ordinal);
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
    for (int qid = 1; qid <= 14; ++qid) {
      const QueryMetrics& metric =
          stats.query_metrics[static_cast<size_t>(qid)];
      total_params += metric.param_rows;
      total_rows += metric.result_rows;
      total_checksum ^= HashMix(metric.checksum + static_cast<uint64_t>(qid));
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
    for (int qid = 1; qid <= 14; ++qid) {
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
    total->skipped_edges += delta.skipped_edges;
    total->sec += delta.sec;
    total->background_wait_sec += delta.background_wait_sec;
    total->wall_sec += delta.wall_sec;
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
      const auto vid = LookupVertexId(*kind, fields[2]);
      if (!vid.has_value()) {
        miss_reasons[i] = "missing_node:" + fields[1];
        return;
      }
      std::string value = ReadPreprocessedSingleNodeProperty(
          *vid, *kind, fields[1], fields[2], fields[3]);
      if (value.empty()) {
        miss_reasons[i] = "unmapped_or_empty_node_property:" + fields[1] +
                          "." + fields[3];
        return;
      }
      found[i] = 1;
      checksums[i] = HashMix(static_cast<uint64_t>(*vid)) ^
                     HashMix(std::hash<std::string>{}(fields[3])) ^
                     HashMix(std::hash<std::string>{}(value));
    });
    const auto t2 = std::chrono::steady_clock::now();
    stats.sec =
        std::chrono::duration_cast<std::chrono::duration<double>>(t2 - t1)
            .count();
    for (size_t i = 0; i < records.size(); ++i) {
      ++stats.ops;
      if (hot[i] != 0) ++stats.hot_ops; else ++stats.cold_ops;
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

  std::string PreprocessedImplicitNodeEdgeTarget(vertex_t src,
                                                 uint8_t edge_type,
                                                 bool is_out) {
    std::string out;
    ForEachEdge(src,
                edge_type,
                is_out,
                "edgeExists",
                [&](vertex_t dst, const std::string& /*value*/) {
                  out = RawId(dst);
                  return false;
                });
    return out;
  }

  std::string ReadPreprocessedSingleNodeProperty(vertex_t vid,
                                                 NodeKind kind,
                                                 const std::string& type_name,
                                                 const std::string& raw_id,
                                                 const std::string& property) {
    if (property == "id" || property == "rawId" ||
        property == type_name + ".id") {
      return raw_id;
    }
    if (property == "nodeLabel") {
      return NodeKindToString(kind);
    }
    std::string value = GetNodeProp(vid, property);
    if (!value.empty()) {
      return value;
    }
    switch (kind) {
      case NodeKind::kPlace:
        if (property == "isPartOf") {
          return PreprocessedImplicitNodeEdgeTarget(vid, kPlacePartOfPlace, true);
        }
        break;
      case NodeKind::kOrganisation:
        if (property == "place") {
          return PreprocessedImplicitNodeEdgeTarget(vid, kOrganisationLocationPlace, true);
        }
        break;
      case NodeKind::kTag:
        if (property == "hasType") {
          return PreprocessedImplicitNodeEdgeTarget(vid, kTagTypeTagClass, true);
        }
        break;
      case NodeKind::kTagClass:
        if (property == "isSubclassOf") {
          return PreprocessedImplicitNodeEdgeTarget(
              vid, kTagClassSubclassTagClass, true);
        }
        break;
      case NodeKind::kForum:
        if (property == "moderator") {
          return PreprocessedImplicitNodeEdgeTarget(
              vid, kForumModeratorPerson, true);
        }
        break;
      case NodeKind::kPost:
        if (property == "creator") {
          return PreprocessedImplicitNodeEdgeTarget(vid, kPostCreatorPerson, true);
        }
        if (property == "Forum.id") {
          return PreprocessedImplicitNodeEdgeTarget(
              vid, kPostContainerForum, true);
        }
        if (property == "place") {
          return PreprocessedImplicitNodeEdgeTarget(
              vid, kPostLocationPlace, true);
        }
        break;
      case NodeKind::kComment:
        if (property == "creator") {
          return PreprocessedImplicitNodeEdgeTarget(
              vid, kCommentCreatorPerson, true);
        }
        if (property == "place") {
          return PreprocessedImplicitNodeEdgeTarget(
              vid, kCommentLocationPlace, true);
        }
        if (property == "replyOfPost") {
          return PreprocessedImplicitNodeEdgeTarget(
              vid, kCommentParentPost, true);
        }
        if (property == "replyOfComment") {
          return PreprocessedImplicitNodeEdgeTarget(
              vid, kCommentParentComment, true);
        }
        break;
      case NodeKind::kPerson:
        if (property == "place") {
          return PreprocessedImplicitNodeEdgeTarget(
              vid, kPersonLocationPlace, true);
        }
        break;
      default:
        break;
    }
    return {};
  }

  bool ReadPreprocessedSingleEdgeProperty(const std::vector<std::string>& fields,
                                          std::string* value,
                                          std::string* miss_reason) {
    if (fields.size() < 8 || value == nullptr) {
      if (miss_reason != nullptr) {
        *miss_reason = "bad_single_edge_record";
      }
      return false;
    }
    const uint8_t edge_type = EdgeTypeFromStringForPreprocessed(fields[1]);
    const auto src_kind = NodeKindFromStringForPreprocessed(fields[2]);
    const auto dst_kind = NodeKindFromStringForPreprocessed(fields[4]);
    if (edge_type == 0 || !src_kind.has_value() || !dst_kind.has_value()) {
      if (miss_reason != nullptr) {
        *miss_reason = "unknown_edge_endpoint_type:" + fields[1];
      }
      return false;
    }
    const std::string& property = fields[6];
    if (property == "edgeExists") {
      *value = "1";
      return true;
    }
    if (property == "edgeTypeCode") {
      *value = EdgeTypeCode(edge_type);
      return true;
    }
    if (property == "src_id" || property == fields[2] + ".id") {
      *value = fields[3];
      return true;
    }
    if (property == "dst_id" || property == fields[4] + ".id") {
      *value = fields[5];
      return true;
    }
    auto src = LookupVertexId(*src_kind, fields[3]);
    auto dst = LookupVertexId(*dst_kind, fields[5]);
    if (!src.has_value() || !dst.has_value()) {
      if (miss_reason != nullptr) {
        *miss_reason = "missing_edge_endpoint:" + fields[1];
      }
      return false;
    }
    if (property == "messageLengthBucket") {
      vertex_t message = lsmgraph::INVALID_VERTEX_ID;
      if (*src_kind == NodeKind::kPost || *src_kind == NodeKind::kComment) {
        message = *src;
      } else if (*dst_kind == NodeKind::kPost ||
                 *dst_kind == NodeKind::kComment) {
        message = *dst;
      }
      if (message != lsmgraph::INVALID_VERTEX_ID) {
        const std::string length = GetNodeProp(message, "length");
        if (!length.empty()) {
          *value = std::to_string(LengthBucket(ToUint64(length)));
          return true;
        }
      }
    }
    vertex_t read_src = *src;
    vertex_t read_dst = *dst;
    bool is_out = true;
    if (ShouldWriteReverseOnly(edge_type)) {
      read_src = *dst;
      read_dst = *src;
      is_out = false;
    }
    std::unordered_map<std::string, std::string> props;
    auto rs = db_->GetEdge(read_src,
                           read_dst,
                           {property},
                           &props,
                           is_out,
                           edge_type);
    if (rs == lsmgraph::Status::kOk) {
      const auto it = props.find(property);
      if (it != props.end() && !it->second.empty()) {
        *value = it->second;
        return true;
      }
    }
    std::unordered_map<std::string, std::string> ref_props;
    rs = db_->GetEdge(read_src,
                      read_dst,
                      {"cold_property"},
                      &ref_props,
                      is_out,
                      edge_type);
    if (rs != lsmgraph::Status::kOk) {
      if (miss_reason != nullptr) {
        *miss_reason = "missing_cold_ref_edge:" + fields[1];
      }
      return false;
    }
    const auto ref_it = ref_props.find("cold_property");
    if (ref_it == ref_props.end() || ref_it->second.empty()) {
      if (miss_reason != nullptr) {
        *miss_reason = "empty_cold_ref_edge:" + fields[1];
      }
      return false;
    }
    const std::string payload = ReadColdBlobPayloadNoCache(ref_it->second);
    if (payload.empty()) {
      if (miss_reason != nullptr) {
        *miss_reason = "empty_cold_blob:" + fields[1];
      }
      return false;
    }
    int slot = -1;
    if (property == "classYear") {
      slot = 0;
    } else if (IsEdgeColdBlobProperty(property)) {
      const std::string key = PreprocessedEdgeSlotKey(fields[1],
                                                      fields[2],
                                                      fields[3],
                                                      fields[4],
                                                      fields[5],
                                                      property);
      const auto it = preprocessed_edge_slot_.find(key);
      if (it != preprocessed_edge_slot_.end()) {
        slot = it->second;
      }
    }
    if (slot < 0) {
      if (miss_reason != nullptr) {
        *miss_reason = "unmapped_cold_edge_property:" + fields[1] + "." +
                       property;
      }
      return false;
    }
    if (!GetPipeFieldBySlot(payload, static_cast<uint16_t>(slot), value) ||
        value->empty()) {
      if (miss_reason != nullptr) {
        *miss_reason = "empty_cold_edge_property:" + fields[1] + "." +
                       property;
      }
      return false;
    }
    return true;
  }

  PreprocessedReadStats RunPreparedSingleEdgeReadChunk(
      const std::vector<std::vector<std::string>>& records) {
    PreprocessedReadStats stats;
    std::vector<uint64_t> checksums(records.size(), 0);
    std::vector<uint8_t> found(records.size(), 0);
    std::vector<uint8_t> hot(records.size(), 0);
    std::vector<std::string> miss_reasons(records.size());
    const auto t1 = std::chrono::steady_clock::now();
    ParallelForIndexDynamic(records.size(), GetReadThreadCount(), [&](size_t i) {
      const std::vector<std::string>& fields = records[i];
      if (fields.size() < 8) {
        miss_reasons[i] = "bad_single_edge_record";
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
      if (hot[i] != 0) ++stats.hot_ops; else ++stats.cold_ops;
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

  static uint32_t SnbNodeLogicalPropertyId(const std::string& name) {
    const auto& props = AllNodeProperties();
    const auto it = std::find(props.begin(), props.end(), name);
    if (it == props.end()) {
      return std::numeric_limits<uint32_t>::max();
    }
    return static_cast<uint32_t>(std::distance(props.begin(), it));
  }

  static uint32_t GeneratedColdPropertyOrdinal(std::string_view name) {
    if (!IsGeneratedColdProperty(name)) {
      return 0;
    }
    const std::string suffix(name.substr(kGeneratedColdPrefix.size()));
    if (!IsDigitsOnly(suffix)) {
      return 0;
    }
    return static_cast<uint32_t>(ToUint64(suffix));
  }

  static uint32_t SnbEdgeLogicalPropertyId(const std::string& name) {
    const auto& props = AllEdgeProperties();
    const auto it = std::find(props.begin(), props.end(), name);
    if (it != props.end()) {
      return static_cast<uint32_t>(std::distance(props.begin(), it));
    }
    if (name == "classYear") {
      return static_cast<uint32_t>(props.size());
    }
    if (IsGeneratedColdProperty(name)) {
      return static_cast<uint32_t>(props.size() + 1U +
                                   GeneratedColdPropertyOrdinal(name));
    }
    return std::numeric_limits<uint32_t>::max();
  }

  bool ResolveSnbNodeUpdateProperty(const std::string& property,
                                    LightNodeUpdate* update) const {
    if (update == nullptr) {
      return false;
    }
    update->logical_property_id = SnbNodeLogicalPropertyId(property);
    if (update->logical_property_id == std::numeric_limits<uint32_t>::max()) {
      return false;
    }
    const auto hot_it = NodePropertyIndexByName().find(property);
    if (hot_it != NodePropertyIndexByName().end()) {
      update->storage_property = property;
      update->storage_property_id = static_cast<uint32_t>(hot_it->second);
      update->cold = false;
      update->cold_slot = 0;
      return true;
    }
    const auto cold_it = ColdNodePropertyIndex().find(property);
    if (cold_it != ColdNodePropertyIndex().end()) {
      update->storage_property = "node_cold_property";
      update->storage_property_id = NodeColdRefSlot();
      update->cold_slot = static_cast<uint16_t>(cold_it->second);
      update->cold = true;
      return true;
    }
    return false;
  }

  bool ResolveSnbEdgeUpdateProperty(const std::string& property,
                                    LightEdgeUpdate* update) const {
    if (update == nullptr || property == "edgeExists") {
      return false;
    }
    update->logical_property_id = SnbEdgeLogicalPropertyId(property);
    if (update->logical_property_id == std::numeric_limits<uint32_t>::max()) {
      return false;
    }
    const auto shard0_it = EdgeShard0PropertyIndex().find(property);
    if (shard0_it != EdgeShard0PropertyIndex().end()) {
      update->storage_property = property;
      update->storage_property_id = static_cast<uint32_t>(shard0_it->second);
      update->cold = false;
      update->cold_slot = 0;
      return true;
    }
    const auto shard1_it = EdgeShard1PropertyIndex().find(property);
    if (shard1_it != EdgeShard1PropertyIndex().end()) {
      update->storage_property = property;
      update->storage_property_id = static_cast<uint32_t>(shard1_it->second);
      update->cold = false;
      update->cold_slot = 0;
      return true;
    }
    const auto shard2_it = EdgeShard2PropertyIndex().find(property);
    if (shard2_it != EdgeShard2PropertyIndex().end()) {
      update->storage_property = property;
      update->storage_property_id = static_cast<uint32_t>(shard2_it->second);
      update->cold = false;
      update->cold_slot = 0;
      return true;
    }
    if (property == "classYear" || IsGeneratedColdProperty(property)) {
      const auto cold_ref_it = EdgeShard3PropertyIndex().find("cold_property");
      if (cold_ref_it == EdgeShard3PropertyIndex().end()) {
        return false;
      }
      update->storage_property = "cold_property";
      update->storage_property_id = static_cast<uint32_t>(cold_ref_it->second);
      update->cold = true;
      update->cold_slot = property == "classYear" ? 0 : 1;
      return true;
    }
    return false;
  }

  bool ResolvePreprocessedNodeUpdate(const std::vector<std::string>& fields,
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
    const auto vid = LookupVertexId(*kind, fields[2]);
    if (!vid.has_value()) {
      ++stats->missing_targets;
      return false;
    }
    update->vid = *vid;
    const std::string property = fields[3];
    if (!ResolveSnbNodeUpdateProperty(property, update)) {
      ++stats->missing_targets;
      return false;
    }
    update->value = NormalizeValueForProperty(property, fields[5]);
    return true;
  }

  bool ResolvePreprocessedEdgeUpdate(const std::vector<std::string>& fields,
                                     LightEdgeUpdate* update,
                                     UpdateWorkloadStats* stats) {
    if (fields.size() < 9 || update == nullptr || stats == nullptr) {
      return false;
    }
    const uint8_t edge_type = EdgeTypeFromStringForPreprocessed(fields[1]);
    const auto src_kind = NodeKindFromStringForPreprocessed(fields[2]);
    const auto dst_kind = NodeKindFromStringForPreprocessed(fields[4]);
    if (edge_type == 0 || !src_kind.has_value() || !dst_kind.has_value()) {
      ++stats->missing_targets;
      return false;
    }
    auto src = LookupVertexId(*src_kind, fields[3]);
    auto dst = LookupVertexId(*dst_kind, fields[5]);
    if (!src.has_value() || !dst.has_value()) {
      ++stats->missing_targets;
      return false;
    }
    update->src = *src;
    update->dst = *dst;
    update->edge_type = edge_type;
    update->is_out = true;
    if (ShouldWriteReverseOnly(edge_type)) {
      std::swap(update->src, update->dst);
      update->is_out = false;
    }
    const std::string property = fields[6];
    if (!ResolveSnbEdgeUpdateProperty(property, update)) {
      ++stats->missing_targets;
      return false;
    }
    if (update->cold && property != "classYear") {
      const std::string key = PreprocessedEdgeSlotKey(fields[1],
                                                      fields[2],
                                                      fields[3],
                                                      fields[4],
                                                      fields[5],
                                                      property);
      const auto slot_it = preprocessed_edge_slot_.find(key);
      if (slot_it == preprocessed_edge_slot_.end()) {
        ++stats->missing_targets;
        return false;
      }
      update->cold_slot = slot_it->second;
    }
    update->value = NormalizeValueForProperty(property, fields[8]);
    return true;
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
    const std::string old_ref = GetNodeProp(update->vid, "node_cold_property");
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
    const size_t min_slots =
        std::max<size_t>(1U, static_cast<size_t>(update->cold_slot) + 1U);
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
    const auto cold_ref_it = EdgeShard3PropertyIndex().find("cold_property");
    if (cold_ref_it == EdgeShard3PropertyIndex().end()) {
      return false;
    }
    update->storage_property_id = static_cast<uint32_t>(cold_ref_it->second);
    stats->blob_payload_bytes += new_payload.size();
    ++stats->cold_blob_rewrites;
    return true;
  }

  void InitEngineUpdateState(EngineUpdateRunState* state,
                             const std::string& phase) {
    if (state == nullptr || state->started) {
      return;
    }
    state->phase = phase;
    state->initial_engine_stats = db_->GetPropertyUpdateStats();
    // Cold properties live in append-only blob files. Make imported payloads
    // visible before translating a logical cold-property update. Property
    // buffering and delta publication are owned exclusively by GraphDb.
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
    InitEngineUpdateState(state, "SNB_UPDATE");
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
    InitEngineUpdateState(state, "SNB_UPDATE");
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
    if (state == nullptr || !state->started) {
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
    std::cout << "[" << phase << "] property_buffer_flushes: "
              << stats.flushes
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
    std::cout << "[" << phase << "] qps(update/s): " << qps << std::endl;
    std::cout << "[" << phase << "] node_qps(update/s): " << node_qps
              << std::endl;
    std::cout << "[" << phase << "] edge_qps(update/s): " << edge_qps
              << std::endl;
  }

  int RunPreprocessed() {
    const prechunk::LoaderOptions options = BuildPreprocessedLoaderOptions();
    std::cout << "=== test_graphdb_snb_v1 preprocessed ===" << std::endl;
    std::cout << "preprocessed_root: " << options.root << std::endl;
    std::cout << "db_path: " << FLAGS_snb_v1_db_path << std::endl;
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
    const std::string schema_path = FLAGS_snb_v1_db_path + "/snb_v1_schema.yaml";
    WriteSchemaFile(schema_path);
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
    write_latency_sampler_.Reset(FLAGS_snb_v1_write_latency_sample_target,
                                 std::max<uint64_t>(
                                     1ULL, index_stats.snapshot_edge_records),
                                 FLAGS_snb_v1_write_latency_sample_seed,
                                 std::max(GetWriteThreadCount(),
                                          GetMixedThreadCount()));
    node_write_latency_sampler_.Reset(
        FLAGS_snb_v1_write_latency_sample_target,
        std::max<uint64_t>(1ULL, index_stats.node_records),
        FLAGS_snb_v1_write_latency_sample_seed ^ 0x9e3779b97f4a7c15ULL,
        std::max(GetWriteThreadCount(), GetMixedThreadCount()));

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
	          ExecutePreparedPreprocessedWriteChunk(
	              chunk, sample_nodes, sample_edges));
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
	          PrintUpdateStatsWithPhase("SNB_NODE_UPDATE", node_update_stats);
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
	          PrintUpdateStatsWithPhase("SNB_EDGE_UPDATE", edge_update_stats);
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
	              FLAGS_snb_v1_skip_single_edge_read) {
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
              if (!flush_blobs_for_reads()) return false;
              single_node_stats.Add(
                  RunPreparedSingleNodeReadChunk(
                      chunk->single_node_read_fields));
              break;
            case prechunk::Stage::kSingleEdgeRead:
              if (!flush_blobs_for_reads()) return false;
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
                                                 "SNB_NODE_UPDATE");
              break;
            case prechunk::Stage::kFinbenchEdgeUpdate:
              RunPreparedPreprocessedUpdateChunk(*chunk,
                                                 &edge_update_state,
                                                 "SNB_EDGE_UPDATE");
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
	    std::cout << "test_graphdb_snb_v1 preprocessed path passed" << std::endl;
    return 0;
  }

  bool ValidateDatasetLayout() const {
    const std::array<std::string, 3> required = {
        FLAGS_snb_v1_dataset_root + "/static",
        FLAGS_snb_v1_dataset_root + "/dynamic",
        FLAGS_snb_v1_params_root,
    };
    for (const auto& path : required) {
      if (!FileOrDirExists(path)) {
        std::cerr << "missing dataset path: " << path << std::endl;
        return false;
      }
    }
    for (const auto& spec : NodeTables()) {
      if (!FileOrDirExists(NodeTablePath(spec))) {
        std::cerr << "missing node table: " << NodeTablePath(spec) << std::endl;
        return false;
      }
    }
    for (const auto& spec : EdgeTables()) {
      if (!FileOrDirExists(EdgeTablePath(spec))) {
        std::cerr << "missing edge table: " << EdgeTablePath(spec) << std::endl;
        return false;
      }
    }
    if (FLAGS_snb_v1_import_update_stream) {
      for (const auto& stem : {"person", "forum"}) {
        const std::string path = UpdateStreamPath(stem);
        if (!FileOrDirExists(path)) {
          std::cerr << "missing update stream: " << path << std::endl;
          return false;
        }
      }
    }
    return true;
  }

  bool PrepareDbDirectory() const {
    std::error_code ec;
    if (FLAGS_snb_v1_reset_db) {
      std::filesystem::remove_all(FLAGS_snb_v1_db_path, ec);
      if (ec) {
        std::cerr << "remove_all failed for path=" << FLAGS_snb_v1_db_path
                  << ", ec=" << ec.message() << std::endl;
        return false;
      }
    }
    std::filesystem::create_directories(FLAGS_snb_v1_db_path, ec);
    if (ec) {
      std::cerr << "create_directories failed for path=" << FLAGS_snb_v1_db_path
                << ", ec=" << ec.message() << std::endl;
      return false;
    }
    return true;
  }

  void ConfigureLowLevelFlags() const {
    FLAGS_support_mulversion = true;
    FLAGS_LOAD_OLD_DATA = false;
    FLAGS_OPEN_SSTDATA_CACHE = true;
    FLAGS_thread_num = FLAGS_snb_v1_system_threads;
    FLAGS_memtable_num = FLAGS_snb_v1_memtable_num;
    FLAGS_memtable_size = FLAGS_snb_v1_memtable_size;
    FLAGS_max_subcompactions = FLAGS_snb_v1_max_subcompactions;
    FLAGS_max_property_length = FLAGS_snb_v1_max_property_length;
    FLAGS_db_path = FLAGS_snb_v1_db_path;
  }

  uint32_t NodeMemtableSize() const {
    return FLAGS_snb_v1_node_memtable_size == 0
               ? FLAGS_snb_v1_memtable_size
               : FLAGS_snb_v1_node_memtable_size;
  }

  std::vector<uint32_t> EdgeMemtableSizes() const {
    return ParseMemtableSizeCsv(FLAGS_snb_v1_edge_memtable_sizes,
                                4,
                                FLAGS_snb_v1_memtable_size,
                                "snb_v1_edge_memtable_sizes");
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
    if (FLAGS_snb_v1_max_vertex_num > 0) {
      return FLAGS_snb_v1_max_vertex_num;
    }
    return std::max<uint64_t>(next_vertex_id_ + 1024ULL, 4096ULL);
  }

  std::string UpdateStreamPath(const std::string& stem) const {
    return FLAGS_snb_v1_dataset_root + "/updateStream_0_0_" + stem + ".csv";
  }

  std::string ColdBlobPathPrefix() const {
    return FLAGS_snb_v1_db_path + "/snb_v1_cold_property";
  }

  std::string NodeColdBlobPathPrefix() const {
    return FLAGS_snb_v1_db_path + "/snb_v1_node_cold_property";
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

  uint32_t GetWriteThreadCount() const {
    return std::max<uint32_t>(1U, FLAGS_snb_v1_write_threads);
  }

  uint32_t GetReadThreadCount() const {
    return std::max<uint32_t>(1U, FLAGS_snb_v1_read_threads);
  }

  uint32_t GetMixedThreadCount() const {
    return std::max<uint32_t>(1U, FLAGS_snb_v1_mixed_threads);
  }

  bool SingleEdgeReadEnabled() const {
    return FLAGS_snb_v1_single_edge_read_ops > 0 &&
           FLAGS_snb_v1_single_edge_candidate_cap > 0;
  }

  uint64_t EstimateImplicitEdgeWritesForNodeTable(
      const NodeTableSpec& spec,
      uint64_t estimated_rows) const {
    switch (spec.kind) {
      case NodeKind::kPlace:
      case NodeKind::kOrganisation:
      case NodeKind::kTag:
      case NodeKind::kTagClass:
      case NodeKind::kPerson:
      case NodeKind::kForum:
        return estimated_rows;
      case NodeKind::kPost:
        return estimated_rows * 3ULL;
      case NodeKind::kComment:
        return estimated_rows * 3ULL;
    }
    return 0;
  }

  uint64_t CountNonEmptyRows(const std::string& path, bool has_header) const {
    std::ifstream in;
    std::vector<char> buffer;
    if (!OpenCsvInput(path, &in, &buffer)) {
      return EstimateCsvRowsBySize(path, 128);
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

  uint64_t CountMixedUpdateRows() const {
    return CountSelectedUpdateRows(true);
  }

  uint32_t WorkloadSampleMod() const {
    return std::max<uint32_t>(1, FLAGS_snb_v1_workload_sample_mod);
  }

  uint32_t WorkloadSampleRemainder() const {
    return FLAGS_snb_v1_workload_sample_remainder % WorkloadSampleMod();
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
      if (FLAGS_snb_v1_param_limit_per_query > 0 &&
          indices.size() >= FLAGS_snb_v1_param_limit_per_query) {
        break;
      }
    }
    return indices;
  }

  uint64_t CountSelectedUpdateRowsInFile(const std::string& path,
                                         bool sampled) const {
    std::ifstream in;
    std::vector<char> buffer;
    if (!OpenCsvInput(path, &in, &buffer)) {
      return 0;
    }
    uint64_t row_index = 0;
    uint64_t selected = 0;
    std::string line;
    while (std::getline(in, line)) {
      if (line.empty()) {
        continue;
      }
      if (FLAGS_snb_v1_update_row_limit_per_file > 0 &&
          row_index >= FLAGS_snb_v1_update_row_limit_per_file) {
        break;
      }
      if (SelectWorkloadRow(row_index, sampled)) {
        ++selected;
      }
      ++row_index;
    }
    return selected;
  }

  uint64_t CountSelectedUpdateRows(bool sampled) const {
    return CountSelectedUpdateRowsInFile(UpdateStreamPath("person"), sampled) +
           CountSelectedUpdateRowsInFile(UpdateStreamPath("forum"), sampled);
  }

  uint64_t EstimateSnbRelationWrites() const {
    uint64_t total = 0;
    for (const auto& spec : EdgeTables()) {
      total += CountNonEmptyRows(EdgeTablePath(spec), true);
    }
    for (const auto& spec : NodeTables()) {
      const uint64_t rows = CountNonEmptyRows(NodeTablePath(spec), true);
      total += EstimateImplicitEdgeWritesForNodeTable(spec, rows);
    }
	    return total;
	  }

  void ConfigureWriteLatencySampling() {
    const uint64_t estimated_total = EstimateSnbRelationWrites();
    write_latency_sampler_.Reset(FLAGS_snb_v1_write_latency_sample_target,
                                 estimated_total,
                                 FLAGS_snb_v1_write_latency_sample_seed,
                                 std::max(GetWriteThreadCount(),
                                          GetMixedThreadCount()));
    std::cout << "write_latency_estimated_total_writes: " << estimated_total
              << std::endl;
  }

  template <typename Fn>
  void ParallelForWriteIndex(size_t count, Fn&& fn) const {
    if (FLAGS_snb_v1_write_dynamic) {
      ParallelForIndexDynamicChunk(count,
                                   GetWriteThreadCount(),
                                   FLAGS_snb_v1_write_dynamic_chunk,
                                   fn);
      return;
    }
    ParallelForIndex(count, GetWriteThreadCount(), fn);
  }

  uint32_t PropertyLength(const std::string& name) const {
    static const std::unordered_map<std::string, uint32_t> lengths = {
        {"nodeLabel", 12}, {"rawId", 20}, {"name", 118}, {"url", 512},
        {"type", 10}, {"firstName", 36}, {"lastName", 19}, {"gender", 6},
        {"birthday", 13}, {"creationDate", 13}, {"locationIP", 15},
        {"browserUsed", 17}, {"place", 20}, {"language", 8},
        {"email", 238}, {"imageFile", 23}, {"content", 1997},
        {"length", 10}, {"title", 125}, {"node_cold_property", 32},
        {"edgeExists", 1}, {"joinDate", 13}, {"classYear", 4},
        {"workFrom", 4}, {"edgeTypeCode", 2}, {"eventMonth", 6},
        {"eventDow", 1}, {"srcCountryId", 3}, {"srcCityId", 5},
        {"dstCountryId", 3}, {"dstCityId", 5},
        {"srcActivityBucket", 2}, {"dstActivityBucket", 2},
        {"edgeWeight", 3}, {"interactionCnt", 3},
        {"sameCountry", 1}, {"sameCity", 1},
        {"tagClassId", 4}, {"tagPopularityBucket", 3},
        {"messageLengthBucket", 3},
        {"cold_property", 32},
    };
    const auto it = lengths.find(name);
    if (it != lengths.end()) {
      return it->second;
    }
    return FLAGS_snb_v1_max_property_length;
  }

  std::string NormalizeValueForProperty(const std::string& name,
                                        const std::string& value) const {
    (void)name;
    if (value.empty()) {
      return value;
    }
    return value;
  }

  void WriteSchemaFile(const std::string& schema_path) const {
    std::ofstream out(schema_path);
    assert(out.is_open());
    out << "max_vertex_num: " << DeriveMaxVertexNum() << "\n";
    out << "use_csr_disk: " << (FLAGS_snb_v1_use_csr_disk ? "true" : "false") << "\n";
    out << "max_property_length: " << FLAGS_snb_v1_max_property_length << "\n";
    out << "system_threads: " << FLAGS_snb_v1_system_threads << "\n";
    out << "property_defs:\n";
    std::set<std::string> props;
    props.insert(AllNodeProperties().begin(), AllNodeProperties().end());
    props.insert(NodeDbProperties().begin(), NodeDbProperties().end());
    props.insert(AllEdgeProperties().begin(), AllEdgeProperties().end());
    for (const auto& name : props) {
      out << "  - name: " << name << "\n";
      out << "    length: " << PropertyLength(name) << "\n";
    }
    out << "edge_shards:\n";
    const std::vector<std::reference_wrapper<const std::vector<std::string>>>
        edge_shards = {EdgeShard0Properties(), EdgeShard1Properties(),
                       EdgeShard2Properties(), EdgeShard3Properties()};
    const auto edge_memtable_sizes = EdgeMemtableSizes();
    for (size_t shard_idx = 0; shard_idx < edge_shards.size(); ++shard_idx) {
      const auto& shard_props = edge_shards[shard_idx].get();
      out << "  - name: edge_Db" << shard_idx << "\n";
      out << "    memtable_size: " << edge_memtable_sizes[shard_idx] << "\n";
      const bool shard_is_csr =
          FLAGS_snb_v1_enable_hot_edge_csr && shard_idx < 3;
      out << "    is_csr: " << (shard_is_csr ? "true" : "false") << "\n";
      out << "    csr_l0_max_sst_num: "
          << FLAGS_snb_v1_hot_edge_csr_l0_max_sst_num << "\n";
      out << "    csr_l1_max_sst_num: "
          << FLAGS_snb_v1_hot_edge_csr_l1_max_sst_num << "\n";
      out << "    properties: [";
      for (size_t i = 0; i < shard_props.size(); ++i) {
        if (i > 0) {
          out << ", ";
        }
        out << shard_props[i];
      }
      out << "]\n";
    }
    out << "node_db:\n";
    out << "  name: node_Db\n";
    out << "  memtable_size: " << NodeMemtableSize() << "\n";
    out << "  is_csr: false\n";
    out << "  csr_l0_max_sst_num: "
        << FLAGS_snb_v1_hot_edge_csr_l0_max_sst_num << "\n";
    out << "  csr_l1_max_sst_num: "
        << FLAGS_snb_v1_hot_edge_csr_l1_max_sst_num << "\n";
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
        std::max<uint32_t>(2U, FLAGS_snb_v1_memproperty_num);
    options.property_updates.buffer_capacity_records =
        static_cast<std::size_t>(std::max(
            FLAGS_snb_v1_update_node_memproperty_cap,
            FLAGS_snb_v1_update_edge_memproperty_cap));
    options.property_updates.buffer_capacity_bytes =
        static_cast<std::size_t>(FLAGS_snb_v1_property_buffer_bytes);
    options.property_updates.delta_chain_merge_threshold =
        FLAGS_snb_v1_delta_merge_threshold;
    options.property_updates.durability = FLAGS_snb_v1_delta_crash_safe
        ? lsmgraph::DeltaDurability::kProcessCrashSafe
        : lsmgraph::DeltaDurability::kNone;
    const auto rs = lsmgraph::GraphDb::OpenFromYaml(
        FLAGS_snb_v1_db_path, schema_path, options, &db_);
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
    std::cout << "[" << phase << "] logical_rows: " << stats.logical_rows
              << std::endl;
    std::cout << "[" << phase << "] node_writes: " << stats.node_writes
              << std::endl;
    std::cout << "[" << phase << "] edge_writes: " << stats.edge_writes
              << std::endl;
    std::cout << "[" << phase << "] skipped_edges: " << stats.skipped_edges
              << std::endl;
    std::cout << "[" << phase << "] logical_writes: " << logical_writes
              << std::endl;
    std::cout << "[" << phase << "] qps(logical_write/s): " << write_qps
              << std::endl;
    std::cout << "[" << phase << "] node_qps(node/s): " << node_qps << std::endl;
    std::cout << "[" << phase << "] edge_qps(edge/s): " << edge_qps << std::endl;
  }

  static void PrintCombinedImportStats(const char* phase,
                                       const ImportStats& lhs,
                                       const ImportStats& rhs) {
    ImportStats total;
    total.logical_rows = lhs.logical_rows + rhs.logical_rows;
    total.node_writes = lhs.node_writes + rhs.node_writes;
    total.edge_writes = lhs.edge_writes + rhs.edge_writes;
	    total.skipped_edges = lhs.skipped_edges + rhs.skipped_edges;
	    total.sec = lhs.sec + rhs.sec;
	    total.background_wait_sec =
	        lhs.background_wait_sec + rhs.background_wait_sec;
      total.wall_sec = ImportStatsElapsedSec(lhs) + ImportStatsElapsedSec(rhs);
	    PrintImportStats(phase, total);
	  }

  bool LoadAllDataset() {
    const auto t1 = std::chrono::steady_clock::now();
    ReleaseVectorMemory(&loaded_node_tables_);
    ReleaseVectorMemory(&loaded_param_files_);
    entity_to_vid_ = {};
    id_maps_sorted_.fill(false);
    vid_to_kind_.clear();
    creation_by_vid_.clear();
    name_to_vertices_.clear();
    hot_nodes_.Clear();
    next_vertex_id_ = 0;
    single_edge_sampler_.Reset(FLAGS_snb_v1_single_edge_candidate_cap,
                               FLAGS_snb_v1_single_edge_hot_weight,
                               FLAGS_snb_v1_single_edge_cold_weight,
                               FLAGS_snb_v1_single_edge_seed);

    for (const auto& spec : NodeTables()) {
      const std::string path = NodeTablePath(spec);
      if (!IndexInitialNodeIdsFromFile(spec, path)) {
        std::cerr << "failed to index node csv: " << path << std::endl;
        return false;
      }
      PrintProcessMemoryUsage(std::string("LOAD_INDEX_NODE:").append(spec.file_name).c_str());
    }

    if (FLAGS_snb_v1_import_update_stream) {
      const std::string person_path = UpdateStreamPath("person");
      if (!IndexUpdateStreamIdsFromFile(person_path, false)) {
        std::cerr << "failed to index update stream: " << person_path
                  << std::endl;
        return false;
      }
      PrintProcessMemoryUsage("LOAD_INDEX_UPDATE_STREAM:person");

      const std::string forum_path = UpdateStreamPath("forum");
      if (!IndexUpdateStreamIdsFromFile(forum_path, true)) {
        std::cerr << "failed to index update stream: " << forum_path
                  << std::endl;
        return false;
      }
      PrintProcessMemoryUsage("LOAD_INDEX_UPDATE_STREAM:forum");
    }
    const auto sort_t1 = std::chrono::steady_clock::now();
    SortAllIdMaps();
    const auto sort_t2 = std::chrono::steady_clock::now();
    std::cout << "[LOAD_INDEX_SORT] time(s): "
              << std::chrono::duration_cast<std::chrono::duration<double>>(
                     sort_t2 - sort_t1)
                     .count()
              << std::endl;
    PrintProcessMemoryUsage("LOAD_INDEX_SORT");

    if (!FLAGS_snb_v1_skip_queries) {
      for (int qid = 1; qid <= 14; ++qid) {
        LoadedParamFile file;
        file.query_id = qid;
        const std::string path = QueryParamFile(qid);
        if (!LoadCsvFile(path, 0, &file.data)) {
          std::cerr << "failed to load query params: " << path << std::endl;
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
    PrintProcessMemoryUsage("LOAD_PREPARE");
    return true;
  }

  size_t ImportBatchRows() const {
    return std::max<uint32_t>(1U, FLAGS_snb_v1_batch_size);
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

  std::unique_ptr<PreparedImportBatch> NewPreparedBatch(
      std::string_view name,
      size_t node_reserve,
      size_t edge_reserve) {
    auto batch = std::make_unique<PreparedImportBatch>(LoadArenaResource());
    batch->name.assign(name.begin(), name.end());
    if (node_reserve > 0) {
      batch->node_writes.reserve(node_reserve);
    }
    if (edge_reserve > 0) {
      batch->edge_writes.reserve(edge_reserve);
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

  bool IndexInitialNodeIdsFromFile(const NodeTableSpec& spec,
                                   const std::string& path) {
    return ForEachCsvRow(path, FLAGS_snb_v1_import_row_limit_per_table,
                         [&](const CsvRow& row) {
                           const std::string& raw_id = row.Get(spec.id_column);
                           if (raw_id.empty()) {
                             return;
                           }
                           const vertex_t vid =
                               AllocateVertexId(spec.kind, ToUint64(raw_id));
                           IndexNode(spec.kind, vid, row);
                         });
  }

  bool IndexUpdateStreamIdsFromFile(const std::string& path, bool forum_stream) {
    std::ifstream in;
    std::vector<char> buffer;
    if (!OpenCsvInput(path, &in, &buffer)) {
      return false;
    }

    uint64_t rows = 0;
    std::string line;
    while (std::getline(in, line)) {
      if (line.empty()) {
        continue;
      }
      const auto fields = SplitPipe(line);
      if (forum_stream) {
        IndexUpdateForumRowIds(fields);
      } else {
        IndexUpdatePersonRowIds(fields);
      }
      ++rows;
      if (FLAGS_snb_v1_update_row_limit_per_file > 0 &&
          rows >= FLAGS_snb_v1_update_row_limit_per_file) {
        break;
      }
    }
    return true;
  }

  void IndexUpdatePersonRowIds(const std::vector<std::string>& fields) {
    if (FieldAt(fields, 2) != "1") {
      return;
    }
    const std::string person_id = FieldAt(fields, 3);
    if (person_id.empty()) {
      return;
    }
    const vertex_t person =
        AllocateVertexId(NodeKind::kPerson, ToUint64(person_id));
    SetVertexCreation(person, FieldAt(fields, 8));
  }

  void IndexUpdateForumRowIds(const std::vector<std::string>& fields) {
    const std::string op = FieldAt(fields, 2);
    if (op == "4") {
      const std::string forum_id = FieldAt(fields, 3);
      if (!forum_id.empty()) {
        const vertex_t forum =
            AllocateVertexId(NodeKind::kForum, ToUint64(forum_id));
        SetVertexCreation(forum, FieldAt(fields, 5));
      }
      return;
    }
    if (op == "6") {
      const std::string post_id = FieldAt(fields, 3);
      if (!post_id.empty()) {
        const vertex_t post =
            AllocateVertexId(NodeKind::kPost, ToUint64(post_id));
        SetVertexCreation(post, FieldAt(fields, 5));
      }
      return;
    }
    if (op == "7") {
      const std::string comment_id = FieldAt(fields, 3);
      if (!comment_id.empty()) {
        const vertex_t comment =
            AllocateVertexId(NodeKind::kComment, ToUint64(comment_id));
        SetVertexCreation(comment, FieldAt(fields, 4));
      }
    }
  }

  void AddImplicitEdgesForPreparedNodeRow(const NodeTableSpec& spec,
                                          const CsvRow& row,
                                          PreparedImportBatch* batch,
                                          ImportStats* stats) {
    auto props = BaseEdgeProps();
    if (spec.kind == NodeKind::kPlace) {
      QueueUpdateEdge(batch == nullptr ? nullptr : &batch->edge_writes, stats,
                      LookupVertexId(NodeKind::kPlace, row.Get("id")),
                      LookupVertexId(NodeKind::kPlace, row.Get("isPartOf")),
                      kPlacePartOfPlace, props);
    } else if (spec.kind == NodeKind::kOrganisation) {
      QueueUpdateEdge(batch == nullptr ? nullptr : &batch->edge_writes, stats,
                      LookupVertexId(NodeKind::kOrganisation, row.Get("id")),
                      LookupVertexId(NodeKind::kPlace, row.Get("place")),
                      kOrganisationLocationPlace, props);
    } else if (spec.kind == NodeKind::kTag) {
      QueueUpdateEdge(batch == nullptr ? nullptr : &batch->edge_writes, stats,
                      LookupVertexId(NodeKind::kTag, row.Get("id")),
                      LookupVertexId(NodeKind::kTagClass, row.Get("hasType")),
                      kTagTypeTagClass, props);
    } else if (spec.kind == NodeKind::kTagClass) {
      QueueUpdateEdge(batch == nullptr ? nullptr : &batch->edge_writes, stats,
                      LookupVertexId(NodeKind::kTagClass, row.Get("id")),
                      LookupVertexId(NodeKind::kTagClass, row.Get("isSubclassOf")),
                      kTagClassSubclassTagClass, props);
    } else if (spec.kind == NodeKind::kPerson) {
      QueueUpdateEdge(batch == nullptr ? nullptr : &batch->edge_writes, stats,
                      LookupVertexId(NodeKind::kPerson, row.Get("id")),
                      LookupVertexId(NodeKind::kPlace, row.Get("place")),
                      kPersonLocationPlace, props);
    } else if (spec.kind == NodeKind::kForum) {
      props["creationDate"] = row.Get("creationDate");
      QueueUpdateEdge(batch == nullptr ? nullptr : &batch->edge_writes, stats,
                      LookupVertexId(NodeKind::kForum, row.Get("id")),
                      LookupVertexId(NodeKind::kPerson, row.Get("moderator")),
                      kForumModeratorPerson, props);
    } else if (spec.kind == NodeKind::kPost) {
      props["creationDate"] = row.Get("creationDate");
      const auto post = LookupVertexId(NodeKind::kPost, row.Get("id"));
      QueueUpdateEdge(batch == nullptr ? nullptr : &batch->edge_writes, stats,
                      post, LookupVertexId(NodeKind::kPerson, row.Get("creator")),
                      kPostCreatorPerson, props);
      QueueUpdateEdge(batch == nullptr ? nullptr : &batch->edge_writes, stats,
                      post, LookupVertexId(NodeKind::kForum, row.Get("Forum.id")),
                      kPostContainerForum, props);
      QueueUpdateEdge(batch == nullptr ? nullptr : &batch->edge_writes, stats,
                      post, LookupVertexId(NodeKind::kPlace, row.Get("place")),
                      kPostLocationPlace, props);
    } else if (spec.kind == NodeKind::kComment) {
      props["creationDate"] = row.Get("creationDate");
      props["messageLengthBucket"] =
          std::to_string(LengthBucket(ToUint64(row.Get("length"))));
      const auto comment = LookupVertexId(NodeKind::kComment, row.Get("id"));
      QueueUpdateEdge(batch == nullptr ? nullptr : &batch->edge_writes, stats,
                      comment, LookupVertexId(NodeKind::kPerson, row.Get("creator")),
                      kCommentCreatorPerson, props);
      QueueUpdateEdge(batch == nullptr ? nullptr : &batch->edge_writes, stats,
                      comment, LookupVertexId(NodeKind::kPlace, row.Get("place")),
                      kCommentLocationPlace, props);
      if (!row.Get("replyOfPost").empty()) {
        auto parent = LookupVertexId(NodeKind::kPost, row.Get("replyOfPost"));
        QueueUpdateEdge(batch == nullptr ? nullptr : &batch->edge_writes, stats,
                        comment, parent, kCommentParentPost, props);
      }
      if (!row.Get("replyOfComment").empty()) {
        auto parent = LookupVertexId(NodeKind::kComment, row.Get("replyOfComment"));
        QueueUpdateEdge(batch == nullptr ? nullptr : &batch->edge_writes, stats,
                        comment, parent, kCommentParentComment, props);
      }
    }
  }

  void ExecutePreparedNodeWriteOne(const PreparedNodeWrite& write) {
    std::chrono::steady_clock::time_point latency_t1;
    if (write.sample_write_latency) {
      latency_t1 = std::chrono::steady_clock::now();
    }
    std::string payload = ToStdString(write.payload);
    if (write.has_node_cold_payload) {
      const std::string cold_payload = ToStdString(write.node_cold_payload);
      const std::string cold_ref = node_cold_blob_writers_.Append(cold_payload);
      if (!SetPipeFieldBySlot(&payload, NodeColdRefSlot(), cold_ref)) {
        std::cerr << "failed to set node cold ref id=" << write.id << std::endl;
        std::exit(1);
      }
    }
    const auto rs = db_->PutNodePayload(write.id, payload, true, kNodeEdgeType);
    if (rs != lsmgraph::Status::kOk) {
      std::cerr << "PutNodePayload failed id=" << write.id << std::endl;
      std::exit(1);
    }
    if (write.sample_write_latency) {
      const auto latency_t2 = std::chrono::steady_clock::now();
      const uint64_t latency_ns =
          static_cast<uint64_t>(
              std::chrono::duration_cast<std::chrono::nanoseconds>(
                  latency_t2 - latency_t1)
                  .count());
      node_write_latency_sampler_.Record(
          CurrentOpenMpThreadId(
              std::max(GetWriteThreadCount(), GetMixedThreadCount())),
          latency_ns);
    }
  }

  void ExecutePreparedNodeWrites(
      const std::pmr::vector<PreparedNodeWrite>& writes) {
    ParallelForWriteIndex(writes.size(), [&](size_t i) {
      ExecutePreparedNodeWriteOne(writes[i]);
    });
  }

  void ExecutePreparedEdgeWriteOne(const PreparedEdgeWrite& write) {
      std::chrono::steady_clock::time_point latency_t1;
      if (write.sample_write_latency) {
        latency_t1 = std::chrono::steady_clock::now();
      }
      const auto sequence = db_->NextSequence();
      const std::string shard0_payload = ToStdString(write.shard0_payload);
      auto rs = db_->PutEdgePayload(0,
                                    write.src,
                                    write.dst,
                                    shard0_payload,
                                    write.insert_mode,
                                    write.is_out,
                                    write.edge_type,
                                    sequence);
      if (rs != lsmgraph::Status::kOk) {
        std::cerr << "PutEdgePayload shard0 failed src=" << write.src
                  << " dst=" << write.dst << " type="
                  << static_cast<int>(write.edge_type) << std::endl;
        std::exit(1);
      }
      if (write.has_shard1) {
        const std::string shard1_payload = ToStdString(write.shard1_payload);
        rs = db_->PutEdgePayload(1,
                                 write.src,
                                 write.dst,
                                 shard1_payload,
                                 write.insert_mode,
                                 write.is_out,
                                 write.edge_type,
                                 sequence);
        if (rs != lsmgraph::Status::kOk) {
          std::cerr << "PutEdgePayload shard1 failed src=" << write.src
                    << " dst=" << write.dst << " type="
                    << static_cast<int>(write.edge_type) << std::endl;
          std::exit(1);
        }
      }
      if (write.has_shard2) {
        const std::string shard2_payload = ToStdString(write.shard2_payload);
        rs = db_->PutEdgePayload(2,
                                 write.src,
                                 write.dst,
                                 shard2_payload,
                                 write.insert_mode,
                                 write.is_out,
                                 write.edge_type,
                                 sequence);
        if (rs != lsmgraph::Status::kOk) {
          std::cerr << "PutEdgePayload shard2 failed src=" << write.src
                    << " dst=" << write.dst << " type="
                    << static_cast<int>(write.edge_type) << std::endl;
          std::exit(1);
        }
      }
      if (write.has_shard3) {
        const std::string shard3_payload = ToStdString(write.shard3_payload);
        const std::string cold_ref = cold_blob_writers_.Append(shard3_payload);
        const PmrString cold_ref_payload = BuildEdgeColdReferencePayload(cold_ref);
        const std::string cold_ref_std = ToStdString(cold_ref_payload);
        rs = db_->PutEdgePayload(3,
                                 write.src,
                                 write.dst,
                                 cold_ref_std,
                                 write.insert_mode,
                                 write.is_out,
                                 write.edge_type,
                                 sequence);
        if (rs != lsmgraph::Status::kOk) {
          std::cerr << "PutEdgePayload shard3 failed src=" << write.src
                    << " dst=" << write.dst << " type="
                    << static_cast<int>(write.edge_type) << std::endl;
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

  void ExecutePreparedEdgeWrites(
      const std::pmr::vector<PreparedEdgeWrite>& writes) {
    ParallelForWriteIndex(writes.size(), [&](size_t i) {
      ExecutePreparedEdgeWriteOne(writes[i]);
    });
  }

  static void AccumulatePreparedStats(const PreparedImportBatch& batch,
                                      ImportStats* stats) {
    if (stats == nullptr) {
      return;
    }
    stats->logical_rows += batch.logical_rows;
    stats->node_writes += batch.node_writes.size();
    stats->edge_writes += batch.edge_writes.size();
    stats->skipped_edges += batch.skipped_edges;
  }

  void ResetPreparedImportBatch(PreparedImportBatch* batch) const {
    if (batch == nullptr) {
      return;
    }
    batch->logical_rows = 0;
    batch->skipped_edges = 0;
    batch->node_writes.clear();
    batch->edge_writes.clear();
  }

  void FlushPreparedImportBatch(PreparedImportBatch* batch,
                                ImportStats* stats) {
    if (batch == nullptr || stats == nullptr ||
        (batch->logical_rows == 0 && batch->node_writes.empty() &&
         batch->edge_writes.empty() && batch->skipped_edges == 0)) {
      return;
    }

    const bool has_writes =
        !batch->node_writes.empty() || !batch->edge_writes.empty();
    const auto t1 = std::chrono::steady_clock::now();
    if (!batch->node_writes.empty()) {
      ExecutePreparedNodeWrites(batch->node_writes);
    }
    if (!batch->edge_writes.empty()) {
      ExecutePreparedEdgeWrites(batch->edge_writes);
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

  bool StreamInitialNodeWritesFromFile(const NodeTableSpec& spec,
                                       const std::string& path,
                                       ImportStats* stats) {
    auto batch = NewPreparedBatch(spec.file_name, ImportBatchRows(), 0);

    const bool ok =
        ForEachCsvRow(path, FLAGS_snb_v1_import_row_limit_per_table,
                      [&](const CsvRow& row) {
                        ++batch->logical_rows;
                        const auto vid =
                            LookupVertexId(spec.kind, row.Get(spec.id_column));
                        if (!vid.has_value()) {
                          std::cerr << "missing prepared vertex id kind="
                                    << NodeKindToString(spec.kind)
                                    << " raw_id=" << row.Get(spec.id_column)
                                    << std::endl;
                          std::exit(1);
                        }
                        batch->node_writes.push_back(
                            BuildPreparedNodeWrite(*vid,
                                                   spec.kind,
                                                   row,
                                                   spec,
                                                   0,
                                                   LoadArenaResource()));
                        if (batch->logical_rows >= ImportBatchRows() ||
                            LoadArenaNearFull()) {
                          FlushPreparedImportBatch(batch.get(), stats);
                          ReleasePreparedBatch(&batch, "initial_nodes");
                          batch = NewPreparedBatch(spec.file_name,
                                                   ImportBatchRows(),
                                                   0);
                        }
                      });
    if (!ok) {
      std::cerr << "failed to stream node csv: " << path << std::endl;
      return false;
    }
    FlushPreparedImportBatch(batch.get(), stats);
    ReleasePreparedBatch(&batch, "initial_nodes");
    return true;
  }

  bool StreamInitialExplicitEdgesFromFile(const EdgeTableSpec& spec,
                                          const std::string& path,
                                          ImportStats* stats) {
    auto batch = NewPreparedBatch(spec.file_name, 0, ImportBatchRows());
    ImportStats batch_prep_stats;

    const bool ok =
        ForEachCsvRow(path, FLAGS_snb_v1_import_row_limit_per_table,
                      [&](const CsvRow& row) {
                        ++batch->logical_rows;
                        const std::string src_raw =
                            EdgeEndpointValue(row, spec.src_column,
                                              spec.src_index);
                        const std::string dst_raw =
                            EdgeEndpointValue(row, spec.dst_column,
                                              spec.dst_index);
                        QueueUpdateEdge(&batch->edge_writes,
                                        &batch_prep_stats,
                                        LookupVertexId(spec.src_kind, src_raw),
                                        LookupVertexId(spec.dst_kind, dst_raw),
                                        spec.edge_type,
                                        EdgePropsFromRow(row, spec.properties));
                        if (batch->logical_rows >= ImportBatchRows() ||
                            LoadArenaNearFull()) {
                          batch->skipped_edges =
                              batch_prep_stats.skipped_edges;
                          FlushPreparedImportBatch(batch.get(), stats);
                          ReleasePreparedBatch(&batch, "initial_explicit_edges");
                          batch = NewPreparedBatch(spec.file_name,
                                                   0,
                                                   ImportBatchRows());
                          batch_prep_stats = ImportStats{};
                        }
                      });
    if (!ok) {
      std::cerr << "failed to stream edge csv: " << path << std::endl;
      return false;
    }
    batch->skipped_edges = batch_prep_stats.skipped_edges;
    FlushPreparedImportBatch(batch.get(), stats);
    ReleasePreparedBatch(&batch, "initial_explicit_edges");
    return true;
  }

  bool StreamInitialImplicitEdgesFromFile(const NodeTableSpec& spec,
                                          const std::string& path,
                                          ImportStats* stats) {
    const std::string batch_name = std::string(spec.file_name) + ":implicit";
    auto batch = NewPreparedBatch(batch_name, 0, ImportBatchRows());
    ImportStats batch_prep_stats;

    const bool ok =
        ForEachCsvRow(path, FLAGS_snb_v1_import_row_limit_per_table,
                      [&](const CsvRow& row) {
                        ++batch->logical_rows;
                        AddImplicitEdgesForPreparedNodeRow(spec,
                                                           row,
                                                           batch.get(),
                                                           &batch_prep_stats);
                        if (batch->logical_rows >= ImportBatchRows() ||
                            LoadArenaNearFull()) {
                          batch->skipped_edges =
                              batch_prep_stats.skipped_edges;
                          FlushPreparedImportBatch(batch.get(), stats);
                          ReleasePreparedBatch(&batch, "initial_implicit_edges");
                          batch = NewPreparedBatch(batch_name,
                                                   0,
                                                   ImportBatchRows());
                          batch_prep_stats = ImportStats{};
                        }
                      });
    if (!ok) {
      std::cerr << "failed to stream implicit edge node csv: " << path
                << std::endl;
      return false;
    }
    batch->skipped_edges = batch_prep_stats.skipped_edges;
    FlushPreparedImportBatch(batch.get(), stats);
    ReleasePreparedBatch(&batch, "initial_implicit_edges");
    return true;
  }

  bool StreamUpdatePersonWritesFromFile(const std::string& name,
                                        const std::string& path,
                                        ImportStats* stats) {
    std::ifstream in;
    std::vector<char> buffer;
    if (!OpenCsvInput(path, &in, &buffer)) {
      std::cerr << "failed to stream update stream: " << path << std::endl;
      return false;
    }

    auto batch = NewPreparedBatch(name, ImportBatchRows(), ImportBatchRows());
    ImportStats batch_prep_stats;
    uint64_t rows = 0;
    std::string line;
    while (std::getline(in, line)) {
      if (line.empty()) {
        continue;
      }
      const auto fields = SplitPipe(line);
      ImportUpdatePersonRow(fields,
                            &batch->node_writes,
                            &batch->edge_writes,
                            &batch_prep_stats);
      ++rows;
      ++batch->logical_rows;
      if (batch->logical_rows >= ImportBatchRows() || LoadArenaNearFull()) {
        batch->skipped_edges = batch_prep_stats.skipped_edges;
        FlushPreparedImportBatch(batch.get(), stats);
        ReleasePreparedBatch(&batch, "update_person");
        batch = NewPreparedBatch(name, ImportBatchRows(), ImportBatchRows());
        batch_prep_stats = ImportStats{};
      }
      if (FLAGS_snb_v1_update_row_limit_per_file > 0 &&
          rows >= FLAGS_snb_v1_update_row_limit_per_file) {
        break;
      }
    }
    batch->skipped_edges = batch_prep_stats.skipped_edges;
    FlushPreparedImportBatch(batch.get(), stats);
    ReleasePreparedBatch(&batch, "update_person");
    return true;
  }

  bool StreamUpdateForumWritesFromFile(const std::string& name,
                                       const std::string& path,
                                       ImportStats* stats) {
    std::ifstream in;
    std::vector<char> buffer;
    if (!OpenCsvInput(path, &in, &buffer)) {
      std::cerr << "failed to stream update stream: " << path << std::endl;
      return false;
    }

    auto batch = NewPreparedBatch(name, ImportBatchRows(), ImportBatchRows());
    ImportStats batch_prep_stats;
    uint64_t rows = 0;
    std::string line;
    while (std::getline(in, line)) {
      if (line.empty()) {
        continue;
      }
      const auto fields = SplitPipe(line);
      ImportUpdateForumRow(fields,
                           &batch->node_writes,
                           &batch->edge_writes,
                           &batch_prep_stats);
      ++rows;
      ++batch->logical_rows;
      if (batch->logical_rows >= ImportBatchRows() || LoadArenaNearFull()) {
        batch->skipped_edges = batch_prep_stats.skipped_edges;
        FlushPreparedImportBatch(batch.get(), stats);
        ReleasePreparedBatch(&batch, "update_forum");
        batch = NewPreparedBatch(name, ImportBatchRows(), ImportBatchRows());
        batch_prep_stats = ImportStats{};
      }
      if (FLAGS_snb_v1_update_row_limit_per_file > 0 &&
          rows >= FLAGS_snb_v1_update_row_limit_per_file) {
        break;
      }
    }
    batch->skipped_edges = batch_prep_stats.skipped_edges;
    FlushPreparedImportBatch(batch.get(), stats);
    ReleasePreparedBatch(&batch, "update_forum");
    return true;
  }

  ImportStats ImportInitialNodes() {
    ImportStats stats;
    for (const auto& spec : NodeTables()) {
      const std::string path = NodeTablePath(spec);
      if (!StreamInitialNodeWritesFromFile(spec, path, &stats)) {
        std::exit(1);
      }
    }
    return stats;
  }

  ImportStats ImportInitialRelations() {
    ImportStats stats;
    for (const auto& spec : EdgeTables()) {
      const std::string path = EdgeTablePath(spec);
      if (!StreamInitialExplicitEdgesFromFile(spec, path, &stats)) {
        std::exit(1);
      }
    }
    for (const auto& spec : NodeTables()) {
      const std::string path = NodeTablePath(spec);
      if (!StreamInitialImplicitEdgesFromFile(spec, path, &stats)) {
        std::exit(1);
      }
    }
    AddColdBlobFlushTime(&stats);
    return stats;
  }

  ImportStats ImportUpdateStream() {
    ImportStats stats;
    const std::string person_path = UpdateStreamPath("person");
    if (!StreamUpdatePersonWritesFromFile("updateStream_person",
                                          person_path,
                                          &stats)) {
      std::exit(1);
    }
    const std::string forum_path = UpdateStreamPath("forum");
    if (!StreamUpdateForumWritesFromFile("updateStream_forum",
                                         forum_path,
                                         &stats)) {
      std::exit(1);
    }
    AddColdBlobFlushTime(&stats);
    return stats;
  }

  void EnsureVertexVectors(vertex_t vid) {
    const size_t idx = static_cast<size_t>(vid);
    if (idx >= vid_to_kind_.size()) {
      vid_to_kind_.resize(idx + 1U, NodeKind::kPlace);
      creation_by_vid_.resize(idx + 1U, 0);
      hot_nodes_.Ensure(idx + 1U);
    }
  }

  static std::vector<IdPair>::iterator LowerBoundIdPair(
      std::vector<IdPair>& pairs,
      uint64_t raw_id) {
    return std::lower_bound(
        pairs.begin(), pairs.end(), raw_id,
        [](const IdPair& item, uint64_t value) {
          return item.raw_id < value;
        });
  }

  static std::vector<IdPair>::const_iterator LowerBoundIdPair(
      const std::vector<IdPair>& pairs,
      uint64_t raw_id) {
    return std::lower_bound(
        pairs.begin(), pairs.end(), raw_id,
        [](const IdPair& item, uint64_t value) {
          return item.raw_id < value;
        });
  }

  void SortAllIdMaps() {
    for (size_t idx = 0; idx < entity_to_vid_.size(); ++idx) {
      auto& pairs = entity_to_vid_[idx];
      std::sort(pairs.begin(), pairs.end(),
                [](const IdPair& a, const IdPair& b) {
                  if (a.raw_id != b.raw_id) {
                    return a.raw_id < b.raw_id;
                  }
                  return a.vid < b.vid;
                });
      id_maps_sorted_[idx] = true;
    }
  }

  vertex_t AllocateVertexId(NodeKind kind, uint64_t raw_id) {
    const size_t idx = KindIndex(kind);
    auto& pairs = entity_to_vid_[idx];
    if (id_maps_sorted_[idx]) {
      const auto it = LowerBoundIdPair(pairs, raw_id);
      if (it != pairs.end() && it->raw_id == raw_id) {
        return it->vid;
      }
      const vertex_t vid = next_vertex_id_++;
      pairs.insert(it, IdPair{raw_id, vid});
      EnsureVertexVectors(vid);
      vid_to_kind_[static_cast<size_t>(vid)] = kind;
      hot_nodes_.raw_id[static_cast<size_t>(vid)] = raw_id;
      return vid;
    }
    const vertex_t vid = next_vertex_id_++;
    pairs.push_back(IdPair{raw_id, vid});
    EnsureVertexVectors(vid);
    vid_to_kind_[static_cast<size_t>(vid)] = kind;
    hot_nodes_.raw_id[static_cast<size_t>(vid)] = raw_id;
    return vid;
  }

  std::optional<vertex_t> LookupVertexId(NodeKind kind,
                                         const std::string& raw) const {
    if (raw.empty()) {
      return std::nullopt;
    }
    const uint64_t raw_id = ToUint64(raw);
    const size_t idx = KindIndex(kind);
    const auto& pairs = entity_to_vid_[idx];
    if (id_maps_sorted_[idx]) {
      const auto it = LowerBoundIdPair(pairs, raw_id);
      if (it == pairs.end() || it->raw_id != raw_id) {
        return std::nullopt;
      }
      return it->vid;
    }
    const auto it = std::find_if(
        pairs.begin(), pairs.end(),
        [&](const IdPair& item) { return item.raw_id == raw_id; });
    if (it == pairs.end()) {
      return std::nullopt;
    }
    return it->vid;
  }

  NodeKind KindOf(vertex_t vid) const {
    const size_t idx = static_cast<size_t>(vid);
    if (idx >= vid_to_kind_.size()) {
      return NodeKind::kPlace;
    }
    return vid_to_kind_[idx];
  }

  PmrString BuildNodePayload(
      NodeKind kind,
      const CsvRow& row,
      const NodeTableSpec& spec,
      std::pmr::memory_resource* mr = std::pmr::get_default_resource()) const {
    (void)kind;
    std::vector<std::string> slots(NodeDbProperties().size());
    const auto& index = NodePropertyIndexByName();
    SetSlot(&slots, index, "rawId", row.Get(spec.id_column));
    for (const auto& prop : spec.properties) {
      SetSlot(&slots, index, prop,
              NormalizeValueForProperty(prop, row.Get(prop)));
    }
    return EncodePayloadSlotsPmr(slots, mr);
  }

  PmrString BuildNodeColdPayload(
      NodeKind kind,
      const CsvRow& row,
      const NodeTableSpec& spec,
      std::pmr::memory_resource* mr = std::pmr::get_default_resource()) const {
    std::vector<std::string> slots(ColdNodeProperties().size());
    const auto& index = ColdNodePropertyIndex();
    SetSlot(&slots, index, "nodeLabel", NodeKindToString(kind));
    for (const auto& prop : spec.properties) {
      SetSlot(&slots, index, prop,
              NormalizeValueForProperty(prop, row.Get(prop)));
    }
    return EncodePayloadSlotsPmr(slots, mr);
  }

  PmrString BuildNodePayloadFromProps(
      NodeKind kind,
      const std::string& raw_id,
      const std::unordered_map<std::string, std::string>& props,
      std::pmr::memory_resource* mr = std::pmr::get_default_resource()) const {
    (void)kind;
    std::vector<std::string> slots(NodeDbProperties().size());
    const auto& index = NodePropertyIndexByName();
    SetSlot(&slots, index, "rawId", raw_id);
    for (const auto& kv : props) {
      SetSlot(&slots, index, kv.first,
              NormalizeValueForProperty(kv.first, kv.second));
    }
    return EncodePayloadSlotsPmr(slots, mr);
  }

  PmrString BuildNodeColdPayloadFromProps(
      NodeKind kind,
      const std::string& raw_id,
      const std::unordered_map<std::string, std::string>& props,
      std::pmr::memory_resource* mr = std::pmr::get_default_resource()) const {
    (void)raw_id;
    std::vector<std::string> slots(ColdNodeProperties().size());
    const auto& index = ColdNodePropertyIndex();
    SetSlot(&slots, index, "nodeLabel", NodeKindToString(kind));
    for (const auto& kv : props) {
      SetSlot(&slots, index, kv.first,
              NormalizeValueForProperty(kv.first, kv.second));
    }
    return EncodePayloadSlotsPmr(slots, mr);
  }

  PreparedNodeWrite BuildPreparedNodeWrite(
      vertex_t id,
      NodeKind kind,
      const CsvRow& row,
      const NodeTableSpec& spec,
      uint64_t scheduled_time = 0,
      std::pmr::memory_resource* mr = std::pmr::get_default_resource()) const {
    PmrString cold = BuildNodeColdPayload(kind, row, spec, mr);
    const bool has_cold = !cold.empty();
    return PreparedNodeWrite{id,
                             BuildNodePayload(kind, row, spec, mr),
                             std::move(cold),
                             has_cold,
                             scheduled_time};
  }

  PreparedNodeWrite BuildPreparedNodeWriteFromProps(
      vertex_t id,
      NodeKind kind,
      const std::string& raw_id,
      const std::unordered_map<std::string, std::string>& props,
      uint64_t scheduled_time = 0,
      std::pmr::memory_resource* mr = std::pmr::get_default_resource()) const {
    PmrString cold = BuildNodeColdPayloadFromProps(kind, raw_id, props, mr);
    const bool has_cold = !cold.empty();
    return PreparedNodeWrite{
        id,
        BuildNodePayloadFromProps(kind, raw_id, props, mr),
        std::move(cold),
        has_cold,
        scheduled_time};
  }

  std::unordered_map<std::string, std::string> EdgePropsFromRow(
      const CsvRow& row,
      const std::vector<std::string>& props) const {
    std::unordered_map<std::string, std::string> out;
    out.emplace("edgeExists", "1");
    for (const auto& prop : props) {
      const std::string value = row.Get(prop);
      if (!value.empty()) {
        out[prop] = NormalizeValueForProperty(prop, value);
      }
    }
    if (row.header != nullptr) {
      for (const auto& column : row.header->columns) {
        if (!IsGeneratedColdProperty(column)) {
          continue;
        }
        const std::string value = row.Get(column);
        if (!value.empty()) {
          out[column] = value;
        }
      }
    }
    return out;
  }

  PmrString BuildEdgePayload(
      const std::unordered_map<std::string, std::string>& props,
      const std::vector<std::string>& shard_props,
      const std::unordered_map<std::string, size_t>& index,
      bool force_edge_exists,
      std::pmr::memory_resource* mr = std::pmr::get_default_resource()) const {
    std::vector<std::string> slots(shard_props.size());
    if (force_edge_exists) {
      SetSlot(&slots, index, "edgeExists", "1");
    }
    for (const auto& kv : props) {
      SetSlot(&slots, index, kv.first,
              NormalizeValueForProperty(kv.first, kv.second));
    }
    return EncodePayloadSlotsPmr(slots, mr);
  }

  bool HasNonEmptyProperty(
      const std::unordered_map<std::string, std::string>& props,
      const std::string& name) const {
    const auto it = props.find(name);
    return it != props.end() && !it->second.empty();
  }

  PmrString BuildEdgeShard0Payload(
      const std::unordered_map<std::string, std::string>& props,
      std::pmr::memory_resource* mr = std::pmr::get_default_resource()) const {
    return BuildEdgePayload(props, EdgeShard0Properties(),
                            EdgeShard0PropertyIndex(), true, mr);
  }

  PmrString BuildEdgeShard1Payload(
      const std::unordered_map<std::string, std::string>& props,
      std::pmr::memory_resource* mr = std::pmr::get_default_resource()) const {
    return BuildEdgePayload(props, EdgeShard1Properties(),
                            EdgeShard1PropertyIndex(), false, mr);
  }

  PmrString BuildEdgeShard2Payload(
      const std::unordered_map<std::string, std::string>& props,
      std::pmr::memory_resource* mr = std::pmr::get_default_resource()) const {
    return BuildEdgePayload(props, EdgeShard2Properties(),
                            EdgeShard2PropertyIndex(), false, mr);
  }

  PmrString BuildEdgeShard3Payload(
      const std::unordered_map<std::string, std::string>& props,
      std::pmr::memory_resource* mr = std::pmr::get_default_resource()) const {
    std::string class_year;
    bool has_any = false;
    const auto class_year_it = props.find("classYear");
    if (class_year_it != props.end() && !class_year_it->second.empty()) {
      class_year = NormalizeValueForProperty("classYear", class_year_it->second);
      has_any = true;
    }
    const std::vector<std::string> cold_props = SortedEdgeColdBlobProperties(props);
    has_any = has_any || !cold_props.empty();
    if (!has_any) {
      return PmrString{mr};
    }

    size_t reserve_bytes = class_year.size() + cold_props.size();
    for (const auto& name : cold_props) {
      const auto it = props.find(name);
      if (it != props.end()) {
        reserve_bytes += NormalizeValueForProperty(name, it->second).size();
      }
    }
    PmrString payload{mr};
    payload.reserve(reserve_bytes);
    payload += class_year;
    for (const auto& name : cold_props) {
      payload.push_back('|');
      const auto it = props.find(name);
      if (it != props.end()) {
        payload += NormalizeValueForProperty(name, it->second);
      }
    }
    return payload;
  }

  PmrString BuildEdgeColdReferencePayload(
      const std::string& cold_ref,
      std::pmr::memory_resource* mr = std::pmr::get_default_resource()) const {
    std::vector<std::string> slots(EdgeShard3Properties().size());
    const auto& index = EdgeShard3PropertyIndex();
    SetSlot(&slots, index, "cold_property", cold_ref);
    return EncodePayloadSlotsPmr(slots, mr);
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
      const PreparedEdgeWrite& write,
      uint16_t property_id,
      uint16_t cold_slot) {
    if (write.insert_mode == lsmgraph::EdgeInsertMode::kBidirectional) {
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
      return;
    }
    AddSingleEdgeCandidate(write.src,
                           write.dst,
                           write.edge_type,
                           write.is_out,
                           property_id,
                           cold_slot);
  }

  void CollectSingleEdgeCandidates(
      const PreparedEdgeWrite& write,
      const std::unordered_map<std::string, std::string>& props) {
    if (!SingleEdgeReadEnabled()) {
      return;
    }
    for (const std::string name : {"creationDate", "joinDate", "workFrom"}) {
      if (HasNonEmptyProperty(props, name)) {
        const uint16_t property_id = RegisterSingleEdgeProperty(name, false);
        AddSingleEdgeCandidateForStoredDirections(write, property_id, 0);
      }
    }
    if (HasNonEmptyProperty(props, "classYear")) {
      const uint16_t property_id = RegisterSingleEdgeProperty("classYear", true);
      AddSingleEdgeCandidateForStoredDirections(write, property_id, 0);
    }

    const std::vector<std::string> cold_props = SortedEdgeColdBlobProperties(props);
    for (size_t i = 0; i < cold_props.size(); ++i) {
      const uint16_t property_id =
          RegisterSingleEdgeProperty(cold_props[i], true);
      AddSingleEdgeCandidateForStoredDirections(
          write, property_id, static_cast<uint16_t>(i + 1));
    }
  }

  PreparedEdgeWrite BuildEdgeWrite(
      vertex_t src,
      vertex_t dst,
      uint8_t edge_type,
      const std::unordered_map<std::string, std::string>& props,
      uint64_t scheduled_time = 0,
      bool collect_single_edge_candidates = true,
      bool mark_latency = true) {
    auto effective_props = props;
    effective_props.emplace("edgeTypeCode", EdgeTypeCode(edge_type));
    const auto creation_it = effective_props.find("creationDate");
    if (creation_it != effective_props.end() && !creation_it->second.empty()) {
      const uint64_t creation = ToUint64(creation_it->second);
      effective_props.emplace("eventMonth", EventMonth(creation));
      effective_props.emplace("eventDow", EventDow(creation));
    }
    PreparedEdgeWrite write(LoadArenaResource());
    write.edge_type = edge_type;
    if (ShouldWriteReverseOnly(edge_type)) {
      write.src = dst;
      write.dst = src;
      write.is_out = false;
      write.insert_mode = lsmgraph::EdgeInsertMode::kSingle;
    } else {
      write.src = src;
      write.dst = dst;
      write.is_out = true;
      write.insert_mode = ShouldWriteBidirectional(edge_type)
                              ? lsmgraph::EdgeInsertMode::kBidirectional
                              : lsmgraph::EdgeInsertMode::kSingle;
    }
    write.shard0_payload =
        BuildEdgeShard0Payload(effective_props, LoadArenaResource());
    if (HasNonEmptyProperty(effective_props, "joinDate")) {
      write.shard1_payload =
          BuildEdgeShard1Payload(effective_props, LoadArenaResource());
      write.has_shard1 = true;
    }
    if (HasNonEmptyProperty(effective_props, "workFrom")) {
      write.shard2_payload =
          BuildEdgeShard2Payload(effective_props, LoadArenaResource());
      write.has_shard2 = true;
    }
	    write.shard3_payload =
	        BuildEdgeShard3Payload(effective_props, LoadArenaResource());
	    write.has_shard3 = !write.shard3_payload.empty();
	    write.scheduled_time = scheduled_time;
	    if (collect_single_edge_candidates) {
	      CollectSingleEdgeCandidates(write, effective_props);
	    }
	    if (mark_latency && write_latency_sampling_active_) {
	      write.sample_write_latency = write_latency_sampler_.MarkNext();
	    }
	    return write;
	  }

	  static void PrintFullGraphWriteStats(const ImportStats& initial_nodes,
	                                       const ImportStats& initial_edges,
	                                       const ImportStats& incremental_nodes,
	                                       const ImportStats& incremental_edges) {
	    const uint64_t node_writes = initial_nodes.node_writes +
	                                 initial_edges.node_writes +
	                                 incremental_nodes.node_writes +
                                   incremental_edges.node_writes;
	    const uint64_t edge_writes = initial_nodes.edge_writes +
	                                 initial_edges.edge_writes +
	                                 incremental_nodes.edge_writes +
                                   incremental_edges.edge_writes;
	    const uint64_t skipped_edges = initial_nodes.skipped_edges +
	                                   initial_edges.skipped_edges +
	                                   incremental_nodes.skipped_edges +
                                     incremental_edges.skipped_edges;
	    const uint64_t logical_writes = node_writes + edge_writes;
		    const double foreground_sec = initial_nodes.sec + initial_edges.sec +
		                                  incremental_nodes.sec +
                                      incremental_edges.sec;
      const double node_sec = ((initial_nodes.node_writes == 0)
                                   ? 0.0
                                   : ImportStatsElapsedSec(initial_nodes)) +
                              ((initial_edges.node_writes == 0)
                                   ? 0.0
                                   : ImportStatsElapsedSec(initial_edges)) +
                              ((incremental_nodes.node_writes == 0)
                                   ? 0.0
                                   : ImportStatsElapsedSec(incremental_nodes)) +
                              ((incremental_edges.node_writes == 0)
                                   ? 0.0
                                   : ImportStatsElapsedSec(incremental_edges));
      const double edge_sec = ((initial_nodes.edge_writes == 0)
                                   ? 0.0
                                   : ImportStatsElapsedSec(initial_nodes)) +
                              ((initial_edges.edge_writes == 0)
                                   ? 0.0
                                   : ImportStatsElapsedSec(initial_edges)) +
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
	    std::cout << "[FULL_GRAPH_WRITE] skipped_edges: " << skipped_edges
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

  void QueueUpdateEdge(std::pmr::vector<PreparedEdgeWrite>* batch,
                       ImportStats* stats,
	                       std::optional<vertex_t> src,
	                       std::optional<vertex_t> dst,
	                       uint8_t edge_type,
	                       const std::unordered_map<std::string, std::string>& props,
	                       uint64_t scheduled_time = 0) {
	    if (!src.has_value() || !dst.has_value()) {
	      ++stats->skipped_edges;
	      return;
	    }
	    batch->push_back(
	        BuildEdgeWrite(*src, *dst, edge_type, props, scheduled_time));
	  }

  void QueueUpdateEdgeByRaw(
      std::pmr::vector<PreparedEdgeWrite>* batch,
      ImportStats* stats,
      NodeKind src_kind,
      const std::string& src_raw,
	      NodeKind dst_kind,
	      const std::string& dst_raw,
	      uint8_t edge_type,
	      const std::unordered_map<std::string, std::string>& props,
	      uint64_t scheduled_time = 0) {
	    QueueUpdateEdge(batch,
	                    stats,
	                    LookupVertexId(src_kind, src_raw),
	                    LookupVertexId(dst_kind, dst_raw),
	                    edge_type,
	                    props,
	                    scheduled_time);
	  }

  uint32_t CachedVid(std::optional<vertex_t> vid) const {
    if (!vid.has_value() || *vid > HotNodeColumns::kInvalidVid) {
      return HotNodeColumns::kInvalidVid;
    }
    return static_cast<uint32_t>(*vid);
  }

  uint8_t GenderCode(const std::string& value) const {
    if (value == "male") {
      return 1;
    }
    if (value == "female") {
      return 2;
    }
    return 0;
  }

  const char* GenderString(uint8_t code) const {
    switch (code) {
      case 1:
        return "male";
      case 2:
        return "female";
      default:
        return "";
    }
  }

  uint32_t InternFirstName(const std::string& value) {
    return hot_nodes_.first_names.Intern(
        NormalizeValueForProperty("firstName", value));
  }

  uint32_t InternLastName(const std::string& value) {
    return hot_nodes_.last_names.Intern(
        NormalizeValueForProperty("lastName", value));
  }

  uint32_t InternName(const std::string& value) {
    return hot_nodes_.names.Intern(NormalizeValueForProperty("name", value));
  }

  void SetHotPlace(vertex_t vid, const std::string& place_raw) {
    if (place_raw.empty() || place_raw == "-1") {
      return;
    }
    const size_t idx = static_cast<size_t>(vid);
    if (idx >= hot_nodes_.place_vid.size()) {
      return;
    }
    hot_nodes_.place_vid[idx] =
        CachedVid(LookupVertexId(NodeKind::kPlace, place_raw));
  }

  void IndexNode(NodeKind kind, vertex_t vid, const CsvRow& row) {
    const size_t idx = static_cast<size_t>(vid);
    creation_by_vid_[idx] = ToUint64(row.Get("creationDate"));
    hot_nodes_.Ensure(idx + 1U);
    if (kind == NodeKind::kPlace) {
      hot_nodes_.name_id[idx] = InternName(row.Get("name"));
      hot_nodes_.is_country_place[idx] = row.Get("type") == "country" ? 1 : 0;
    } else if (kind == NodeKind::kTag ||
               kind == NodeKind::kTagClass ||
               kind == NodeKind::kOrganisation) {
      hot_nodes_.name_id[idx] = InternName(row.Get("name"));
    } else if (kind == NodeKind::kPerson) {
      hot_nodes_.first_name_id[idx] = InternFirstName(row.Get("firstName"));
      hot_nodes_.last_name_id[idx] = InternLastName(row.Get("lastName"));
      hot_nodes_.gender_code[idx] = GenderCode(row.Get("gender"));
      hot_nodes_.birthday[idx] = ToUint64(row.Get("birthday"));
      SetHotPlace(vid, row.Get("place"));
    } else if (kind == NodeKind::kPost || kind == NodeKind::kComment) {
      SetHotPlace(vid, row.Get("place"));
    }
    if (kind == NodeKind::kPlace || kind == NodeKind::kTag ||
        kind == NodeKind::kTagClass || kind == NodeKind::kOrganisation) {
      const std::string name = row.Get("name");
      if (!name.empty()) {
        name_to_vertices_[kind][name].push_back(vid);
      }
    }
  }

  void SetVertexCreation(vertex_t vid, const std::string& creation) {
    EnsureVertexVectors(vid);
    creation_by_vid_[static_cast<size_t>(vid)] = ToUint64(creation);
  }

  void IndexUpdatePersonHotNode(
      vertex_t vid,
      const std::string& place_raw,
      const std::unordered_map<std::string, std::string>& props) {
    const size_t idx = static_cast<size_t>(vid);
    hot_nodes_.Ensure(idx + 1U);
    const auto get = [&](const std::string& name) -> std::string {
      const auto it = props.find(name);
      return it == props.end() ? std::string() : it->second;
    };
    hot_nodes_.first_name_id[idx] = InternFirstName(get("firstName"));
    hot_nodes_.last_name_id[idx] = InternLastName(get("lastName"));
    hot_nodes_.gender_code[idx] = GenderCode(get("gender"));
    hot_nodes_.birthday[idx] = ToUint64(get("birthday"));
    SetHotPlace(vid, place_raw);
  }

  void IndexUpdateMessageHotNode(vertex_t vid, const std::string& place_raw) {
    hot_nodes_.Ensure(static_cast<size_t>(vid) + 1U);
    SetHotPlace(vid, place_raw);
  }

  std::string EdgeEndpointValue(const CsvRow& row,
                                const char* column,
                                size_t index) const {
    if (index != std::numeric_limits<size_t>::max()) {
      return row.GetByIndex(index);
    }
    return row.Get(column);
  }

  static bool ValidRef(const std::string& value) {
    return !value.empty() && value != "-1";
  }

  static void PutProp(std::unordered_map<std::string, std::string>* props,
                      const std::string& name,
                      const std::string& value) {
    if (props != nullptr && !value.empty() && value != "-1") {
      (*props)[name] = value;
    }
  }

  std::unordered_map<std::string, std::string> BaseEdgeProps() const {
    return {{"edgeExists", "1"}};
  }

	  void AddMessageTagUpdateEdges(
	      std::pmr::vector<PreparedEdgeWrite>* edge_batch,
	      ImportStats* stats,
	      NodeKind message_kind,
	      const std::string& message_raw,
	      const std::string& tags,
	      const std::string& length_bucket,
	      uint64_t scheduled_time) {
    const uint8_t edge_type =
        message_kind == NodeKind::kPost ? kPostHasTagTag : kCommentHasTagTag;
    for (const auto& tag : SplitSemicolonList(tags)) {
      auto props = BaseEdgeProps();
      PutProp(&props, "messageLengthBucket", length_bucket);
      QueueUpdateEdgeByRaw(edge_batch,
                           stats,
                           message_kind,
                           message_raw,
                           NodeKind::kTag,
	                           tag,
	                           edge_type,
	                           props,
	                           scheduled_time);
	    }
	  }

	  void ImportUpdatePersonRow(const std::vector<std::string>& fields,
	                             std::pmr::vector<PreparedNodeWrite>* node_batch,
	                             std::pmr::vector<PreparedEdgeWrite>* edge_batch,
	                             ImportStats* stats) {
	    if (FieldAt(fields, 2) != "1") {
	      return;
	    }
	    const uint64_t scheduled_time = ToUint64(FieldAt(fields, 0));
    const std::string person_id = FieldAt(fields, 3);
    if (person_id.empty()) {
      return;
    }

    std::unordered_map<std::string, std::string> node_props;
    PutProp(&node_props, "firstName", FieldAt(fields, 4));
    PutProp(&node_props, "lastName", FieldAt(fields, 5));
    PutProp(&node_props, "gender", FieldAt(fields, 6));
    PutProp(&node_props, "birthday", FieldAt(fields, 7));
    PutProp(&node_props, "creationDate", FieldAt(fields, 8));
    PutProp(&node_props, "locationIP", FieldAt(fields, 9));
    PutProp(&node_props, "browserUsed", FieldAt(fields, 10));
    PutProp(&node_props, "place", FieldAt(fields, 11));
    PutProp(&node_props, "language", FieldAt(fields, 12));
    PutProp(&node_props, "email", FieldAt(fields, 13));

    const vertex_t person = AllocateVertexId(NodeKind::kPerson, ToUint64(person_id));
    SetVertexCreation(person, FieldAt(fields, 8));
    IndexUpdatePersonHotNode(person, FieldAt(fields, 11), node_props);
	    node_batch->push_back(
	        BuildPreparedNodeWriteFromProps(person,
	                                        NodeKind::kPerson,
	                                        person_id,
	                                        node_props,
	                                        scheduled_time,
	                                        LoadArenaResource()));

    QueueUpdateEdgeByRaw(edge_batch,
                         stats,
                         NodeKind::kPerson,
                         person_id,
                         NodeKind::kPlace,
	                         FieldAt(fields, 11),
	                         kPersonLocationPlace,
	                         BaseEdgeProps(),
	                         scheduled_time);

    for (const auto& tag : SplitSemicolonList(FieldAt(fields, 14))) {
      auto props = BaseEdgeProps();
      props["edgeWeight"] =
          std::to_string(1 + ((ToUint64(person_id) ^ ToUint64(tag)) % 100));
      QueueUpdateEdgeByRaw(edge_batch,
                           stats,
                           NodeKind::kPerson,
                           person_id,
                           NodeKind::kTag,
	                           tag,
	                           kPersonHasInterestTag,
	                           props,
	                           scheduled_time);
    }

    for (const auto& item : SplitPairList(FieldAt(fields, 15))) {
      auto props = BaseEdgeProps();
      props["classYear"] = item.second;
      QueueUpdateEdgeByRaw(edge_batch,
                           stats,
                           NodeKind::kPerson,
                           person_id,
                           NodeKind::kOrganisation,
	                           item.first,
	                           kPersonStudyAtOrganisation,
	                           props,
	                           scheduled_time);
    }
    for (const auto& item : SplitPairList(FieldAt(fields, 16))) {
      auto props = BaseEdgeProps();
      props["workFrom"] = item.second;
      QueueUpdateEdgeByRaw(edge_batch,
                           stats,
                           NodeKind::kPerson,
                           person_id,
                           NodeKind::kOrganisation,
	                           item.first,
	                           kPersonWorkAtOrganisation,
	                           props,
	                           scheduled_time);
	    }
	  }

	  void ImportUpdateForumRow(const std::vector<std::string>& fields,
	                            std::pmr::vector<PreparedNodeWrite>* node_batch,
	                            std::pmr::vector<PreparedEdgeWrite>* edge_batch,
	                            ImportStats* stats) {
	    const uint64_t scheduled_time = ToUint64(FieldAt(fields, 0));
	    const std::string op = FieldAt(fields, 2);
    if (op == "2" || op == "3") {
      auto props = BaseEdgeProps();
      const std::string creation = FieldAt(fields, 5);
      PutProp(&props, "creationDate", creation);
      if (fields.size() >= 17) {
        PutProp(&props, "edgeTypeCode", FieldAt(fields, 6));
        PutProp(&props, "eventMonth", FieldAt(fields, 7));
        PutProp(&props, "eventDow", FieldAt(fields, 8));
        PutProp(&props, "srcCountryId", FieldAt(fields, 9));
        PutProp(&props, "srcCityId", FieldAt(fields, 10));
        PutProp(&props, "dstCountryId", FieldAt(fields, 11));
        PutProp(&props, "dstCityId", FieldAt(fields, 12));
        PutProp(&props, "srcActivityBucket", FieldAt(fields, 13));
        PutProp(&props, "dstActivityBucket", FieldAt(fields, 14));
        PutProp(&props, "edgeWeight", FieldAt(fields, 15));
        PutProp(&props, "messageLengthBucket", FieldAt(fields, 16));
      }
      QueueUpdateEdgeByRaw(edge_batch,
                           stats,
                           NodeKind::kPerson,
                           FieldAt(fields, 3),
                           op == "2" ? NodeKind::kPost : NodeKind::kComment,
	                           FieldAt(fields, 4),
	                           op == "2" ? kPersonLikesPost : kPersonLikesComment,
	                           props,
	                           scheduled_time);
      return;
    }

    if (op == "4") {
      const std::string forum_id = FieldAt(fields, 3);
      std::unordered_map<std::string, std::string> node_props;
      PutProp(&node_props, "title", FieldAt(fields, 4));
      PutProp(&node_props, "creationDate", FieldAt(fields, 5));
      const vertex_t forum = AllocateVertexId(NodeKind::kForum, ToUint64(forum_id));
      SetVertexCreation(forum, FieldAt(fields, 5));
      node_batch->push_back(
		          BuildPreparedNodeWriteFromProps(forum,
		                                          NodeKind::kForum,
		                                          forum_id,
		                                          node_props,
		                                          scheduled_time,
		                                          LoadArenaResource()));

      auto moderator_props = BaseEdgeProps();
      PutProp(&moderator_props, "creationDate", FieldAt(fields, 5));
      QueueUpdateEdgeByRaw(edge_batch,
                           stats,
                           NodeKind::kForum,
                           forum_id,
                           NodeKind::kPerson,
	                           FieldAt(fields, 6),
	                           kForumModeratorPerson,
	                           moderator_props,
	                           scheduled_time);

      const auto tags = SplitSemicolonList(FieldAt(fields, 7));
      for (const auto& tag : tags) {
        auto props = BaseEdgeProps();
        PutProp(&props, "srcActivityBucket", FieldAt(fields, 8));
        QueueUpdateEdgeByRaw(edge_batch,
                             stats,
                             NodeKind::kForum,
                             forum_id,
                             NodeKind::kTag,
	                             tag,
	                             kForumHasTagTag,
	                             props,
	                             scheduled_time);
      }
      return;
    }

    if (op == "5") {
      auto props = BaseEdgeProps();
      PutProp(&props, "joinDate", FieldAt(fields, 5));
      if (fields.size() >= 17) {
        PutProp(&props, "edgeTypeCode", FieldAt(fields, 6));
        PutProp(&props, "eventMonth", FieldAt(fields, 7));
        PutProp(&props, "eventDow", FieldAt(fields, 8));
        PutProp(&props, "srcCountryId", FieldAt(fields, 9));
        PutProp(&props, "srcCityId", FieldAt(fields, 10));
        PutProp(&props, "dstCountryId", FieldAt(fields, 11));
        PutProp(&props, "dstCityId", FieldAt(fields, 12));
        PutProp(&props, "srcActivityBucket", FieldAt(fields, 13));
        PutProp(&props, "dstActivityBucket", FieldAt(fields, 14));
        PutProp(&props, "sameCountry", FieldAt(fields, 15));
        PutProp(&props, "sameCity", FieldAt(fields, 16));
      }
      QueueUpdateEdgeByRaw(edge_batch,
                           stats,
                           NodeKind::kForum,
                           FieldAt(fields, 3),
                           NodeKind::kPerson,
	                           FieldAt(fields, 4),
	                           kForumHasMemberPerson,
	                           props,
	                           scheduled_time);
      return;
    }

    if (op == "6") {
      const std::string post_id = FieldAt(fields, 3);
      std::unordered_map<std::string, std::string> node_props;
      PutProp(&node_props, "imageFile", FieldAt(fields, 4));
      PutProp(&node_props, "creationDate", FieldAt(fields, 5));
      PutProp(&node_props, "locationIP", FieldAt(fields, 6));
      PutProp(&node_props, "browserUsed", FieldAt(fields, 7));
      PutProp(&node_props, "language", FieldAt(fields, 8));
      PutProp(&node_props, "content", FieldAt(fields, 9));
      PutProp(&node_props, "length", FieldAt(fields, 10));
      PutProp(&node_props, "place", FieldAt(fields, 13));
      const vertex_t post = AllocateVertexId(NodeKind::kPost, ToUint64(post_id));
      SetVertexCreation(post, FieldAt(fields, 5));
      IndexUpdateMessageHotNode(post, FieldAt(fields, 13));
      node_batch->push_back(
		          BuildPreparedNodeWriteFromProps(post,
		                                          NodeKind::kPost,
		                                          post_id,
		                                          node_props,
		                                          scheduled_time,
		                                          LoadArenaResource()));

      auto props = BaseEdgeProps();
      PutProp(&props, "creationDate", FieldAt(fields, 5));
	      QueueUpdateEdgeByRaw(edge_batch, stats, NodeKind::kPost, post_id,
	                           NodeKind::kPerson, FieldAt(fields, 11),
	                           kPostCreatorPerson, props, scheduled_time);
	      QueueUpdateEdgeByRaw(edge_batch, stats, NodeKind::kPost, post_id,
	                           NodeKind::kForum, FieldAt(fields, 12),
	                           kPostContainerForum, props, scheduled_time);
	      QueueUpdateEdgeByRaw(edge_batch, stats, NodeKind::kPost, post_id,
	                           NodeKind::kPlace, FieldAt(fields, 13),
	                           kPostLocationPlace, props, scheduled_time);

      const std::string length_bucket =
          !FieldAt(fields, 15).empty()
              ? FieldAt(fields, 15)
              : std::to_string(LengthBucket(ToUint64(FieldAt(fields, 10))));
	      AddMessageTagUpdateEdges(edge_batch, stats, NodeKind::kPost, post_id,
	                               FieldAt(fields, 14), length_bucket,
	                               scheduled_time);
      return;
    }

    if (op == "7") {
      const std::string comment_id = FieldAt(fields, 3);
      std::unordered_map<std::string, std::string> node_props;
      PutProp(&node_props, "creationDate", FieldAt(fields, 4));
      PutProp(&node_props, "locationIP", FieldAt(fields, 5));
      PutProp(&node_props, "browserUsed", FieldAt(fields, 6));
      PutProp(&node_props, "content", FieldAt(fields, 7));
      PutProp(&node_props, "length", FieldAt(fields, 8));
      PutProp(&node_props, "place", FieldAt(fields, 10));
      const vertex_t comment =
          AllocateVertexId(NodeKind::kComment, ToUint64(comment_id));
      SetVertexCreation(comment, FieldAt(fields, 4));
      IndexUpdateMessageHotNode(comment, FieldAt(fields, 10));
      node_batch->push_back(
		          BuildPreparedNodeWriteFromProps(comment,
		                                          NodeKind::kComment,
		                                          comment_id,
		                                          node_props,
		                                          scheduled_time,
		                                          LoadArenaResource()));

      auto props = BaseEdgeProps();
      PutProp(&props, "creationDate", FieldAt(fields, 4));
	      QueueUpdateEdgeByRaw(edge_batch, stats, NodeKind::kComment, comment_id,
	                           NodeKind::kPerson, FieldAt(fields, 9),
	                           kCommentCreatorPerson, props, scheduled_time);
	      QueueUpdateEdgeByRaw(edge_batch, stats, NodeKind::kComment, comment_id,
	                           NodeKind::kPlace, FieldAt(fields, 10),
	                           kCommentLocationPlace, props, scheduled_time);

      auto parent_props = props;
      const std::string length_bucket =
          !FieldAt(fields, 14).empty()
              ? FieldAt(fields, 14)
              : std::to_string(LengthBucket(ToUint64(FieldAt(fields, 8))));
      PutProp(&parent_props, "messageLengthBucket", length_bucket);
      if (ValidRef(FieldAt(fields, 11))) {
	        QueueUpdateEdgeByRaw(edge_batch, stats, NodeKind::kComment, comment_id,
	                             NodeKind::kPost, FieldAt(fields, 11),
	                             kCommentParentPost, parent_props,
	                             scheduled_time);
      }
      if (ValidRef(FieldAt(fields, 12))) {
	        QueueUpdateEdgeByRaw(edge_batch, stats, NodeKind::kComment, comment_id,
	                             NodeKind::kComment, FieldAt(fields, 12),
	                             kCommentParentComment, parent_props,
	                             scheduled_time);
      }

	      AddMessageTagUpdateEdges(edge_batch, stats, NodeKind::kComment,
	                               comment_id, FieldAt(fields, 13), length_bucket,
	                               scheduled_time);
      return;
    }

    if (op == "8") {
      auto props = BaseEdgeProps();
      PutProp(&props, "creationDate", FieldAt(fields, 5));
      if (fields.size() >= 19) {
        PutProp(&props, "edgeTypeCode", FieldAt(fields, 6));
        PutProp(&props, "eventMonth", FieldAt(fields, 7));
        PutProp(&props, "eventDow", FieldAt(fields, 8));
        PutProp(&props, "srcCountryId", FieldAt(fields, 9));
        PutProp(&props, "srcCityId", FieldAt(fields, 10));
        PutProp(&props, "dstCountryId", FieldAt(fields, 11));
        PutProp(&props, "dstCityId", FieldAt(fields, 12));
        PutProp(&props, "srcActivityBucket", FieldAt(fields, 13));
        PutProp(&props, "dstActivityBucket", FieldAt(fields, 14));
        PutProp(&props, "edgeWeight", FieldAt(fields, 15));
        PutProp(&props, "interactionCnt", FieldAt(fields, 16));
        PutProp(&props, "sameCountry", FieldAt(fields, 17));
        PutProp(&props, "sameCity", FieldAt(fields, 18));
      }
      QueueUpdateEdgeByRaw(edge_batch,
                           stats,
                           NodeKind::kPerson,
                           FieldAt(fields, 3),
                           NodeKind::kPerson,
	                           FieldAt(fields, 4),
	                           kPersonKnowsPerson,
	                           props,
	                           scheduled_time);
    }
  }

  uint64_t RawIdValue(vertex_t vid) const {
    const size_t idx = static_cast<size_t>(vid);
    return idx < hot_nodes_.raw_id.size() ? hot_nodes_.raw_id[idx] : 0;
  }

  bool IsCountryPlace(vertex_t vid) const {
    const size_t idx = static_cast<size_t>(vid);
    return idx < hot_nodes_.is_country_place.size() &&
           hot_nodes_.is_country_place[idx] != 0;
  }

  std::optional<vertex_t> CachedPlaceOf(vertex_t vid) const {
    const size_t idx = static_cast<size_t>(vid);
    if (idx >= hot_nodes_.place_vid.size()) {
      return std::nullopt;
    }
    const uint32_t cached = hot_nodes_.place_vid[idx];
    if (cached == HotNodeColumns::kInvalidVid) {
      return std::nullopt;
    }
    return static_cast<vertex_t>(cached);
  }

  bool GetHotNodeProp(vertex_t vid,
                      const std::string& prop,
                      std::string* out) const {
    if (out == nullptr) {
      return false;
    }
    const size_t idx = static_cast<size_t>(vid);
    if (idx >= hot_nodes_.raw_id.size()) {
      return false;
    }
    if (prop == "rawId") {
      *out = std::to_string(hot_nodes_.raw_id[idx]);
      return true;
    }
    if (prop == "creationDate" && idx < creation_by_vid_.size() &&
        creation_by_vid_[idx] != 0) {
      *out = std::to_string(creation_by_vid_[idx]);
      return true;
    }
    if (prop == "birthday" && hot_nodes_.birthday[idx] != 0) {
      *out = std::to_string(hot_nodes_.birthday[idx]);
      return true;
    }
    if (prop == "firstName" && hot_nodes_.first_name_id[idx] != 0) {
      *out = hot_nodes_.first_names.Get(hot_nodes_.first_name_id[idx]);
      return true;
    }
    if (prop == "lastName" && hot_nodes_.last_name_id[idx] != 0) {
      *out = hot_nodes_.last_names.Get(hot_nodes_.last_name_id[idx]);
      return true;
    }
    if (prop == "name" && hot_nodes_.name_id[idx] != 0) {
      *out = hot_nodes_.names.Get(hot_nodes_.name_id[idx]);
      return true;
    }
    if (prop == "gender" && hot_nodes_.gender_code[idx] != 0) {
      *out = GenderString(hot_nodes_.gender_code[idx]);
      return true;
    }
    if (prop == "place") {
      const auto place = CachedPlaceOf(vid);
      if (place.has_value()) {
        *out = std::to_string(RawIdValue(*place));
        return true;
      }
    }
    return false;
  }

  std::string GetNodeProp(vertex_t vid, const std::string& prop) {
    std::string hot_value;
    if (GetHotNodeProp(vid, prop, &hot_value)) {
      return hot_value;
    }
    if (IsColdNodeProperty(prop)) {
      std::unordered_map<std::string, std::string> ref_props;
      const auto rs = db_->GetNode(vid,
                                   {"node_cold_property"},
                                   &ref_props,
                                   true,
                                   kNodeEdgeType);
      if (rs != lsmgraph::Status::kOk) {
        return {};
      }
      const auto ref_it = ref_props.find("node_cold_property");
      if (ref_it == ref_props.end() || ref_it->second.empty()) {
        return {};
      }
      const std::string payload =
          ReadNodeColdBlobPayloadNoCache(ref_it->second);
      if (payload.empty()) {
        return {};
      }
      const auto& index = ColdNodePropertyIndex();
      const auto it = index.find(prop);
      if (it == index.end()) {
        return {};
      }
      std::string value;
      return GetPipeFieldBySlot(payload,
                                static_cast<uint16_t>(it->second),
                                &value)
                 ? value
                 : std::string();
    }
    std::unordered_map<std::string, std::string> props;
    const auto rs = db_->GetNode(vid, {prop}, &props, true, kNodeEdgeType);
    if (rs != lsmgraph::Status::kOk) {
      return {};
    }
    const auto it = props.find(prop);
    return it == props.end() ? std::string() : it->second;
  }

  std::string RawId(vertex_t vid) {
    return std::to_string(RawIdValue(vid));
  }

  std::vector<vertex_t> FindByName(NodeKind kind, const std::string& name) const {
    const auto kind_it = name_to_vertices_.find(kind);
    if (kind_it == name_to_vertices_.end()) {
      return {};
    }
    const auto it = kind_it->second.find(name);
    return it == kind_it->second.end() ? std::vector<vertex_t>{} : it->second;
  }

  std::unordered_set<std::string> TagClassRawIdsByName(const std::string& name) {
    std::unordered_set<std::string> out;
    for (const auto vid : FindByName(NodeKind::kTagClass, name)) {
      out.insert(RawId(vid));
    }
    return out;
  }

  std::vector<lsmgraph::GraphDbEdgeScanRecord> Scan(
      vertex_t src,
      uint8_t edge_type,
      bool is_out,
      const std::vector<std::string>& props) {
    std::vector<lsmgraph::GraphDbEdgeScanRecord> records;
    if (props.size() == 1) {
      const auto& prop = props.front();
      const bool ok =
          ForEachEdge(src,
                      edge_type,
                      is_out,
                      prop,
                      [&](vertex_t dst, const std::string& value) {
                        records.push_back(
                            MakeSinglePropRecord(dst, prop, value));
                        return true;
                      });
      if (!ok) {
        return {};
      }
      return records;
    }
    const auto rs = db_->ScanEdges(src, props, &records, is_out, edge_type);
    if (rs != lsmgraph::Status::kOk) {
      return {};
    }
    return records;
  }

  static lsmgraph::GraphDbEdgeScanRecord MakeSinglePropRecord(
      vertex_t dst,
      const std::string& prop,
      const std::string& value) {
    lsmgraph::GraphDbEdgeScanRecord rec;
    rec.dst = dst;
    rec.properties.reserve(1);
    rec.properties.emplace(prop, value);
    return rec;
  }

  template <typename Callback>
  bool ForEachEdge(vertex_t src,
                   uint8_t edge_type,
                   bool is_out,
                   const std::string& prop,
                   Callback&& callback) {
    const auto rs = db_->ForEachEdge(
        src,
        prop,
        [&](vertex_t dst,
            lsmgraph::SequenceNumber_t /*sequence*/,
            const std::string& value) {
          return callback(dst, value);
        },
        is_out,
        edge_type);
    return rs == lsmgraph::Status::kOk;
  }

  uint64_t EdgeU64(const lsmgraph::GraphDbEdgeScanRecord& rec,
                   const std::string& name) const {
    const auto it = rec.properties.find(name);
    return it == rec.properties.end() ? 0 : ToUint64(it->second);
  }

  std::vector<lsmgraph::GraphDbEdgeScanRecord> MessagesByCreator(vertex_t person) {
    std::vector<lsmgraph::GraphDbEdgeScanRecord> out;
    const auto append = [&](vertex_t dst, const std::string& value) {
      out.push_back(MakeSinglePropRecord(dst, "creationDate", value));
      return true;
    };
    ForEachEdge(person, kPostCreatorPerson, false, "creationDate", append);
    ForEachEdge(person, kCommentCreatorPerson, false, "creationDate", append);
    if (FLAGS_snb_v1_frontier_limit > 0 &&
        out.size() > FLAGS_snb_v1_frontier_limit) {
      out.resize(FLAGS_snb_v1_frontier_limit);
    }
    return out;
  }

  std::vector<lsmgraph::GraphDbEdgeScanRecord> TagsForMessage(vertex_t message) {
    const NodeKind kind = KindOf(message);
    const uint8_t edge_type =
        kind == NodeKind::kPost ? kPostHasTagTag : kCommentHasTagTag;
    return Scan(message, edge_type, true, {"edgeExists"});
  }

  std::optional<vertex_t> MessageCreator(vertex_t message) {
    const NodeKind kind = KindOf(message);
    const uint8_t edge_type =
        kind == NodeKind::kPost ? kPostCreatorPerson : kCommentCreatorPerson;
    std::optional<vertex_t> creator;
    ForEachEdge(message,
                edge_type,
                true,
                "edgeExists",
                [&](vertex_t dst, const std::string& /*value*/) {
                  creator = dst;
                  return false;
                });
    if (!creator.has_value()) {
      return std::nullopt;
    }
    return creator;
  }

  std::vector<lsmgraph::GraphDbEdgeScanRecord> LikesForMessage(vertex_t message) {
    const NodeKind kind = KindOf(message);
    const uint8_t edge_type =
        kind == NodeKind::kPost ? kPersonLikesPost : kPersonLikesComment;
    return Scan(message, edge_type, false, {"creationDate"});
  }

  std::vector<lsmgraph::GraphDbEdgeScanRecord> RepliesToMessage(vertex_t message) {
    const NodeKind kind = KindOf(message);
    const uint8_t edge_type =
        kind == NodeKind::kPost ? kCommentParentPost : kCommentParentComment;
    return Scan(message, edge_type, false, {"creationDate"});
  }

  bool TagClassMatches(vertex_t tag_class,
                       const std::unordered_set<std::string>& target_raw_ids) {
    std::unordered_set<vertex_t> visited;
    std::deque<vertex_t> queue;
    queue.push_back(tag_class);
    while (!queue.empty()) {
      const vertex_t curr = queue.front();
      queue.pop_front();
      if (!visited.insert(curr).second) {
        continue;
      }
      if (target_raw_ids.find(RawId(curr)) != target_raw_ids.end()) {
        return true;
      }
      ForEachEdge(curr,
                  kTagClassSubclassTagClass,
                  true,
                  "edgeExists",
                  [&](vertex_t dst, const std::string& /*value*/) {
                    queue.push_back(dst);
                    return true;
                  });
    }
    return false;
  }

  bool TagMatchesClass(vertex_t tag,
                       const std::unordered_set<std::string>& target_raw_ids) {
    bool matched = false;
    ForEachEdge(tag,
                kTagTypeTagClass,
                true,
                "edgeExists",
                [&](vertex_t dst, const std::string& /*value*/) {
                  matched = TagClassMatches(dst, target_raw_ids);
                  return !matched;
                });
    return matched;
  }

  size_t EffectiveResultLimit(size_t official_limit) const {
    if (FLAGS_snb_v1_result_limit_per_param == 0) {
      return official_limit;
    }
    if (official_limit == 0) {
      return FLAGS_snb_v1_result_limit_per_param;
    }
    return std::min<size_t>(official_limit,
                            FLAGS_snb_v1_result_limit_per_param);
  }

  template <typename Row, typename Less>
  void SortAndLimit(std::vector<Row>* rows,
                    Less less,
                    size_t official_limit) const {
    if (rows == nullptr) {
      return;
    }
    std::sort(rows->begin(), rows->end(), less);
    const size_t limit = EffectiveResultLimit(official_limit);
    if (limit > 0 && rows->size() > limit) {
      rows->resize(limit);
    }
  }

  template <typename Row, typename Less>
  void PushBounded(std::vector<Row>* rows,
                   const Row& row,
                   size_t official_limit,
                   Less less) const {
    if (rows == nullptr) {
      return;
    }
    const size_t limit = EffectiveResultLimit(official_limit);
    if (limit == 0 || rows->size() < limit) {
      rows->push_back(row);
      return;
    }
    size_t worst = 0;
    for (size_t i = 1; i < rows->size(); ++i) {
      if (less((*rows)[worst], (*rows)[i])) {
        worst = i;
      }
    }
    if (less(row, (*rows)[worst])) {
      (*rows)[worst] = row;
    }
  }

  void EmitRows(std::vector<std::vector<std::string>> rows,
                QueryRunResult* out,
                size_t official_limit = 0) const {
    const size_t limit = EffectiveResultLimit(official_limit);
    if (limit > 0 && rows.size() > limit) {
      rows.resize(limit);
    }
    HashRowsInOrder(rows, out);
  }

  uint64_t RawIdNumber(vertex_t vid) {
    return RawIdValue(vid);
  }

  void SortVerticesByRawId(std::vector<vertex_t>* values) {
    if (values == nullptr) {
      return;
    }
    std::sort(values->begin(), values->end(), [&](vertex_t a, vertex_t b) {
      return RawIdNumber(a) < RawIdNumber(b);
    });
  }

  std::vector<vertex_t> Friends(vertex_t person) {
    std::vector<vertex_t> out;
    ForEachEdge(person,
                kPersonKnowsPerson,
                true,
                "edgeExists",
                [&](vertex_t dst, const std::string& /*value*/) {
                  out.push_back(dst);
                  return true;
                });
    SortVerticesByRawId(&out);
    return out;
  }

  std::unordered_set<vertex_t> FriendSet(vertex_t person) {
    std::unordered_set<vertex_t> out;
    for (const auto friend_id : Friends(person)) {
      out.insert(friend_id);
    }
    return out;
  }

  std::vector<vertex_t> FriendsAndFoaf(vertex_t start) {
    std::unordered_set<vertex_t> seen;
    std::vector<vertex_t> out;
    seen.insert(start);
    const auto direct = Friends(start);
    for (const auto friend_id : direct) {
      if (seen.insert(friend_id).second) {
        out.push_back(friend_id);
      }
    }
    for (const auto friend_id : direct) {
      for (const auto foaf : Friends(friend_id)) {
        if (seen.insert(foaf).second) {
          out.push_back(foaf);
        }
      }
    }
    SortVerticesByRawId(&out);
    return out;
  }

  std::vector<lsmgraph::GraphDbEdgeScanRecord> PostsByCreator(vertex_t person) {
    return Scan(person, kPostCreatorPerson, false, {"creationDate"});
  }

  std::vector<lsmgraph::GraphDbEdgeScanRecord> CommentsByCreator(vertex_t person) {
    return Scan(person, kCommentCreatorPerson, false, {"creationDate"});
  }

  std::optional<vertex_t> FirstDst(vertex_t src,
                                   uint8_t edge_type,
                                   bool is_out) {
    std::optional<vertex_t> dst;
    ForEachEdge(src,
                edge_type,
                is_out,
                "edgeExists",
                [&](vertex_t value, const std::string& /*prop*/) {
                  dst = value;
                  return false;
                });
    if (!dst.has_value()) {
      return std::nullopt;
    }
    return dst;
  }

  std::optional<vertex_t> PlaceOfPerson(vertex_t person) {
    return FirstDst(person, kPersonLocationPlace, true);
  }

  std::optional<vertex_t> PlaceOfOrganisation(vertex_t organisation) {
    return FirstDst(organisation, kOrganisationLocationPlace, true);
  }

  std::optional<vertex_t> PlaceOfMessage(vertex_t message) {
    const auto cached = CachedPlaceOf(message);
    if (cached.has_value()) {
      return cached;
    }
    return LookupVertexId(NodeKind::kPlace, GetNodeProp(message, "place"));
  }

  std::optional<vertex_t> CountryOfPlace(vertex_t place) {
    std::unordered_set<vertex_t> seen;
    vertex_t curr = place;
    for (size_t depth = 0; depth < 8; ++depth) {
      if (!seen.insert(curr).second) {
        break;
      }
      if (IsCountryPlace(curr)) {
        return curr;
      }
      const auto parent = FirstDst(curr, kPlacePartOfPlace, true);
      if (!parent.has_value()) {
        break;
      }
      curr = *parent;
    }
    return place;
  }

  std::string PlaceName(std::optional<vertex_t> place) {
    return place.has_value() ? GetNodeProp(*place, "name") : std::string();
  }

  std::string CountryNameOfPlace(std::optional<vertex_t> place) {
    if (!place.has_value()) {
      return {};
    }
    return PlaceName(CountryOfPlace(*place));
  }

  std::string PersonCityName(vertex_t person) {
    return PlaceName(PlaceOfPerson(person));
  }

  std::string PersonCountryName(vertex_t person) {
    return CountryNameOfPlace(PlaceOfPerson(person));
  }

  std::string OrganisationCityName(vertex_t organisation) {
    return PlaceName(PlaceOfOrganisation(organisation));
  }

  std::string OrganisationCountryName(vertex_t organisation) {
    return CountryNameOfPlace(PlaceOfOrganisation(organisation));
  }

  std::string MessageCountryName(vertex_t message) {
    return CountryNameOfPlace(PlaceOfMessage(message));
  }

  uint64_t MessageCreation(vertex_t message) {
    const size_t idx = static_cast<size_t>(message);
    if (idx < creation_by_vid_.size() && creation_by_vid_[idx] != 0) {
      return creation_by_vid_[idx];
    }
    return ToUint64(GetNodeProp(message, "creationDate"));
  }

  std::string MessageContent(vertex_t message) {
    const std::string image = GetNodeProp(message, "imageFile");
    if (!image.empty()) {
      return image;
    }
    return GetNodeProp(message, "content");
  }

  std::optional<vertex_t> MessageForum(vertex_t post) {
    if (KindOf(post) != NodeKind::kPost) {
      return std::nullopt;
    }
    return FirstDst(post, kPostContainerForum, true);
  }

  std::optional<vertex_t> ParentPostOfComment(vertex_t comment) {
    return FirstDst(comment, kCommentParentPost, true);
  }

  std::string ReadColdBlobPayload(const std::string& ref) {
    if (ref.empty()) {
      return {};
    }
    {
      std::lock_guard<std::mutex> lock(cold_blob_cache_mu_);
      const auto it = cold_blob_cache_.find(ref);
      if (it != cold_blob_cache_.end()) {
        return it->second;
      }
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
    const std::string path =
        ColdBlobPathPrefix() + "_" + std::to_string(file_id) + ".blob";
    std::ifstream in(path, std::ios::binary);
    if (!in.is_open()) {
      return {};
    }
    std::string payload(static_cast<size_t>(length), '\0');
    in.seekg(static_cast<std::streamoff>(offset));
    in.read(payload.data(), static_cast<std::streamsize>(payload.size()));
    if (!in) {
      return {};
    }
    {
      std::lock_guard<std::mutex> lock(cold_blob_cache_mu_);
      cold_blob_cache_.emplace(ref, payload);
    }
    return payload;
  }

  std::string FirstColdBlobField(const std::string& ref) {
    const std::string payload = ReadColdBlobPayload(ref);
    const size_t sep = payload.find('|');
    return sep == std::string::npos ? payload : payload.substr(0, sep);
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

  std::string PersonUniversities(vertex_t person) {
    std::vector<std::string> items;
    ForEachEdge(person,
                kPersonStudyAtOrganisation,
                true,
                "cold_property",
                [&](vertex_t dst, const std::string& value) {
                  items.push_back(GetNodeProp(dst, "name") + "," +
                                  FirstColdBlobField(value) + "," +
                                  OrganisationCityName(dst));
                  return true;
                });
    std::sort(items.begin(), items.end());
    return JoinStrings(items);
  }

  std::string PersonCompanies(vertex_t person) {
    std::vector<std::string> items;
    ForEachEdge(person,
                kPersonWorkAtOrganisation,
                true,
                "workFrom",
                [&](vertex_t dst, const std::string& value) {
                  items.push_back(GetNodeProp(dst, "name") + "," + value + "," +
                                  OrganisationCountryName(dst));
                  return true;
                });
    std::sort(items.begin(), items.end());
    return JoinStrings(items);
  }

  bool BirthdayInMonthWindow(uint64_t birthday_millis, uint32_t month) {
    if (birthday_millis == 0 || month < 1 || month > 12) {
      return false;
    }
    const time_t seconds = static_cast<time_t>(birthday_millis / 1000ULL);
    std::tm tm = {};
    gmtime_r(&seconds, &tm);
    const uint32_t m = static_cast<uint32_t>(tm.tm_mon + 1);
    const uint32_t d = static_cast<uint32_t>(tm.tm_mday);
    const uint32_t next_month = month == 12 ? 1 : month + 1;
    if (m == month && d >= 21) {
      return true;
    }
    return m == next_month && d < 22;
  }

  double InteractionWeight(vertex_t a, vertex_t b) {
    double weight = 0.0;
    const auto add_replies = [&](vertex_t reply_author,
                                 vertex_t message_author) {
      double subtotal = 0.0;
      for (const auto& msg : MessagesByCreator(message_author)) {
        const double reply_weight =
            KindOf(msg.dst) == NodeKind::kPost ? 1.0 : 0.5;
        for (const auto& reply : RepliesToMessage(msg.dst)) {
          const auto creator = MessageCreator(reply.dst);
          if (creator.has_value() && *creator == reply_author) {
            subtotal += reply_weight;
          }
        }
      }
      return subtotal;
    };
    weight += add_replies(a, b);
    weight += add_replies(b, a);
    return weight;
  }

  QueryRunResult RunQuery1(const CsvRow& row) {
    QueryRunResult out;
    const auto start = LookupVertexId(NodeKind::kPerson, row.Get("personId"));
    if (!start.has_value()) {
      return out;
    }
    const std::string first_name = row.Get("firstName");
    struct Row {
      vertex_t person = 0;
      uint32_t distance = 0;
    };
    std::unordered_set<vertex_t> visited{*start};
    std::vector<vertex_t> frontier{*start};
    std::vector<Row> found;
    for (uint32_t depth = 1; depth <= 3 && !frontier.empty(); ++depth) {
      std::vector<vertex_t> next;
      for (const auto curr : frontier) {
        for (const auto friend_id : Friends(curr)) {
          if (!visited.insert(friend_id).second) {
            continue;
          }
          if (GetNodeProp(friend_id, "firstName") == first_name) {
            found.push_back(Row{friend_id, depth});
          }
          next.push_back(friend_id);
        }
      }
      frontier.swap(next);
    }
    SortAndLimit(&found,
                 [&](const Row& a, const Row& b) {
                   if (a.distance != b.distance) {
                     return a.distance < b.distance;
                   }
                   const std::string a_last = GetNodeProp(a.person, "lastName");
                   const std::string b_last = GetNodeProp(b.person, "lastName");
                   if (a_last != b_last) {
                     return a_last < b_last;
                   }
                   return RawIdNumber(a.person) < RawIdNumber(b.person);
                 },
                 20);
    std::vector<std::vector<std::string>> rows;
    rows.reserve(found.size());
    for (const auto& item : found) {
      rows.push_back({RawId(item.person),
                      GetNodeProp(item.person, "lastName"),
                      std::to_string(item.distance),
                      GetNodeProp(item.person, "birthday"),
                      GetNodeProp(item.person, "creationDate"),
                      GetNodeProp(item.person, "gender"),
                      GetNodeProp(item.person, "browserUsed"),
                      GetNodeProp(item.person, "locationIP"),
                      GetNodeProp(item.person, "email"),
                      GetNodeProp(item.person, "language"),
                      PersonCityName(item.person),
                      PersonUniversities(item.person),
                      PersonCompanies(item.person)});
    }
    EmitRows(std::move(rows), &out);
    return out;
  }

  QueryRunResult RunQuery2(const CsvRow& row) {
    QueryRunResult out;
    const auto start = LookupVertexId(NodeKind::kPerson, row.Get("personId"));
    if (!start.has_value()) {
      return out;
    }
    const uint64_t max_date = ParamMillis(row, "maxDate");
    struct Row {
      vertex_t person = 0;
      vertex_t message = 0;
      uint64_t created = 0;
    };
    const auto less = [&](const Row& a, const Row& b) {
      if (a.created != b.created) {
        return a.created > b.created;
      }
      return RawIdNumber(a.message) < RawIdNumber(b.message);
    };
    std::vector<Row> top;
    for (const auto friend_id : Friends(*start)) {
      for (const auto& msg : MessagesByCreator(friend_id)) {
        const uint64_t created = MessageCreation(msg.dst);
        if (max_date != 0 && created >= max_date) {
          continue;
        }
        PushBounded(&top, Row{friend_id, msg.dst, created}, 20, less);
      }
    }
    SortAndLimit(&top, less, 20);
    std::vector<std::vector<std::string>> rows;
    rows.reserve(top.size());
    for (const auto& item : top) {
      rows.push_back({RawId(item.person),
                      GetNodeProp(item.person, "firstName"),
                      GetNodeProp(item.person, "lastName"),
                      RawId(item.message),
                      MessageContent(item.message),
                      std::to_string(item.created)});
    }
    EmitRows(std::move(rows), &out);
    return out;
  }

  QueryRunResult RunQuery3(const CsvRow& row) {
    QueryRunResult out;
    const auto start = LookupVertexId(NodeKind::kPerson, row.Get("personId"));
    if (!start.has_value()) {
      return out;
    }
    const uint64_t begin = ParamMillis(row, "startDate");
    const uint64_t end =
        begin + ToUint64(row.Get("durationDays")) * kMillisPerDay;
    const std::string country_x = row.Get("countryXName");
    const std::string country_y = row.Get("countryYName");
    struct Row {
      vertex_t person = 0;
      uint64_t x = 0;
      uint64_t y = 0;
    };
    std::vector<Row> rows_raw;
    for (const auto person : FriendsAndFoaf(*start)) {
      const std::string person_country = PersonCountryName(person);
      if (person_country == country_x || person_country == country_y) {
        continue;
      }
      uint64_t x = 0;
      uint64_t y = 0;
      for (const auto& msg : MessagesByCreator(person)) {
        const uint64_t created = MessageCreation(msg.dst);
        if (created < begin || created >= end) {
          continue;
        }
        const std::string msg_country = MessageCountryName(msg.dst);
        if (msg_country == country_x) {
          ++x;
        }
        if (msg_country == country_y) {
          ++y;
        }
      }
      if (x > 0 && y > 0) {
        rows_raw.push_back(Row{person, x, y});
      }
    }
    SortAndLimit(&rows_raw,
                 [&](const Row& a, const Row& b) {
                   const uint64_t a_count = a.x + a.y;
                   const uint64_t b_count = b.x + b.y;
                   if (a_count != b_count) {
                     return a_count > b_count;
                   }
                   return RawIdNumber(a.person) < RawIdNumber(b.person);
                 },
                 20);
    std::vector<std::vector<std::string>> rows;
    rows.reserve(rows_raw.size());
    for (const auto& item : rows_raw) {
      rows.push_back({RawId(item.person),
                      GetNodeProp(item.person, "firstName"),
                      GetNodeProp(item.person, "lastName"),
                      std::to_string(item.x),
                      std::to_string(item.y),
                      std::to_string(item.x + item.y)});
    }
    EmitRows(std::move(rows), &out);
    return out;
  }

  QueryRunResult RunQuery4(const CsvRow& row) {
    QueryRunResult out;
    const auto start = LookupVertexId(NodeKind::kPerson, row.Get("personId"));
    if (!start.has_value()) {
      return out;
    }
    const uint64_t begin = ParamMillis(row, "startDate");
    const uint64_t end =
        begin + ToUint64(row.Get("durationDays")) * kMillisPerDay;
    std::unordered_map<vertex_t, uint64_t> counts;
    std::unordered_set<vertex_t> seen_before;
    for (const auto friend_id : Friends(*start)) {
      for (const auto& post : PostsByCreator(friend_id)) {
        const uint64_t created = MessageCreation(post.dst);
        if (created >= begin && created < end) {
          for (const auto& tag : TagsForMessage(post.dst)) {
            ++counts[tag.dst];
          }
          continue;
        }
        if (created < begin || created >= end) {
          if (created < begin) {
            for (const auto& tag : TagsForMessage(post.dst)) {
              seen_before.insert(tag.dst);
            }
          }
        }
      }
    }
    struct Row {
      vertex_t tag = 0;
      uint64_t count = 0;
    };
    std::vector<Row> rows_raw;
    for (const auto& kv : counts) {
      if (seen_before.find(kv.first) == seen_before.end()) {
        rows_raw.push_back(Row{kv.first, kv.second});
      }
    }
    SortAndLimit(&rows_raw,
                 [&](const Row& a, const Row& b) {
                   if (a.count != b.count) {
                     return a.count > b.count;
                   }
                   return GetNodeProp(a.tag, "name") < GetNodeProp(b.tag, "name");
                 },
                 10);
    std::vector<std::vector<std::string>> rows;
    rows.reserve(rows_raw.size());
    for (const auto& item : rows_raw) {
      rows.push_back({GetNodeProp(item.tag, "name"),
                      std::to_string(item.count)});
    }
    EmitRows(std::move(rows), &out);
    return out;
  }

  QueryRunResult RunQuery5(const CsvRow& row) {
    QueryRunResult out;
    const auto start = LookupVertexId(NodeKind::kPerson, row.Get("personId"));
    if (!start.has_value()) {
      return out;
    }
    const uint64_t min_date = ParamMillis(row, "minDate");
    std::unordered_map<vertex_t, uint64_t> forum_counts;
    for (const auto person : FriendsAndFoaf(*start)) {
      std::unordered_set<vertex_t> joined_forums;
      ForEachEdge(person,
                  kForumHasMemberPerson,
                  false,
                  "joinDate",
                  [&](vertex_t forum, const std::string& value) {
                    if (ToUint64(value) > min_date) {
                      joined_forums.insert(forum);
                    }
                    return true;
                  });
      if (joined_forums.empty()) {
        continue;
      }
      for (const auto& post : PostsByCreator(person)) {
        const auto forum = MessageForum(post.dst);
        if (forum.has_value() &&
            joined_forums.find(*forum) != joined_forums.end()) {
          ++forum_counts[*forum];
        }
      }
    }
    struct Row {
      vertex_t forum = 0;
      uint64_t count = 0;
    };
    std::vector<Row> rows_raw;
    rows_raw.reserve(forum_counts.size());
    for (const auto& kv : forum_counts) {
      rows_raw.push_back(Row{kv.first, kv.second});
    }
    SortAndLimit(&rows_raw,
                 [&](const Row& a, const Row& b) {
                   if (a.count != b.count) {
                     return a.count > b.count;
                   }
                   return RawIdNumber(a.forum) < RawIdNumber(b.forum);
                 },
                 20);
    std::vector<std::vector<std::string>> rows;
    rows.reserve(rows_raw.size());
    for (const auto& item : rows_raw) {
      rows.push_back({GetNodeProp(item.forum, "title"),
                      std::to_string(item.count)});
    }
    EmitRows(std::move(rows), &out);
    return out;
  }

  QueryRunResult RunQuery6(const CsvRow& row) {
    QueryRunResult out;
    const auto start = LookupVertexId(NodeKind::kPerson, row.Get("personId"));
    if (!start.has_value()) {
      return out;
    }
    std::unordered_set<vertex_t> input_tags;
    for (const auto tag : FindByName(NodeKind::kTag, row.Get("tagName"))) {
      input_tags.insert(tag);
    }
    std::unordered_map<vertex_t, uint64_t> related;
    for (const auto person : FriendsAndFoaf(*start)) {
      for (const auto& post : PostsByCreator(person)) {
        bool has_input = false;
        std::vector<vertex_t> tags;
        for (const auto& tag : TagsForMessage(post.dst)) {
          tags.push_back(tag.dst);
          if (input_tags.find(tag.dst) != input_tags.end()) {
            has_input = true;
          }
        }
        if (!has_input) {
          continue;
        }
        for (const auto tag : tags) {
          if (input_tags.find(tag) == input_tags.end()) {
            ++related[tag];
          }
        }
      }
    }
    struct Row {
      vertex_t tag = 0;
      uint64_t count = 0;
    };
    std::vector<Row> rows_raw;
    for (const auto& kv : related) {
      rows_raw.push_back(Row{kv.first, kv.second});
    }
    SortAndLimit(&rows_raw,
                 [&](const Row& a, const Row& b) {
                   if (a.count != b.count) {
                     return a.count > b.count;
                   }
                   return GetNodeProp(a.tag, "name") < GetNodeProp(b.tag, "name");
                 },
                 10);
    std::vector<std::vector<std::string>> rows;
    rows.reserve(rows_raw.size());
    for (const auto& item : rows_raw) {
      rows.push_back({GetNodeProp(item.tag, "name"),
                      std::to_string(item.count)});
    }
    EmitRows(std::move(rows), &out);
    return out;
  }

  QueryRunResult RunQuery7(const CsvRow& row) {
    QueryRunResult out;
    const auto person = LookupVertexId(NodeKind::kPerson, row.Get("personId"));
    if (!person.has_value()) {
      return out;
    }
    const auto friends = FriendSet(*person);
    struct Row {
      vertex_t liker = 0;
      vertex_t message = 0;
      uint64_t like_date = 0;
      int64_t latency_minutes = 0;
      bool is_new = true;
    };
    std::unordered_map<vertex_t, Row> best_by_liker;
    for (const auto& msg : MessagesByCreator(*person)) {
      for (const auto& like : LikesForMessage(msg.dst)) {
        const uint64_t like_date = EdgeU64(like, "creationDate");
        const uint64_t msg_date = MessageCreation(msg.dst);
        const int64_t latency =
            like_date >= msg_date
                ? static_cast<int64_t>((like_date - msg_date) / 60000ULL)
                : -static_cast<int64_t>((msg_date - like_date) / 60000ULL);
        Row candidate{like.dst,
                      msg.dst,
                      like_date,
                      latency,
                      friends.find(like.dst) == friends.end()};
        auto it = best_by_liker.find(like.dst);
        if (it == best_by_liker.end() ||
            candidate.like_date > it->second.like_date ||
            (candidate.like_date == it->second.like_date &&
             RawIdNumber(candidate.message) < RawIdNumber(it->second.message))) {
          best_by_liker[like.dst] = candidate;
        }
      }
    }
    std::vector<Row> rows_raw;
    rows_raw.reserve(best_by_liker.size());
    for (const auto& kv : best_by_liker) {
      rows_raw.push_back(kv.second);
    }
    SortAndLimit(&rows_raw,
                 [&](const Row& a, const Row& b) {
                   if (a.like_date != b.like_date) {
                     return a.like_date > b.like_date;
                   }
                   return RawIdNumber(a.liker) < RawIdNumber(b.liker);
                 },
                 20);
    std::vector<std::vector<std::string>> rows;
    rows.reserve(rows_raw.size());
    for (const auto& item : rows_raw) {
      rows.push_back({RawId(item.liker),
                      GetNodeProp(item.liker, "firstName"),
                      GetNodeProp(item.liker, "lastName"),
                      std::to_string(item.like_date),
                      RawId(item.message),
                      MessageContent(item.message),
                      std::to_string(item.latency_minutes),
                      item.is_new ? "true" : "false"});
    }
    EmitRows(std::move(rows), &out);
    return out;
  }

  QueryRunResult RunQuery8(const CsvRow& row) {
    QueryRunResult out;
    const auto person = LookupVertexId(NodeKind::kPerson, row.Get("personId"));
    if (!person.has_value()) {
      return out;
    }
    struct Row {
      vertex_t author = 0;
      vertex_t comment = 0;
      uint64_t created = 0;
    };
    const auto less = [&](const Row& a, const Row& b) {
      if (a.created != b.created) {
        return a.created > b.created;
      }
      return RawIdNumber(a.comment) < RawIdNumber(b.comment);
    };
    std::vector<Row> top;
    for (const auto& msg : MessagesByCreator(*person)) {
      for (const auto& reply : RepliesToMessage(msg.dst)) {
        const auto author = MessageCreator(reply.dst);
        if (!author.has_value()) {
          continue;
        }
        PushBounded(&top,
                    Row{*author, reply.dst, MessageCreation(reply.dst)},
                    20,
                    less);
      }
    }
    SortAndLimit(&top, less, 20);
    std::vector<std::vector<std::string>> rows;
    rows.reserve(top.size());
    for (const auto& item : top) {
      rows.push_back({RawId(item.author),
                      GetNodeProp(item.author, "firstName"),
                      GetNodeProp(item.author, "lastName"),
                      std::to_string(item.created),
                      RawId(item.comment),
                      GetNodeProp(item.comment, "content")});
    }
    EmitRows(std::move(rows), &out);
    return out;
  }

  QueryRunResult RunQuery9(const CsvRow& row) {
    QueryRunResult out;
    const auto start = LookupVertexId(NodeKind::kPerson, row.Get("personId"));
    if (!start.has_value()) {
      return out;
    }
    const uint64_t max_date = ParamMillis(row, "maxDate");
    struct Row {
      vertex_t person = 0;
      vertex_t message = 0;
      uint64_t created = 0;
    };
    const auto less = [&](const Row& a, const Row& b) {
      if (a.created != b.created) {
        return a.created > b.created;
      }
      return RawIdNumber(a.message) < RawIdNumber(b.message);
    };
    std::vector<Row> top;
    for (const auto person : FriendsAndFoaf(*start)) {
      for (const auto& msg : MessagesByCreator(person)) {
        const uint64_t created = MessageCreation(msg.dst);
        if (max_date != 0 && created >= max_date) {
          continue;
        }
        PushBounded(&top, Row{person, msg.dst, created}, 20, less);
      }
    }
    SortAndLimit(&top, less, 20);
    std::vector<std::vector<std::string>> rows;
    rows.reserve(top.size());
    for (const auto& item : top) {
      rows.push_back({RawId(item.person),
                      GetNodeProp(item.person, "firstName"),
                      GetNodeProp(item.person, "lastName"),
                      RawId(item.message),
                      MessageContent(item.message),
                      std::to_string(item.created)});
    }
    EmitRows(std::move(rows), &out);
    return out;
  }

  QueryRunResult RunQuery10(const CsvRow& row) {
    QueryRunResult out;
    const auto person = LookupVertexId(NodeKind::kPerson, row.Get("personId"));
    if (!person.has_value()) {
      return out;
    }
    const uint32_t month = static_cast<uint32_t>(ToUint64(row.Get("month")));
    const auto direct_friends = FriendSet(*person);
    std::unordered_set<vertex_t> interests;
    ForEachEdge(*person,
                kPersonHasInterestTag,
                true,
                "edgeExists",
                [&](vertex_t dst, const std::string& /*value*/) {
                  interests.insert(dst);
                  return true;
                });
    struct Row {
      vertex_t person = 0;
      int64_t score = 0;
    };
    std::vector<Row> rows_raw;
    for (const auto candidate : FriendsAndFoaf(*person)) {
      if (candidate == *person ||
          direct_friends.find(candidate) != direct_friends.end() ||
          !BirthdayInMonthWindow(ToUint64(GetNodeProp(candidate, "birthday")),
                                 month)) {
        continue;
      }
      int64_t common = 0;
      int64_t uncommon = 0;
      for (const auto& post : PostsByCreator(candidate)) {
        bool has_common = false;
        for (const auto& tag : TagsForMessage(post.dst)) {
          if (interests.find(tag.dst) != interests.end()) {
            has_common = true;
            break;
          }
        }
        if (has_common) {
          ++common;
        } else {
          ++uncommon;
        }
      }
      rows_raw.push_back(Row{candidate, common - uncommon});
    }
    SortAndLimit(&rows_raw,
                 [&](const Row& a, const Row& b) {
                   if (a.score != b.score) {
                     return a.score > b.score;
                   }
                   return RawIdNumber(a.person) < RawIdNumber(b.person);
                 },
                 10);
    std::vector<std::vector<std::string>> rows;
    rows.reserve(rows_raw.size());
    for (const auto& item : rows_raw) {
      rows.push_back({RawId(item.person),
                      GetNodeProp(item.person, "firstName"),
                      GetNodeProp(item.person, "lastName"),
                      std::to_string(item.score),
                      GetNodeProp(item.person, "gender"),
                      PersonCityName(item.person)});
    }
    EmitRows(std::move(rows), &out);
    return out;
  }

  bool OrganisationInCountry(vertex_t org, const std::string& country_name) {
    return OrganisationCountryName(org) == country_name;
  }

  QueryRunResult RunQuery11(const CsvRow& row) {
    QueryRunResult out;
    const auto start = LookupVertexId(NodeKind::kPerson, row.Get("personId"));
    if (!start.has_value()) {
      return out;
    }
    const std::string country = row.Get("countryName");
    const uint64_t work_from = ToUint64(row.Get("workFromYear"));
    struct Row {
      vertex_t person = 0;
      vertex_t company = 0;
      uint64_t work_from = 0;
    };
    std::vector<Row> rows_raw;
    for (const auto person : FriendsAndFoaf(*start)) {
      ForEachEdge(person,
                  kPersonWorkAtOrganisation,
                  true,
                  "workFrom",
                  [&](vertex_t company, const std::string& value) {
                    const uint64_t started = ToUint64(value);
                    if (started >= work_from ||
                        !OrganisationInCountry(company, country)) {
                      return true;
                    }
                    rows_raw.push_back(Row{person, company, started});
                    return true;
                  });
    }
    SortAndLimit(&rows_raw,
                 [&](const Row& a, const Row& b) {
                   if (a.work_from != b.work_from) {
                     return a.work_from < b.work_from;
                   }
                   if (RawIdNumber(a.person) != RawIdNumber(b.person)) {
                     return RawIdNumber(a.person) < RawIdNumber(b.person);
                   }
                   return GetNodeProp(a.company, "name") >
                          GetNodeProp(b.company, "name");
                 },
                 10);
    std::vector<std::vector<std::string>> rows;
    rows.reserve(rows_raw.size());
    for (const auto& item : rows_raw) {
      rows.push_back({RawId(item.person),
                      GetNodeProp(item.person, "firstName"),
                      GetNodeProp(item.person, "lastName"),
                      GetNodeProp(item.company, "name"),
                      std::to_string(item.work_from)});
    }
    EmitRows(std::move(rows), &out);
    return out;
  }

  QueryRunResult RunQuery12(const CsvRow& row) {
    QueryRunResult out;
    const auto start = LookupVertexId(NodeKind::kPerson, row.Get("personId"));
    if (!start.has_value()) {
      return out;
    }
    const auto tag_class_ids = TagClassRawIdsByName(row.Get("tagClassName"));
    struct Agg {
      uint64_t reply_count = 0;
      std::set<std::string> tag_names;
    };
    std::unordered_map<vertex_t, Agg> by_friend;
    for (const auto friend_id : Friends(*start)) {
      for (const auto& comment : CommentsByCreator(friend_id)) {
        const auto post = ParentPostOfComment(comment.dst);
        if (!post.has_value()) {
          continue;
        }
        std::vector<std::string> matching_tags;
        for (const auto& tag : TagsForMessage(*post)) {
          if (TagMatchesClass(tag.dst, tag_class_ids)) {
            matching_tags.push_back(GetNodeProp(tag.dst, "name"));
          }
        }
        if (matching_tags.empty()) {
          continue;
        }
        auto& agg = by_friend[friend_id];
        ++agg.reply_count;
        for (const auto& name : matching_tags) {
          agg.tag_names.insert(name);
        }
      }
    }
    struct Row {
      vertex_t person = 0;
      uint64_t reply_count = 0;
      std::string tag_names;
    };
    std::vector<Row> rows_raw;
    rows_raw.reserve(by_friend.size());
    for (const auto& kv : by_friend) {
      std::vector<std::string> names(kv.second.tag_names.begin(),
                                     kv.second.tag_names.end());
      rows_raw.push_back(Row{kv.first,
                             kv.second.reply_count,
                             JoinStrings(names)});
    }
    SortAndLimit(&rows_raw,
                 [&](const Row& a, const Row& b) {
                   if (a.reply_count != b.reply_count) {
                     return a.reply_count > b.reply_count;
                   }
                   return RawIdNumber(a.person) < RawIdNumber(b.person);
                 },
                 20);
    std::vector<std::vector<std::string>> rows;
    rows.reserve(rows_raw.size());
    for (const auto& item : rows_raw) {
      rows.push_back({RawId(item.person),
                      GetNodeProp(item.person, "firstName"),
                      GetNodeProp(item.person, "lastName"),
                      item.tag_names,
                      std::to_string(item.reply_count)});
    }
    EmitRows(std::move(rows), &out);
    return out;
  }

  QueryRunResult RunQuery13(const CsvRow& row) {
    QueryRunResult out;
    const auto p1 = LookupVertexId(NodeKind::kPerson, row.Get("person1Id"));
    const auto p2 = LookupVertexId(NodeKind::kPerson, row.Get("person2Id"));
    if (!p1.has_value() || !p2.has_value()) {
      return out;
    }
    int32_t found = -1;
    if (*p1 == *p2) {
      found = 0;
    } else {
      std::unordered_set<vertex_t> visited_l{*p1};
      std::unordered_set<vertex_t> visited_r{*p2};
      std::vector<vertex_t> frontier_l{*p1};
      std::vector<vertex_t> frontier_r{*p2};
      int32_t depth_l = 0;
      int32_t depth_r = 0;
      while (!frontier_l.empty() && !frontier_r.empty()) {
        const bool expand_left = frontier_l.size() <= frontier_r.size();
        auto& frontier = expand_left ? frontier_l : frontier_r;
        auto& visited_this = expand_left ? visited_l : visited_r;
        const auto& visited_other = expand_left ? visited_r : visited_l;
        std::vector<vertex_t> next;
        for (const auto curr : frontier) {
          for (const auto nb : Friends(curr)) {
            if (!visited_this.insert(nb).second) {
              continue;
            }
            if (visited_other.find(nb) != visited_other.end()) {
              found = depth_l + depth_r + 1;
              break;
            }
            next.push_back(nb);
          }
          if (found >= 0) {
            break;
          }
        }
        if (found >= 0) {
          break;
        }
        frontier.swap(next);
        if (expand_left) {
          ++depth_l;
        } else {
          ++depth_r;
        }
        if (FLAGS_snb_v1_frontier_limit > 0 &&
            visited_l.size() + visited_r.size() > FLAGS_snb_v1_frontier_limit) {
          break;
        }
      }
    }
    std::vector<std::vector<std::string>> rows = {{std::to_string(found)}};
    EmitRows(std::move(rows), &out);
    return out;
  }

  QueryRunResult RunQuery14(const CsvRow& row) {
    QueryRunResult out;
    const auto p1 = LookupVertexId(NodeKind::kPerson, row.Get("person1Id"));
    const auto p2 = LookupVertexId(NodeKind::kPerson, row.Get("person2Id"));
    if (!p1.has_value() || !p2.has_value()) {
      return out;
    }
    if (*p1 == *p2) {
      std::vector<std::vector<std::string>> rows = {{RawId(*p1), "0.0"}};
      EmitRows(std::move(rows), &out);
      return out;
    }

    std::unordered_map<vertex_t, int32_t> dist;
    std::unordered_map<vertex_t, std::vector<vertex_t>> parents;
    std::deque<vertex_t> q;
    dist[*p1] = 0;
    q.push_back(*p1);
    int32_t shortest = -1;
    while (!q.empty()) {
      const vertex_t curr = q.front();
      q.pop_front();
      const int32_t curr_dist = dist[curr];
      if (shortest >= 0 && curr_dist >= shortest) {
        continue;
      }
      for (const auto nb : Friends(curr)) {
        const int32_t next_dist = curr_dist + 1;
        if (shortest >= 0 && next_dist > shortest) {
          continue;
        }
        const auto dist_it = dist.find(nb);
        if (dist_it == dist.end()) {
          dist[nb] = next_dist;
          parents[nb].push_back(curr);
          q.push_back(nb);
          if (nb == *p2) {
            shortest = next_dist;
          }
        } else if (dist_it->second == next_dist) {
          parents[nb].push_back(curr);
        }
      }
      if (FLAGS_snb_v1_frontier_limit > 0 &&
          dist.size() > FLAGS_snb_v1_frontier_limit) {
        break;
      }
    }
    if (shortest < 0) {
      return out;
    }

    for (auto& kv : parents) {
      SortVerticesByRawId(&kv.second);
    }
    struct PathRow {
      std::vector<vertex_t> path;
      double weight = 0.0;
    };
    std::unordered_map<std::string, double> weight_cache;
    const auto pair_weight = [&](vertex_t a, vertex_t b) {
      const uint64_t ra = RawIdNumber(a);
      const uint64_t rb = RawIdNumber(b);
      const std::string key = ra < rb
                                  ? std::to_string(ra) + ":" + std::to_string(rb)
                                  : std::to_string(rb) + ":" + std::to_string(ra);
      const auto it = weight_cache.find(key);
      if (it != weight_cache.end()) {
        return it->second;
      }
      const double value = InteractionWeight(a, b);
      weight_cache.emplace(key, value);
      return value;
    };

    std::vector<PathRow> path_rows;
    std::vector<vertex_t> suffix{*p2};
    std::function<void(vertex_t)> dfs = [&](vertex_t curr) {
      if (curr == *p1) {
        std::vector<vertex_t> path = suffix;
        std::reverse(path.begin(), path.end());
        double weight = 0.0;
        for (size_t i = 1; i < path.size(); ++i) {
          weight += pair_weight(path[i - 1], path[i]);
        }
        path_rows.push_back(PathRow{std::move(path), weight});
        return;
      }
      const auto it = parents.find(curr);
      if (it == parents.end()) {
        return;
      }
      for (const auto parent : it->second) {
        suffix.push_back(parent);
        dfs(parent);
        suffix.pop_back();
        if (FLAGS_snb_v1_frontier_limit > 0 &&
            path_rows.size() > FLAGS_snb_v1_frontier_limit) {
          return;
        }
      }
    };
    dfs(*p2);
    SortAndLimit(&path_rows,
                 [&](const PathRow& a, const PathRow& b) {
                   if (a.weight != b.weight) {
                     return a.weight > b.weight;
                   }
                   std::vector<std::string> a_ids;
                   std::vector<std::string> b_ids;
                   for (const auto vid : a.path) {
                     a_ids.push_back(RawId(vid));
                   }
                   for (const auto vid : b.path) {
                     b_ids.push_back(RawId(vid));
                   }
                   return a_ids < b_ids;
                 },
                 0);
    std::vector<std::vector<std::string>> rows;
    rows.reserve(path_rows.size());
    for (const auto& item : path_rows) {
      std::vector<std::string> ids;
      ids.reserve(item.path.size());
      for (const auto vid : item.path) {
        ids.push_back(RawId(vid));
      }
      rows.push_back({JoinStrings(ids), FormatPathWeight(item.weight)});
    }
    EmitRows(std::move(rows), &out);
    return out;
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
      case 13:
        return RunQuery13(row);
      case 14:
        return RunQuery14(row);
      default:
        return QueryRunResult{};
    }
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
        single_edge_sampler_.BuildRequests(FLAGS_snb_v1_single_edge_read_ops,
                                           FLAGS_snb_v1_single_edge_hot_weight,
                                           FLAGS_snb_v1_single_edge_cold_weight,
                                           FLAGS_snb_v1_single_edge_seed);
    const auto prepare_t2 = std::chrono::steady_clock::now();
    const double prepare_sec =
        std::chrono::duration_cast<std::chrono::duration<double>>(prepare_t2 -
                                                                  prepare_t1)
            .count();

    std::cout << "[SINGLE_EDGE_READ_PREPARE] requested_ops: "
              << FLAGS_snb_v1_single_edge_read_ops << std::endl;
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
        checksums[i] =
            HashMix(static_cast<uint64_t>(requests[i].src)) ^
            HashMix(static_cast<uint64_t>(requests[i].dst)) ^
            HashMix(static_cast<uint64_t>(requests[i].edge_type)) ^
            HashMix(static_cast<uint64_t>(requests[i].property_id)) ^
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

  std::string QueryParamFile(int query_id) const {
    const std::string plus = FLAGS_snb_v1_params_root + "/interactive_plus_" +
                             std::to_string(query_id) + "_param.txt";
    if (FLAGS_snb_v1_use_plus_params && FileOrDirExists(plus)) {
      return plus;
    }
    return FLAGS_snb_v1_params_root + "/interactive_" +
           std::to_string(query_id) + "_param.txt";
  }

	  const LoadedCsvFile* LoadedParamsForQuery(int query_id) const {
	    for (const auto& file : loaded_param_files_) {
	      if (file.query_id == query_id) {
	        return &file.data;
      }
	    }
	    return nullptr;
	  }

	  void PrintMixedWorkloadStats(const MixedWorkloadStats& stats) const {
		    const uint64_t total_ops =
		        stats.node_writes + stats.edge_writes + stats.query_ops;
		    const uint64_t total_writes = stats.node_writes + stats.edge_writes;
		    const double total_sec = MixedTotalElapsedSec(stats);
		    const double write_sec = stats.write_sec + stats.background_wait_sec;
		    const double total_qps = total_sec <= 0.0 || total_ops == 0
		                                 ? 0.0
		                                 : static_cast<double>(total_ops) / total_sec;
		    const double write_qps =
		        write_sec <= 0.0 || total_writes == 0
		            ? 0.0
		            : static_cast<double>(total_writes) / write_sec;
	    const double query_qps =
	        stats.query_sec <= 0.0 || stats.query_ops == 0
	            ? 0.0
	            : static_cast<double>(stats.query_ops) / stats.query_sec;
		    std::cout << "[MIXED_WORKLOAD] time(s): " << total_sec << std::endl;
		    std::cout << "[MIXED_WORKLOAD] write_time(s): " << stats.write_sec
		              << std::endl;
    std::cout << "[MIXED_WORKLOAD] query_time(s): " << stats.query_sec
              << std::endl;
	    std::cout << "[MIXED_WORKLOAD] logical_update_rows: "
	              << stats.logical_update_rows << std::endl;
	    std::cout << "[MIXED_WORKLOAD] node_writes: " << stats.node_writes
	              << std::endl;
	    std::cout << "[MIXED_WORKLOAD] edge_writes: " << stats.edge_writes
	              << std::endl;
	    std::cout << "[MIXED_WORKLOAD] skipped_edges: " << stats.skipped_edges
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
	    for (int qid = 1; qid <= 14; ++qid) {
	      const auto& metric = stats.query_metrics[static_cast<size_t>(qid)];
	      const double qps =
	          metric.sec <= 0.0 || metric.param_rows == 0
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
    if (!FLAGS_snb_v1_enable_hot_edge_csr) {
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
            &stream.file->rows[row_index],
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
        !batch->node_writes.empty() || !batch->edge_writes.empty();
    if (!has_writes && tasks.empty() && batch->skipped_edges == 0) {
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
          if (idx >= batch->edge_writes.size()) {
            return false;
          }
          ExecutePreparedEdgeWriteOne(batch->edge_writes[idx]);
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
              next_edge.load() >= batch->edge_writes.size()) {
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
      metric.checksum ^= HashMix(run.checksum + task.ordinal);
      metric.sec += query_secs[i];
      ++stats->query_ops;
      stats->result_rows += run.rows;
      stats->checksum ^=
          HashMix(run.checksum + static_cast<uint64_t>(task.query_id));
    }
	    stats->total_sec += foreground_batch_sec;
    ResetPreparedImportBatch(batch);
  }

  ImportStats ImportRemainingUpdateStream() {
    struct UpdateCursor {
      std::string name;
      bool person = false;
      std::ifstream in;
      std::vector<char> buffer;
      std::string line;
      std::vector<std::string> fields;
      uint64_t time = 0;
      uint64_t rows = 0;
      bool has = false;
      const SnbV1GraphDbTest* owner = nullptr;

      bool Open(const std::string& path) {
        return OpenCsvInput(path, &in, &buffer);
      }

      bool Advance(uint64_t row_limit) {
        while (std::getline(in, line)) {
          if (line.empty()) {
            continue;
          }
          if (row_limit > 0 && rows >= row_limit) {
            has = false;
            return false;
          }
          const uint64_t row_index = rows++;
          if (owner != nullptr && !owner->IsRemainingWorkloadRow(row_index)) {
            continue;
          }
          fields = SplitPipe(line);
          time = ToUint64(FieldAt(fields, 0));
          has = true;
          return true;
        }
        has = false;
        return false;
      }
    };

    ImportStats import_stats;
    if (WorkloadSampleMod() <= 1) {
      return import_stats;
    }

    UpdateCursor person;
    person.name = "updateStream_person";
    person.person = true;
    person.owner = this;
    UpdateCursor forum;
    forum.name = "updateStream_forum";
    forum.person = false;
    forum.owner = this;
    if (!person.Open(UpdateStreamPath("person")) ||
        !forum.Open(UpdateStreamPath("forum"))) {
      std::cerr << "failed to open update streams for remaining import"
                << std::endl;
      std::exit(1);
    }
    person.Advance(FLAGS_snb_v1_update_row_limit_per_file);
    forum.Advance(FLAGS_snb_v1_update_row_limit_per_file);

    auto next_update = [&]() -> UpdateCursor* {
      if (person.has && (!forum.has || person.time <= forum.time)) {
        return &person;
      }
      if (forum.has) {
        return &forum;
      }
      return nullptr;
    };

    auto batch = NewPreparedBatch("remaining_update_batch",
                                  ImportBatchRows(),
                                  ImportBatchRows());
    ImportStats batch_prep_stats;
    while (person.has || forum.has) {
      UpdateCursor* update = next_update();
      if (update == nullptr) {
        break;
      }

      if (update->person) {
        ImportUpdatePersonRow(update->fields,
                              &batch->node_writes,
                              &batch->edge_writes,
                              &batch_prep_stats);
      } else {
        ImportUpdateForumRow(update->fields,
                             &batch->node_writes,
                             &batch->edge_writes,
                             &batch_prep_stats);
      }
      ++batch->logical_rows;
      update->Advance(FLAGS_snb_v1_update_row_limit_per_file);
      if (batch->logical_rows >= ImportBatchRows() || LoadArenaNearFull()) {
        batch->skipped_edges = batch_prep_stats.skipped_edges;
        FlushPreparedImportBatch(batch.get(), &import_stats);
        ReleasePreparedBatch(&batch, "remaining_update");
        batch = NewPreparedBatch("remaining_update_batch",
                                 ImportBatchRows(),
                                 ImportBatchRows());
        batch_prep_stats = ImportStats{};
      }
    }
    batch->skipped_edges = batch_prep_stats.skipped_edges;
    FlushPreparedImportBatch(batch.get(), &import_stats);
    ReleasePreparedBatch(&batch, "remaining_update");
    AddColdBlobFlushTime(&import_stats);
    return import_stats;
  }

	  MixedWorkloadStats RunMixedWorkload() {
	    struct UpdateCursor {
	      std::string name;
	      bool person = false;
	      std::ifstream in;
	      std::vector<char> buffer;
	      std::string line;
	      std::vector<std::string> fields;
	      uint64_t time = 0;
	      uint64_t rows = 0;
	      bool has = false;
	      const SnbV1GraphDbTest* owner = nullptr;
	      bool sampled = true;

	      bool Open(const std::string& path) {
	        return OpenCsvInput(path, &in, &buffer);
	      }

	      bool Advance(uint64_t row_limit) {
	        while (std::getline(in, line)) {
	          if (line.empty()) {
	            continue;
	          }
	          if (row_limit > 0 && rows >= row_limit) {
	            has = false;
	            return false;
	          }
	          const uint64_t row_index = rows++;
	          if (owner != nullptr && !owner->SelectWorkloadRow(row_index, sampled)) {
	            continue;
	          }
	          fields = SplitPipe(line);
	          time = ToUint64(FieldAt(fields, 0));
	          has = true;
	          return true;
	        }
	        has = false;
	        return false;
	      }
	    };
	    MixedWorkloadStats stats;
	    for (int qid = 1; qid <= 14; ++qid) {
	      stats.query_metrics[static_cast<size_t>(qid)].query_id = qid;
	    }

	    UpdateCursor person;
	    person.name = "updateStream_person";
	    person.person = true;
	    person.owner = this;
	    person.sampled = true;
	    UpdateCursor forum;
	    forum.name = "updateStream_forum";
	    forum.person = false;
	    forum.owner = this;
	    forum.sampled = true;
	    if (!person.Open(UpdateStreamPath("person")) ||
	        !forum.Open(UpdateStreamPath("forum"))) {
	      std::cerr << "failed to open update streams for mixed workload"
	                << std::endl;
	      std::exit(1);
	    }
	    person.Advance(FLAGS_snb_v1_update_row_limit_per_file);
	    forum.Advance(FLAGS_snb_v1_update_row_limit_per_file);
	    if (!person.has && !forum.has) {
	      PrintMixedWorkloadStats(stats);
	      return stats;
	    }

    const uint64_t total_update_rows = CountMixedUpdateRows();
    std::cout << "[MIXED_WORKLOAD] total_update_rows_for_progress: "
              << total_update_rows << std::endl;

		    std::vector<MixedQueryStream> streams;
		    if (FLAGS_snb_v1_mix_enable_queries) {
		      streams.reserve(14);
		      for (int qid = 1; qid <= 14; ++qid) {
		        const LoadedCsvFile* params = LoadedParamsForQuery(qid);
		        if (params == nullptr) {
		          continue;
		        }
		        auto indices = SelectedParamRowIndices(params->rows.size());
		        if (indices.empty()) {
		          continue;
		        }
		        streams.push_back(MixedQueryStream{qid, params, std::move(indices), 0});
		      };
		    }

	    auto next_update = [&]() -> UpdateCursor* {
	      if (person.has && (!forum.has || person.time <= forum.time)) {
	        return &person;
	      }
	      if (forum.has) {
	        return &forum;
	      }
	      return nullptr;
	    };

	    auto batch = NewPreparedBatch("mixed_update_batch",
	                                  ImportBatchRows(),
	                                  ImportBatchRows());
	    ImportStats import_stats;
	    ImportStats batch_prep_stats;
    uint64_t cumulative_updates = 0;

    auto execute_batch = [&](bool recreate) {
	      batch->skipped_edges = batch_prep_stats.skipped_edges;
	      std::vector<MixedQueryTask> tasks;
	      if (FLAGS_snb_v1_mix_enable_queries) {
	        AppendQueryTasksByProgress(&streams,
	                                   cumulative_updates,
	                                   std::max<uint64_t>(1, total_update_rows),
	                                   &tasks);
	      }
      ExecuteMixedConcurrentBatch(batch.get(), tasks, &import_stats, &stats);
      ReleasePreparedBatch(&batch, "mixed_update");
      if (recreate) {
        batch = NewPreparedBatch("mixed_update_batch",
                                 ImportBatchRows(),
                                 ImportBatchRows());
      }
      batch_prep_stats = ImportStats{};
    };

	    while (person.has || forum.has) {
	      UpdateCursor* update = next_update();
	      if (update == nullptr) {
	        break;
	      }

	      if (update->person) {
	        ImportUpdatePersonRow(update->fields,
	                              &batch->node_writes,
	                              &batch->edge_writes,
	                              &batch_prep_stats);
	      } else {
	        ImportUpdateForumRow(update->fields,
	                             &batch->node_writes,
	                             &batch->edge_writes,
	                             &batch_prep_stats);
	      }
	      ++batch->logical_rows;
      ++cumulative_updates;
	      update->Advance(FLAGS_snb_v1_update_row_limit_per_file);
	      if (batch->logical_rows >= ImportBatchRows() || LoadArenaNearFull()) {
	        execute_batch(true);
	      }
	    }
    cumulative_updates = std::max(cumulative_updates, total_update_rows);
	    execute_batch(false);
	    stats.logical_update_rows = import_stats.logical_rows;
	    stats.node_writes = import_stats.node_writes;
	    stats.edge_writes = import_stats.edge_writes;
	    stats.skipped_edges = import_stats.skipped_edges;
	    stats.write_sec = import_stats.sec;
	    PrintMixedWorkloadStats(stats);
	    return stats;
	  }

	  QueryMetrics RunOneQueryType(int query_id) {
    QueryMetrics metric;
    metric.query_id = query_id;
    const LoadedCsvFile* loaded = LoadedParamsForQuery(query_id);
    if (loaded == nullptr) {
      std::cerr << "missing loaded params for query: " << query_id << std::endl;
      return metric;
    }
    const std::vector<size_t> row_indices =
        SelectedParamRowIndices(loaded->rows.size());

    std::vector<QueryRunResult> results(row_indices.size());
    const auto t1 = std::chrono::steady_clock::now();
    ParallelForIndexDynamic(row_indices.size(), GetReadThreadCount(), [&](size_t i) {
      results[i] = RunQuery(query_id, loaded->rows[row_indices[i]]);
    });
    const auto t2 = std::chrono::steady_clock::now();

    for (size_t i = 0; i < results.size(); ++i) {
      metric.result_rows += results[i].rows;
      metric.checksum ^= HashMix(results[i].checksum + row_indices[i] + 1);
    }
    metric.param_rows = row_indices.size();
    metric.sec =
        std::chrono::duration_cast<std::chrono::duration<double>>(t2 - t1).count();
    return metric;
  }

  void RunAllQueries() {
    std::vector<QueryMetrics> metrics;
    metrics.reserve(14);
    const auto total_t1 = std::chrono::steady_clock::now();
    uint64_t total_rows = 0;
    uint64_t total_params = 0;
    uint64_t total_checksum = 0;
    for (int qid = 1; qid <= 14; ++qid) {
      const QueryMetrics metric = RunOneQueryType(qid);
      total_rows += metric.result_rows;
      total_params += metric.param_rows;
      total_checksum ^= HashMix(metric.checksum + static_cast<uint64_t>(qid));
      metrics.push_back(metric);
      const double qps = metric.sec <= 0.0 || metric.param_rows == 0
                             ? 0.0
                             : static_cast<double>(metric.param_rows) / metric.sec;
      std::cout << "[QUERY_" << qid << "] time(s): " << metric.sec << std::endl;
      std::cout << "[QUERY_" << qid << "] params: " << metric.param_rows
                << std::endl;
      std::cout << "[QUERY_" << qid << "] qps(param/s): " << qps << std::endl;
      std::cout << "[QUERY_" << qid << "] result_rows: " << metric.result_rows
                << std::endl;
      std::cout << "[QUERY_" << qid << "] checksum: " << metric.checksum
                << std::endl;
    }
    const auto total_t2 = std::chrono::steady_clock::now();
    const double total_sec =
        std::chrono::duration_cast<std::chrono::duration<double>>(total_t2 - total_t1)
            .count();
    const double total_qps = total_sec <= 0.0 || total_params == 0
                                 ? 0.0
                                 : static_cast<double>(total_params) / total_sec;
    std::cout << "[QUERY_TOTAL] time(s): " << total_sec << std::endl;
    std::cout << "[QUERY_TOTAL] total_params: " << total_params << std::endl;
    std::cout << "[QUERY_TOTAL] qps(param/s): " << total_qps << std::endl;
    std::cout << "[QUERY_TOTAL] total_result_rows: " << total_rows << std::endl;
    std::cout << "[QUERY_TOTAL] checksum: " << total_checksum << std::endl;
  }

  lsmgraph::GraphDb* db_ = nullptr;
  uint64_t next_vertex_id_ = 0;
  std::array<std::vector<IdPair>, 16> entity_to_vid_;
  std::array<bool, 16> id_maps_sorted_{};
  std::vector<NodeKind> vid_to_kind_;
  std::vector<uint64_t> creation_by_vid_;
  HotNodeColumns hot_nodes_;
  std::vector<LoadedNodeTable> loaded_node_tables_;
  std::vector<LoadedParamFile> loaded_param_files_;
  std::unordered_map<NodeKind,
                     std::unordered_map<std::string, std::vector<vertex_t>>>
      name_to_vertices_;
  std::unordered_map<std::string, std::string> cold_blob_cache_;
  std::mutex cold_blob_cache_mu_;
  ColdBlobWriterSet cold_blob_writers_;
  ColdBlobWriterSet node_cold_blob_writers_;
  FixedResidentArena load_arena_;
	  SingleEdgeReadSampler single_edge_sampler_;
	  WriteLatencySampler write_latency_sampler_;
	  WriteLatencySampler node_write_latency_sampler_;
	  bool write_latency_sampling_active_ = true;
  std::unordered_set<std::string> preprocessed_edge_slot_need_;
  std::unordered_map<std::string, uint16_t> preprocessed_edge_slot_;
  std::mutex preprocessed_edge_slot_mu_;
  static thread_local std::pmr::memory_resource* tls_load_resource_override_;
  std::array<std::unordered_map<uint64_t, vertex_t>, 16>
      preprocessed_id_index_;
	};

thread_local std::pmr::memory_resource*
    SnbV1GraphDbTest::tls_load_resource_override_ = nullptr;

}  // namespace

int main(int argc, char** argv) {
  google::ParseCommandLineFlags(&argc, &argv, true);
  SnbV1GraphDbTest test;
  return test.Run();
}
