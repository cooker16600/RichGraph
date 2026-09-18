#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

namespace lsmgraph {

enum class DeltaDurability {
  kNone,
  kProcessCrashSafe,
};

struct PropertyUpdateOptions {
  bool enabled{true};
  std::size_t buffer_capacity_records{5'000'000};
  std::size_t buffer_capacity_bytes{256ULL * 1024ULL * 1024ULL};
  std::uint32_t buffer_count{2};
  std::uint32_t delta_chain_merge_threshold{4};
  DeltaDurability durability{DeltaDurability::kProcessCrashSafe};
};

struct GraphDbOptions {
  std::uint32_t memtable_count{2};
  std::size_t default_memtable_capacity{1'000'000};
  std::uint32_t max_subcompactions{1};
  std::uint32_t background_threads{0};

  bool support_multi_version{true};
  bool load_existing{false};
  bool cache_sst_data{true};
  bool verbose_logging{false};
  std::string mmap_path;

  PropertyUpdateOptions property_updates;

  // Returns an empty string when the options are valid.
  [[nodiscard]] std::string Validate() const;
};

}  // namespace lsmgraph
