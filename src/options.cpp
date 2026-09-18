#include "richgraph/options.h"

#include <limits>

namespace lsmgraph {

std::string GraphDbOptions::Validate() const {
  if (memtable_count < 2) {
    return "memtable_count must be at least 2";
  }
  if (default_memtable_capacity == 0) {
    return "default_memtable_capacity must be greater than 0";
  }
  if (default_memtable_capacity > std::numeric_limits<std::uint32_t>::max()) {
    return "default_memtable_capacity exceeds the current engine limit";
  }
  if (max_subcompactions == 0) {
    return "max_subcompactions must be greater than 0";
  }
  if (property_updates.enabled) {
    if (!cache_sst_data) {
      return "property updates currently require cache_sst_data=true";
    }
    if (property_updates.buffer_count < 2) {
      return "property update buffer_count must be at least 2 when enabled";
    }
    if (property_updates.buffer_capacity_records == 0) {
      return "property update buffer_capacity_records must be greater than 0";
    }
    if (property_updates.buffer_capacity_bytes == 0) {
      return "property update buffer_capacity_bytes must be greater than 0";
    }
    if (property_updates.buffer_capacity_records >
        std::numeric_limits<std::uint32_t>::max()) {
      return "property update buffer_capacity_records exceeds the current "
             "engine limit";
    }
    if (property_updates.delta_chain_merge_threshold == 0) {
      return "delta_chain_merge_threshold must be greater than 0";
    }
  }
  return {};
}

}  // namespace lsmgraph
