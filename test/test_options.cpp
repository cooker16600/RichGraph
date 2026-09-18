#include <cassert>
#include <iostream>

#include "richgraph/options.h"
#include "richgraph/types.h"

int main() {
  lsmgraph::GraphDbOptions options;
  assert(options.Validate().empty());

  options.memtable_count = 1;
  assert(!options.Validate().empty());
  options.memtable_count = 2;

  options.max_subcompactions = 0;
  assert(!options.Validate().empty());
  options.max_subcompactions = 1;

  options.property_updates.buffer_count = 1;
  assert(!options.Validate().empty());
  options.property_updates.buffer_count = 2;

  options.property_updates.delta_chain_merge_threshold = 0;
  assert(!options.Validate().empty());

  options.property_updates.delta_chain_merge_threshold = 4;
  options.cache_sst_data = false;
  assert(!options.Validate().empty());

  options.property_updates.enabled = false;
  assert(options.Validate().empty());

  assert(lsmgraph::StatusName(lsmgraph::Status::kOk) == "ok");
  assert(lsmgraph::StatusName(lsmgraph::Status::kInvalidArgument) ==
         "invalid_argument");

  std::cout << "test_options passed\n";
  return 0;
}
