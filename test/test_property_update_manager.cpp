#include "core/property_update_manager.h"

#include <cassert>
#include <condition_variable>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace {

lsmgraph::BufferedPropertyUpdate MakeUpdate(
    lsmgraph::VertexId_t src,
    lsmgraph::VertexId_t dst,
    lsmgraph::SequenceNumber_t base_sequence,
    std::string value) {
  lsmgraph::BufferedPropertyUpdate update;
  update.target.kind = lsmgraph::PropertyObjectKind::kEdge;
  update.target.shard_id = 2;
  update.target.base_file_id = 17;
  update.target.base_generation = 17;
  update.target.property_id = 1;
  update.record.src = src;
  update.record.dst = dst;
  update.record.base_sequence = base_sequence;
  update.record.is_out = true;
  update.record.edge_type = 3;
  update.record.value = std::move(value);
  update.target_is_persistent = true;
  return update;
}

}  // namespace

int main() {
  lsmgraph::PropertyUpdateOptions options;
  options.enabled = true;
  options.buffer_count = 2;
  options.buffer_capacity_records = 2;
  options.buffer_capacity_bytes = 4096;
  options.delta_chain_merge_threshold = 4;
  options.durability = lsmgraph::DeltaDurability::kNone;

  std::mutex flush_mutex;
  std::condition_variable flush_cv;
  bool allow_first_flush = false;
  std::vector<std::vector<lsmgraph::PropertyDeltaRecord>> published;

  lsmgraph::PropertyUpdateManager manager(
      options,
      [&](const lsmgraph::PropertyDeltaTarget& target,
          std::vector<lsmgraph::PropertyDeltaRecord> records,
          bool persistent,
          std::string*) {
        assert(target.base_file_id == 17);
        assert(persistent);
        std::unique_lock<std::mutex> lock(flush_mutex);
        if (published.empty()) {
          flush_cv.wait(lock, [&] { return allow_first_flush; });
        }
        published.push_back(std::move(records));
        return true;
      },
      100);

  lsmgraph::PropertyCommitSequence first_commit = 0;
  lsmgraph::PropertyCommitSequence second_commit = 0;
  std::string error;
  assert(manager.Submit(MakeUpdate(1, 9, 41, "v1"),
                        &first_commit, &error));
  assert(manager.Submit(MakeUpdate(1, 9, 41, "v2"),
                        &second_commit, &error));
  assert(first_commit == 100);
  assert(second_commit == 101);

  // The first full buffer is flushing but remains query-visible until the
  // callback confirms that its delta file has been published.
  std::string value;
  assert(manager.LookupLatest(lsmgraph::PropertyObjectKind::kEdge,
                              2, 1, 1, 9, true, 3, &value));
  assert(value == "v2");
  assert(manager.LookupExact(lsmgraph::PropertyObjectKind::kEdge,
                             2, 1, 1, 9, 41, true, 3, &value));
  assert(!manager.LookupExact(lsmgraph::PropertyObjectKind::kEdge,
                              2, 1, 1, 9, 42, true, 3, &value));

  {
    std::lock_guard<std::mutex> lock(flush_mutex);
    allow_first_flush = true;
  }
  flush_cv.notify_all();
  assert(manager.FlushAndWait(&error));
  assert(!manager.LookupLatest(lsmgraph::PropertyObjectKind::kEdge,
                               2, 1, 1, 9, true, 3, &value));
  assert(published.size() == 1);
  assert(published.front().size() == 2);

  std::vector<lsmgraph::BufferedPropertyUpdate> batch;
  batch.push_back(MakeUpdate(2, 10, 42, "left"));
  auto second_property = MakeUpdate(2, 10, 42, "right");
  second_property.target.property_id = 2;
  batch.push_back(std::move(second_property));
  lsmgraph::PropertyCommitSequence batch_commit = 0;
  assert(manager.SubmitBatch(std::move(batch), &batch_commit, &error));
  assert(batch_commit == 102);
  assert(manager.FlushAndWait(&error));
  assert(published.size() == 3);
  assert(published[1].size() == 1);
  assert(published[2].size() == 1);
  assert(published[1].front().commit_sequence == batch_commit);
  assert(published[2].front().commit_sequence == batch_commit);

  std::vector<lsmgraph::BufferedPropertyUpdate> oversized;
  oversized.push_back(MakeUpdate(3, 11, 43, "one"));
  oversized.push_back(MakeUpdate(3, 11, 43, "two"));
  oversized.push_back(MakeUpdate(3, 11, 43, "three"));
  assert(!manager.SubmitBatch(std::move(oversized), nullptr, &error));

  const auto stats = manager.Stats();
  assert(stats.submitted_records == 4);
  assert(stats.buffers_flushed == 2);
  assert(stats.delta_batches_published == 3);
  assert(stats.pending_keys == 0);
  return 0;
}
