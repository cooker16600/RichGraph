#include "core/property_delta_store.h"

#include <algorithm>
#include <filesystem>
#include <unordered_set>
#include <utility>

namespace lsmgraph {
namespace {

void SetError(std::string* error, std::string message) {
  if (error != nullptr) *error = std::move(message);
}

bool MetadataMatches(const PropertyDeltaFileMetadata& expected,
                     const PropertyDeltaFileMetadata& actual) {
  return expected.target == actual.target &&
         expected.file_generation == actual.file_generation &&
         expected.record_count == actual.record_count &&
         expected.file_bytes == actual.file_bytes &&
         expected.min_commit_sequence == actual.min_commit_sequence &&
         expected.max_commit_sequence == actual.max_commit_sequence &&
         expected.payload_checksum == actual.payload_checksum;
}

std::string DeltaFileName(const PropertyDeltaTarget& target,
                          std::uint64_t generation) {
  const char kind = target.kind == PropertyObjectKind::kNode ? 'n' : 'e';
  return std::string("delta-") + kind + "-s" +
         std::to_string(target.shard_id) + "-f" +
         std::to_string(target.base_file_id) + "-b" +
         std::to_string(target.base_generation) + "-p" +
         std::to_string(target.property_id) + "-g" +
         std::to_string(generation) + ".rgd";
}

std::string BaseName(const std::string& path) {
  return std::filesystem::path(path).filename().string();
}

template <typename T>
void AtomicMax(std::atomic<T>* target, T candidate) {
  T current = target->load(std::memory_order_relaxed);
  while (current < candidate &&
         !target->compare_exchange_weak(current,
                                        candidate,
                                        std::memory_order_release,
                                        std::memory_order_relaxed)) {
  }
}

}  // namespace

bool PropertyDeltaReadView::Lookup(
    VertexId_t src,
    VertexId_t dst,
    SequenceNumber_t base_sequence,
    bool is_out,
    std::uint8_t edge_type,
    PropertyCommitSequence read_sequence,
    std::string* value,
    PropertyCommitSequence* found_commit) const {
  if (value == nullptr) return false;
  if (counters != nullptr) {
    counters->lookups.fetch_add(1, std::memory_order_relaxed);
  }

  bool found = false;
  PropertyCommitSequence best_commit = 0;
  std::string best_value;
  for (auto it = deltas.rbegin(); it != deltas.rend(); ++it) {
    if ((*it)->metadata().min_commit_sequence > read_sequence ||
        (found && (*it)->metadata().max_commit_sequence <= best_commit)) {
      continue;
    }
    if (counters != nullptr) {
      counters->files_probed.fetch_add(1, std::memory_order_relaxed);
    }
    PropertyCommitSequence candidate_commit = 0;
    std::string candidate_value;
    if ((*it)->Lookup(src,
                      dst,
                      base_sequence,
                      is_out,
                      edge_type,
                      read_sequence,
                      &candidate_value,
                      &candidate_commit) &&
        (!found || candidate_commit > best_commit)) {
      found = true;
      best_commit = candidate_commit;
      best_value = std::move(candidate_value);
    }
  }
  if (found) {
    *value = std::move(best_value);
    if (found_commit != nullptr) *found_commit = best_commit;
    if (counters != nullptr) {
      counters->hits.fetch_add(1, std::memory_order_relaxed);
    }
    return true;
  }
  if (counters != nullptr) {
    counters->base_fallbacks.fetch_add(1, std::memory_order_relaxed);
  }
  return false;
}

std::unique_ptr<PropertyDeltaStore> PropertyDeltaStore::Open(
    const std::string& directory,
    std::uint32_t merge_threshold,
    DeltaDurability durability,
    PropertyDeltaMergeCallback merge_callback,
    std::string* error) {
  auto store = std::unique_ptr<PropertyDeltaStore>(new PropertyDeltaStore());
  if (!store->OpenAndRecover(directory,
                             merge_threshold,
                             durability,
                             std::move(merge_callback),
                             error)) {
    return nullptr;
  }
  return store;
}

bool PropertyDeltaStore::OpenAndRecover(
    const std::string& directory,
    std::uint32_t merge_threshold,
    DeltaDurability durability,
    PropertyDeltaMergeCallback merge_callback,
    std::string* error) {
  if (merge_threshold == 0) {
    SetError(error, "property delta merge threshold must be greater than 0");
    return false;
  }
  directory_ = directory;
  merge_threshold_ = merge_threshold;
  durability_ = durability;
  merge_callback_ = std::move(merge_callback);
  manifest_ = PropertyDeltaManifest::Open(directory_, durability_, error);
  if (manifest_ == nullptr) return false;

  std::uint64_t maximum_generation = 0;
  std::vector<std::string> abandoned_entries;
  std::vector<PropertyDeltaManifestEntry> promoted_entries;
  for (auto entry : manifest_->ActiveEntries()) {
    maximum_generation = std::max(maximum_generation,
                                  entry.metadata.file_generation);

    // A delta attached to a mutable MemTable is recoverable only if the
    // matching topology SST reached disk before the crash. MemTable and SST
    // deliberately retain the same file ID, which lets recovery finish an
    // interrupted handoff without guessing from timestamps.
    if (!entry.target_is_persistent) {
      const std::filesystem::path base_path =
          std::filesystem::path(directory_).parent_path() /
          (std::to_string(entry.metadata.target.base_file_id) + ".sst");
      if (std::filesystem::exists(base_path)) {
        entry.target_is_persistent = true;
        promoted_entries.push_back(entry);
      } else {
        abandoned_entries.push_back(entry.file_name);
        continue;
      }
    }

    std::string open_error;
    const std::string path =
        (std::filesystem::path(directory_) / entry.file_name).string();
    auto file = PropertyDeltaFile::Open(path, &open_error);
    if (file == nullptr) {
      SetError(error, "recover " + entry.file_name + ": " + open_error);
      return false;
    }
    if (!MetadataMatches(entry.metadata, file->metadata())) {
      SetError(error, "manifest metadata mismatch for " + entry.file_name);
      return false;
    }
    const auto chain = GetOrCreateChain(entry.metadata.target,
                                        entry.target_is_persistent);
    std::lock_guard<std::mutex> lock(chain->mutex);
    auto old_view = std::atomic_load_explicit(&chain->read_view,
                                              std::memory_order_acquire);
    auto view = std::make_shared<PropertyDeltaReadView>();
    if (old_view != nullptr) *view = *old_view;
    view->counters = lookup_counters_;
    view->deltas.push_back(std::move(file));
    std::sort(view->deltas.begin(), view->deltas.end(), [](const auto& lhs,
                                                           const auto& rhs) {
      return lhs->metadata().file_generation <
             rhs->metadata().file_generation;
    });
    view->generation = std::max(view->generation,
                                entry.metadata.file_generation);
    std::atomic_store_explicit(
        &chain->read_view,
        std::shared_ptr<const PropertyDeltaReadView>(std::move(view)),
        std::memory_order_release);
    AtomicMax(&maximum_commit_sequence_,
              entry.metadata.max_commit_sequence);
    files_recovered_.fetch_add(1, std::memory_order_relaxed);
  }
  for (const auto& entry : promoted_entries) {
    if (!manifest_->AppendAdd(entry, error)) return false;
  }
  if (!abandoned_entries.empty()) {
    if (!manifest_->AppendRemove(abandoned_entries, error)) return false;
    for (const auto& name : abandoned_entries) {
      std::error_code remove_error;
      std::filesystem::remove(
          std::filesystem::path(directory_) / name, remove_error);
    }
  }
  next_file_generation_.store(maximum_generation + 1,
                              std::memory_order_release);
  RemoveOrphanFiles();
  worker_ = std::thread(&PropertyDeltaStore::WorkerMain, this);

  // Recovery may expose a chain already at threshold. Schedule it only after
  // the worker exists and only when the target has a persistent base.
  for (auto& bucket : registry_) {
    std::vector<std::pair<PropertyDeltaTarget, std::shared_ptr<Chain>>> items;
    {
      std::shared_lock<std::shared_mutex> lock(bucket.mutex);
      items.reserve(bucket.chains.size());
      for (const auto& item : bucket.chains) items.push_back(item);
    }
    for (const auto& item : items) {
      bool schedule = false;
      {
        std::lock_guard<std::mutex> lock(item.second->mutex);
        const auto view = std::atomic_load_explicit(&item.second->read_view,
                                                    std::memory_order_acquire);
        if (merge_callback_ && item.second->target_is_persistent &&
            view != nullptr &&
            view->deltas.size() >= merge_threshold_) {
          item.second->merge_scheduled_or_running = true;
          schedule = true;
        }
      }
      if (schedule) ScheduleMerge(item.first, item.second);
    }
  }
  return true;
}

PropertyDeltaStore::~PropertyDeltaStore() {
  StopWorker();
}

std::size_t PropertyDeltaStore::BucketIndex(
    const PropertyDeltaTarget& target) const {
  return PropertyDeltaTargetHash{}(target) % kRegistryBucketCount;
}

std::shared_ptr<PropertyDeltaStore::Chain>
PropertyDeltaStore::GetOrCreateChain(const PropertyDeltaTarget& target,
                                     bool target_is_persistent) {
  {
    std::lock_guard<std::mutex> persistent_lock(persistent_targets_mutex_);
    target_is_persistent = target_is_persistent ||
        persistent_base_files_.find(target.base_file_id) !=
            persistent_base_files_.end();
  }
  RegistryBucket& bucket = registry_[BucketIndex(target)];
  std::unique_lock<std::shared_mutex> lock(bucket.mutex);
  auto [it, inserted] = bucket.chains.try_emplace(target);
  if (inserted) {
    it->second = std::make_shared<Chain>();
    auto view = std::make_shared<PropertyDeltaReadView>();
    view->counters = lookup_counters_;
    std::atomic_store_explicit(
        &it->second->read_view,
        std::shared_ptr<const PropertyDeltaReadView>(std::move(view)),
        std::memory_order_release);
  }
  if (target_is_persistent) {
    std::lock_guard<std::mutex> chain_lock(it->second->mutex);
    it->second->target_is_persistent = true;
  }
  return it->second;
}

std::shared_ptr<PropertyDeltaStore::Chain> PropertyDeltaStore::FindChain(
    const PropertyDeltaTarget& target) const {
  const RegistryBucket& bucket = registry_[BucketIndex(target)];
  std::shared_lock<std::shared_mutex> lock(bucket.mutex);
  const auto it = bucket.chains.find(target);
  return it == bucket.chains.end() ? nullptr : it->second;
}

bool PropertyDeltaStore::Attach(const PropertyDeltaTarget& target,
                                std::vector<PropertyDeltaRecord> records,
                                bool target_is_persistent,
                                std::string* error) {
  if (records.empty()) {
    SetError(error, "cannot attach an empty property delta batch");
    return false;
  }
  const auto chain = GetOrCreateChain(target, target_is_persistent);
  const std::uint64_t generation =
      next_file_generation_.fetch_add(1, std::memory_order_acq_rel);
  const std::string file_name = DeltaFileName(target, generation);
  const std::string path =
      (std::filesystem::path(directory_) / file_name).string();
  auto file = PropertyDeltaFile::Create(path,
                                        target,
                                        generation,
                                        std::move(records),
                                        durability_,
                                        error);
  if (file == nullptr) return false;

  PropertyDeltaManifestEntry manifest_entry;
  manifest_entry.file_name = file_name;
  manifest_entry.metadata = file->metadata();
  bool schedule = false;
  {
    std::lock_guard<std::mutex> lock(chain->mutex);
    // MarkTargetPersistent serializes on the same chain mutex. Therefore a
    // concurrent MemTable-to-SST handoff cannot leave this just-created file
    // recorded with stale non-persistent metadata.
    manifest_entry.target_is_persistent = chain->target_is_persistent;
    if (!manifest_->AppendAdd(manifest_entry, error)) {
      file->MarkObsolete();
      return false;
    }
    const auto old_view = std::atomic_load_explicit(&chain->read_view,
                                                    std::memory_order_acquire);
    auto view = std::make_shared<PropertyDeltaReadView>();
    if (old_view != nullptr) *view = *old_view;
    view->counters = lookup_counters_;
    view->deltas.push_back(file);
    view->generation = std::max(view->generation, generation);
    std::atomic_store_explicit(
        &chain->read_view,
        std::shared_ptr<const PropertyDeltaReadView>(std::move(view)),
        std::memory_order_release);
    if (merge_callback_ && chain->target_is_persistent &&
        !chain->merge_scheduled_or_running &&
        old_view != nullptr &&
        old_view->deltas.size() + 1 >= merge_threshold_) {
      chain->merge_scheduled_or_running = true;
      schedule = true;
    }
  }
  files_created_.fetch_add(1, std::memory_order_relaxed);
  records_written_.fetch_add(file->metadata().record_count,
                             std::memory_order_relaxed);
  bytes_written_.fetch_add(file->metadata().file_bytes,
                           std::memory_order_relaxed);
  AtomicMax(&maximum_commit_sequence_,
            file->metadata().max_commit_sequence);
  if (schedule) ScheduleMerge(target, chain);
  return true;
}

std::shared_ptr<const PropertyDeltaReadView> PropertyDeltaStore::ReadView(
    const PropertyDeltaTarget& target) const {
  const auto chain = FindChain(target);
  if (chain == nullptr) return nullptr;
  return std::atomic_load_explicit(&chain->read_view,
                                   std::memory_order_acquire);
}

bool PropertyDeltaStore::MarkTargetPersistent(
    const PropertyDeltaTarget& target,
    std::string* error) {
  {
    std::lock_guard<std::mutex> lock(persistent_targets_mutex_);
    persistent_base_files_.insert(target.base_file_id);
  }
  const auto chain = FindChain(target);
  if (chain == nullptr) return true;
  bool schedule = false;
  {
    std::lock_guard<std::mutex> lock(chain->mutex);
    const auto view = std::atomic_load_explicit(&chain->read_view,
                                                std::memory_order_acquire);
    if (!chain->target_is_persistent && view != nullptr) {
      for (const auto& file : view->deltas) {
        PropertyDeltaManifestEntry entry;
        entry.file_name = BaseName(file->path());
        entry.metadata = file->metadata();
        entry.target_is_persistent = true;
        std::string manifest_error;
        if (!manifest_->AppendAdd(entry, &manifest_error)) {
          SetError(error, manifest_error);
          RecordBackgroundError(manifest_error);
          return false;
        }
      }
    }
    chain->target_is_persistent = true;
    if (merge_callback_ && !chain->merge_scheduled_or_running &&
        view != nullptr && view->deltas.size() >= merge_threshold_) {
      chain->merge_scheduled_or_running = true;
      schedule = true;
    }
  }
  if (schedule) ScheduleMerge(target, chain);
  return true;
}

void PropertyDeltaStore::ScheduleMerge(
    const PropertyDeltaTarget& target,
    const std::shared_ptr<Chain>& chain) {
  {
    std::lock_guard<std::mutex> lock(worker_mutex_);
    jobs_.push_back(MergeJob{target, chain});
  }
  merges_started_.fetch_add(1, std::memory_order_relaxed);
  worker_cv_.notify_one();
}

void PropertyDeltaStore::WorkerMain() {
  while (true) {
    MergeJob job;
    {
      std::unique_lock<std::mutex> lock(worker_mutex_);
      worker_cv_.wait(lock, [&] { return stop_ || !jobs_.empty(); });
      if (stop_ && jobs_.empty()) break;
      job = std::move(jobs_.front());
      jobs_.pop_front();
      ++running_jobs_;
    }

    std::string error;
    if (!MergeChain(job.target, job.chain, false, &error)) {
      merge_failures_.fetch_add(1, std::memory_order_relaxed);
      RecordBackgroundError(std::move(error));
      std::lock_guard<std::mutex> chain_lock(job.chain->mutex);
      job.chain->merge_scheduled_or_running = false;
    }

    {
      std::lock_guard<std::mutex> lock(worker_mutex_);
      --running_jobs_;
      if (jobs_.empty() && running_jobs_ == 0) idle_cv_.notify_all();
    }
  }
}

bool PropertyDeltaStore::MergeChain(
    const PropertyDeltaTarget& target,
    const std::shared_ptr<Chain>& chain,
    bool merge_all,
    std::string* error) {
  if (chain == nullptr || !merge_callback_) {
    SetError(error, "property delta merge callback is not configured");
    return false;
  }

  std::shared_ptr<const PropertyDeltaReadView> captured;
  std::vector<std::shared_ptr<PropertyDeltaFile>> batch;
  {
    std::lock_guard<std::mutex> lock(chain->mutex);
    captured = std::atomic_load_explicit(&chain->read_view,
                                         std::memory_order_acquire);
    if (!chain->target_is_persistent || captured == nullptr ||
        captured->deltas.empty() ||
        (!merge_all && captured->deltas.size() < merge_threshold_)) {
      chain->merge_scheduled_or_running = false;
      return true;
    }
    const std::size_t count = merge_all ? captured->deltas.size()
                                        : merge_threshold_;
    batch.assign(captured->deltas.begin(),
                 captured->deltas.begin() + count);
  }

  const std::uint64_t output_generation =
      next_file_generation_.fetch_add(1, std::memory_order_acq_rel);
  std::shared_ptr<MappedPropertyFile> merged_base;
  if (!merge_callback_(target,
                       captured->merged_base,
                       batch,
                       output_generation,
                       &merged_base,
                       error) ||
      merged_base == nullptr) {
    if (error != nullptr && error->empty()) {
      *error = "property delta merge callback failed without an error";
    }
    return false;
  }

  std::vector<std::string> merged_names;
  merged_names.reserve(batch.size());
  for (const auto& file : batch) merged_names.push_back(BaseName(file->path()));
  if (!manifest_->AppendRemove(merged_names, error)) return false;

  bool schedule_next = false;
  {
    std::lock_guard<std::mutex> lock(chain->mutex);
    const auto current = std::atomic_load_explicit(&chain->read_view,
                                                   std::memory_order_acquire);
    if (current == nullptr || current->deltas.size() < batch.size()) {
      SetError(error, "property delta chain changed before merge publish");
      return false;
    }
    for (std::size_t i = 0; i < batch.size(); ++i) {
      if (current->deltas[i] != batch[i]) {
        SetError(error, "property delta chain prefix changed before publish");
        return false;
      }
    }
    auto next = std::make_shared<PropertyDeltaReadView>();
    next->merged_base = std::move(merged_base);
    next->deltas.assign(current->deltas.begin() + batch.size(),
                        current->deltas.end());
    next->counters = lookup_counters_;
    next->generation = output_generation;
    std::atomic_store_explicit(
        &chain->read_view,
        std::shared_ptr<const PropertyDeltaReadView>(std::move(next)),
        std::memory_order_release);
    for (const auto& file : batch) file->MarkObsolete();
    chain->merge_scheduled_or_running = false;
    if (!merge_all && chain->target_is_persistent &&
        current->deltas.size() - batch.size() >= merge_threshold_) {
      chain->merge_scheduled_or_running = true;
      schedule_next = true;
    }
  }
  merges_completed_.fetch_add(1, std::memory_order_relaxed);
  if (schedule_next) ScheduleMerge(target, chain);
  return true;
}

bool PropertyDeltaStore::WaitForIdle(std::string* error) {
  std::unique_lock<std::mutex> lock(worker_mutex_);
  idle_cv_.wait(lock, [&] { return jobs_.empty() && running_jobs_ == 0; });
  if (!background_error_.empty()) {
    SetError(error, background_error_);
    return false;
  }
  return true;
}

bool PropertyDeltaStore::MergeAllForBaseFiles(
    const std::vector<FileId_t>& base_file_ids,
    std::string* error) {
  if (!WaitForIdle(error)) return false;
  const std::unordered_set<FileId_t> selected(base_file_ids.begin(),
                                               base_file_ids.end());
  std::vector<std::pair<PropertyDeltaTarget, std::shared_ptr<Chain>>> chains;
  for (auto& bucket : registry_) {
    std::shared_lock<std::shared_mutex> lock(bucket.mutex);
    for (const auto& item : bucket.chains) {
      if (selected.find(item.first.base_file_id) != selected.end()) {
        chains.push_back(item);
      }
    }
  }
  for (const auto& item : chains) {
    {
      std::lock_guard<std::mutex> lock(item.second->mutex);
      if (item.second->merge_scheduled_or_running) {
        SetError(error, "property delta chain unexpectedly busy");
        return false;
      }
      item.second->merge_scheduled_or_running = true;
    }
    if (!MergeChain(item.first, item.second, true, error)) {
      std::lock_guard<std::mutex> lock(item.second->mutex);
      item.second->merge_scheduled_or_running = false;
      return false;
    }
  }
  return true;
}

bool PropertyDeltaStore::MergeAllPersistent(std::string* error) {
  if (!WaitForIdle(error)) return false;
  std::vector<std::pair<PropertyDeltaTarget, std::shared_ptr<Chain>>> chains;
  for (auto& bucket : registry_) {
    std::shared_lock<std::shared_mutex> lock(bucket.mutex);
    for (const auto& item : bucket.chains) {
      chains.push_back(item);
    }
  }
  for (const auto& item : chains) {
    bool needs_merge = false;
    {
      std::lock_guard<std::mutex> lock(item.second->mutex);
      const auto view = std::atomic_load_explicit(&item.second->read_view,
                                                  std::memory_order_acquire);
      needs_merge = item.second->target_is_persistent && view != nullptr &&
                    !view->deltas.empty();
      if (needs_merge) {
        if (item.second->merge_scheduled_or_running) {
          SetError(error, "property delta chain unexpectedly busy");
          return false;
        }
        item.second->merge_scheduled_or_running = true;
      }
    }
    if (needs_merge &&
        !MergeChain(item.first, item.second, true, error)) {
      std::lock_guard<std::mutex> lock(item.second->mutex);
      item.second->merge_scheduled_or_running = false;
      return false;
    }
  }
  return true;
}

PropertyDeltaStoreStats PropertyDeltaStore::Stats() const {
  PropertyDeltaStoreStats stats;
  stats.files_created = files_created_.load(std::memory_order_relaxed);
  stats.files_recovered = files_recovered_.load(std::memory_order_relaxed);
  stats.records_written = records_written_.load(std::memory_order_relaxed);
  stats.bytes_written = bytes_written_.load(std::memory_order_relaxed);
  stats.merges_started = merges_started_.load(std::memory_order_relaxed);
  stats.merges_completed = merges_completed_.load(std::memory_order_relaxed);
  stats.merge_failures = merge_failures_.load(std::memory_order_relaxed);
  stats.lookups = lookup_counters_->lookups.load(std::memory_order_relaxed);
  stats.files_probed =
      lookup_counters_->files_probed.load(std::memory_order_relaxed);
  stats.hits = lookup_counters_->hits.load(std::memory_order_relaxed);
  stats.base_fallbacks =
      lookup_counters_->base_fallbacks.load(std::memory_order_relaxed);
  for (const auto& bucket : registry_) {
    std::shared_lock<std::shared_mutex> lock(bucket.mutex);
    for (const auto& item : bucket.chains) {
      const auto view = std::atomic_load_explicit(&item.second->read_view,
                                                  std::memory_order_acquire);
      ++stats.chain_count;
      const std::uint64_t length = view == nullptr ? 0 : view->deltas.size();
      stats.visible_files += length;
      stats.max_chain_length = std::max(stats.max_chain_length, length);
    }
  }
  return stats;
}

std::string PropertyDeltaStore::LastBackgroundError() const {
  std::lock_guard<std::mutex> lock(worker_mutex_);
  return background_error_;
}

void PropertyDeltaStore::RecordBackgroundError(std::string error) {
  std::lock_guard<std::mutex> lock(worker_mutex_);
  if (background_error_.empty()) background_error_ = std::move(error);
}

void PropertyDeltaStore::StopWorker() {
  {
    std::unique_lock<std::mutex> lock(worker_mutex_);
    if (!worker_.joinable()) return;
    idle_cv_.wait(lock, [&] { return jobs_.empty() && running_jobs_ == 0; });
    stop_ = true;
  }
  worker_cv_.notify_all();
  worker_.join();
}

void PropertyDeltaStore::RemoveOrphanFiles() {
  std::unordered_set<std::string> active;
  for (const auto& entry : manifest_->ActiveEntries()) {
    active.insert(entry.file_name);
  }
  std::error_code error;
  for (const auto& directory_entry :
       std::filesystem::directory_iterator(directory_, error)) {
    if (error) break;
    if (!directory_entry.is_regular_file()) continue;
    const std::string name = directory_entry.path().filename().string();
    const bool is_delta = name.rfind("delta-", 0) == 0 &&
                          name.find(".rgd") != std::string::npos;
    const bool is_temporary = name.find(".tmp.") != std::string::npos;
    if ((is_delta && active.find(name) == active.end()) || is_temporary) {
      std::filesystem::remove(directory_entry.path(), error);
      error.clear();
    }
  }
}

}  // namespace lsmgraph
