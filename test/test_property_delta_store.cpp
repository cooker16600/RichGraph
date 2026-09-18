#include "core/property_delta_store.h"

#include <algorithm>
#include <cassert>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <string>
#include <unistd.h>
#include <vector>

namespace {

constexpr std::size_t kSlotBytes = 16;
constexpr std::size_t kSlotCount = 2;

void StoreSlot(std::vector<std::byte>* bytes,
               std::size_t slot,
               const std::string& value) {
  assert(bytes != nullptr);
  assert(slot < kSlotCount);
  const std::size_t offset = slot * kSlotBytes;
  std::fill(bytes->begin() + offset,
            bytes->begin() + offset + kSlotBytes,
            std::byte{0});
  const std::size_t size = std::min(kSlotBytes, value.size());
  std::memcpy(bytes->data() + offset, value.data(), size);
}

std::string LoadSlot(const std::byte* bytes, std::size_t slot) {
  const char* begin =
      reinterpret_cast<const char*>(bytes + slot * kSlotBytes);
  std::size_t size = 0;
  while (size < kSlotBytes && begin[size] != '\0') ++size;
  return std::string(begin, size);
}

lsmgraph::PropertyDeltaRecord Record(std::uint64_t src,
                                     std::uint64_t commit,
                                     std::string value) {
  lsmgraph::PropertyDeltaRecord record;
  record.src = src;
  record.dst = src + 1;
  record.base_sequence = 99;
  record.commit_sequence = commit;
  record.is_out = true;
  record.edge_type = 7;
  record.value = std::move(value);
  return record;
}

}  // namespace

int main() {
  const std::filesystem::path root =
      std::filesystem::temp_directory_path() /
      ("rich_property_delta_store_" + std::to_string(::getpid()));
  const std::filesystem::path delta_directory = root / "property_delta";
  const std::filesystem::path base_path = root / "base-property.bin";
  std::error_code ec;
  std::filesystem::remove_all(root, ec);
  std::filesystem::create_directories(root, ec);
  assert(!ec);

  std::vector<std::byte> initial(kSlotBytes * kSlotCount, std::byte{0});
  StoreSlot(&initial, 0, "base-0");
  StoreSlot(&initial, 1, "base-1");
  {
    std::ofstream out(base_path, std::ios::binary | std::ios::trunc);
    out.write(reinterpret_cast<const char*>(initial.data()), initial.size());
    assert(out.good());
  }

  std::uint64_t callback_count = 0;
  lsmgraph::PropertyDeltaMergeCallback merge_callback =
      [&](const lsmgraph::PropertyDeltaTarget&,
          const std::shared_ptr<const lsmgraph::MappedPropertyFile>& current,
          const std::vector<std::shared_ptr<lsmgraph::PropertyDeltaFile>>& files,
          std::uint64_t generation,
          std::shared_ptr<lsmgraph::MappedPropertyFile>* output,
          std::string* error) {
        ++callback_count;
        std::vector<std::byte> bytes(kSlotBytes * kSlotCount, std::byte{0});
        if (current != nullptr) {
          assert(current->size() == bytes.size());
          std::memcpy(bytes.data(), current->data(), bytes.size());
        } else {
          std::ifstream in(base_path, std::ios::binary);
          in.read(reinterpret_cast<char*>(bytes.data()), bytes.size());
          if (!in.good()) {
            if (error != nullptr) *error = "read base failed";
            return false;
          }
        }
        for (const auto& file : files) {
          if (!file->ForEachRecord(
                  [&](const lsmgraph::PropertyDeltaRecord& record) {
                    StoreSlot(&bytes,
                              record.src == 10 ? 0 : 1,
                              record.value);
                    return true;
                  })) {
            if (error != nullptr) *error = "visit delta failed";
            return false;
          }
        }
        const std::filesystem::path temporary =
            base_path.string() + ".tmp." + std::to_string(generation);
        {
          std::ofstream out(temporary, std::ios::binary | std::ios::trunc);
          out.write(reinterpret_cast<const char*>(bytes.data()), bytes.size());
          if (!out.good()) {
            if (error != nullptr) *error = "write replacement failed";
            return false;
          }
        }
        std::filesystem::rename(temporary, base_path, ec);
        if (ec) {
          if (error != nullptr) *error = ec.message();
          return false;
        }
        *output = lsmgraph::MappedPropertyFile::Open(
            base_path.string(), generation, error);
        return *output != nullptr;
      };

  const lsmgraph::PropertyDeltaTarget target{
      lsmgraph::PropertyObjectKind::kEdge, 0, 8, 8, 0};
  std::string error;
  auto store = lsmgraph::PropertyDeltaStore::Open(
      delta_directory.string(),
      2,
      lsmgraph::DeltaDurability::kProcessCrashSafe,
      merge_callback,
      &error);
  assert(store != nullptr);

  assert(store->Attach(target, {Record(10, 1, "delta-1")}, true, &error));
  auto view = store->ReadView(target);
  assert(view != nullptr);
  std::string value;
  lsmgraph::PropertyCommitSequence found_commit = 0;
  assert(view->Lookup(10, 11, 99, true, 7, 1, &value, &found_commit));
  assert(value == "delta-1");
  assert(found_commit == 1);

  // Reaching (not exceeding) threshold=2 schedules one merge.
  assert(store->Attach(target, {Record(10, 2, "delta-2")}, true, &error));
  assert(store->WaitForIdle(&error));
  assert(callback_count == 1);
  view = store->ReadView(target);
  assert(view != nullptr);
  assert(view->deltas.empty());
  assert(view->merged_base != nullptr);
  assert(LoadSlot(view->merged_base->data(), 0) == "delta-2");
  auto stats = store->Stats();
  assert(stats.merges_started == 1);
  assert(stats.merges_completed == 1);
  assert(stats.merge_failures == 0);

  // Leave one committed delta below threshold, then recover it from manifest.
  assert(store->Attach(target, {Record(20, 3, "recovered")}, true, &error));
  store.reset();
  store = lsmgraph::PropertyDeltaStore::Open(
      delta_directory.string(),
      2,
      lsmgraph::DeltaDurability::kProcessCrashSafe,
      merge_callback,
      &error);
  assert(store != nullptr);
  view = store->ReadView(target);
  assert(view != nullptr);
  assert(view->deltas.size() == 1);
  assert(view->Lookup(20, 21, 99, true, 7, 3, &value, &found_commit));
  assert(value == "recovered");
  assert(found_commit == 3);
  assert(store->Stats().files_recovered == 1);

  // A non-persistent MemTable target may reach threshold without merging. It
  // becomes eligible immediately after the MemTable-to-SST handoff.
  const lsmgraph::PropertyDeltaTarget pending{
      lsmgraph::PropertyObjectKind::kNode, 0, 77, 77, 0};
  assert(store->Attach(pending, {Record(10, 4, "pending-1")}, false, &error));
  assert(store->Attach(pending, {Record(10, 5, "pending-2")}, false, &error));
  assert(store->WaitForIdle(&error));
  assert(store->ReadView(pending)->deltas.size() == 2);
  assert(store->MarkTargetPersistent(pending, &error));
  assert(store->WaitForIdle(&error));
  assert(store->ReadView(pending)->deltas.empty());
  assert(callback_count == 2);

  store.reset();
  std::filesystem::remove_all(root, ec);
  return 0;
}
