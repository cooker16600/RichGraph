#include "core/property_delta_manifest.h"

#include <cassert>
#include <filesystem>
#include <fcntl.h>
#include <string>
#include <unistd.h>

namespace {

lsmgraph::PropertyDeltaManifestEntry Entry(const std::string& file_name,
                                           std::uint64_t generation) {
  lsmgraph::PropertyDeltaManifestEntry entry;
  entry.file_name = file_name;
  entry.metadata.target.kind = lsmgraph::PropertyObjectKind::kNode;
  entry.metadata.target.shard_id = 0;
  entry.metadata.target.base_file_id = 12;
  entry.metadata.target.base_generation = 12;
  entry.metadata.target.property_id = 1;
  entry.metadata.file_generation = generation;
  entry.metadata.record_count = 3;
  entry.metadata.file_bytes = 512;
  entry.metadata.min_commit_sequence = 7;
  entry.metadata.max_commit_sequence = 9;
  entry.metadata.payload_checksum = 44;
  entry.target_is_persistent = true;
  return entry;
}

}  // namespace

int main() {
  const std::filesystem::path directory =
      std::filesystem::temp_directory_path() /
      ("rich_property_manifest_" + std::to_string(::getpid()));
  std::error_code ec;
  std::filesystem::remove_all(directory, ec);

  std::string error;
  auto manifest = lsmgraph::PropertyDeltaManifest::Open(
      directory.string(),
      lsmgraph::DeltaDurability::kProcessCrashSafe,
      &error);
  assert(manifest != nullptr);
  assert(manifest->ActiveEntries().empty());
  assert(manifest->AppendAdd(Entry("delta-1.rgd", 1), &error));
  assert(manifest->AppendAdd(Entry("delta-2.rgd", 2), &error));
  assert(manifest->ActiveEntries().size() == 2);
  assert(manifest->AppendRemove({"delta-1.rgd"}, &error));
  assert(manifest->ActiveEntries().size() == 1);
  manifest.reset();

  manifest = lsmgraph::PropertyDeltaManifest::Open(
      directory.string(),
      lsmgraph::DeltaDurability::kProcessCrashSafe,
      &error);
  assert(manifest != nullptr);
  auto entries = manifest->ActiveEntries();
  assert(entries.size() == 1);
  assert(entries[0].file_name == "delta-2.rgd");
  assert(entries[0].metadata.file_generation == 2);
  assert(entries[0].target_is_persistent);
  const std::string path = manifest->path();
  manifest.reset();

  // Simulate a process dying after writing only part of the next record. The
  // valid prefix must recover and the torn suffix must be removed.
  const int fd = ::open(path.c_str(), O_WRONLY | O_APPEND);
  assert(fd >= 0);
  const char torn[] = "RGMANI";
  assert(::write(fd, torn, sizeof(torn)) ==
         static_cast<ssize_t>(sizeof(torn)));
  ::close(fd);
  const auto size_with_torn_tail = std::filesystem::file_size(path);

  manifest = lsmgraph::PropertyDeltaManifest::Open(
      directory.string(),
      lsmgraph::DeltaDurability::kProcessCrashSafe,
      &error);
  assert(manifest != nullptr);
  assert(manifest->ActiveEntries().size() == 1);
  assert(std::filesystem::file_size(path) < size_with_torn_tail);

  std::filesystem::remove_all(directory, ec);
  return 0;
}
