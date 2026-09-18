#include "core/property_delta.h"

#include <cassert>
#include <filesystem>
#include <fcntl.h>
#include <string>
#include <unistd.h>
#include <vector>

namespace {

lsmgraph::PropertyDeltaRecord Record(
    lsmgraph::VertexId_t src,
    lsmgraph::VertexId_t dst,
    lsmgraph::SequenceNumber_t base_sequence,
    lsmgraph::PropertyCommitSequence commit_sequence,
    std::string value) {
  lsmgraph::PropertyDeltaRecord record;
  record.src = src;
  record.dst = dst;
  record.base_sequence = base_sequence;
  record.commit_sequence = commit_sequence;
  record.is_out = true;
  record.edge_type = 7;
  record.value = std::move(value);
  return record;
}

}  // namespace

int main() {
  const std::filesystem::path directory =
      std::filesystem::temp_directory_path() /
      ("rich_property_delta_file_" + std::to_string(::getpid()));
  std::error_code ec;
  std::filesystem::remove_all(directory, ec);
  std::filesystem::create_directories(directory, ec);
  assert(!ec);

  const lsmgraph::PropertyDeltaTarget target{
      lsmgraph::PropertyObjectKind::kEdge, 3, 42, 9, 1};
  const std::string path = (directory / "delta-1.rgd").string();
  std::string error;
  auto file = lsmgraph::PropertyDeltaFile::Create(
      path,
      target,
      1,
      {Record(1, 2, 17, 5, "five"),
       Record(1, 2, 17, 1, "one"),
       Record(9, 10, 18, 2, "other"),
       Record(1, 2, 17, 3, "three"),
       Record(1, 2, 17, 3, "three-last")},
      lsmgraph::DeltaDurability::kProcessCrashSafe,
      &error);
  assert(file != nullptr);
  assert(error.empty());
  assert(file->metadata().target == target);
  assert(file->metadata().record_count == 4);
  assert(file->metadata().min_commit_sequence == 1);
  assert(file->metadata().max_commit_sequence == 5);

  std::string value;
  lsmgraph::PropertyCommitSequence commit = 0;
  assert(file->Lookup(1, 2, 17, true, 7, 4, &value, &commit));
  assert(value == "three-last");
  assert(commit == 3);
  assert(file->Lookup(1,
                      2,
                      17,
                      true,
                      7,
                      lsmgraph::kLatestPropertyCommit,
                      &value,
                      &commit));
  assert(value == "five");
  assert(commit == 5);
  assert(!file->Lookup(1, 2, 17, true, 7, 0, &value));
  assert(!file->Lookup(1, 99, 17, true, 7, 99, &value));

  std::vector<std::string> visited;
  assert(file->ForEachRecord(
      [&](const lsmgraph::PropertyDeltaRecord& record) {
        visited.push_back(record.value);
        return true;
      }));
  assert(visited.size() == 4);

  file.reset();
  auto reopened = lsmgraph::PropertyDeltaFile::Open(path, &error);
  assert(reopened != nullptr);
  assert(reopened->Lookup(9, 10, 18, true, 7, 2, &value));
  assert(value == "other");

  // A payload mutation must be rejected during recovery.
  const std::string corrupt_path = (directory / "corrupt.rgd").string();
  std::filesystem::copy_file(path, corrupt_path);
  const int fd = ::open(corrupt_path.c_str(), O_RDWR);
  assert(fd >= 0);
  unsigned char byte = 0;
  assert(::pread(fd, &byte, 1, 130) == 1);
  byte ^= 0x5aU;
  assert(::pwrite(fd, &byte, 1, 130) == 1);
  ::close(fd);
  error.clear();
  assert(lsmgraph::PropertyDeltaFile::Open(corrupt_path, &error) == nullptr);
  assert(error.find("checksum") != std::string::npos);

  reopened->MarkObsolete();
  reopened.reset();
  assert(!std::filesystem::exists(path));

  std::filesystem::remove_all(directory, ec);
  return 0;
}
