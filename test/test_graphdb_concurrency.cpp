#include <richgraph/graph_db.h>
#include <unistd.h>

#include <atomic>
#include <cassert>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace {

constexpr lsmgraph::VertexId_t kSource = 0;
constexpr lsmgraph::VertexId_t kTarget = 1;
constexpr std::uint8_t kEdgeType = 3;
constexpr std::uint64_t kUpdateCount = 300;

std::string VersionValue(std::uint64_t version) {
  std::ostringstream stream;
  stream << std::setw(8) << std::setfill('0') << version;
  return stream.str();
}

std::uint64_t ParseVersion(const std::string& value) {
  return static_cast<std::uint64_t>(std::stoull(value));
}

void WriteSchema(const std::filesystem::path& path) {
  std::ofstream out(path);
  assert(out.is_open());
  out << "max_vertex_num: 32\n"
      << "use_csr_disk: false\n"
      << "sub_property_num: 1\n"
      << "max_property_length: 16\n"
      << "system_threads: 4\n"
      << "property_defs:\n"
      << "  - name: value\n"
      << "    length: 16\n"
      << "edge_shards:\n"
      << "  - name: edge_Db0\n"
      << "    properties: [value]\n"
      << "    memtable_size: 4\n"
      << "node_db:\n"
      << "  name: node_Db\n"
      << "  properties: [value]\n"
      << "  memtable_size: 4\n";
  assert(out.good());
}

lsmgraph::GraphDbOptions Options(bool load_existing) {
  lsmgraph::GraphDbOptions options;
  options.load_existing = load_existing;
  options.default_memtable_capacity = 4;
  options.memtable_count = 2;
  options.max_subcompactions = 1;
  options.background_threads = 4;
  options.property_updates.buffer_count = 2;
  options.property_updates.buffer_capacity_records = 4;
  options.property_updates.buffer_capacity_bytes = 4096;
  options.property_updates.delta_chain_merge_threshold = 2;
  options.property_updates.durability =
      lsmgraph::DeltaDurability::kProcessCrashSafe;
  return options;
}

bool ReadPoint(lsmgraph::GraphDb* db, std::uint64_t* version) {
  std::unordered_map<std::string, std::string> properties;
  if (db->GetEdge(kSource, kTarget, {"value"}, &properties, true, kEdgeType) !=
      lsmgraph::Status::kOk) {
    return false;
  }
  *version = ParseVersion(properties.at("value"));
  return true;
}

bool ReadScan(lsmgraph::GraphDb* db, std::uint64_t* version) {
  std::vector<lsmgraph::GraphDbEdgeScanRecord> records;
  if (db->ScanEdges(kSource, {"value"}, &records, true, kEdgeType) !=
      lsmgraph::Status::kOk) {
    return false;
  }
  for (const auto& record : records) {
    if (record.dst == kTarget) {
      *version = ParseVersion(record.properties.at("value"));
      return true;
    }
  }
  return false;
}

}  // namespace

int main() {
  const std::filesystem::path root =
      std::filesystem::temp_directory_path() /
      ("rich_graphdb_concurrency_" + std::to_string(::getpid()));
  std::error_code filesystem_error;
  std::filesystem::remove_all(root, filesystem_error);
  filesystem_error.clear();
  std::filesystem::create_directories(root, filesystem_error);
  assert(!filesystem_error);
  const auto schema = root / "schema.yaml";
  WriteSchema(schema);

  std::unique_ptr<lsmgraph::GraphDb> db;
  std::string error;
  assert(lsmgraph::GraphDb::OpenFromYaml(root.string(), schema.string(),
                                         Options(false), &db,
                                         &error) == lsmgraph::Status::kOk);
  db->InitVerticesUpTo(8);

  // Fill the tiny MemTable so the target topology and base property become
  // persistent before concurrent property-only updates begin.
  for (lsmgraph::VertexId_t dst = 1; dst <= 4; ++dst) {
    assert(db->PutEdge(kSource, dst, {{"value", VersionValue(0)}},
                       lsmgraph::EdgeInsertMode::kSingle, true,
                       kEdgeType) == lsmgraph::Status::kOk);
  }
  db->WaitBackgroundIdle();

  std::atomic<std::uint64_t> submitted{0};
  std::atomic<bool> writer_done{false};
  std::atomic<bool> maintenance_done{false};
  std::atomic<bool> failed{false};

  std::thread writer([&] {
    for (std::uint64_t version = 1; version <= kUpdateCount; ++version) {
      if (db->UpdateEdge(kSource, kTarget, {{"value", VersionValue(version)}},
                         true, kEdgeType) != lsmgraph::Status::kOk) {
        failed.store(true, std::memory_order_release);
        break;
      }
      submitted.store(version, std::memory_order_release);
    }
    writer_done.store(true, std::memory_order_release);
  });

  std::thread maintenance([&] {
    for (const std::uint64_t milestone : {75ULL, 150ULL, 225ULL}) {
      while (submitted.load(std::memory_order_acquire) < milestone &&
             !writer_done.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
      if (db->ForceCompactAllShards() != lsmgraph::Status::kOk) {
        failed.store(true, std::memory_order_release);
        break;
      }
    }
    maintenance_done.store(true, std::memory_order_release);
  });

  std::uint64_t last_point = 0;
  std::uint64_t last_scan = 0;
  while (!writer_done.load(std::memory_order_acquire) ||
         !maintenance_done.load(std::memory_order_acquire)) {
    std::uint64_t point = 0;
    std::uint64_t scan = 0;
    if (!ReadPoint(db.get(), &point) || !ReadScan(db.get(), &scan) ||
        point < last_point || scan < last_scan) {
      failed.store(true, std::memory_order_release);
      break;
    }
    last_point = point;
    last_scan = scan;
  }

  writer.join();
  maintenance.join();
  assert(!failed.load(std::memory_order_acquire));
  assert(db->FlushPropertyUpdates() == lsmgraph::Status::kOk);
  db->WaitBackgroundIdle();

  std::uint64_t final_point = 0;
  std::uint64_t final_scan = 0;
  assert(ReadPoint(db.get(), &final_point));
  assert(ReadScan(db.get(), &final_scan));
  assert(final_point == kUpdateCount);
  assert(final_scan == kUpdateCount);

  db.reset();
  assert(lsmgraph::GraphDb::OpenFromYaml(root.string(), schema.string(),
                                         Options(true), &db,
                                         &error) == lsmgraph::Status::kOk);
  assert(ReadPoint(db.get(), &final_point));
  assert(ReadScan(db.get(), &final_scan));
  assert(final_point == kUpdateCount);
  assert(final_scan == kUpdateCount);

  db.reset();
  std::filesystem::remove_all(root, filesystem_error);
  std::cout << "test_graphdb_concurrency passed\n";
  return 0;
}
