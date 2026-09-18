#include "richgraph/graph_db.h"
#include "core/flags.h"

#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <unordered_map>
#include <vector>

#include <unistd.h>

namespace {

void WriteSchemaFile(const std::string& path) {
  std::ofstream out(path);
  assert(out.is_open());

  // 受限 YAML（当前 GraphDb 解析器支持的格式）：
  // 1) 根级参数。
  // 2) edge_shards 下每个分片的 name/properties。
  // 3) node_db 的 name/properties。
  out << "max_vertex_num: 128\n";
  out << "use_csr_disk: false\n";
  out << "sub_property_num: 2\n";
  out << "max_property_length: 10\n";
  out << "system_threads: 4\n";
  out << "edge_shards:\n";
  out << "  - name: edge_Db0\n";
  out << "    properties: [A]\n";
  out << "  - name: edge_Db1\n";
  out << "    properties: [B, C]\n";
  out << "node_db:\n";
  out << "  name: node_Db\n";
  out << "  properties: [N0, N1]\n";
}

}  // namespace

int main() {
  lsmgraph::GraphDbOptions options;
  options.support_multi_version = true;
  options.load_existing = false;
  options.cache_sst_data = true;
  options.default_memtable_capacity = 8;
  options.memtable_count = 2;
  options.max_subcompactions = 1;
  options.property_updates.enabled = true;
  options.property_updates.buffer_count = 2;
  options.property_updates.buffer_capacity_records = 64;
  options.property_updates.buffer_capacity_bytes = 64 * 1024;
  options.property_updates.delta_chain_merge_threshold = 2;
  options.property_updates.durability =
      lsmgraph::DeltaDurability::kProcessCrashSafe;

  const std::string base_dir =
      "/tmp/rich_graphdb_e2e_" + std::to_string(getpid());
  std::error_code ec;
  std::filesystem::remove_all(base_dir, ec);
  std::filesystem::create_directories(base_dir, ec);
  assert(!ec);

  FLAGS_db_path = base_dir;

  const std::string schema_path = base_dir + "/schema.yaml";
  WriteSchemaFile(schema_path);

  lsmgraph::GraphDb* db = nullptr;
  const auto open_rs =
      lsmgraph::GraphDb::OpenFromYaml(base_dir, schema_path, options, &db);
  assert(open_rs == lsmgraph::Status::kOk);
  assert(db != nullptr);

  // 先分配一些点，确保所有子库中的 vertex id 对齐。
  for (int i = 0; i < 8; ++i) {
    const auto id = db->NewVertex(false);
    assert(id == static_cast<lsmgraph::VertexId_t>(i));
  }

  // 节点写读：按 src->src 存取。
  {
    const std::unordered_map<std::string, std::string> node_props = {
        {"N0", "alice"},
        {"N1", "beijing"},
    };
    assert(db->PutNode(1, node_props) == lsmgraph::Status::kOk);

    std::unordered_map<std::string, std::string> out;
    assert(db->GetNode(1, {"N0", "N1"}, &out) == lsmgraph::Status::kOk);
    assert(out["N0"] == "alice");
    assert(out["N1"] == "beijing");
  }

  // 边写入：A 单独在 edge_Db0，B/C 在 edge_Db1。
  // e12 同时写 A/B/C；e13 只写 B/C。
  {
    const std::unordered_map<std::string, std::string> e12 = {
        {"A", "a12"},
        {"B", "b12"},
        {"C", "c12"},
    };
    const std::unordered_map<std::string, std::string> e13 = {
        {"B", "b13"},
        {"C", "c13"},
    };

    assert(db->PutEdge(1, 2, e12, lsmgraph::EdgeInsertMode::kSingle,
                       true, 7) == lsmgraph::Status::kOk);
    assert(db->PutEdge(1, 3, e13, lsmgraph::EdgeInsertMode::kSingle,
                       true, 7) == lsmgraph::Status::kOk);
  }

  // 单边聚合读。
  {
    std::unordered_map<std::string, std::string> out12;
    assert(db->GetEdge(1, 2, {"A", "C"}, &out12, true, 7)
           == lsmgraph::Status::kOk);
    assert(out12["A"] == "a12");
    assert(out12["C"] == "c12");

    std::unordered_map<std::string, std::string> out13;
    assert(db->GetEdge(1, 3, {"B", "C"}, &out13, true, 7)
           == lsmgraph::Status::kOk);
    assert(out13["B"] == "b13");
    assert(out13["C"] == "c13");

    std::unordered_map<std::string, std::string> miss;
    // e13 没写 A，对 A 的请求应返回 NotFound。
    assert(db->GetEdge(1, 3, {"A"}, &miss, true, 7)
           == lsmgraph::Status::kNotFound);
  }

  // scan 聚合读：
  // - 请求 B/C 时，dst=2 和 dst=3 都应返回；
  // - 请求 A/C 时，只有 dst=2（dst=3 缺 A，应被过滤）。
  {
    std::vector<lsmgraph::GraphDbEdgeScanRecord> bc_records;
    assert(db->ScanEdges(1, {"B", "C"}, &bc_records, true, 7)
           == lsmgraph::Status::kOk);
    assert(bc_records.size() == 2);
    assert(bc_records[0].dst == 2);
    assert(bc_records[0].properties.at("B") == "b12");
    assert(bc_records[0].properties.at("C") == "c12");
    assert(bc_records[1].dst == 3);
    assert(bc_records[1].properties.at("B") == "b13");
    assert(bc_records[1].properties.at("C") == "c13");

    std::vector<lsmgraph::GraphDbEdgeScanRecord> ac_records;
    assert(db->ScanEdges(1, {"A", "C"}, &ac_records, true, 7)
           == lsmgraph::Status::kOk);
    assert(ac_records.size() == 1);
    assert(ac_records[0].dst == 2);
    assert(ac_records[0].properties.at("A") == "a12");
    assert(ac_records[0].properties.at("C") == "c12");
  }

  // ForEachEdge and ScanEdges must expose the same single-property values.
  {
    std::vector<std::pair<lsmgraph::VertexId_t, std::string>> callback_values;
    assert(db->ForEachEdge(
               1,
               "B",
               [&](lsmgraph::VertexId_t dst,
                   lsmgraph::SequenceNumber_t,
                   const std::string& value) {
                 callback_values.emplace_back(dst, value);
                 return true;
               },
               true,
               7) == lsmgraph::Status::kOk);

    std::vector<lsmgraph::GraphDbEdgeScanRecord> scan_values;
    assert(db->ScanEdges(1, {"B"}, &scan_values, true, 7)
           == lsmgraph::Status::kOk);
    assert(callback_values.size() == scan_values.size());
    for (size_t i = 0; i < callback_values.size(); ++i) {
      assert(callback_values[i].first == scan_values[i].dst);
      assert(callback_values[i].second == scan_values[i].properties.at("B"));
    }
  }

  // Engine-owned PropertyBuffer updates are visible immediately, before the
  // buffer has been flushed to a delta file. Point and scan APIs must agree.
  {
    lsmgraph::PropertyCommitSequence node_commit = 0;
    lsmgraph::PropertyCommitSequence edge_commit = 0;
    assert(db->UpdateNode(1, {{"N0", "alice-v2"}}, true, 0,
                          &node_commit) == lsmgraph::Status::kOk);
    assert(db->UpdateEdge(1,
                          2,
                          {{"A", "a12-v2"}, {"B", "b12-v2"}},
                          true,
                          7,
                          &edge_commit) == lsmgraph::Status::kOk);
    assert(node_commit != 0);
    assert(edge_commit > node_commit);

    std::unordered_map<std::string, std::string> node;
    assert(db->GetNode(1, {"N0", "N1"}, &node) == lsmgraph::Status::kOk);
    assert(node.at("N0") == "alice-v2");
    assert(node.at("N1") == "beijing");

    std::unordered_map<std::string, std::string> edge;
    assert(db->GetEdge(1, 2, {"A", "B", "C"}, &edge, true, 7)
           == lsmgraph::Status::kOk);
    assert(edge.at("A") == "a12-v2");
    assert(edge.at("B") == "b12-v2");
    assert(edge.at("C") == "c12");

    std::vector<lsmgraph::GraphDbEdgeScanRecord> scan;
    assert(db->ScanEdges(1, {"A", "B"}, &scan, true, 7)
           == lsmgraph::Status::kOk);
    assert(scan.size() == 1);
    assert(scan.front().properties.at("A") == "a12-v2");
    assert(scan.front().properties.at("B") == "b12-v2");

    std::string callback_value;
    assert(db->ForEachEdge(
               1,
               "A",
               [&](lsmgraph::VertexId_t dst,
                   lsmgraph::SequenceNumber_t,
                   const std::string& value) {
                 if (dst == 2) callback_value = value;
                 return true;
               },
               true,
               7) == lsmgraph::Status::kOk);
    assert(callback_value == "a12-v2");

    assert(db->FlushPropertyUpdates() == lsmgraph::Status::kOk);
    assert(db->GetEdge(1, 2, {"A"}, &edge, true, 7)
           == lsmgraph::Status::kOk);
    assert(edge.at("A") == "a12-v2");
  }

  // Add enough records to flush every shard, then repeat representative reads
  // to characterize parity between memory and SST paths.
  for (lsmgraph::VertexId_t v = 0; v < 8; ++v) {
    if (v == 1) {
      continue;
    }
    assert(db->PutNode(v, {{"N0", "node-" + std::to_string(v)},
                           {"N1", "city-" + std::to_string(v)}})
           == lsmgraph::Status::kOk);
  }
  for (lsmgraph::VertexId_t dst = 0; dst < 8; ++dst) {
    assert(db->PutEdge(2,
                       dst,
                       {{"A", "a-" + std::to_string(dst)},
                        {"B", "b-" + std::to_string(dst)},
                        {"C", "c-" + std::to_string(dst)}},
                       lsmgraph::EdgeInsertMode::kSingle,
                       true,
                       7) == lsmgraph::Status::kOk);
  }
  db->WaitBackgroundIdle();

  {
    std::unordered_map<std::string, std::string> node;
    assert(db->GetNode(1, {"N0", "N1"}, &node) == lsmgraph::Status::kOk);
    assert(node.at("N0") == "alice-v2");
    assert(node.at("N1") == "beijing");

    std::unordered_map<std::string, std::string> edge;
    assert(db->GetEdge(1, 2, {"A", "B", "C"}, &edge, true, 7)
           == lsmgraph::Status::kOk);
    assert(edge.at("A") == "a12-v2");
    assert(edge.at("B") == "b12-v2");
    assert(edge.at("C") == "c12");
  }

  // A second durable batch reaches threshold=2. The background merge may
  // replace the base property column, but externally visible values cannot
  // regress during or after that transition.
  {
    assert(db->UpdateNode(1, {{"N0", "alice-v3"}})
           == lsmgraph::Status::kOk);
    assert(db->UpdateEdge(1, 2, {{"A", "a12-v3"}}, true, 7)
           == lsmgraph::Status::kOk);
    assert(db->FlushPropertyUpdates() == lsmgraph::Status::kOk);

    std::unordered_map<std::string, std::string> node;
    std::unordered_map<std::string, std::string> edge;
    assert(db->GetNode(1, {"N0"}, &node) == lsmgraph::Status::kOk);
    assert(db->GetEdge(1, 2, {"A"}, &edge, true, 7)
           == lsmgraph::Status::kOk);
    assert(node.at("N0") == "alice-v3");
    assert(edge.at("A") == "a12-v3");

    const auto stats = db->GetPropertyUpdateStats();
    assert(stats.submitted_records == 5);
    assert(stats.delta_batches_published >= 4);
    assert(stats.pending_keys == 0);
  }

  // Leave one durable delta below the merge threshold.  A clean close and
  // reopen must reconstruct that chain from the manifest instead of relying
  // on the in-process read view.
  assert(db->UpdateNode(1, {{"N1", "shanghai"}})
         == lsmgraph::Status::kOk);
  assert(db->UpdateEdge(1, 2, {{"C", "c12-v4"}}, true, 7)
         == lsmgraph::Status::kOk);
  assert(db->FlushPropertyUpdates() == lsmgraph::Status::kOk);

  delete db;
  db = nullptr;

  // Exercise one-time migration from the legacy file.info layout, which did
  // not contain the 16-byte topology-sequence trailer.
  const auto legacy_file_info =
      std::filesystem::path(base_dir) / "edge_Db0" / "file.info";
  const auto file_info_size = std::filesystem::file_size(legacy_file_info);
  assert(file_info_size > 16);
  std::filesystem::resize_file(legacy_file_info, file_info_size - 16);

  auto reopen_options = options;
  reopen_options.load_existing = true;
  assert(lsmgraph::GraphDb::OpenFromYaml(base_dir,
                                         schema_path,
                                         reopen_options,
                                         &db) == lsmgraph::Status::kOk);
  assert(db != nullptr);

  {
    std::unordered_map<std::string, std::string> node;
    std::unordered_map<std::string, std::string> edge;
    assert(db->GetNode(1, {"N0", "N1"}, &node) == lsmgraph::Status::kOk);
    assert(node.at("N0") == "alice-v3");
    assert(node.at("N1") == "shanghai");

    assert(db->GetEdge(1, 2, {"A", "B", "C"}, &edge, true, 7)
           == lsmgraph::Status::kOk);
    assert(edge.at("A") == "a12-v3");
    assert(edge.at("B") == "b12-v2");
    assert(edge.at("C") == "c12-v4");

    std::vector<lsmgraph::GraphDbEdgeScanRecord> scan;
    assert(db->ScanEdges(1, {"A", "C"}, &scan, true, 7)
           == lsmgraph::Status::kOk);
    assert(scan.size() == 1);
    assert(scan.front().properties.at("A") == "a12-v3");
    assert(scan.front().properties.at("C") == "c12-v4");
  }

  // Compaction must first materialize every delta chain, including chains
  // below the normal merge threshold, and must carry those values into the
  // output SST. Verify both the live view and another restart.
  assert(db->ForceCompactAllShards() == lsmgraph::Status::kOk);
  {
    std::unordered_map<std::string, std::string> node;
    std::unordered_map<std::string, std::string> edge;
    assert(db->GetNode(1, {"N0"}, &node) == lsmgraph::Status::kOk);
    assert(node.at("N0") == "alice-v3");
    assert(db->GetNode(1, {"N1"}, &node) == lsmgraph::Status::kOk);
    assert(node.at("N1") == "shanghai");
    assert(db->GetEdge(1, 2, {"A"}, &edge, true, 7)
           == lsmgraph::Status::kOk);
    assert(edge.at("A") == "a12-v3");
    assert(db->GetEdge(1, 2, {"B"}, &edge, true, 7)
           == lsmgraph::Status::kOk);
    assert(edge.at("B") == "b12-v2");
    assert(db->GetEdge(1, 2, {"C"}, &edge, true, 7)
           == lsmgraph::Status::kOk);
    assert(edge.at("C") == "c12-v4");
  }

  // A reopened database must allocate topology versions and SST ids above
  // every recovered value.  Reusing sequence/fid zero can make the older edge
  // win the merge order or overwrite an existing SST at the next shutdown.
  lsmgraph::GraphDbStorageLocation recovered_location;
  assert(db->LocateEdgeProperty(1, 2, "A", &recovered_location, true, 7)
         == lsmgraph::Status::kOk);
  assert(db->PutEdge(1, 2,
                     {{"A", "a12-v5"},
                      {"B", "b12-v5"},
                      {"C", "c12-v5"}},
                     lsmgraph::EdgeInsertMode::kSingle,
                     true,
                     7) == lsmgraph::Status::kOk);
  lsmgraph::GraphDbStorageLocation new_location;
  assert(db->LocateEdgeProperty(1, 2, "A", &new_location, true, 7)
         == lsmgraph::Status::kOk);
  assert(new_location.sequence > recovered_location.sequence);
  assert(db->PutNode(1, {{"N0", "alice-v4"},
                         {"N1", "beijing"}})
         == lsmgraph::Status::kOk);

  delete db;
  db = nullptr;
  assert(lsmgraph::GraphDb::OpenFromYaml(base_dir,
                                         schema_path,
                                         reopen_options,
                                         &db) == lsmgraph::Status::kOk);
  {
    std::unordered_map<std::string, std::string> node;
    std::unordered_map<std::string, std::string> edge;
    assert(db->GetNode(1, {"N0", "N1"}, &node)
           == lsmgraph::Status::kOk);
    assert(node.at("N0") == "alice-v4");
    assert(node.at("N1") == "beijing");
    assert(db->GetEdge(1, 2, {"A", "B", "C"}, &edge, true, 7)
           == lsmgraph::Status::kOk);
    assert(edge.at("A") == "a12-v5");
    assert(edge.at("B") == "b12-v5");
    assert(edge.at("C") == "c12-v5");
  }

  delete db;
  db = nullptr;
  assert(lsmgraph::GraphDb::OpenFromYaml(base_dir,
                                         schema_path,
                                         reopen_options,
                                         &db) == lsmgraph::Status::kOk);
  {
    std::unordered_map<std::string, std::string> node;
    std::unordered_map<std::string, std::string> edge;
    assert(db->GetNode(1, {"N1"}, &node) == lsmgraph::Status::kOk);
    assert(node.at("N1") == "beijing");
    assert(db->GetEdge(1, 2, {"C"}, &edge, true, 7)
           == lsmgraph::Status::kOk);
    assert(edge.at("C") == "c12-v5");
  }

  delete db;
  std::filesystem::remove_all(base_dir, ec);
  std::cout << "test_graphdb_e2e passed" << std::endl;
  return 0;
}
