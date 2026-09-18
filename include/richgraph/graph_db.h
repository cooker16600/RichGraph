#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "richgraph/options.h"
#include "richgraph/types.h"

namespace lsmgraph {

class LSMGraph;
class PropertyUpdateManager;

struct GraphDbEdgeScanRecord {
  VertexId_t dst{0};
  SequenceNumber_t sequence{0};
  std::unordered_map<std::string, std::string> properties;
};

struct GraphDbStorageLocation {
  bool in_memtable{false};
  std::size_t shard_idx{0};
  int local_property_id{0};
  FileId_t target_id{INVALID_File_ID};
  SequenceNumber_t sequence{MAX_SEQ_ID};
};

using GraphDbEdgeCallback = std::function<bool(
    VertexId_t dst, SequenceNumber_t sequence, const std::string& value)>;

class GraphDb {
 public:
  struct PropertyDef {
    std::string name;
    std::uint32_t length{10};
  };

  struct EdgeShardSchema {
    std::string name;
    std::vector<std::string> properties;
    std::uint32_t memtable_size{0};
    bool is_csr{false};
    std::uint32_t csr_l0_max_sst_num{8};
    std::uint32_t csr_l1_max_sst_num{10000};
  };

  struct NodeShardSchema {
    std::string name{"node_Db"};
    std::vector<std::string> properties;
    std::uint32_t memtable_size{0};
    bool is_csr{false};
    std::uint32_t csr_l0_max_sst_num{8};
    std::uint32_t csr_l1_max_sst_num{10000};
  };

  struct Schema {
    std::uint64_t max_vertex_num{0};
    bool use_csr_disk{false};
    std::uint32_t sub_property_num{0};
    std::uint32_t max_property_length{10};
    int system_threads{1};

    std::vector<PropertyDef> property_defs;
    std::vector<EdgeShardSchema> edge_shards;
    NodeShardSchema node_db;
  };

  GraphDb(const GraphDb&) = delete;
  GraphDb& operator=(const GraphDb&) = delete;
  GraphDb(GraphDb&&) = delete;
  GraphDb& operator=(GraphDb&&) = delete;
  ~GraphDb();

  // Compatibility entry point for existing benchmark programs. New clients
  // should pass GraphDbOptions explicitly.
  static Status OpenFromYaml(const std::string& base_dir,
                             const std::string& schema_path, GraphDb** out_db,
                             std::string* error = nullptr);

  static Status OpenFromYaml(const std::string& base_dir,
                             const std::string& schema_path,
                             const GraphDbOptions& options, GraphDb** out_db,
                             std::string* error = nullptr);

  static Status OpenFromYaml(const std::string& base_dir,
                             const std::string& schema_path,
                             const GraphDbOptions& options,
                             std::unique_ptr<GraphDb>* out_db,
                             std::string* error = nullptr);

  static GraphDbOptions LegacyOptionsFromFlags();

  static Status LoadSchemaFromYaml(const std::string& schema_path,
                                   Schema* out_schema,
                                   std::string* error = nullptr);

  VertexId_t NewVertex(bool use_recycled_vertex = false);
  SequenceNumber_t NextSequence();
  void InitVerticesUpTo(VertexId_t next_vertex_id);

  Status PutNode(VertexId_t vertex_id,
                 const std::unordered_map<std::string, std::string>& properties,
                 bool is_out = true, std::uint8_t edge_type = 0);

  Status PutNodePayload(VertexId_t vertex_id, const std::string& payload,
                        bool is_out = true, std::uint8_t edge_type = 0);

  Status GetNode(VertexId_t vertex_id,
                 const std::vector<std::string>& property_names,
                 std::unordered_map<std::string, std::string>* out_properties,
                 bool is_out = true, std::uint8_t edge_type = 0) const;

  Status UpdateNode(
      VertexId_t vertex_id,
      const std::unordered_map<std::string, std::string>& properties,
      bool is_out = true, std::uint8_t edge_type = 0,
      PropertyCommitSequence* commit_sequence = nullptr);

  Status LocateNodeProperty(VertexId_t vertex_id,
                            const std::string& property_name,
                            GraphDbStorageLocation* location,
                            bool is_out = true,
                            std::uint8_t edge_type = 0) const;

  Status PutEdge(VertexId_t src, VertexId_t dst,
                 const std::unordered_map<std::string, std::string>& properties,
                 EdgeInsertMode insert_mode = EdgeInsertMode::kSingle,
                 bool is_out = true, std::uint8_t edge_type = 0);

  Status PutEdgePayload(std::size_t shard_idx, VertexId_t src, VertexId_t dst,
                        const std::string& payload,
                        EdgeInsertMode insert_mode = EdgeInsertMode::kSingle,
                        bool is_out = true, std::uint8_t edge_type = 0,
                        SequenceNumber_t sequence = -1);

  Status UpdateEdge(
      VertexId_t src, VertexId_t dst,
      const std::unordered_map<std::string, std::string>& properties,
      bool is_out = true, std::uint8_t edge_type = 0,
      PropertyCommitSequence* commit_sequence = nullptr);

  Status GetEdge(VertexId_t src, VertexId_t dst,
                 const std::vector<std::string>& property_names,
                 std::unordered_map<std::string, std::string>* out_properties,
                 bool is_out = true, std::uint8_t edge_type = 0) const;

  Status LocateEdgeProperty(VertexId_t src, VertexId_t dst,
                            const std::string& property_name,
                            GraphDbStorageLocation* location,
                            bool is_out = true,
                            std::uint8_t edge_type = 0) const;

  Status ScanEdges(VertexId_t src,
                   const std::vector<std::string>& property_names,
                   std::vector<GraphDbEdgeScanRecord>* out_records,
                   bool is_out = true, std::uint8_t edge_type = 0) const;

  Status ForEachEdge(VertexId_t src, const std::string& property_name,
                     const GraphDbEdgeCallback& callback, bool is_out = true,
                     std::uint8_t edge_type = 0) const;

  void WaitCSRUpToDate();
  [[nodiscard]] bool HasBackgroundWork();
  void WaitBackgroundIdle();

  // Flushes the current engine-owned property buffers and waits for already
  // scheduled delta merges. Updates are query-visible before this barrier.
  Status FlushPropertyUpdates();
  [[nodiscard]] PropertyUpdateManagerStats GetPropertyUpdateStats() const;

  // Primarily intended for maintenance and deterministic integration tests.
  Status ForceCompactAllShards();
  bool ForceCompactCsrEdgeShards();

  [[nodiscard]] const Schema& schema() const noexcept { return schema_; }

 private:
  struct PropertyRoute {
    std::size_t shard_idx{0};
    int local_property_id{0};
  };

  struct EdgeShardHandle {
    std::string name;
    std::vector<std::string> properties;
    std::vector<std::uint32_t> property_lengths;
    std::unordered_map<std::string, int> property_to_local_id;
    std::unique_ptr<LSMGraph> db;
  };

  struct NodeShardHandle {
    std::string name;
    std::vector<std::string> properties;
    std::vector<std::uint32_t> property_lengths;
    std::unordered_map<std::string, int> property_to_local_id;
    std::unique_ptr<LSMGraph> db;
  };

  struct ResolvedPropertyRoute {
    std::string property_name;
    PropertyRoute route;
  };

  GraphDb();

  void ObserveSequence(SequenceNumber_t sequence);
  Status Init(const std::string& base_dir, const Schema& schema,
              const GraphDbOptions& options, std::string* error);

  Status ResolveEdgePropertyRoutes(
      const std::vector<std::string>& property_names,
      std::vector<ResolvedPropertyRoute>* out_routes,
      std::string* error = nullptr) const;

  Status ResolveNodePropertyRoutes(
      const std::vector<std::string>& property_names,
      std::vector<ResolvedPropertyRoute>* out_routes,
      std::string* error = nullptr) const;

  std::string BuildShardPropertyString(
      const std::vector<std::string>& local_property_order,
      const std::unordered_map<std::string, std::string>& incoming_properties)
      const;

  Schema schema_;
  GraphDbOptions options_;
  std::string base_dir_;
  std::vector<EdgeShardHandle> edge_shards_;
  NodeShardHandle node_shard_;
  std::unordered_map<std::string, PropertyRoute> edge_property_to_route_;
  std::unordered_map<std::string, PropertyRoute> node_property_to_route_;
  std::atomic<SequenceNumber_t> next_sequence_{0};
  std::unique_ptr<PropertyUpdateManager> property_update_manager_;
};

}  // namespace lsmgraph
