#pragma once

#include "core/SSTable.h"
#include "core/edge_iterator.h"
#include "core/flags.h"
#include "core/graph/edge.h"
#include "core/superversion.h"
#include "core/types.h"
#include <cstdint>
#include <string>
#include <vector>

namespace lsmgraph {

struct LSMEdgeLocation {
  bool in_memtable = false;
  FileId_t target_id = INVALID_File_ID;
  SequenceNumber_t sequence = MAX_SEQ_ID;
};

class LSMGraph {
public:

  static void open(const std::string &dir, const size_t max_vertex_num,
                   LSMGraph **db);


  virtual Status del_edge(VertexId_t src, VertexId_t dis, int property_id) = 0;

  virtual Status del_edge_sep(VertexId_t src, VertexId_t dis) = 0;

  virtual VertexId_t get_max_vertex_num() = 0;

  virtual VertexId_t new_vertex(bool use_recycled_vertex = false) = 0;

  virtual void put_vertex(VertexId_t vertex_id, std::string_view data) = 0;

  /**
   * Insert/Update the edge.
   * No return values for simplicity.
   */
  // 默认把 (src, dst) 当作“出边”插入。
  // insert_mode:
  // - kSingle: 仅插入一条边；
  // - kBidirectional: 额外补一条反向边（方向位取反）。
  virtual void put_edge(VertexId_t src, VertexId_t dis, const EdgeProperty_t &s,
                        EdgeInsertMode insert_mode = EdgeInsertMode::kSingle,
                        bool is_out = true, uint8_t edge_type = 0,
                        SequenceNumber_t sq = -1) = 0;

  virtual void update_edge(VertexId_t src, VertexId_t dis,
                           const EdgeProperty_t &s, Marker_t mk = false,
                           SequenceNumber_t sq = -1, bool is_out = true,
                           uint8_t edge_type = 0) = 0;

  /**
   * Returns the edge.
   * An empty string indicates not found.
   */
  virtual Status get_edge(VertexId_t src, VertexId_t dis, std::string *property,
                          int property_id = -1, bool is_out = true,
                          uint8_t edge_type = 0) = 0;

  virtual Status find_edge(VertexId_t src, VertexId_t dis,
                           std::string *property, SSTableCache *it,
                           int property_id, bool is_out = true,
                           uint8_t edge_type = 0) = 0;

  virtual Status find_edge_by_levelindex(VertexId_t src, VertexId_t dst,
                                         std::string *property,
                                         SuperVersion &sv, int property_id,
                                         bool is_out = true,
                                         uint8_t edge_type = 0) = 0;

  virtual Status find_edge_in_memtable(VertexId_t src, VertexId_t dst,
                                       std::string *property, int property_id,
                                       bool is_out = true,
                                       uint8_t edge_type = 0) = 0;

  virtual Status find_edge_in_SStableCache(VertexId_t src, VertexId_t dst,
                                           std::string *property,
                                           int property_id, bool is_out = true,
                                           uint8_t edge_type = 0) = 0;

  virtual Status find_edge_in_lonely_SStableCache(
      VertexId_t src, VertexId_t dst, std::string *property, int property_id,
      SSTableCache *it, bool is_out = true, uint8_t edge_type = 0) = 0;

  virtual Status GetEdge(VertexId_t src, VertexId_t dst, std::string *property,
                         int property_id = -1, bool is_out = true,
                         uint8_t edge_type = 0) = 0;

  virtual Status LocateEdge(VertexId_t src, VertexId_t dst,
                            LSMEdgeLocation *location, bool is_out = true,
                            uint8_t edge_type = 0) = 0;
  /**
   * Returns the all adj edge of vertex src.
   * An empty vector indicates not found.
   */
  virtual bool get_edges(VertexId_t src, std::vector<Edge> &edges) = 0;

  virtual EdgeIterator get_edges(VertexId_t src,
                                 SequenceNumber_t seq = MAX_SEQ_ID,
                                 int sub_property_id = -1, bool is_out = true,
                                 uint8_t edge_type = 0) = 0;

  virtual EdgeIterator Get_Edges(VertexId_t src,
                                 SequenceNumber_t seq = MAX_SEQ_ID,
                                 int sub_property_id = -1, bool is_out = true,
                                 uint8_t edge_type = 0) = 0;

  virtual void print_memTable(std::string label) = 0;

  /**
   * Binary search target vertex in file.
   */
  virtual bool find(VertexId_t target, uint32_t s_offset, uint32_t e_offset,
                    uint32_t step_offset, std::ifstream &file,
                    uint32_t &obj_offset) = 0;

  virtual ~LSMGraph() {
    if (FLAGS_richgraph_verbose) {
      std::cout << "in LSMGraph ~" << std::endl;
    }
  }

  virtual bool GetCompactionState() = 0;

  virtual bool HasBackgroundWork() = 0;

  virtual void WaitBackgroundIdle() = 0;

  virtual bool ForceCompactAllL0ToL1() = 0;

  virtual SequenceNumber_t get_sequence() const = 0;

  virtual void debug() = 0;


  void breakdown(const std::string &label);

  virtual SSTDataManager *GetSSTDataManager() = 0;

  virtual void Debug() = 0;

  virtual void un_map() = 0;

  virtual void clean() = 0;

};

} // namespace lsmgraph
