#include <chrono>
#include <algorithm>
#include <iostream>
#include <fstream>
#include <thread>
#include <random>
#include <memory>
#include <tbb/tbb.h>
#include <string.h>
#include <mutex>
#include <cassert>
#include <vector>
#include <set>

#include "MemProperty.h"
#include "io/file_io.h"
#include "edge_iterator.h"
#include "util/atomic.hpp"


namespace lsmgraph{

    void MemProperty::clear_vertex_adj() {
      src_vertex_num = 0;
      #pragma omp parallel for num_threads(FLAGS_thread_num)
      for (size_t i = 0; i < vertex_id_.load(std::memory_order_relaxed); i++) {
        vertex_adjs[i] = NULLPOINTER;
      }
    }

    
    Status MemProperty::get_property(VertexId_t src, 
                                    VertexId_t dst, 
                                    std::string *property) {
      return get_property(src, dst, true, 0, property);
    }

    Status MemProperty::get_property(VertexId_t src,
                                     VertexId_t dst,
                                     bool is_out,
                                     uint8_t edge_type,
                                     std::string *property) {
      auto prt = vertex_adjs[src];
      if (prt == NULLPOINTER) {
        return Status::kNotFound;
      } else {
        Status temp = (reinterpret_cast<NeighBors *>(
                prt))->get(dst, is_out, edge_type, property);
        return temp;
      }
    }

    void MemProperty::update_edge(VertexId_t src, 
                      VertexId_t dst, 
                      const EdgeProperty_t& s, 
                      Marker_t marker, 
                      SequenceNumber_t seq, 
                      size_t id,
                      bool is_out,
                      uint8_t edge_type){
      
      const size_t lock_slot = static_cast<size_t>(src % vertex_lock_count_);
      vertex_futexes_[lock_slot].lock();
      write_max(&newest_edge, seq);
      if (vertex_adjs[src] == NULLPOINTER) {
        NeighBors *tempEdge_ = edge_arena.GetAnElementById(id);
        tempEdge_->put_edge(dst, seq, marker, s, is_out, edge_type);

        vertex_adjs[src] = reinterpret_cast<uintptr_t>(tempEdge_);
        vertex_futexes_[lock_slot].unlock();
        __sync_fetch_and_add(&src_vertex_num, 1);
        write_max(&vertex_max_level_[src], Level_t(0));
      } else {
        (reinterpret_cast<NeighBors *>(vertex_adjs[src]))->put_edge(
                dst, seq, marker, s, is_out, edge_type);
        vertex_futexes_[lock_slot].unlock();
        
      }
      __sync_fetch_and_add(&listLength, 1);
    }
    void MemProperty::Ref() {
      refs.fetch_add(1);
    }

    void MemProperty::Unref() {
      assert(refs.load(std::memory_order_acquire) >= 0);
      refs.fetch_sub(1);
    }

    bool MemProperty::IsLive() {
      return islive.load(std::memory_order_acquire);
    }

    bool MemProperty::IsFlash() {
      return isflash.load(std::memory_order_acquire);
    }

    int32_t MemProperty::Getref() {
      return refs.load(std::memory_order_acquire);
    }

    void MemProperty::SetLive(bool _islive) {
      islive.store(_islive, std::memory_order_release);
    }

    void MemProperty::SetFlash(bool _isflash) {
      isflash.store(_isflash, std::memory_order_release);
    }

    void MemProperty::reset() {
      clear_vertex_adj();
      listLength = 0;
      refs.store(0);
      remain_capacity = max_edge_num;
      newest_edge.store(0, std::memory_order_relaxed);
      edge_arena.Reset();
      SetLive(false);
      SetFlash(false);
    }

    void MemProperty::SetFid(FileId_t fid) { fid_ = fid; }

    FileId_t MemProperty::GetFid(){
      return fid_;
    }

    uint64_t MemProperty::GetMaxEdgeNum() { return max_edge_num; }

    void MemProperty::SetStartTime(SequenceNumber_t start_time) {
      start_time_ = start_time;
    }

    SequenceNumber_t MemProperty::GetStartTime() {
      return start_time_;
    }

    NeighBors *MemProperty::get_vertex_adj(VertexId_t src) {
      assert(vertex_id_.load(std::memory_order_relaxed) >= src);
      return reinterpret_cast<NeighBors *>(vertex_adjs[src]);
    }

    uint64_t MemProperty::GetListLength() { return listLength; }
}
