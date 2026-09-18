#pragma once

#include <cstdint>
#include <string>
#include "core/types.h"
#include "core/graph/edge.h"
#include "core/MemTable.h"
#include "core/edge_iterator_base.h"
#include "core/graph/edge.h"
#include "core/fixed_property_layout.h"
#include "util/atomic.hpp"

namespace lsmgraph {
// Iterator over an SST already mapped into memory.


template <size_t N>
struct tmp_eb{
  uint32_t prop_pointer; // 4 最后一位存marker
  uint32_t dst_low;  // 4
  uint32_t seq_low;  // 4
  uint16_t dst_high; // 2
  uint16_t seq_high; // 2
  uint32_t offsets[N];

  VertexId_t get_dst() const { 
    return ((VertexId_t)dst_high << 32) + (VertexId_t)dst_low; 
  }
  VertexId_t get_seq() const { 
    return ((VertexId_t)seq_high << 32) + (VertexId_t)seq_low; 
  }

  EdgePropertyOffset_t get_Offset(int sub_property_id) {
    return offsets[sub_property_id];
  }

  Marker_t get_marker() const { 
    return Marker_t(prop_pointer);
  }

};

struct tmp_eb_new_fixed{
  uint32_t prop_pointer; // 4 最后一位存marker
  uint32_t dst_low;  // 4
  uint32_t seq_low;  // 4
  uint16_t dst_high; // 2
  uint16_t seq_high; // 2

  VertexId_t get_dst() const { 
    return ((VertexId_t)dst_high << 32) + (VertexId_t)dst_low; 
  }
  VertexId_t get_seq() const { 
    return ((VertexId_t)seq_high << 32) + (VertexId_t)seq_low; 
  }

  Marker_t get_marker() const { 
    return Marker_t(prop_pointer);
  }

};





#ifndef NO_VIRTUAL
  class SSTEdgeIterator : public EdgeIteratorBase {
    public:
      SSTEdgeIterator(EdgeBody_t* body_data,
                      char* property_data,
                      size_t body_num,
                      FileId_t fid=INVALID_File_ID,
                      SequenceNumber_t newest_edge_ = 0,
                      EdgePropertyOffset_t begin_Offset_ = 0,
                      EdgePropertyOffset_t fixed_property_len_ = GetSubPropertyFixedLength(0))
          : body_data_(body_data),
            property_data_(property_data),
            body_num_(body_num),
            body_cursor_(body_data),
            fid_(fid),
            body_end_(body_data_ + body_num_ ),
            now_offset(begin_Offset_),
            fixed_property_len_(fixed_property_len_),
            newest_edge(newest_edge_) {
      }

      SSTEdgeIterator() = default;

      void init() {
      }

      bool valid() const override {
        return body_cursor_ < body_end_;
      }

      void next() {
        body_cursor_++;
        now_offset += fixed_property_len_;
      }



      VertexId_t dst_id() const override {
        return body_cursor_->get_dst();
      }

      Marker_t marker() const override {
        return body_cursor_->get_marker();
      }

      SequenceNumber_t sequence() const override {
        return body_cursor_->get_seq();
      }

      bool is_out() const override {
        return body_cursor_->get_is_out();
      }

      uint8_t edge_type() const override {
        return body_cursor_->get_edge_type();
      }
      

      EdgeProperty_t edge_data(int id) const override {
        if (!valid()) {
          return EdgeProperty_t();
        } else {
          EdgePropertyOffset_t property_offset = now_offset;
          EdgePropertyOffset_t property_offset_nxt = now_offset + fixed_property_len_;

          // 定长属性在文件中可能带有尾部补零，这里统一去掉补零后再返回逻辑字符串，
          // 保持迭代器读取语义与单边查询（GetEdge/find_edge_*）一致。
          EdgeProperty_t ep = EdgeProperty_t(
                  property_data_ + property_offset,
                  property_offset_nxt
                    - property_offset);
          TrimTrailingZero(&ep);
          return ep;
        }
      }

      Slice edge_slice_data(int id) {
        if (!valid()) {
          return EdgeProperty_t();
        } else {
          return Slice();
        }
      }

      bool empty() const override{
        return !body_data_;
      }

      size_t size() const override {
        return body_num_; // we need to merge before we can count
      }

      FileId_t get_fid() const override {
        return fid_;
      }

      bool IsMemTable() const override {
        return is_mem_table;
      }

    private:
      EdgeBody_t* body_data_;
      char* property_data_;
      size_t body_num_;
      EdgeBody_t* body_cursor_;
      EdgeBody_t* body_end_;
      FileId_t fid_;
      bool is_mem_table = false;
      SequenceNumber_t newest_edge = 0;
      mutable EdgePropertyOffset_t now_offset = 0;
      EdgePropertyOffset_t fixed_property_len_ = GetSubPropertyFixedLength(0);
  };

#else

  class SSTEdgeIterator:public EdgeIteratorBase {
  public:
    SSTEdgeIterator(EdgeBody_t *body_data,
                    char *property_data,
                    size_t body_num,
                    FileId_t fid = INVALID_File_ID)
            : body_data_(body_data),
              property_data_(property_data),
              body_num_(body_num),
              body_cursor_(body_data),
              fid_(fid),
              body_end_(body_data_ + body_num) {
    }

    SSTEdgeIterator() = default;

    void init() {
    }

    bool valid() const {
      return body_cursor_ < body_end_;
    }

    void next() {
      body_cursor_++;
    }

    VertexId_t dst_id() const {
      return body_cursor_->get_dst();
    }

    Marker_t marker() const {
      return body_cursor_->get_marker();
    }

    SequenceNumber_t sequence() const {
      return body_cursor_->get_seq();
    }

    EdgeProperty_t edge_data() const {
      if (!valid()) {
        return EdgeProperty_t();
      } else {
        return EdgeProperty_t(
                property_data_ + body_cursor_->get_prop_pointer(),
                (body_cursor_ + 1)->get_prop_pointer()
                - body_cursor_->get_prop_pointer());
      }
    }

    Slice edge_slice_data() {
      if (!valid()) {
        return EdgeProperty_t();
      } else {
        return Slice(
                property_data_ + body_cursor_->get_prop_pointer(),
                (body_cursor_ + 1)->get_prop_pointer()
                - body_cursor_->get_prop_pointer());
      }
    }

    bool empty() const {
      return !body_data_;
    }

    size_t size() const {
      return body_num_; // we need to merge before we can count
    }

    FileId_t get_fid() const {
      return fid_;
    }

  private:
    EdgeBody_t *body_data_;
    EdgeBody_t *body_cursor_;
    EdgeBody_t *body_end_;
    size_t body_num_;
    FileId_t fid_;
    char *property_data_;
  };

#endif
}
