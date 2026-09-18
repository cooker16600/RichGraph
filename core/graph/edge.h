/*
Copyright (c) 2023 The LSMGraph Authors, Northeastern University

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

#ifndef EDGE_H_
#define EDGE_H_

#include <memory.h>
#include <iostream>
#include <limits>
#include "core/types.h"
#include "util/slice.h"
#include "util/fix_string.h"
#include "core/flags.h"

namespace lsmgraph {

struct EdgeBody_t_24;
struct EdgeBody_t_16;
struct EdgeBody_t_var;
struct EdgeBody_t_Dym;

using EdgeBody_t = EdgeBody_t_Dym;

class Edge_string;
class Edge_fixstring;

using Edge = Edge_string;

struct EdgeBody_t_24 {
private:
  VertexId_t dst_;
  SequenceNumber_t seq_;
  EdgePropertyOffset_t prop_pointer_;
  Marker_t marker_;

public:
  EdgeBody_t_24(VertexId_t dst=INVALID_VERTEX_ID, SequenceNumber_t seq = 0,
             EdgePropertyOffset_t prop_pointer = 0x00, Marker_t marker = false)
      : dst_{dst}, seq_{seq}, prop_pointer_{prop_pointer}, marker_{marker} {}
  
  friend bool operator < (const EdgeBody_t_24& rhs1, const EdgeBody_t_24& rhs2) {
    if (rhs1.dst_ < rhs2.dst_) {
        return true;
    } else if (rhs1.dst_ == rhs2.dst_) {
        return rhs1.seq_ > rhs2.seq_; // The newer the time, the higher the front.
    }
    return false;
  }

  void set_dst(VertexId_t dst) {
    dst_ = dst;
  }

  VertexId_t get_dst() const { 
    return dst_; 
  }

  void set_seq(VertexId_t seq) {
    seq_ = seq;
  }

  VertexId_t get_seq() const { 
    return seq_; 
  }

  void set_prop_pointer(EdgePropertyOffset_t _prop_pointer) {
    prop_pointer_ = _prop_pointer;
  }

  EdgePropertyOffset_t get_prop_pointer() const {
    return prop_pointer_;
  }

  void set_marker(bool marker) {
    marker_ = marker; 
  }

  bool get_marker() const { 
    return marker_;
  }

};

struct EdgeBody_t_var {
  private:
    int prop_pointer;
    uint32_t dst_low;  // 4
    uint32_t seq_low;  // 4
    uint16_t dst_high; // 2
    uint16_t seq_high; // 2
    EdgePropertyOffset_t *Offset; //8 内容：FLAGS_sub_property_num*4
  
  public:
    EdgeBody_t_var(VertexId_t dst=INVALID_VERTEX_ID, SequenceNumber_t seq = 0, EdgePropertyOffset_t* Offset_ = nullptr, 
               Marker_t marker = false)
        {
          
          Offset = new EdgePropertyOffset_t[FLAGS_sub_property_num];
          set_dst(dst);
          set_seq(seq);
          set_prop_pointer(Offset_);
          set_marker(marker);
        }
  
    void set_dst(VertexId_t dst) {
      assert(dst <= ((VertexId_t)UINT16_MAX << 32) + UINT32_MAX);
      dst_high = (dst >> 32) & UINT16_MAX;
      dst_low = dst & UINT32_MAX;
    }
  
    VertexId_t get_dst() const { 
      return ((VertexId_t)dst_high << 32) + (VertexId_t)dst_low; 
    }
  
    void set_seq(VertexId_t seq) {
      assert(seq <= ((VertexId_t)UINT16_MAX << 32) + UINT32_MAX);
      seq_high = (seq >> 32) & UINT16_MAX;
      seq_low = seq & UINT32_MAX;
    }
  
    VertexId_t get_seq() const { 
      return ((VertexId_t)seq_high << 32) + (VertexId_t)seq_low; 
    }
  
    void set_prop_pointer(EdgePropertyOffset_t *Offset_) {
      if(Offset_ != nullptr){
        for(int i = 0; i < FLAGS_sub_property_num; i++){
          Offset[i] = Offset_[i];

        }
      }
      else{
        for(int i = 0; i < FLAGS_sub_property_num; i++){
          Offset[i] = std::numeric_limits<EdgePropertyOffset_t>::max();
        }
      }
    }

    void set_prop_pointer_spa(EdgePropertyOffset_t _prop_pointer, int sub_property_id) {
      Offset[sub_property_id] = _prop_pointer;
    }

    EdgePropertyOffset_t get_prop_pointer(int id) {
      return Offset[id];
    }

    uint32_t get_prop_pointer_size() {
      return Offset == nullptr ? 0
                               : static_cast<uint32_t>(FLAGS_sub_property_num);
    }

    void set_marker(bool marker) {
      prop_pointer = (prop_pointer & 0x7FFFFFFF) | (marker << 31);
    }

    bool get_marker() const { return (prop_pointer >> 31) & 0x1; }

    friend bool operator < (const EdgeBody_t_var& rhs1, const EdgeBody_t_var& rhs2) {
      if (rhs1.get_dst() < rhs2.get_dst()) {
          return true;
      } else if (rhs1.get_dst() == rhs2.get_dst()) {
          return rhs1.get_seq() > rhs2.get_seq(); // The newer the time, the higher the front.
      }
      return false;
    }
  
    void print(std::string label = "") const {
      if (label != "")
        printf("print edge information (%s):\n", label.c_str());
        std::cout << "  dst_: " << get_dst() 
                  << "  seq_: " << get_seq() 
                  << "  marker_: " << get_marker() 
                  << "  prop_pointer_: " << prop_pointer
                  << std::endl;
    }
  }; 
struct EdgeBody_t_Dym {
private:
  // prop_pointer 位布局（从高到低）：
  // bit31: marker（1=删除）
  // bit30: is_out（1=出边，0=入边）
  // bit29..22: edge_type（8位，范围 0~255）
  // bit21..0: 预留 payload（当前保留 offset 低位兼容语义）
  uint32_t prop_pointer = 0; // 4
  uint32_t dst_low;  // 4
  uint32_t seq_low;  // 4
  uint16_t dst_high; // 2
  uint16_t seq_high; // 2

public:
  static constexpr uint32_t kMarkerMask = 0x80000000u;
  static constexpr uint32_t kIsOutMask = 0x40000000u;
  static constexpr uint32_t kEdgeTypeShift = 22u;
  static constexpr uint32_t kEdgeTypeMask = 0x3fc00000u;
  static constexpr uint32_t kPayloadMask = 0x003fffffu;

  EdgeBody_t_Dym(VertexId_t dst=INVALID_VERTEX_ID, SequenceNumber_t seq = 0,
             EdgePropertyOffset_t _prop_pointer = 0x00,
             Marker_t marker = false,
             bool is_out = true,
             uint8_t edge_type = 0)
      {
        set_dst(dst);
        set_seq(seq);
        set_Offset(_prop_pointer);
        set_marker(marker);
        set_is_out(is_out);
        set_edge_type(edge_type);
      }

  void set_dst(VertexId_t dst) {
    assert(dst <= ((VertexId_t)UINT16_MAX << 32) + UINT32_MAX);
    dst_high = (dst >> 32) & UINT16_MAX;
    dst_low = dst & UINT32_MAX;
  }

  VertexId_t get_dst() const { 
    return ((VertexId_t)dst_high << 32) + (VertexId_t)dst_low; 
  }

  void set_seq(VertexId_t seq) {
    assert(seq <= ((VertexId_t)UINT16_MAX << 32) + UINT32_MAX);
    seq_high = (seq >> 32) & UINT16_MAX;
    seq_low = seq & UINT32_MAX;
  }

  VertexId_t get_seq() const { 
    return ((VertexId_t)seq_high << 32) + (VertexId_t)seq_low; 
  }

  void set_Offset(EdgePropertyOffset_t _prop_pointer) {
    // 定长属性路径不再依赖 body 内 offset 进行物理定位。
    // 这里保留低位 payload 仅用于兼容，超过范围时按低位截断。
    prop_pointer = (_prop_pointer & kPayloadMask) | (prop_pointer & (~kPayloadMask));
  }

  EdgePropertyOffset_t get_Offset() const {
    return prop_pointer & kPayloadMask;
  }

  void set_marker(bool marker) {
    prop_pointer = (prop_pointer & (~kMarkerMask))
                 | (static_cast<uint32_t>(marker) << 31);
  }

  bool get_marker() const { 
    return (prop_pointer >> 31) & 0x1;
  }

  void set_is_out(bool is_out) {
    prop_pointer = (prop_pointer & (~kIsOutMask))
                 | (static_cast<uint32_t>(is_out) << 30);
  }

  bool get_is_out() const {
    return (prop_pointer >> 30) & 0x1;
  }

  void set_edge_type(uint8_t edge_type) {
    prop_pointer = (prop_pointer & (~kEdgeTypeMask))
                 | ((static_cast<uint32_t>(edge_type) << kEdgeTypeShift) & kEdgeTypeMask);
  }

  uint8_t get_edge_type() const {
    return static_cast<uint8_t>((prop_pointer & kEdgeTypeMask) >> kEdgeTypeShift);
  }
  
  friend bool operator < (const EdgeBody_t_Dym& rhs1, const EdgeBody_t_Dym& rhs2) {
    if (rhs1.get_dst() < rhs2.get_dst()) {
        return true;
    } else if (rhs1.get_dst() == rhs2.get_dst()) {
        return rhs1.get_seq() > rhs2.get_seq(); // The newer the time, the higher the front.
    }
    return false;
  }

  void print(std::string label = "") const {
    if (label != "")
      printf("print edge information (%s):\n", label.c_str());
      std::cout << "  dst_: " << get_dst() 
                << "  seq_: " << get_seq() 
                << "  marker_: " << get_marker() 
                << "  is_out_: " << get_is_out()
                << "  edge_type_: " << static_cast<uint32_t>(get_edge_type())
                << "  prop_pointer_: " << prop_pointer
                << std::endl;
  }
};   

struct EdgeBody_t_16 {
private:
  uint32_t prop_pointer; // 4 最后一位存marker
  uint32_t dst_low;  // 4
  uint32_t seq_low;  // 4
  uint16_t dst_high; // 2
  uint16_t seq_high; // 2

public:
  EdgeBody_t_16(VertexId_t dst=INVALID_VERTEX_ID, SequenceNumber_t seq = 0,
             EdgePropertyOffset_t _prop_pointer = 0x00, Marker_t marker = false)
      {
        set_dst(dst);
        set_seq(seq);
        set_prop_pointer(_prop_pointer);
        set_marker(marker);
      }

  void set_dst(VertexId_t dst) {
    assert(dst <= ((VertexId_t)UINT16_MAX << 32) + UINT32_MAX);
    dst_high = (dst >> 32) & UINT16_MAX;
    dst_low = dst & UINT32_MAX;
  }

  VertexId_t get_dst() const { 
    return ((VertexId_t)dst_high << 32) + (VertexId_t)dst_low; 
  }

  void set_seq(VertexId_t seq) {
    assert(seq <= ((VertexId_t)UINT16_MAX << 32) + UINT32_MAX);
    seq_high = (seq >> 32) & UINT16_MAX;
    seq_low = seq & UINT32_MAX;
  }

  VertexId_t get_seq() const { 
    return ((VertexId_t)seq_high << 32) + (VertexId_t)seq_low; 
  }

  void set_prop_pointer(EdgePropertyOffset_t _prop_pointer) {
    assert(_prop_pointer <= (UINT32_MAX >> 1));
    prop_pointer = (_prop_pointer & 0x7FFFFFFF) | (prop_pointer & 0x80000000);
  }

  EdgePropertyOffset_t get_prop_pointer() {
    return prop_pointer & 0x7FFFFFFF; // prop_pointer & 0x7FFFFFFF
  }

  void set_marker(bool marker) {
    prop_pointer = (prop_pointer & 0x7FFFFFFF) | (marker << 31); 
  }

  bool get_marker() const { 
    return (prop_pointer >> 31) & 0x1;
  }
  
  friend bool operator < (const EdgeBody_t_16& rhs1, const EdgeBody_t_16& rhs2) {
    if (rhs1.get_dst() < rhs2.get_dst()) {
        return true;
    } else if (rhs1.get_dst() == rhs2.get_dst()) {
        return rhs1.get_seq() > rhs2.get_seq(); // The newer the time, the higher the front.
    }
    return false;
  }

  void print(std::string label = "") const {
    if (label != "")
      printf("print edge information (%s):\n", label.c_str());
      std::cout << "  dst_: " << get_dst() 
                << "  seq_: " << get_seq() 
                << "  marker_: " << get_marker() 
                << "  prop_pointer_: " << prop_pointer
                << std::endl;
  }
}; 
static_assert(sizeof(EdgeBody_t_16) == 16);



class Edge_fixstring {
public:
  Edge_fixstring(VertexId_t dst=INVALID_VERTEX_ID, SequenceNumber_t seq = 0,
    Marker_t marker = false, std::string property = "") {
    set_destination(dst);
    set_sequence(seq);
    set_marker(marker);
    set_property(property);
  }

  ~Edge_fixstring() {};

  // Copy constructor
  Edge_fixstring(const Edge_fixstring& other) {
    dst_ = other.dst_;
    seq_ = other.seq_;
    property_ = other.property_;
  }

  // Move constructor
  Edge_fixstring(Edge_fixstring&& other) {
    dst_ = std::move(other.dst_);
    seq_ = std::move(other.seq_);
    property_ = std::move(other.property_);
  }

  Edge_fixstring(EdgeBody_t & other) {
    dst_ = other.get_dst();
    seq_ = other.get_seq();
  }

  void reset() {
    set_destination(INVALID_VERTEX_ID);
    set_sequence(0);
    set_marker(false);
    set_property("");
  }

  // Copy assignment operator
  Edge_fixstring& operator=(const Edge_fixstring& other) {
    if (this != &other) {
      dst_ = other.dst_;
      seq_ = other.seq_;
      property_ = other.property_;
    }
    return *this;
  }

  // Move assignment operator
  Edge_fixstring& operator=(Edge_fixstring&& other) {
    if (this != &other) {
      dst_ = std::move(other.dst_);
      seq_ = std::move(other.seq_);
      property_ = std::move(other.property_);
    }
    return *this;
  }

  // Equality operator
  bool operator==(const Edge_fixstring& other) const {
    return dst_ == other.dst_;
  }

  // Inequality operator
  bool operator!=(const Edge_fixstring& other) const {
    return !(*this == other);
  }

  // Used for sorting in the SkipList
  bool operator<(const Edge_fixstring& rhs) {
    if (dst_ < rhs.dst_) {
    return true;
    } else if (dst_ == rhs.dst_) {
    return seq_ > rhs.seq_; // The newer the time, the higher the front.
    }
    return false;
  }
   friend bool operator < (const Edge_fixstring& rhs1, const Edge_fixstring& rhs2) {
        if (rhs1.dst_ < rhs2.dst_) {
            return true;
        } else if (rhs1.dst_ == rhs2.dst_) {
            return rhs1.seq_ > rhs2.seq_; // The newer the time, the higher the front.
        }
        return false;
    }
  // Used for comparison in the merge phase
  bool operator>(const Edge_fixstring& rhs) {
    if (dst_ > rhs.dst_) {
        return true;
    } else if (dst_ == rhs.dst_) {
        return seq_ < rhs.seq_; // The newer the time, the higher the front.
    }
    return false;
  }

  bool Equal(const Edge_fixstring& rhs) const {
    return dst_ == rhs.dst_;
  }
  // Used for comparison in lookups
  bool LessThan(const Edge_fixstring& rhs) {
    if (dst_ < rhs.dst_) {
        return true;
    }
    return false;
  }

  VertexId_t destination() const { return dst_; }
  void set_destination(VertexId_t dst) { dst_ = dst; }

  SequenceNumber_t sequence() const { 
    return seq_ >> 1; 
  }
  void set_sequence(SequenceNumber_t seq) { 
    assert(seq <= (INT64_MAX >> 1));
    seq_ = (seq << 1) | (seq_ & 0x1); 
  }

  Marker_t marker() const { 
    return seq_ & 0x1; 
  }
  void set_marker(Marker_t marker) { 
    seq_ = (seq_ & (~(static_cast<SequenceNumber_t>(1)))) | marker;
  }

  void set_property(const std::string& property) { property_ = property; }
  const FixString& property() const { return property_; }
  void set_property(FixString& property) { property_ = property; }

  // Serialize Edge to a buffer
  void Serialize(char* buffer) const {
    memcpy(buffer, this, sizeof(Edge_fixstring));
  }

  // Deserialize Edge from a buffer
  void Deserialize(const char* buffer) {
    memcpy(this, buffer, sizeof(Edge_fixstring));
  }

  // Return the size of each outgoing edge that needs to be stored.
  //    include: body
  uint32_t get_body_size() const {
    return sizeof(EdgeBody_t); // + property_.size();
  }

  // Return the size of each outgoing edge that needs to be stored.
  //    include: body + property
  uint32_t propertySize() const {
    return property_.size();
  }

  void print(std::string label = "") const {
    if (label != "")
      printf("print edge information (%s):\n", label.c_str());
    std::cout << "  dst_: " << destination() 
              << "  seq_: " << sequence() 
              << "  marker_: " << marker() 
              << "  property_: " << property() << std::endl;
  }

private:
  VertexId_t dst_;
  SequenceNumber_t seq_;
  FixString property_;
};

class Edge_string {
public:
  Edge_string(VertexId_t dst=INVALID_VERTEX_ID, SequenceNumber_t seq = 0,
    Marker_t marker = false, EdgeProperty_t property = "", bool is_out = true,
    uint8_t edge_type = 0)
      : dst_{dst}, seq_{seq}, marker_{marker}, property_{property},
        is_out_{is_out}, edge_type_{edge_type} {}

  ~Edge_string() {};

  // Copy constructor
  Edge_string(const Edge_string& other) {
    dst_ = other.dst_;
    seq_ = other.seq_;
    marker_ = other.marker_;
    property_ = other.property_;
    is_out_ = other.is_out_;
    edge_type_ = other.edge_type_;
  }

  // Move constructor
  Edge_string(Edge_string&& other) {
    dst_ = std::move(other.dst_);
    seq_ = std::move(other.seq_);
    marker_ = std::move(other.marker_);
    property_ = std::move(other.property_);
    is_out_ = std::move(other.is_out_);
    edge_type_ = std::move(other.edge_type_);
  }

  Edge_string(EdgeBody_t & other) {
    dst_ = other.get_dst();
    seq_ = other.get_seq();
    marker_ = other.get_marker();
    is_out_ = other.get_is_out();
    edge_type_ = other.get_edge_type();
    property_ = "no get property";
  }

  void reset() {
    dst_ = INVALID_VERTEX_ID;
    seq_ = 0;
    marker_ = false;
    property_ = "";
    is_out_ = true;
    edge_type_ = 0;
  }

  // Copy assignment operator
  Edge_string& operator=(const Edge_string& other) {
    if (this != &other) {
      dst_ = other.dst_;
      seq_ = other.seq_;
      marker_ = other.marker_;
      property_ = other.property_;
      is_out_ = other.is_out_;
      edge_type_ = other.edge_type_;
    }
    return *this;
  }

  // Move assignment operator
  Edge_string& operator=(Edge_string&& other) {
    if (this != &other) {
      dst_ = std::move(other.dst_);
      seq_ = std::move(other.seq_);
      marker_ = std::move(other.marker_);
      property_ = std::move(other.property_);
      is_out_ = std::move(other.is_out_);
      edge_type_ = std::move(other.edge_type_);
    }
    return *this;
  }

  // Equality operator
  bool operator==(const Edge_string& other) const {
    return dst_ == other.dst_;
  }

  // Inequality operator
  bool operator!=(const Edge_string& other) const {
    return !(*this == other);
  }

  // Used for sorting in the SkipList
  bool operator<(const Edge_string& rhs) {
    if (dst_ < rhs.dst_) {
    return true;
    } else if (dst_ == rhs.dst_) {
    return seq_ > rhs.seq_; // The newer the time, the higher the front.
    }
    return false;
  }
   friend bool operator < (const Edge_string& rhs1, const Edge_string& rhs2) {
        if (rhs1.dst_ < rhs2.dst_) {
            return true;
        } else if (rhs1.dst_ == rhs2.dst_) {
            return rhs1.seq_ > rhs2.seq_; // The newer the time, the higher the front.
        }
        return false;
    }
  // Used for comparison in the merge phase
  bool operator>(const Edge_string& rhs) {
    if (dst_ > rhs.dst_) {
        return true;
    } else if (dst_ == rhs.dst_) {
        return seq_ < rhs.seq_; // The newer the time, the higher the front.
    }
    return false;
  }

  bool Equal(const Edge_string& rhs) const {
    return dst_ == rhs.dst_;
  }
  // Used for comparison in lookups
  bool LessThan(const Edge_string& rhs) {
    if (dst_ < rhs.dst_) {
        return true;
    }
    return false;
  }

  VertexId_t destination() const { return dst_; }
  void set_destination(VertexId_t dst) { dst_ = dst; }

  SequenceNumber_t sequence() const { return seq_; }
  void set_sequence(SequenceNumber_t seq) { seq_ = seq; }

  Marker_t marker() const { return marker_; }
  void set_marker(Marker_t marker) { marker_ = marker; }

  bool is_out() const { return is_out_; }
  void set_is_out(bool is_out) { is_out_ = is_out; }

  uint8_t edge_type() const { return edge_type_; }
  void set_edge_type(uint8_t edge_type) { edge_type_ = edge_type; }

  EdgeProperty_t property(int sub_property_id = -1) const {
    if(sub_property_id == -1){
      return property_;
    }
    std::string ans;
    int now_id = 0;
    for(auto c:property_){
      if(c == '|'){
        now_id++;
      } else {
        if(now_id == sub_property_id){
          ans += c;
        }
      }
      if(now_id > sub_property_id){
        break;
      }
    }
    return ans; 
  }
  void set_property(EdgeProperty_t property) { property_ = property; }

  // Serialize Edge to a buffer
  void Serialize(char* buffer) const {
    memcpy(buffer, this, sizeof(Edge_string));
  }

  // Deserialize Edge from a buffer
  void Deserialize(const char* buffer) {
    memcpy(this, buffer, sizeof(Edge_string));
  }

  // Return the size of each outgoing edge that needs to be stored.
  //    include: body
  uint32_t get_body_size() const {
    return sizeof(EdgeBody_t); // + property_.size();
  }

  // Return the size of each outgoing edge that needs to be stored.
  //    include: body + property
  uint32_t propertySize() const {
    return property_.size();
  }

  void print(std::string label = "") const {
    if (label != "")
      printf("print edge information (%s):\n", label.c_str());
    std::cout << "  dst_: " << dst_ 
              << "  seq_: " << seq_ 
              << "  marker_: " << marker_ 
              << "  is_out_: " << is_out_
              << "  edge_type_: " << static_cast<uint32_t>(edge_type_)
              << "  property_: " << property_ << std::endl;
  }

private:
  VertexId_t dst_;
  SequenceNumber_t seq_;
  Marker_t marker_;
  EdgeProperty_t property_; // In memory, edge properties stay with the edge
  bool is_out_ = true;
  uint8_t edge_type_ = 0;
};

struct EdgeComparator {
  static int compare(const Edge& a, const Edge& b) {
    if (a.destination() < b.destination()) {
      return -1;
    } else if (a.destination() > b.destination()) {
      return +1; // The newer the time, the higher the front.
    } else {
      if (a.sequence() < b.sequence()) {
        return +1;
      } else if (a.sequence() > b.sequence()) {
        return -1;
      } else {
        return 0;
      }
    }
  }
};

}  // namespace lsmgraph

#endif  // EDGE_H_
