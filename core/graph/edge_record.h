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

#pragma once

#include "core/SSTEdgeIterator.h"

namespace lsmgraph {

struct EdgeRecord {
    VertexId_t src_;
    VertexId_t dst_;
    SequenceNumber_t seq_;
    std::vector<Slice> props_;
    Marker_t marker_;
    bool is_out_ = true;
    uint8_t edge_type_ = 0;

    EdgeRecord() {}

    EdgeRecord(VertexId_t src, VertexId_t dst, SequenceNumber_t seq,
               std::vector<Slice> props, Marker_t marker, bool is_out = true,
               uint8_t edge_type = 0)
               : src_(src), dst_(dst), seq_(seq), props_(props),
                 marker_(marker), is_out_(is_out), edge_type_(edge_type) {}

    EdgeRecord(VertexId_t src, char * edge_body, const std::vector<Slice> &props)
               : src_(src), 
                 dst_(get_dst(edge_body)), 
                 seq_(get_seq(edge_body)),
                 marker_(get_marker(edge_body)),
                 props_(props),
                 is_out_(reinterpret_cast<EdgeBody_t *>(edge_body)->get_is_out()),
                 edge_type_(reinterpret_cast<EdgeBody_t *>(edge_body)->get_edge_type()) {}

    void SetValue(VertexId_t src, VertexId_t dst, SequenceNumber_t seq,
                  std::vector<Slice> props, Marker_t marker, bool is_out = true,
                  uint8_t edge_type = 0) {
      src_ = src;
      dst_ = dst;
      seq_ = seq;
      props_ = props;
      marker_ = marker;
      is_out_ = is_out;
      edge_type_ = edge_type;
    }

    bool operator<(const EdgeRecord& rhs) const {
        if (src_ < rhs.src_) {
            return true;
        } else if (src_ == rhs.src_) {
            if (dst_ < rhs.dst_) {
                return true;
            } else if (dst_ == rhs.dst_) {
                return seq_ > rhs.seq_; // The newer the time, the higher the front.
            }
        }
        return false;
    }

    bool operator>(const SSTEdgeIterator& rhs) const {
        if (dst_ > rhs.dst_id()) {
            return true;
        } else if (dst_ == rhs.dst_id()) {
            return seq_ < rhs.sequence(); // The newer the time, the higher the front.
        }
        return false;
    }
};

}
