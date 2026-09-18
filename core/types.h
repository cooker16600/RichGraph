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

#ifndef TYPES_H_
#define TYPES_H_

#include <stdint.h>
#include <array>
#include <limits>
#include <shared_mutex>
#include "richgraph/types.h"
#include "util/lock.h"



namespace lsmgraph
{



    using VertexProperty_t = std::array<char, 8>;    // vertex property
    using VertexOffset_t = uint32_t;                  // vertex address offset_

    using EdgeOffset_t = uint32_t;                    // edge address offset_
    using Marker_t = bool;                            // marker bit

    using EdgeProperty_t = std::string;               // edge property
    using EdgePropertyOffset_t = uint32_t;            // edge property address offset_

    using RWLock_t = CASRWLock;

    using Level_t = int8_t;

    constexpr static uintptr_t NULLPOINTER = 0;

    struct tmp_Edge{
        VertexId_t src;
        VertexId_t dst;
        Marker_t marker;
        bool is_out = true;
        uint8_t edge_type = 0;
        SequenceNumber_t seq;
        std::string property;
        
        friend bool operator <(tmp_Edge a, tmp_Edge b){
            if(a.src == b.src) {
                if(a.dst == b.dst){
                    if (a.is_out != b.is_out) {
                        return a.is_out < b.is_out;
                    }
                    if (a.edge_type != b.edge_type) {
                        return a.edge_type < b.edge_type;
                    }
                    return a.seq > b.seq;
                }
                return a.dst < b.dst;
            }
            return a.src < b.src;
        }
        };

    struct tmp_Edge_slice{
        VertexId_t src;
        VertexId_t dst;
        Marker_t marker;
        bool is_out = true;
        uint8_t edge_type = 0;
        SequenceNumber_t seq;
        char* ptr;
        size_t len;
        
        friend bool operator <(tmp_Edge_slice a, tmp_Edge_slice b){
            if(a.src == b.src) {
                if(a.dst == b.dst){
                    if (a.is_out != b.is_out) {
                        return a.is_out < b.is_out;
                    }
                    if (a.edge_type != b.edge_type) {
                        return a.edge_type < b.edge_type;
                    }
                    return a.seq > b.seq;
                }
                return a.dst < b.dst;
            }
            return a.src < b.src;
        }
        };
} // namespace lsmgraph


#endif  // TYPES_H_
