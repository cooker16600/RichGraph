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

#ifndef VERTEX_H_
#define VERTEX_H_

#include <iostream>
#include "types.h"

namespace lsmgraph {

class Vertex{
public:
  Vertex() = default;
  Vertex(VertexId_t id, SequenceNumber_t seq, VertexOffset_t prev_pointer, VertexProperty_t property)
      : id_(id), seq_(seq), prev_pointer_(prev_pointer), property_(property) {}

  ~Vertex() {}

  // Copy Constructor
  Vertex(const Vertex& other) {
    id_ = other.id_;
    seq_ = other.seq_;
    prev_pointer_ = other.prev_pointer_;
    property_ = other.property_;
  }

  // Move Constructor
  Vertex(Vertex&& other) {
    id_ = std::move(other.id_);
    seq_ = std::move(other.seq_);
    prev_pointer_ = std::move(other.prev_pointer_);
    property_ = std::move(other.property_);
  }

  // Assignment operator
  Vertex& operator=(const Vertex& other) {
    if (this != &other) {
      id_ = other.id_;
      seq_ = other.seq_;
      prev_pointer_ = other.prev_pointer_;
      property_ = other.property_;
    }
    return *this;
  }

  // Move Assignment operator
  Vertex& operator=(Vertex&& other) {
    if (this != &other) {
      id_ = std::move(other.id_);
      seq_ = std::move(other.seq_);
      prev_pointer_ = std::move(other.prev_pointer_);
      property_ = std::move(other.property_);
    }
    return *this;
  }

  bool operator==(const Vertex& other) const {
    return id_ == other.id_ && seq_ == other.seq_ &&
           prev_pointer_ == other.prev_pointer_ &&
           property_ == other.property_;
  }

  bool operator!=(const Vertex& other) const { return !(*this == other); }

  bool operator<(const Vertex& rhs) const {
    if (id_ == rhs.id_) {
      return seq_ < rhs.seq_;
    }
    return id_ < rhs.id_;
  }

  // Serialize Edge to a buffer
  void Serialize(char* buffer) const {
    memcpy(buffer, this, sizeof(Vertex));
  }

  // Deserialize Edge from a buffer
  void Deserialize(const char* buffer) {
    memcpy(this, buffer, sizeof(Vertex));
  }

  VertexId_t id() const { return id_; }
  void set_id(VertexId_t id) { id_ = id; }

  SequenceNumber_t sequence() const { return seq_; }
  void set_sequence(SequenceNumber_t seq) { seq_ = seq; }

  VertexOffset_t prev_pointer() const { return prev_pointer_; }
  void set_prev_pointer(VertexOffset_t prev_pointer) { prev_pointer_ = prev_pointer; }

  VertexProperty_t property() const { return property_; }
  void set_property(VertexProperty_t property) { property_ = property; }

  void print() const {
    printf("print vertex information:\n");
    std::cout << "  id: " << id_ << std::endl;
    std::cout << "  seq: " << seq_ << std::endl;
    std::cout << "  prev_pointer: " << prev_pointer_ << std::endl;
    std::cout << "  spropertyeq: ";
    for(const auto& s: property_)
      std::cout << s;
    std::cout << std::endl;
  }

private:
  VertexId_t id_;
  SequenceNumber_t seq_;           // timestamp 
  VertexOffset_t prev_pointer_;    // vertex offset
  VertexProperty_t property_;     // property data
};

}  // namespace lsmgraph

#endif  // VERTEX_H_