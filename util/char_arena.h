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
#ifndef CharArena_H
#define CharArena_H

#include <cstddef>
#include <memory>
#include <vector>
#include <cassert>

namespace lsmgraph {

/**
 * Space management and allocation for objects of type T.
 * Note that the maximum demand needs to be estimated in advance, and the 
 * capacity cannot be expanded!
*/

template <typename T>
class CharArena {
public:
    CharArena(): capacity_(0), data_(nullptr) {}

    CharArena(int capacity) : capacity_(capacity), used_size_(0)  {
      data_ = (T*)malloc(capacity_);
    }

    void Init(const size_t capacity) {
      capacity_ = capacity;
      used_size_ = 0;
      data_ = (T*)malloc(capacity_);
      if (!data_) {
          std::cerr << "Memory allocation failed." << std::endl;
      }
    }

    // support concurrency 
    T* Alloc(const size_t need_size) {
        size_t pointer = 0;
        if (used_size_ + need_size >= capacity_) {
            std::cout << " no a free node!\n";
            exit(0);
        }
        pointer = __sync_fetch_and_add(&used_size_, need_size);
        assert(pointer < capacity_);
        return data_ + pointer;
    }

    void Resize(size_t size) {
      // pass
    }

    void Reset() {
      used_size_ = 0;
    }

    size_t FreeSize() {
      assert(used_size_ <= capacity_);
      return capacity_ - used_size_;
    }

    size_t GetUsedSize() {
      return used_size_;
    }

    ~CharArena() {
      free(data_);
    }

private:
  size_t capacity_;
  size_t used_size_;
  T *data_;
};

}  // namespace lsmgraph

#endif // CharArena_H