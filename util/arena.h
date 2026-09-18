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
#ifndef ARENA_H
#define ARENA_H

#include "core/flags.h"
#include <cassert>
#include <cstddef>
#include <memory>
#include <vector>

using order_t = uint8_t;

namespace lsmgraph {

/**
 * Space management and allocation for objects of type T.
 * Note that the maximum demand needs to be estimated in advance, and the
 * capacity cannot be expanded.
 * Note:
 * 通过GetAnElement()方式获取的元素会进行计数，通过GetAnElementById()获得的元素则不会统计
 * 当前元素数量，FreeSize()这个方法此时不能使用。
 */

template <typename T> class Arena {
public:
  Arena() = delete;

  explicit Arena(size_t node_num)
      : nodelist_(node_num), node_num_(node_num), free_prt_(0) {
    if (FLAGS_richgraph_verbose) {
      std::cout << "node_num=" << node_num << std::endl;
    }
  }

  // support concurrency
  T *GetAnElement() {
    T *pointer = NULL;
    if (free_prt_ >= node_num_) {
      std::cout
          << " no a free node!\n"; // 注意:不能用上面的进行扩展，不然前面所有记录的节点的地址会失效。所以，得保证第一次分配的大小就足够。或者改成记录下标或者偏移量。
      exit(0);
    }
    size_t temp_ptr = __sync_fetch_and_add(&free_prt_, 1);
    pointer = &nodelist_[temp_ptr];
    assert(pointer != NULL);
    return pointer;
  }
  // support concurrency
  T *GetAnElementById(size_t id) { //给定ID，返回对应的空间，用于存储数据
    T *pointer = NULL;
    if (id >= node_num_) {
      std::cout
          << " no a free node!\n"; // 注意:不能用上面的进行扩展，不然前面所有记录的节点的地址会失效。所以，得保证第一次分配的大小就足够。或者改成记录下标或者偏移量。
      exit(0);
    }
    assert(id < node_num_);
    pointer = &nodelist_[id];
    assert(pointer != NULL);
    return pointer;
  }

  void Reset() {
    free_prt_ = 0;
// Can be executed in parallel
#pragma omp parallel for num_threads(FLAGS_thread_num)
    for (int i = 0; i < node_num_; i++) {
      nodelist_[i].reset(); // 是否必要
    }
  }

  size_t FreeSize() { return node_num_ - free_prt_; }

private:
  std::vector<T> nodelist_;
  size_t node_num_;
  size_t free_prt_;
};

} // namespace lsmgraph

#endif // ARENA_H
