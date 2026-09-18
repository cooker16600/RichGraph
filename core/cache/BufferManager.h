#pragma once

#include "core/DataStructs.h"
#include "core/flags.h"
#include "core/graph/edge.h"
#include <fcntl.h>
#include <mutex>
#include <sys/mman.h>
#include <unistd.h>
#include <vector>

namespace lsmgraph {
template <typename T> class BufferSet {
public:
  BufferSet(int buffer_num = 0, size_t buffer_size = 0)
      : buffer_num_(buffer_num), buffer_size_(buffer_size), mutex() {
    free_prt_ = 0;
    free_list_.resize(buffer_num_);
    for (int i = 0; i < buffer_num_; i++) {
      T *buffer_prt = new T[buffer_size_];
      free_list_[free_prt_++] = buffer_prt;
    }
    free_prt_--;
    assert(free_prt_ == buffer_num_ - 1);
  }

  ~BufferSet() {
    if (FLAGS_richgraph_verbose) {
      size_t mem_cost = buffer_num_ * buffer_size_ * sizeof(T);
      printf("~BufferManager buffer_num_=%d mem_cost=%.2fM\n", buffer_num_,
             mem_cost * 1.0 / 1024 / 1024);
    }
    // TODO(correctness): BufferSet owns these allocations but cannot release
    // them until every checked-out buffer has been returned. Replace this raw
    // pointer pool with RAII ownership and explicit borrower lifetime tracking.
    assert(free_prt_ == buffer_num_ - 1);
  }

  T *alloc() {
    T *pointer = nullptr;
    std::lock_guard<std::mutex> lock(mutex);
    if (free_prt_ >= 0) {
      pointer = free_list_[free_prt_--];
    } else if (free_prt_ == -1) {
      free_list_.resize(buffer_num_ * 2);
      for (int i = 0; i < buffer_num_; i++) {
        T *buffer_prt = new T[buffer_size_];
        free_list_[++free_prt_] = buffer_prt;
      }
      buffer_num_ *= 2;
      pointer = free_list_[free_prt_--];
    } else {
      throw std::runtime_error("no free buffer.");
    }
    return pointer;
  }

  void free(T *pointer) {

    std::lock_guard<std::mutex> lock(mutex);
    if (free_prt_ < buffer_num_ - 1) {
      free_list_[++free_prt_] = pointer;
    } else {
      throw std::runtime_error("erro: free buffer.");
    }
  }

  int free_num() { return free_prt_ + 1; }

private:
  int buffer_num_;
  const size_t buffer_size_;
  std::vector<T *> free_list_;
  int free_prt_;
  std::mutex mutex;
};

class BufferManager {
public:
  BufferManager(int max_index_buffer_num = 0, size_t max_index_buffer_size = 0,
                int index_buffer_num = 0, size_t index_buffer_size = 0,
                int EdgeBody_buffer_num = 0, size_t EdgeBody_buffer_size = 0,
                int Property_buffer_num = 0, size_t Property_buffer_size = 0)
      : max_indexBuffer_(max_index_buffer_num, max_index_buffer_size),
        indexBuffer_(index_buffer_num, index_buffer_size),
        edgeBodyBuffer_(EdgeBody_buffer_num, EdgeBody_buffer_size),
        propertyBuffer_(Property_buffer_num, Property_buffer_size) {}

  Index *GetMaxIndexBuffer() { return max_indexBuffer_.alloc(); }

  void FreeMaxIndexBuffer(Index *prt) { max_indexBuffer_.free(prt); }

  int GetMaxIndexBufferNum() { return max_indexBuffer_.free_num(); }

  Index *GetIndexBuffer() { return indexBuffer_.alloc(); }

  void FreeIndexBuffer(Index *prt) { indexBuffer_.free(prt); }

  int GetIndexBufferNum() { return indexBuffer_.free_num(); }

  EdgeBody_t *GetEdgeBodyBuffer() { return edgeBodyBuffer_.alloc(); }

  void FreeEdgeBodyBuffer(EdgeBody_t *prt) { edgeBodyBuffer_.free(prt); }

  int GetEdgeBodyBufferNum() { return edgeBodyBuffer_.free_num(); }

  std::vector<char *> *GetPropertyBuffer() { return propertyBuffer_.alloc(); }

  void FreePropertyBuffer(std::vector<char *> *prt) {
    propertyBuffer_.free(prt);
  }

  int GetPropertyBufferNum() { return propertyBuffer_.free_num(); }

private:
  BufferSet<Index> max_indexBuffer_;
  BufferSet<Index> indexBuffer_;
  BufferSet<EdgeBody_t> edgeBodyBuffer_;
  BufferSet<std::vector<char *>> propertyBuffer_;
};

} // namespace lsmgraph
