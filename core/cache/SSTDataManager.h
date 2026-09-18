#pragma once

#include "core/cache/SSTDataCache.h"
#include "core/storage_internal.h"
#include "core/flags.h"
#include "core/types.h"
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <tbb/concurrent_hash_map.h>
#include <unistd.h>
#include <vector>

namespace lsmgraph {

typedef tbb::concurrent_hash_map<FileId_t, SSTDataCache *> HashMap;
typedef typename HashMap::const_accessor HashMapConstAccessor;
typedef typename HashMap::accessor HashMapAccessor;
typedef typename HashMap::iterator HashMapIterator;
typedef HashMap::value_type HashMapValuePair;

class SSTDataManagerVector;
class SSTDataManagerHash;

using SSTDataManager = SSTDataManagerHash;
class SSTDataManagerVector {
public:
  SSTDataManagerVector() { sst_data_cache.resize(1); }

  void put_data(FileId_t fid, SSTDataCache *data) {
    if (fid >= sst_data_cache.size()) {
      sst_data_cache.resize(fid * 2);
    }
    assert(fid < sst_data_cache.size());
    sst_data_cache[fid] = data;
  }

  SSTDataCache *get_data(FileId_t fid) { return sst_data_cache[fid]; }

  void del_data(FileId_t fid) {
    delete sst_data_cache[fid];
    sst_data_cache[fid] = nullptr;
  }

  ~SSTDataManagerVector() {
    for (auto data : sst_data_cache) {
      if (data != nullptr) {
        delete data;
      }
    }
  }

private:
  std::vector<SSTDataCache *> sst_data_cache; // cache sstable data
};

class SSTDataManagerHash {
public:
  SSTDataManagerHash() {}

  void put_data(const FileId_t fid, const size_t edge_num, uintptr_t it,
                SequenceNumber_t newest_edge) {
    SSTDataCache *sst = new SSTDataCache(eFileName(fid), edge_num,
                                         pFileName(fid), it, newest_edge);
    HashMapValuePair hashMapValuePair(fid, sst);
    bool rt = hashMap.insert(hashMapValuePair);
    assert(rt == true);
  }

  SSTDataCache *get_data(const FileId_t fid) {
    HashMapConstAccessor hashAccessor;
    if (hashMap.find(hashAccessor, fid)) {
      return hashAccessor->second;
    } else {
      throw std::runtime_error("No found sstdatacache, fid=" +
                               std::to_string(fid));
      return nullptr;
    }
  }

  void del_data(const FileId_t fid) {
    HashMapConstAccessor hashAccessor;
    if (hashMap.find(hashAccessor, fid)) {
      delete hashAccessor->second;
      hashMap.erase(hashAccessor);
    } else {
      throw std::runtime_error("The deleted fid does not exist, fid=" +
                               std::to_string(fid));
    }
  }

  void un_map() { // c除了第0个属性，其他都解除mmap
    for (auto it = hashMap.begin(); it != hashMap.end(); ++it) {
      const FileId_t &file_id = it->first;
      SSTDataCache *cache_ptr = it->second;

      for (int i = 1; i < cache_ptr->sub_property_num_; i++) {
        if (cache_ptr->property_ptrs_[i] == nullptr) {
          continue;
        }
        munmap(cache_ptr->property_ptrs_[i], cache_ptr->property_size_[i]);
        cache_ptr->property_ptrs_[i] = nullptr;
      }
    }
  }

  ~SSTDataManagerHash() {
    if (FLAGS_richgraph_verbose) {
      printf("~sstdatacache.size=%ld\n", hashMap.size());
    }
    for (HashMapIterator iterator1 = hashMap.begin();
         iterator1 != hashMap.end(); ++iterator1) {
      if (iterator1->second != nullptr) {
        delete iterator1->second;
      }
    }
  }

  HashMap GetHashMap() { return hashMap; }

private:
  HashMap hashMap;  // cache sstable data
};

} // namespace lsmgraph
