#pragma once

#include "core/LazyFile.h"
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

typedef tbb::concurrent_hash_map<FileId_t, LazyUpdate *> HashMapLazyUpdate;
typedef
    typename HashMapLazyUpdate::const_accessor HashMapLazyUpdateConstAccessor;
typedef typename HashMapLazyUpdate::accessor HashMapLazyUpdateAccessor;
typedef typename HashMapLazyUpdate::iterator HashMapLazyUpdateIterator;
typedef HashMapLazyUpdate::value_type HashMapLazyUpdateValuePair;

typedef tbb::concurrent_hash_map<FileId_t, LazyFile *> HashMapLazyFile;
typedef typename HashMapLazyFile::const_accessor HashMapLazyFileConstAccessor;
typedef typename HashMapLazyFile::accessor HashMapLazyFileAccessor;
typedef typename HashMapLazyFile::iterator HashMapLazyFileIterator;
typedef HashMapLazyFile::value_type HashMapLazyFileValuePair;

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

  void put_LazyFile(const FileId_t fid, LazyFile *lf) {

    HashMapLazyFileValuePair hashMapValuePairOfLazyFile(fid, lf);

    bool rt = hashMapOfLazyFile.emplace(fid, lf);
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

  LazyFile *get_LazyFile(FileId_t fid) {
    HashMapLazyFileConstAccessor hashAccessor;
    if (hashMapOfLazyFile.find(hashAccessor, fid)) {
      return hashAccessor->second;
    } else {
      throw std::runtime_error("No found sstdatacache, fid=" +
                               std::to_string(fid));
      return nullptr;
    }
  }

  LazyUpdate *get_lazyupdate(const FileId_t fid) {
    HashMapLazyUpdateConstAccessor hashAccessor;
    if (hashMapOfLazyUpdate.find(hashAccessor, fid)) {
      return hashAccessor->second;
    } else {
      throw std::runtime_error("No found lazyupdate, fid=" +
                               std::to_string(fid));
      return nullptr;
    }
  }

  std::vector<FileId_t> get_all_fid() {
    std::vector<FileId_t> fids;
    for (auto it = hashMapOfLazyUpdate.begin(); it != hashMapOfLazyUpdate.end();
         it++) {
      fids.push_back(it->first);
    }
    return fids;
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

  void del_LazyFile(FileId_t fid) {
    HashMapLazyFileConstAccessor hashAccessor;
    if (hashMapOfLazyFile.find(hashAccessor, fid)) {
      delete hashAccessor->second;
      hashMapOfLazyFile.erase(hashAccessor);
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

  HashMapLazyUpdate GetHashMapOfLazyUpdate() { return hashMapOfLazyUpdate; }

private:
  HashMap hashMap;                       // cache sstable data
  HashMapLazyUpdate hashMapOfLazyUpdate; // cache lazyupdate
  HashMapLazyFile hashMapOfLazyFile;     // cache lazyfile
};

} // namespace lsmgraph
