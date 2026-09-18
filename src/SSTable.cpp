#include "core/SSTable.h"
#include <fstream>
#include <iostream>
#include <string.h>
#include "utils.h"
#include "core/storage_internal.h"

namespace lsmgraph {

SSTableCache::SSTableCache(const std::string dir, 
                           SSTDataManager& sstdata_manager)
                           : sstdata_manager_(sstdata_manager),
                             property_file_count_(
                                 GetActiveSubPropertyNum()) {
    path = dir;
    std::ifstream file(dir, std::ios::binary);
    if(!file) {
        printf("Fail to open file %s\n", dir.c_str());
        exit(-1);
    }
    // load header
    int offset = -(HEADER_SIZE);
    file.seekg(offset, std::ios::end);
    file.read((char*)&header, sizeof(lsmgraph::Header));
    int64_t index_length = header.index_size;

    char *indexBuf = new char[index_length * 12];
    file.seekg(header.size * EDGEBODY_SIZE, std::ios::beg);

    file.read(indexBuf, index_length * 12);
    for(int32_t i = 0; i < index_length; ++i) {
        VertexId_t key = 0;
        EdgeOffset_t offset = 0;
        std::memcpy(&key, indexBuf + sizeof(Index) * i, sizeof(key));
        std::memcpy(&offset,
                    indexBuf + sizeof(Index) * i + sizeof(key),
                    sizeof(offset));
        indexes.emplace_back(key, offset);
    }

    sstdata_manager_.put_data(header.timeStamp, header.size, reinterpret_cast<uintptr_t>(this), newest_edge);
    delete[] indexBuf;
    indexBuf = nullptr;
    file.close();

}

Header SSTableCache::readHeadFromFile(const std::string dir) {
  std::ifstream file(dir, std::ios::binary);
  if(!file) {
      printf("Fail to open file %s\n", dir.c_str());
      exit(-1);
  }
  // load header
  Header header;
  int offset = -(HEADER_SIZE);
  file.seekg(offset, std::ios::end);
  file.read((char*)&header, sizeof(lsmgraph::Header));
  return header;
}

// query the offset of the src
int SSTableCache::get(const uint64_t &src)
{
    return find(src, 0, indexes.size() - 1);
}

// First determine whether the target edge exists, and then query the offset of the src
int SSTableCache::get(const uint64_t &src, const uint64_t &dst)
{
    return find(src, 0, indexes.size() - 1);
}

// Returns the position of <key, offset> in the index array.
int SSTableCache::find(const uint64_t &key, int start, int last)
{
	while (start <= last) {
		int mid = start + ((last - start) >> 1);
		if (indexes[mid].key == key) {
			return mid;
		} else if (indexes[mid].key < key) {
			start = mid + 1;
		} else {
      last = mid - 1;
    }
	}
	return -1;
}

// Returns a position of iterator pointing to the first element in the range 
// [start,last) which does not compare less than key.
int SSTableCache::low_bound(const uint64_t &key, int start, int last) {

  auto it = std::lower_bound(indexes.begin() + start, 
                             indexes.begin() + last, 
                             key, 
                             [](const Index &index, uint64_t key){
                               return index.key < key;
                             });
  return it - indexes.begin();
}

// Returns a position of iterator pointing to the first element in the range 
// [start,last) which does not compare less than key.
int SSTableCache::low_bound(const uint64_t &key, 
                            int start, 
                            int last, 
                            char *indexBuf) {
  while (start < last) {
    int mid = (start + last) / 2;
    VertexId_t mid_val = *(uint64_t*)(indexBuf + mid*12);
    if (mid_val >= key) {
      last = mid;
		} else {
      start = mid + 1; // sizeof(index)=8+4
    }
  }
  return start;
}

// 注意使用完，需要释放指针: delte indexBuf
char* SSTableCache::GetIndex() {
  std::ifstream file(path, std::ios::binary);
  if (!file) {
      printf("Fail to open file %s\n", path.c_str());
      exit(-1);
  }
  int64_t index_length = header.index_size;
  char *indexBuf = new char[index_length * 12];
  file.seekg(header.size * EDGEBODY_SIZE, std::ios::beg);

  file.read(indexBuf, index_length * 12);
  file.close();
  return indexBuf;
}

void SSTableCache::Ref() {
    refs.fetch_add(1);
}

void SSTableCache::Unref() {
    int32_t old_refs = refs.fetch_sub(1);
    assert(old_refs >= 1);
    if (old_refs == 1) { // means refs = 0
        sstdata_manager_.del_data(header.timeStamp);
        auto rt = utils::rmfile(eFileName(header.timeStamp).c_str());
        assert(rt == 0);
        for (uint32_t i = 0; i < property_file_count_; ++i) {
          rt = utils::rmfile(pFileName_with_id(pFileName(header.timeStamp), i).c_str());
          assert(rt == 0);
        }
        
        delete this; // 删除自己
    }
}

int32_t SSTableCache::Getref() {
    return refs.load(std::memory_order_acquire);
}


}  // namespace lsmgraph
