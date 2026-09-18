#ifndef SSTABLE_H
#define SSTABLE_H


#include "core/DataStructs.h"
#include <string>
#include <vector>
#include <list>
#include <unordered_set>
#include <limits>
#include <cassert>
#include <shared_mutex>
#include <unordered_map>
#include "core/config.h"
#include "core/fixed_property_layout.h"
#include "core/cache/SSTDataManager.h"

namespace lsmgraph {

class SSTableCache
{
public:
    Header header;
    // Recovered SST metadata does not persist this lower bound.  Zero is a
    // conservative default: readers may inspect an extra SST, but must never
    // skip a file because of an indeterminate sequence number.
    SequenceNumber_t seq_{0}; // earliest edge sequence number
    SSTDataManager& sstdata_manager_;  

    std::vector<Index> indexes; // 索引数组，存储 <key, offset> 信息
    std::string path;

    std::atomic<int32_t> refs = {1};
    SequenceNumber_t newest_edge = 0;

    // Number of property files physically owned by this SST.  This must be
    // captured when the SST is created/loaded: FLAGS_sub_property_num is a
    // process-wide compatibility flag and may describe a different shard by
    // the time this object is retired.
    const uint32_t property_file_count_;






    // Store file paths for different property types
    std::vector<std::string> property_files;  // Store property files dynamically

    SSTableCache(SSTDataManager& sstdata_manager, SequenceNumber_t newest_edge_):
                                        sstdata_manager_(sstdata_manager),
                                        newest_edge(newest_edge_),
                                        property_file_count_(
                                            GetActiveSubPropertyNum()) {}
    SSTableCache(const std::string dir, SSTDataManager& sstdata_manager); // 不能用引用，否则局部变量释放导致错误
    Header readHeadFromFile(const std::string dir);

    void insert_property(const uint64_t &src, const uint64_t &dst, std::string property);
    // Appends an edge-property update to its delta file. A full delta must be
    // merged into the base property file before accepting more records.

    int get(const uint64_t &src);
    int get(const uint64_t &src, const uint64_t &dst);
    int find(const uint64_t &key, int start, int end);
    int low_bound(const uint64_t &key, int start, int last);
    int low_bound(const uint64_t &key, int start, int last, char *indexBuf);
    char* GetIndex();

    void Ref();
    void Unref();
    int32_t Getref();
};


}  // namespace lsmgraph

#endif // SSTABLE_H
