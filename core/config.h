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

#ifndef CONFIG_H_
#define CONFIG_H_

#include "core/DataStructs.h"
#include <gflags/gflags.h>



namespace lsmgraph
{
    #define MAX_TABLE_SIZE (64 * (1 << 20))
    const int EDGEBODY_SIZE = sizeof(EdgeBody_t);
    const int HEADER_SIZE = sizeof(Header);

    /************************** compaction config *****************************/
    // set th level num and efile size for each level.
    const size_t BODY_BUFFER_SIZE = (4 * (1 << 20) / EDGEBODY_SIZE);
    // A property buffer must hold at least one complete edge property.
    const size_t PROPERTY_BUFFER_SIZE = (4 * (1 << 20));
    const size_t MAX_EFILE_SiZE = 64 * (1 << 20);
    // Conservative upper bound: an SST cannot contain more source-index
    // entries than edge bodies. Reserve one additional entry for the sentinel.
    const int MAX_INDEX_NUM =
        (MAX_TABLE_SIZE - HEADER_SIZE) / EDGEBODY_SIZE + 1;
    const int LEVEL_FILE_RATIO = 10; // default 10
    
    static int BINARY_THRESHOLD_FIND_EDGE = 20; // find_edge_from_file_with_cache


    extern double query_time_mem;
    extern double query_time_file;
    extern double query_time_find_index;
    extern double query_time_find_edge;
    extern double query_time_find_property;
    extern double find_all_iterator;
    extern double compaction_time;
    extern double get_curr_version_time;
    extern double iterate_memtable; // level-0 find
    extern double iterate_level_0; // level-0 find
    extern double iterate_level_1; // level-0 find
    extern double iterate_find_first; // level-0 find
    extern double iterate_check_entry_valid; // check_entry_valid
    extern double put_memtable_time_per; // insert to memtable time of each time
    extern double put_memtable_time_sum; // insert to memtable time
    extern double LF_get;
    extern double LF_find_lo;
    extern double LF_construct;
    extern double mp_iter; //mp构建
    extern double mt_iter; //mt构建
    extern double lv0_iter; //0层sst构建
    extern double high_lv_iter;
    extern double find_first;
    extern double LF_total; //LF花费的总时间
    extern double map_check;
    extern double get_store;
    extern double for_iter; //for遍历时间
    extern double sst_get_ep;//sst_iter中获取属性
    extern double sst_get_offset;//sst 中获取偏移量
    extern double cast_time;
    extern double cul_time;
    extern double get_value_time;
    extern double head_time;
    extern double sst_dst;
    extern int block_cnt_;
} // namespace lsmgraph


#endif  // CONFIG_H_
