#pragma once
#include <string>
#include <vector>
#include <list>
#include <unordered_set>
#include <limits>
#include <cassert>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_map>
#include <fstream>
#include <sstream>
#include <iostream>
#include <algorithm>
#include <fcntl.h>
#include <unistd.h>

#include "core/types.h"
#include "core/flags.h"

namespace lsmgraph {


    class LazyUpdate {
    public:


        LazyUpdate(FileId_t fid) : 
            fid_(fid)
             {
                creation_time = time(nullptr);
                for(int i = 0; i < FLAGS_sub_property_num; i++) {
                    mutexes.emplace_back(std::make_unique<std::mutex>());
                    read_count.emplace_back(0);
                    write_count.emplace_back(0);
                    item_num.emplace_back(1024);
                    used_item.emplace_back(0);
                }
                file_ptr.resize(FLAGS_sub_property_num, nullptr);
                map_size.resize(FLAGS_sub_property_num, 0);
                p_fd.resize(FLAGS_sub_property_num, -1);
            }

        ~LazyUpdate() {
            // Ensure all operations are complete before destruction
            // Clear any resources if needed

            // TODO(correctness): Define ownership before deleting persistent
            // lazy-update files during destruction.
            for(int i = 0; i < FLAGS_sub_property_num; i++){
                if(p_fd[i] != -1){
                    close(p_fd[i]);
                }
            }
        }


        // Get data from the lazy update
        Status getData(VertexId_t src, VertexId_t dst, std::string& data, int sub_property_id);

        bool expandSize(int sub_property_id);
        bool shrinkSize(int sub_property_id);

        bool isExpand(int sub_property_id);

        bool isShrink(int sub_property_id);

        void setItemNum(int sub_property_id, uint32_t num);

        uint32_t getItemNum(int sub_property_id);

        uint32_t getUsedItem(int sub_property_id);

        void setUsedItem(int sub_property_id, uint32_t num);

        void clear_data(int sub_property_id);

        void try_map_file(int sub_property_id);

        VertexId_t get_src_from_lazy_mmp(char* ptr);

        VertexId_t get_dst_from_lazy_mmp(char* ptr);

        Marker_t get_marker_from_lazy_mmp(char* ptr);

        SequenceNumber_t get_seq_from_lazy_mmp(char* ptr);

        std::string get_property_from_lazy_mmp(char* ptr);

        int get_propertyLen_from_lazy_mmp(char* ptr);

        int next_from_lazy_mmp(char* ptr, int sub_property_id);

    private:
    
        std::string filename;
        
        FileId_t fid_;
        std::vector<uint32_t> read_count;
        std::vector<uint32_t> write_count;
        time_t creation_time;
        std::vector<uint32_t> item_num;
        std::vector<uint32_t> used_item;
        // One mutex protects each independently mapped property file.
    public:
        std::vector<std::unique_ptr<std::mutex>>mutexes;
        FileId_t getFid();
        
        std::vector<int>p_fd;
        std::vector<uint32_t> map_size;
        std::vector<char*> file_ptr;
        void printLazy(int sub_property_id);
    };

} //namespace lsmgraph
