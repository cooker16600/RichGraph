#pragma once

#include <sys/stat.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <fstream>

#include "core/cache/BlockManager.h"
#include "core/SSTable.h"
#include "core/storage_internal.h"
#include "core/fixed_property_layout.h"
#include "core/flags.h"
#include "core/LazyUpdate.h"

namespace lsmgraph
{
  // Caches edge bodies and property columns from an SST.
  class SSTDataCache {
    public:
      SSTDataCache ()
          : sub_property_num_(GetActiveSubPropertyNum()),
            property_size_(new size_t[static_cast<size_t>(sub_property_num_)]) {}

      // Loads files into memory managed by BlockManager.
      SSTDataCache (const std::string& e_path, const size_t edge_num, 
                    const std::string& p_path, BlockManager& block_manager) 
                    : it_(NULLPOINTER),
                      sub_property_num_(GetActiveSubPropertyNum()),
                      property_size_(new size_t[static_cast<size_t>(sub_property_num_)]) {
        
        LoadEdgesByBlockManager(e_path, edge_num, block_manager);

        property_ptrs_.reserve(sub_property_num_);
        for(int i = 0; i < sub_property_num_; i++) {
          char * tmp_ptr;
          property_ptrs_.push_back(tmp_ptr);
          LoadPropertyByBlockManager(pFileName_with_id(p_path, i).data(),  block_manager, i); 
        }        
      }

      // 加载文件到独立匿名的mmap中
      SSTDataCache (const std::string& e_path, const size_t edge_num, 
                    const std::string& p_path, uintptr_t it, SequenceNumber_t newest_edge_)
                    : it_(it),
                      newest_edge(newest_edge_),
                      sub_property_num_(GetActiveSubPropertyNum()),
                      property_size_(new size_t[static_cast<size_t>(sub_property_num_)]) {
        
        
        LoadFile(e_path, p_path, edge_num);        
      }

      void LoadEdgesByBlockManager (const std::string& path, 
                                    const size_t edge_num, 
                                    BlockManager& block_manager) {
        edgebody_size_ = EDGEBODY_SIZE * edge_num;
        edgebody_ptr_ =  reinterpret_cast<char *>(block_manager.get_data()) 
                      + block_manager.alloc(edgebody_size_);
        std::ifstream file(path, std::ios::binary|std::ios::in);
        file.seekg(0);
        auto read_bytes = file.readsome((char*)edgebody_ptr_, edgebody_size_);
        assert(edgebody_size_ == read_bytes);
        file.close();
      }

      void LoadPropertyByBlockManager (const std::string& path, 
                                       BlockManager& block_manager,
                                      int sub_property_id) {
        property_size_[sub_property_id] = GetFileSize(path.data());
        property_ptrs_[sub_property_id] =  reinterpret_cast<char *>(block_manager.get_data()) 
                      + block_manager.alloc(property_size_[sub_property_id]);
        std::ifstream file(path, std::ios::binary|std::ios::in);
        auto read_bytes = file.readsome((char*)property_ptrs_[sub_property_id], property_size_[sub_property_id]);
        assert(property_size_[sub_property_id] == read_bytes);
        file.close();
      }

      // Load edge bodies and property columns into the same managed block.
      void LoadFile (const std::string& e_path, 
                     const std::string& p_path, 
                     const size_t edge_num) {
        edgebody_size_ = EDGEBODY_SIZE * edge_num;
        
        for(int i = 0; i < sub_property_num_; i++) {
          property_size_[i] = GetFileSize(pFileName_with_id(p_path, i).data());
        }
        

        {
          e_fd = open(e_path.c_str(), O_RDONLY);
          if (e_fd == EMPTY_FD) 
            throw std::runtime_error("open error. e_path=" + e_path);
          lseek(e_fd, 0, SEEK_SET);
          edgebody_ptr_ = reinterpret_cast<char *>(mmap(nullptr, edgebody_size_, 
                          PROT_READ, MAP_PRIVATE, e_fd, 0));
          
          madvise(edgebody_ptr_, edgebody_size_, MADV_RANDOM);
          if (data == MAP_FAILED)
            throw std::runtime_error("mmap edgebody error.");
            close(e_fd);
        }

        {
          property_ptrs_.reserve(sub_property_num_);
          for(int i = 0; i < sub_property_num_; i++) {
            char * tmp_ptr;
            property_ptrs_.push_back(tmp_ptr);
            const::std::string tmp_p_path = p_path + "_" + std::to_string(i);
            p_fd = open(tmp_p_path.c_str(), O_RDONLY);
            if (p_fd == EMPTY_FD) 
              throw std::runtime_error("open error. p_path=" + p_path + std::to_string(i));
            lseek(p_fd, 0, SEEK_SET);
            property_ptrs_[i] = reinterpret_cast<char *>(mmap(nullptr, property_size_[i], 
                            PROT_READ, MAP_PRIVATE, p_fd, 0));

            madvise(property_ptrs_[i], property_size_[i], MADV_RANDOM);
            if (data == MAP_FAILED)
              throw std::runtime_error("mmap property error.");
              close(p_fd);
        }
          }

          
      }

      char* GetEdgeData () {
        return edgebody_ptr_;
      }

      char* GetPropertyData (int sub_property_id) {
        return property_ptrs_[sub_property_id];
      }

      size_t GetPropertySize (int id) {
        return property_size_[id];
      }

      size_t GetFileSize (const char *fileName) {
        if (fileName == NULL) {
          return 0;
        }
        struct stat statbuf;
        stat(fileName, &statbuf);
        return statbuf.st_size;
      }
      
      uintptr_t GetSSTableCache () {
        return it_;
      }

      int GetEFileFD() {
        return e_fd;
      }

      ~SSTDataCache() {
        if (data != nullptr) {
          munmap(data, capacity);
          std::cout << " ~munmap" << std::endl;
        }
        munmap(edgebody_ptr_, edgebody_size_);
        for(int i = 0; i < sub_property_num_; i++){
          if(property_ptrs_[i] == nullptr){
            continue;
          }
          munmap(property_ptrs_[i], property_size_[i]);
          property_ptrs_[i]= nullptr;
        }
        delete[] property_size_;
      }

    private:
      char* edgebody_ptr_;
      size_t edgebody_size_;
      
      size_t capacity = 0;
      void *data = nullptr;
      constexpr static int EMPTY_FD = -1;
      int e_fd = EMPTY_FD;
      int p_fd = EMPTY_FD;
      uintptr_t it_;
      

    public:
      int sub_property_num_ = 0;
      LazyUpdate* lazy_update_;
      SequenceNumber_t newest_edge = 0;
      size_t *property_size_ = nullptr;
      std::vector<char*>property_ptrs_;
  };

} // namespace lsmgraph
