#pragma once

#include <fstream>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <vector>



#include "core/types.h"
#include "core/storage_internal.h"
#include "core/DataStructs.h"



namespace  lsmgraph{


    
class LazyFile{
public:
    FileId_t fid_;
    std::atomic<int32_t> refs;
    int file_size;
    SequenceNumber_t newest_edge = 0;

    LazyFile(FileId_t fid, Header header_, std::vector<Index>indexs_, SequenceNumber_t newest_edge_): fid_(fid), header(header_), indexs(indexs_), refs(0), newest_edge(newest_edge_){
        LoadFile(fid);
    }
    ~LazyFile(){
        if (fd != -1) {
            munmap(file_ptr, GetFileSize_(pLazyFileName(fid_).data()));
            close(fd);
        }
    }
    
    int fd = -1;
    char* file_ptr;
    Header header;
    std::vector<Index>indexs;

    void Ref();
    void Unref();
    int32_t Getref();
    void LoadFile(FileId_t fid);
    int Get_Reseted_EdgeBody_Offset();
    int Get_Reseted_Property_Offset();
    int Get_Reseted_Index_Offset();
    bool get_next_edge(int& edge_Offset, int& index_Offset, tmp_Edge& tmp_edge);
    bool get_next_edge_slice(int& edge_Offset, int& index_Offset, tmp_Edge_slice& er);
    int get(VertexId_t src);
};
    
}
