#include "core/LazyUpdate.h"
#include "core/storage_internal.h"
#include "core/utils.h"

namespace lsmgraph {


    
    
    
    
    
    
    


    uint32_t LazyUpdate::getItemNum(int sub_property_id) {
        return item_num[sub_property_id];
    }


    void LazyUpdate::setItemNum(int sub_property_id, uint32_t new_num) {
        item_num[sub_property_id] = new_num;
    }

    Status LazyUpdate::getData(VertexId_t src, VertexId_t dst, std::string& data, int sub_property_id) {
        try_map_file(sub_property_id);
        // Records are laid out as padded property bytes, sequence, marker,
        // property length, destination, and source.
        char *ptr = file_ptr[sub_property_id];
        int now_offset = map_size[sub_property_id];
        while(true){
            if(now_offset <= 0){
                return Status::kNotFound;
            }
            VertexId_t nsrc = get_src_from_lazy_mmp(ptr + now_offset);
            VertexId_t ndst = get_dst_from_lazy_mmp(ptr + now_offset);
            if(src != nsrc || dst != ndst){
                now_offset = next_from_lazy_mmp(ptr + now_offset, sub_property_id);
            } else {
                Marker_t marker = get_marker_from_lazy_mmp(ptr + now_offset);
                if(marker == 1){
                    return Status::kDelete;
                }
                data = get_property_from_lazy_mmp(ptr + now_offset);
                return Status::kOk;
            }
        }
    }


    bool LazyUpdate::expandSize(int sub_property_id) {
        uint32_t new_size = getItemNum(sub_property_id) * 2;
        setItemNum(sub_property_id, new_size);
        return true;
    }

    bool LazyUpdate::shrinkSize(int sub_property_id) {
        uint32_t new_size = getItemNum(sub_property_id) / 2;
        setItemNum(sub_property_id, new_size);
        return true;
    }

    bool LazyUpdate::isExpand(int sub_property_id) {
        time_t now = time(nullptr);
        double elapsed = difftime(now, creation_time);
        if (elapsed <= 0) {
            return false;
        }
        double write_rate = static_cast<double>(write_count[sub_property_id]) / elapsed;
        return write_rate > 10.0;
    }

    bool LazyUpdate::isShrink(int sub_property_id) {
        time_t now = time(nullptr);
        double elapsed = difftime(now, creation_time);
        if (elapsed <= 0) {
            return false; 
            }

        double read_rate = static_cast<double>(read_count[sub_property_id]) / elapsed;
        return read_rate > 10.0;
    }

    uint32_t LazyUpdate::getUsedItem(int sub_property_id){
        return used_item[sub_property_id];
    }
    void LazyUpdate::setUsedItem(int sub_property_id, uint32_t num){
        used_item[sub_property_id] = num;
    }

    FileId_t LazyUpdate::getFid() {
        return fid_;
    }

    void LazyUpdate::clear_data(int sub_property_id){
        close(p_fd[sub_property_id]);
        p_fd[sub_property_id] = -1;
        utils::rmfile(pLazyFileName_with_id(fid_, sub_property_id).c_str());
        used_item[sub_property_id] = 0;
        munmap(file_ptr[sub_property_id], map_size[sub_property_id]);
        file_ptr[sub_property_id] = nullptr;
        map_size[sub_property_id] = 0;
    }

    void LazyUpdate::try_map_file(int sub_property_id){
        if(p_fd[sub_property_id] == -1){
            p_fd[sub_property_id] = open(pLazyFileName_with_id(fid_, sub_property_id).data(), O_RDONLY);
            if(p_fd[sub_property_id] == -1){
                return;
            }
        }
        struct stat statbuf;
        stat(pLazyFileName_with_id(fid_, sub_property_id).data(), &statbuf);
        size_t now_file_size = statbuf.st_size;
        if(now_file_size == map_size[sub_property_id]){
            return;
        }
        if(map_size[sub_property_id] != 0){
            munmap(file_ptr[sub_property_id], map_size[sub_property_id]);
        }

        
        file_ptr[sub_property_id] = reinterpret_cast<char *>(mmap(NULL, now_file_size, PROT_READ, MAP_PRIVATE, p_fd[sub_property_id], 0));
        map_size[sub_property_id] = now_file_size;
    }

    VertexId_t LazyUpdate::get_src_from_lazy_mmp(char* ptr){
        return *(VertexId_t*)(ptr - sizeof(VertexId_t));
    }

    VertexId_t LazyUpdate::get_dst_from_lazy_mmp(char* ptr){
        return *(VertexId_t*)(ptr - 2 * sizeof(VertexId_t));
    }

    Marker_t LazyUpdate::get_marker_from_lazy_mmp(char* ptr){
        return (Marker_t)(*(int*)(ptr - sizeof(VertexId_t) * 2 - 4 - 4));
    }

    SequenceNumber_t LazyUpdate::get_seq_from_lazy_mmp(char* ptr){
        return *(SequenceNumber_t*)(ptr - sizeof(VertexId_t) * 2 - 4 - 4 - 8);
    }

    std::string LazyUpdate::get_property_from_lazy_mmp(char* ptr){
        int propertyLen = get_propertyLen_from_lazy_mmp(ptr);
        int paddingLen = (propertyLen + 3) / 4 * 4;
        std::string ans(ptr - sizeof(VertexId_t) * 2 - 4 - 4 - sizeof(SequenceNumber_t) - paddingLen, propertyLen);
        return ans;
    }

    int LazyUpdate::get_propertyLen_from_lazy_mmp(char* ptr){
        return *(int*)(ptr - sizeof(VertexId_t) * 2 - 4);
    }

    int LazyUpdate::next_from_lazy_mmp(char* ptr, int sub_property_id){
        int property_len = get_propertyLen_from_lazy_mmp(ptr);
        int ans = ptr - file_ptr[sub_property_id] - sizeof(VertexId_t) * 2 - 4 - 4 - sizeof(SequenceNumber_t) - (property_len + 3) / 4 * 4;
        return ans;
    }

    void LazyUpdate::printLazy(int sub_property_id){
        try_map_file(sub_property_id);
        char *ptr = file_ptr[sub_property_id];
        int now_offset = 0;
        std::cout<<"map_size: "<<map_size[sub_property_id]<<"--\n";
        while(now_offset < map_size[sub_property_id]){
            std::cout<<"nf:"<<now_offset<<" ";
            std::cout<<get_src_from_lazy_mmp(ptr + now_offset)<<" "
                     <<get_dst_from_lazy_mmp(ptr + now_offset)<<" "
                     <<get_propertyLen_from_lazy_mmp(ptr + now_offset)<<" "
                     <<get_marker_from_lazy_mmp(ptr + now_offset)<<" "
                     <<get_seq_from_lazy_mmp(ptr + now_offset)<<" "
                     <<get_property_from_lazy_mmp(ptr + now_offset)<<"--\n";
            now_offset = next_from_lazy_mmp(ptr + now_offset, sub_property_id);
        }
        std::cout<<"OVER!!\n";
    }
}
