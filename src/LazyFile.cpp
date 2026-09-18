#include "core/LazyFile.h"

namespace lsmgraph {
    void LazyFile::Ref() {
        refs.fetch_add(1);
    }
  
    void LazyFile::Unref() {
        assert(refs.load(std::memory_order_acquire) >= 0);
        int old_refs = refs.fetch_sub(1);
        ///
    }

    int32_t LazyFile::Getref() {
        return refs.load(std::memory_order_acquire);
    }

    void LazyFile::LoadFile (FileId_t fid) {
        std::string file_path = pLazyFileName(fid);
        file_size = GetFileSize_(file_path.data());
        {
            fd = open(file_path.c_str(), O_RDONLY);
            if (fd == -1) {
                throw std::runtime_error("open error. e_path=" + file_path);
            }
            
            file_ptr = reinterpret_cast<char *>(mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0));
            madvise(file_ptr, file_size, MADV_RANDOM); //MADV_SEQUENTIAL
        }
    }
    int LazyFile::Get_Reseted_EdgeBody_Offset(){
        return 0;
    }

    int LazyFile::Get_Reseted_Property_Offset(){
        return header.size * (8 + 8 + 4 + 4);
    }

    int LazyFile::Get_Reseted_Index_Offset(){
        return 0;
    }

    bool LazyFile::get_next_edge(int& edge_Offset, int& index_Offset, tmp_Edge& tmp_edge){
        if(edge_Offset >= indexs.back().offset){
            return false;
        }
        while(indexs[index_Offset + 1].offset <= edge_Offset){
            index_Offset ++;
        }
        int next_jump = 24;
        tmp_edge.src = indexs[index_Offset].key;
        tmp_edge.dst = *((VertexId_t *)(file_ptr + edge_Offset));
        tmp_edge.seq = *((SequenceNumber_t *)(file_ptr + (edge_Offset + 8)));
        const uint32_t marker_meta =
            *((uint32_t *)(file_ptr + (edge_Offset + 8 + 8)));
        tmp_edge.marker = ((marker_meta >> 31) & 0x1) != 0;
        tmp_edge.is_out = ((marker_meta >> 30) & 0x1) != 0;
        tmp_edge.edge_type = static_cast<uint8_t>((marker_meta >> 22) & 0xff);
        int now_property_offset = *((int *)(file_ptr + (edge_Offset + 8 + 8 + 4)));
        int next_property_offset = *((int *)(file_ptr + (edge_Offset + 8 + 8 + 4 + next_jump)));
        tmp_edge.property = std::string(file_ptr + Get_Reseted_Property_Offset() + now_property_offset, next_property_offset - now_property_offset);
        edge_Offset += next_jump;
        return true;
    }

    bool LazyFile::get_next_edge_slice(int& Offset, int& index_Offset, tmp_Edge_slice& er){
        if(Offset >= indexs.back().offset){
            return false;
        }
        while(indexs[index_Offset + 1].offset <= Offset){
            index_Offset ++;
        }
        int next_jump = 24;
        er.src = indexs[index_Offset].key;
        er.dst = *((VertexId_t *)(file_ptr + Offset));
        er.seq = *((SequenceNumber_t *)(file_ptr + (Offset + 8)));
        const uint32_t marker_meta =
            *((uint32_t *)(file_ptr + (Offset + 8 + 8)));
        er.marker = ((marker_meta >> 31) & 0x1) != 0;
        er.is_out = ((marker_meta >> 30) & 0x1) != 0;
        er.edge_type = static_cast<uint8_t>((marker_meta >> 22) & 0xff);
        int now_property_offset = *((int *)(file_ptr + (Offset + 8 + 8 + 4)));
        int next_property_offset = *((int *)(file_ptr + (Offset + 8 + 8 + 4 + next_jump)));
        er.ptr = file_ptr + Get_Reseted_Property_Offset() + now_property_offset;
        er.len = next_property_offset - now_property_offset;
        Offset += next_jump;
        return true;
    }

    int LazyFile::get(VertexId_t src){
        int l = 0;
        int r = indexs.size() - 1;
        while(l < r){
            int mid = (l + r) / 2;
            if(indexs[mid].key < src){
                l = mid + 1;
            } else {
                r = mid;
            }
        }
        if(indexs[l].key == src){
            return l;
        }
        return -1;
    }
}
