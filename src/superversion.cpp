#include "core/superversion.h"

namespace lsmgraph {

template<typename T>
inline void vectorDel(std::vector<T>& vec, T a) {
    auto it = std::find(vec.begin(), vec.end(), a);
    vec.erase(it);
}

VersionAndMemTableAndMemPropertyAndLf::~VersionAndMemTableAndMemPropertyAndLf(){
    current_->Unref();
    // 这里析构时才释放memtable, 可能对于长的查询任务可能造成写阻塞，因为不能及时释放
    for (auto tb : menTables) {
        tb->Unref();
    }

    for (auto pp : memProperties) {
        pp->Unref();
    }
}
void VersionAndMemTableAndMemPropertyAndLf::set_vs(Version* current) {
    current->Ref();
    current_ = current;
}
void VersionAndMemTableAndMemPropertyAndLf::batch_insert_tb (std::vector<MemTable*>& _menTables) {
    for (auto tb : _menTables) {
        tb->Ref();
        menTables.push_back(tb);
    }
}
void VersionAndMemTableAndMemPropertyAndLf::insert_tb(MemTable* tb) {
    tb->Ref();
    menTables.push_back(tb);
}
void VersionAndMemTableAndMemPropertyAndLf::remove_tb(MemTable* tb) {
    vectorDel<MemTable*>(menTables, tb);
    tb->Unref();
}
void VersionAndMemTableAndMemPropertyAndLf::batch_insert_pp(std::vector<MemProperty*>& _memProperties){
    for(auto pp : _memProperties){
        pp->Ref();
        memProperties.push_back(pp);
    }
}
void VersionAndMemTableAndMemPropertyAndLf::insert_pp(MemProperty* pp){
    pp->Ref();
    memProperties.push_back(pp);
}
void VersionAndMemTableAndMemPropertyAndLf::remove_pp(MemProperty* pp){
    vectorDel<MemProperty*>(memProperties, pp);
    pp->Unref();
}

void VersionAndMemTableAndMemPropertyAndLf::batch_insert_lf(std::map<std::pair<FileId_t, int32_t>, std::vector<LazyFile*>>*_sst_has_lf){
    for(auto [sst_fid, lf_vec] : *_sst_has_lf){
        for(auto lf : lf_vec){
            lf->Ref();
        }
        sst_has_lf.insert({sst_fid, lf_vec});
    }
}
void VersionAndMemTableAndMemPropertyAndLf::insert_lf(FileId_t sst_fid, int propert_id, LazyFile* lf){
    sst_has_lf[{sst_fid, propert_id}].push_back(lf);
}
void VersionAndMemTableAndMemPropertyAndLf::remove_lf(FileId_t sst_fid, int propert_id, LazyFile* lf){
    vectorDel(sst_has_lf[{sst_fid, propert_id}], lf);
    lf->Unref();
}

}
