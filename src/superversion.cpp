#include "core/superversion.h"

namespace lsmgraph {

template<typename T>
inline void vectorDel(std::vector<T>& vec, T a) {
    auto it = std::find(vec.begin(), vec.end(), a);
    if (it != vec.end()) {
        vec.erase(it);
    }
}

VersionAndMemTable::~VersionAndMemTable(){
    if (current_ != nullptr) {
        current_->Unref();
    }
    for (auto tb : menTables) {
        tb->Unref();
    }
}
void VersionAndMemTable::set_vs(Version* current) {
    current->Ref();
    current_ = current;
}
void VersionAndMemTable::batch_insert_tb(
    const std::vector<MemTable*>& memtables) {
    for (auto tb : memtables) {
        tb->Ref();
        menTables.push_back(tb);
    }
}
void VersionAndMemTable::insert_tb(MemTable* tb) {
    tb->Ref();
    menTables.push_back(tb);
}
void VersionAndMemTable::remove_tb(MemTable* tb) {
    vectorDel<MemTable*>(menTables, tb);
    tb->Unref();
}

}
