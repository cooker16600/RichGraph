#pragma once
#define VM_RW_LOCK
#include <algorithm>
#include <memory>
#include <shared_mutex>
#include <vector>

#include "core/MemTable.h"

namespace lsmgraph {

class MemTable;

struct VersionAndMemTable {
    std::vector<MemTable*> menTables;
    Version* current_ = nullptr;
    ~VersionAndMemTable();
    void set_vs(Version* current);

    void batch_insert_tb(const std::vector<MemTable*>& memtables);
    void insert_tb(MemTable* tb);
    void remove_tb(MemTable* tb);
};

struct SuperVersion {
    std::shared_ptr<VersionAndMemTable> version_memtable;
#ifdef VM_RW_LOCK
    std::shared_mutex vm_rw_mtx;
#endif
    MulLevelIndex findex;

    SuperVersion() = default;
    ~SuperVersion() = default;

    Version* get_version() {
        return version_memtable->current_;
    }

    std::vector<MemTable*>& get_memtable() {
        return version_memtable->menTables;
    }
};

}
