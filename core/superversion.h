#pragma once
#define VM_RW_LOCK
#include <mutex>
#include <vector>
#include <atomic>
#include <set>
#include <assert.h>
#include <algorithm>
#include <map>

#include "core/MemTable.h"
#include "core/MemProperty.h"


namespace lsmgraph {

class MemTable;
class MemProperty;



struct VersionAndMemTableAndMemPropertyAndLf {
    std::vector<MemTable*> menTables;
    std::vector<MemProperty*>memProperties;
    std::map<std::pair<FileId_t, int>, std::vector<LazyFile*>>sst_has_lf;
    Version* current_;
    ~VersionAndMemTableAndMemPropertyAndLf();
    void set_vs(Version* current);

    void batch_insert_tb (std::vector<MemTable*>& _menTables);
    void insert_tb(MemTable* tb);
    void remove_tb(MemTable* tb);

    void batch_insert_pp(std::vector<MemProperty*>& _memProperties);
    void insert_pp(MemProperty* pp);
    void remove_pp(MemProperty* pp);

    void batch_insert_lf(std::map<std::pair<FileId_t, int>, std::vector<LazyFile*>>*_sst_has_lf);
    void insert_lf(FileId_t sst_fid, int propert_id, LazyFile* lf);
    void remove_lf(FileId_t sst_fid, int propert_id, LazyFile* lf);
    void remove_batch_lf(FileId_t sst_fid);
};

struct SuperVersion {
    std::shared_ptr<VersionAndMemTableAndMemPropertyAndLf> version_memtable_memproperty_lazyfile;
#ifdef VM_RW_LOCK
    std::shared_mutex vm_rw_mtx;
#endif
    MulLevelIndex findex;

    SuperVersion(): version_memtable_memproperty_lazyfile(nullptr) {}
    ~SuperVersion() {}

    Version* get_version() {
        return version_memtable_memproperty_lazyfile->current_;
    }

    std::vector<MemTable*>& get_memtable() {
        return version_memtable_memproperty_lazyfile->menTables;
    }

    std::vector<MemProperty*>& get_memproperty() {
        return version_memtable_memproperty_lazyfile->memProperties;
    }

    std::map<std::pair<FileId_t, int>, std::vector<LazyFile*>>& get_lazyfile_store() {
        return version_memtable_memproperty_lazyfile->sst_has_lf;
    }
};

}
