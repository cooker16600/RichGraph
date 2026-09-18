#pragma once
#include <mutex>
#include <vector>
#include <atomic>
#include <set>
#include <assert.h>
#include <algorithm>
#include "core/SSTable.h"
#include "core/utils.h"

// 这个用于控制verion_set集合记录的层数
// 默认关闭时, 由于使用了level-index,仅仅需要记录level-0的file_id set即可
// 如果不使用level-index，则需要开启本宏，来记录每一级的fild_id，以规范可读取的范围

namespace lsmgraph {

class Version;
class VersionSet;

// 本质上是level-0的当前文件的集合
class Version {
public:
    Version *next_;
    Version *prev_;
    VersionSet *belong_vset_;
    std::atomic<int32_t> refs_;
    std::vector<SSTableCache*> l0_filemetas_;

  #ifdef MULTI_LEVEL_VERSION
    std::vector<SSTableCache*> down_level_filemetas_;
  #endif

    Version(VersionSet *vset)
            : belong_vset_(vset),
              next_(this),
              prev_(this),
              refs_(0) {}

    ~Version() {
        assert(refs_ <= 0);
    }

    void Ref();

    void Unref();

    Status GetEdge(VertexId_t src, VertexId_t dst,
                            std::string* property);
    std::vector<SSTableCache*>* GetLevel0Files();

    void PrintFilesMetaInfo();

};

class VersionEdit {
public:
    VersionEdit() { Clear(); }
    ~VersionEdit() = default;

    void Clear() {
        new_files_.clear();
        deleted_files_.clear();
    }

    // Add the specified file at the specified number.
    // REQUIRES: This version has not been saved (see VersionSet::SaveTo)
    void AddFile(SSTableCache* f) {
        new_files_.push_back(f);
    }

  #ifdef MULTI_LEVEL_VERSION
    void AddDownLevelFile(SSTableCache* f) {
      down_level_new_files_.push_back(f);
    }
  #endif

    // Delete the specified "file" from level-0.
    void RemoveFile(FileId_t fid) {
        deleted_files_.insert(fid);
    }

private:
    friend class VersionSet;
    std::vector<SSTableCache*> new_files_;
#ifdef MULTI_LEVEL_VERSION
    std::vector<SSTableCache*> down_level_new_files_;
#endif
    std::set<FileId_t> deleted_files_;
};

class VersionSet {
public:

    VersionSet(std::mutex& version_mu_);
    VersionSet(const VersionSet&) = delete;
    VersionSet& operator=(const VersionSet&) = delete;
    Version* GetCurrent();
    void AppendVersion(Version *v);
    void DeleteVersion(Version *v);

    bool LogAndApply(VersionEdit& version_edit, Version *v);

    void VersionLock();
    void VersionUnLock();

    ~VersionSet();

private:
    friend class Version;
    Version* current_; // == dummy_versions_.prev_
    Version dummy_versions_; // 链表头结点 // 可以整成并发链表，通过原子操作进行添加和删除
    std::mutex mu_;
    std::mutex& version_mu_; // level_0_mutex
#ifdef MULTI_LEVEL_VERSION
    size_t size_{0};
#endif

};


}
