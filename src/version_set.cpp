#include "core/version_set.h"
#include "core/storage_internal.h"

namespace lsmgraph {

// Version
void Version::Ref() {
    refs_.fetch_add(1);
}

void Version::Unref() {
    assert(this != &belong_vset_->dummy_versions_);
    assert(refs_ >= 1);
    // lock order: version lock -> VersionSet lock
    int32_t previous_refs = refs_.fetch_sub(1);
    assert(previous_refs >= 1);
    if (previous_refs > 1) {
        return;
    }
    for(auto meta: l0_filemetas_) {
        meta->Unref();
    }

  #ifdef MULTI_LEVEL_VERSION
    for (auto meta : down_level_filemetas_) {
      meta->Unref();
    }
  #endif

    belong_vset_->DeleteVersion(this);
    delete this;
}

Status Version::GetEdge(VertexId_t src, VertexId_t dst, std::string* property) {
    (void)src;
    (void)dst;
    (void)property;
    // 即在belong_vset_中传入需要的内容

    return Status::kNotFound;
}

std::vector<SSTableCache*>* Version::GetLevel0Files() {
    return &l0_filemetas_;
}

void Version::PrintFilesMetaInfo() {
    std::cout << " file_num=" << l0_filemetas_.size()
              << std::endl;
    for (auto it : l0_filemetas_) {
        std::cout << " fid=" << it->header.timeStamp
                  << " min_key=" << it->header.minKey
                  << " max_key=" << it->header.maxKey
                  << " ref=" << it->Getref()
                  << std::endl;
    }
  #ifdef MULTI_LEVEL_VERSION
    std::cout << "down level version\n";
    std::cout << " file_num=" << down_level_filemetas_.size()
              << std::endl;
    for (auto it : down_level_filemetas_) {
      std::cout << " fid=" << it->header.timeStamp
                << " min_key=" << it->header.minKey
                << " max_key=" << it->header.maxKey
                << " ref=" << it->Getref()
                << std::endl;
    }
  #endif

}


// VersionSet
VersionSet::VersionSet(std::mutex& version_mu)
                            : dummy_versions_(this),
                              current_(nullptr),
                              version_mu_(version_mu) {
    AppendVersion(new Version(this));
}

VersionSet::~VersionSet() {
    if (FLAGS_richgraph_verbose) {
        std::cout << "~VersionSet" << std::endl;
    }
    current_->Unref();
    assert(dummy_versions_.next_ == &dummy_versions_);  // List must be empty
}

Version* VersionSet::GetCurrent() {
    return current_;
}

void VersionSet::AppendVersion(Version *v) {


    assert(v->refs_ == 0);
    assert(v != current_);
    if (current_ != nullptr) {
        current_->Unref();
    }
    current_ = v;
    v->Ref();

    // Append to linked list
    mu_.lock();
    v->next_ = dummy_versions_.next_;
    dummy_versions_.next_->prev_ = v;
    dummy_versions_.next_ = v;
    v->prev_ = &dummy_versions_;
    mu_.unlock();

}

void VersionSet::DeleteVersion(Version *v) {
    mu_.lock();
    v->prev_->next_ = v->next_;
    v->next_->prev_ = v->prev_;
    mu_.unlock();
}

void VersionSet::VersionLock() {
    version_mu_.lock();
}

void VersionSet::VersionUnLock() {
    version_mu_.unlock();
}

bool VersionSet::LogAndApply(VersionEdit& version_edit, Version *v) {

    int reserve_size = current_->l0_filemetas_.size()
                     + version_edit.new_files_.size()
                     - version_edit.deleted_files_.size();
    if (reserve_size > 0) {
        v->l0_filemetas_.reserve(reserve_size);
    }

    for (auto filemeta : current_->l0_filemetas_) {
        if (version_edit.deleted_files_.count(
            filemeta->header.timeStamp) == 0) {
          filemeta->Ref();
          v->l0_filemetas_.push_back(filemeta);
        }
    }

  #ifdef MULTI_LEVEL_VERSION
    for (auto filemeta : current_->down_level_filemetas_) {
      if (version_edit.deleted_files_.count(filemeta->header.timeStamp) == 0) {
        filemeta->Ref();
        v->down_level_filemetas_.push_back(filemeta);
      }
    }
  #endif


    for (auto filemeta : version_edit.new_files_) {
        if (version_edit.deleted_files_.count(
            filemeta->header.timeStamp) == 0) {
          filemeta->Ref();
          v->l0_filemetas_.push_back(filemeta);
        }
    }

  #ifdef MULTI_LEVEL_VERSION
    for (auto filemeta : version_edit.down_level_new_files_) {
      if (version_edit.deleted_files_.count(
              filemeta->header.timeStamp) == 0) {
        filemeta->Ref();
        v->down_level_filemetas_.push_back(filemeta);
      }
    }
  #endif

    if (v->l0_filemetas_.size() > 1) {
        std::sort(v->l0_filemetas_.begin(), v->l0_filemetas_.end(),
                cacheTimeCompare);
    }

    AppendVersion(v); // 切换新版本 (current_ --> v)
    version_edit.Clear();


    return true;
}


} // lsmgraph
