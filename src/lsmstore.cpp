#include "core/lsmstore.h"
#include "core/storage_internal.h"
#include "src/MemTable.cpp"
#include "edge_iterator.h"
#include "core/flags.h"
#include "core/fixed_property_layout.h"
#include "util/atomic.hpp"
#include "io/file_io.h"
#include <chrono>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <tuple>
#include <array>
#include <unordered_set>
#include <shared_mutex>

namespace lsmgraph {

// 初始化静态 thread_local 成员变量
thread_local SequenceNumber_t LSMStore::local_version_id_ = -1;
thread_local SuperVersion LSMStore::local_sv_ = SuperVersion();
thread_local std::unordered_set<VertexId_t> EdgeIteratorTraverseOpt::threadLocalSet;

double query_time_mem = 0;
double query_time_file = 0;
double query_time_find_index = 0;
double query_time_find_edge = 0;
double query_time_find_property = 0;
double find_all_iterator = 0;
double compaction_time = 0;
double get_curr_version_time = 0;
double iterate_memtable = 0; // level-0 find
double iterate_level_0 = 0; // level-0 find
double iterate_level_1 = 0; // level-0 find
double iterate_find_first = 0; // level-0 find
double iterate_check_entry_valid = 0; // check_entry_valid
double put_memtable_time_per = -1; // insert to memtable time of each time
double put_memtable_time_sum = 0; // insert to memtable time
double LF_get = 0;
double LF_find_lo = 0;
double LF_construct = 0;
double mp_iter = 0; //mp构建
double mt_iter = 0; //mt构建
double lv0_iter = 0; //0层sst构建
double high_lv_iter = 0;
double find_first = 0;
double LF_total = 0; //LF花费的总时间
double map_check = 0;
double get_store = 0;
double for_iter = 0; //for遍历时间
double sst_get_ep = 0;//sst_iter中获取属性
double sst_get_offset = 0;//sst 中获取偏移量
double cast_time = 0;
double cul_time = 0;
double get_value_time = 0;
double head_time = 0;
double sst_dst = 0;
int block_cnt_ = 0;

namespace {

constexpr size_t kVertexLockShrinkFactor = 4;
constexpr uint64_t kFileInfoSequenceMagic = 0x5249434853455131ULL;

SequenceNumber_t ScanSstNextTopologySequence(
    SSTableCache* cache,
    SSTDataManager& data_manager) {
  if (cache == nullptr || cache->header.size <= 1) {
    return 0;
  }
  SSTDataCache* data = data_manager.get_data(cache->header.timeStamp);
  SequenceNumber_t newest = -1;
  const uint64_t edge_count = cache->header.size - 1;  // omit sentinel
  for (uint64_t i = 0; i < edge_count; ++i) {
    newest = std::max(
        newest,
        get_seq(data->GetEdgeData() + i * EDGEBODY_SIZE));
  }
  cache->newest_edge = std::max<SequenceNumber_t>(0, newest);
  data->newest_edge = cache->newest_edge;
  return newest < 0 ? 0 : newest + 1;
}

void RestoreConservativeNewestEdge(SSTableCache* cache,
                                   SSTDataManager& data_manager,
                                   SequenceNumber_t next_sequence) {
  if (cache == nullptr) return;
  const SequenceNumber_t newest = next_sequence == 0 ? 0 : next_sequence - 1;
  cache->newest_edge = newest;
  data_manager.get_data(cache->header.timeStamp)->newest_edge = newest;
}

size_t ComputeVertexLockCount(size_t max_vertex_num) {
  return std::max<size_t>(
      1, (max_vertex_num + kVertexLockShrinkFactor - 1) /
             kVertexLockShrinkFactor);
}

inline size_t VertexLockSlot(VertexId_t vid, size_t lock_count) {
  return lock_count == 0 ? 0 : static_cast<size_t>(vid % lock_count);
}

void SetPropertyDeltaError(std::string* error, std::string message) {
  if (error != nullptr) *error = std::move(message);
}

std::string PropertyDeltaErrno(const char* operation,
                               const std::string& path) {
  return std::string(operation) + " " + path + ": " + std::strerror(errno);
}

bool SyncPropertyDeltaParent(const std::string& path, std::string* error) {
  const auto parent = std::filesystem::path(path).parent_path();
  const std::string directory = parent.empty() ? "." : parent.string();
  const int fd = ::open(directory.c_str(), O_RDONLY | O_DIRECTORY);
  if (fd < 0) {
    SetPropertyDeltaError(error, PropertyDeltaErrno("open directory", directory));
    return false;
  }
  const bool ok = ::fsync(fd) == 0;
  if (!ok) {
    SetPropertyDeltaError(error,
                          PropertyDeltaErrno("fsync directory", directory));
  }
  ::close(fd);
  return ok;
}

struct EdgeLookupResult {
  lsmgraph::Status status = lsmgraph::Status::kNotFound;
  int body_offset = -1;
};

std::string ExtractSubProperty(std::string_view payload, int property_id) {
  if (property_id < 0) return std::string(payload);
  int current = 0;
  size_t begin = 0;
  for (size_t i = 0; i <= payload.size(); ++i) {
    if (i == payload.size() || payload[i] == '|') {
      if (current == property_id) {
        return std::string(payload.substr(begin, i - begin));
      }
      ++current;
      begin = i + 1;
    }
  }
  return {};
}

// 在 [low, high)（按 EDGEBODY_SIZE 对齐）范围内，
// 查找满足 (dst, is_out, edge_type) 的“最新”版本（同一 dst 内按 seq 从新到旧）。
inline EdgeLookupResult FindEdgeByDstDirectionAndType(const char* body_buffer,
                                                      int low,
                                                      int high,
                                                      lsmgraph::VertexId_t dst,
                                                      bool is_out,
                                                      uint8_t edge_type) {
  EdgeLookupResult result;
  const int step = static_cast<int>(EDGEBODY_SIZE);
  if (low >= high) {
    return result;
  }

  int left = low;
  int right = high;
  while (left < right) {
    const int mid = left + (right - left) / 2 / step * step;
    const auto* body =
        reinterpret_cast<const lsmgraph::EdgeBody_t*>(body_buffer + mid);
    if (body->get_dst() >= dst) {
      right = mid;
    } else {
      left = mid + step;
    }
  }

  for (int pos = left; pos < high; pos += step) {
    const auto* body =
        reinterpret_cast<const lsmgraph::EdgeBody_t*>(body_buffer + pos);
    const auto now_dst = body->get_dst();
    if (now_dst != dst) {
      break;
    }
    if (body->get_is_out() != is_out || body->get_edge_type() != edge_type) {
      continue;
    }
    result.body_offset = pos;
    result.status = body->get_marker() ? lsmgraph::Status::kDelete
                                       : lsmgraph::Status::kOk;
    return result;
  }
  return result;
}

struct CSRSelectedEdgeRef {
  lsmgraph::VertexId_t dst = 0;
  lsmgraph::SequenceNumber_t seq = 0;
  bool is_out = true;
  uint8_t edge_type = 0;
  lsmgraph::SSTDataCache* sst_data = nullptr;
  uint32_t body_offset = 0;
};

struct CSRSourceFileView {
  lsmgraph::SSTableCache* cache = nullptr;
  lsmgraph::SSTDataCache* sst_data = nullptr;
};

inline bool CSRSelectedEdgeRefLess(const CSRSelectedEdgeRef& a,
                                   const CSRSelectedEdgeRef& b) {
  if (a.dst != b.dst) {
    return a.dst < b.dst;
  }
  if (a.seq != b.seq) {
    return a.seq > b.seq;
  }
  if (a.is_out != b.is_out) {
    return a.is_out > b.is_out;
  }
  return a.edge_type < b.edge_type;
}

}  // namespace

LSMStore::CSRReadSnapshot::~CSRReadSnapshot() {
  for (auto* cache : active_files) {
    if (cache != nullptr) {
      cache->Unref();
    }
  }
  active_files.clear();
  src_index.clear();
}

std::shared_ptr<const LSMStore::CSRReadSnapshot>
LSMStore::LoadCSRReadSnapshot() const {
  return std::atomic_load(&csr_read_snapshot_);
}

const LSMStore::CSRReadIndexEntry*
LSMStore::FindCSRIndexEntry(const CSRReadSnapshot* snapshot,
                            VertexId_t src) const {
  if (snapshot == nullptr) {
    return nullptr;
  }
  if (src >= snapshot->src_index.size()) {
    return nullptr;
  }
  const auto& entry = snapshot->src_index[static_cast<size_t>(src)];
  if (entry.cache == nullptr || entry.s_offset >= entry.e_offset) {
    return nullptr;
  }
  return &entry;
}

LSMStore::LSMStore(const std::string &dir, const size_t max_vertex_num,
                   int thread_num, int memtable_num,
                   std::vector<uint32_t> sub_property_lengths,
                   size_t memtable_size,
                   bool is_csr,
                   uint32_t csr_l0_max_sst_num,
                   uint32_t csr_l1_max_sst_num,
                   PropertyObjectKind property_object_kind,
                   uint32_t property_shard_id,
                   PropertyUpdateOptions property_update_options)
                   : worker_pool(thread_num),
                     vertex_id_(0),
                     memtabl_state_cv_(&mu_),
                     max_vertex_num_(max_vertex_num),
                     memtable_size_(std::max<size_t>(1, memtable_size)),
                     block_manager_(FLAGS_mmap_path),
                     l0_versionset_(new VersionSet(level_0_mux_)),
                     sub_property_lengths_(
                         sub_property_lengths.empty()
                             ? BuildUniformSubPropertyLengths(
                                   static_cast<int>(FLAGS_sub_property_num),
                                   FLAGS_max_property_length)
                             : std::move(sub_property_lengths)),
                     property_object_kind_(property_object_kind),
                     property_shard_id_(property_shard_id),
                     property_update_options_(property_update_options),
                     compactor_(fileMetaCache, dataDir, currentTime,
                                &level_0_mux_, l0_versionset_,
                                global_version_id_,
                                sstdata_manager_,
                                del_record_manager_,
                                sv_,
                                sub_property_lengths_),
                     use_csr_disk_(FLAGS_use_csr_disk),
                     is_csr_(is_csr),
                     csr_l0_max_sst_num_(
                         std::max<uint32_t>(1, csr_l0_max_sst_num)),
                     csr_l1_max_sst_num_(
                         std::max<uint32_t>(1, csr_l1_max_sst_num)) {
    const ScopedFixedPropertyLayout property_layout(&sub_property_lengths_);
    compactor_.SetL0AutoCompactionLimit(is_csr_ ? csr_l0_max_sst_num_ : 4);
    if (is_csr_) {
      compactor_.SetL1AutoCompactionLimit(csr_l1_max_sst_num_);
    }

    if (!dir.empty() && dir.back() == '/')
      dataDir = dir.substr(0, dir.length() - 1);
    else
      dataDir = dir;
    // Several legacy helpers still derive topology/property filenames through
    // otherwise reopening a multi-shard GraphDb reads from the process-wide
    // FLAGS_db_path instead of this store's directory.
    const ScopedDbPathOverride db_path_override(&dataDir);
    currentTime = 0;
    global_seq.store(0, std::memory_order_relaxed);

    if (FLAGS_LOAD_OLD_DATA == false) {
      if (FLAGS_richgraph_verbose) {
        std::cout << " *clean up the old sstable file..." << std::endl;
      }
      // clean up the old sstable file
      if (utils::dirExists(dataDir)) {
        int s = utils::rmdir(dataDir.c_str());
        assert(s != -1);
        if (FLAGS_richgraph_verbose) {
          std::cout << " delete: " << dataDir << " " << s << " file(s)."
                    << std::endl;
        }
      }
      utils::mkdir(dataDir.c_str());
      if (FLAGS_richgraph_verbose) {
        std::cout << " save dataDir=" << dataDir << std::endl;
      }
      assert(utils::dirExists(dataDir));
      utils::mkdir((dataDir).c_str());
    }

    // init lock
    vertex_lock_count_ = ComputeVertexLockCount(max_vertex_num_);
    auto futex_allocater =
        std::allocator_traits<decltype(array_allocator)>::rebind_alloc<Futex>(
            array_allocator);
    vertex_futexes_ = futex_allocater.allocate(vertex_lock_count_);
    for (size_t lock_id = 0; lock_id < vertex_lock_count_; ++lock_id) {
      std::allocator_traits<decltype(futex_allocater)>::construct(
          futex_allocater, vertex_futexes_ + lock_id);
    }

    auto rwlock_allocater = std::allocator_traits<
        decltype(array_allocator)>::rebind_alloc<RWLock_t>(array_allocator);
    vertex_rwlocks_ = rwlock_allocater.allocate(vertex_lock_count_);
    for (size_t lock_id = 0; lock_id < vertex_lock_count_; ++lock_id) {
      std::allocator_traits<decltype(rwlock_allocater)>::construct(
          rwlock_allocater, vertex_rwlocks_ + lock_id);
    }
    if (FLAGS_richgraph_verbose) {
      std::cout << " vertex_lock_count=" << vertex_lock_count_
                << " shrink_factor=" << kVertexLockShrinkFactor << std::endl;
    }

    auto vertex_max_level_allocater =
        std::allocator_traits<decltype(array_allocator)>::rebind_alloc<Level_t>(
            array_allocator);
    vertex_max_level_ = vertex_max_level_allocater.allocate(max_vertex_num_);

    // init level_index
    if (FLAGS_support_mulversion == false) {
      size_t real_vector_size = LEVEL_INDEX_SIZE * max_vertex_num_;
      auto levelIndex_allocater = std::allocator_traits<
          decltype(array_allocator)>::rebind_alloc<LevelIndex>(array_allocator);
      vid_to_levelIndex_ = levelIndex_allocater.allocate(real_vector_size);
      if (FLAGS_richgraph_verbose) {
        printf(" levelIndex space: %.2fGB\n",
               (real_vector_size * sizeof(LevelIndex))
               /1024.0/1024/1024);
      }
    } else {
      VertexId_t tmp_max_vertex_num = MMAP_INITIAL_SIZE >> 6; 
      if (FLAGS_LOAD_OLD_DATA == true) {
        std::string file_info_path = dataDir + "/file.info";
        std::ifstream file(file_info_path, std::ios::binary);
        file.read((char*)&tmp_max_vertex_num, sizeof(tmp_max_vertex_num));
        file.close();
        if (FLAGS_richgraph_verbose) {
          std::cout << " tmp_max_vertex_num=" << tmp_max_vertex_num
                    << std::endl;
        }
      }

      size_t real_vector_size = max_vertex_num_;
      #ifdef MMAP_LEVEL_INDEX
      vid_to_mullevelIndex_ 
          = MulLevelIndexSharedArray{dataDir, tmp_max_vertex_num}; // mmap size = vertex_num * 64B
      #elif defined(MMAP_DIFF_SIZE_LEVEL_INDEX)
      vid_to_mullevelIndex_ 
          = MulLevelIndexArrayWrapper{dataDir, tmp_max_vertex_num};
      #elif defined(MMAP_COLD_HOT_LEVEL_INDEX)
      vid_to_mullevelIndex_ 
          = MulLevelIndexWrapper{dataDir, tmp_max_vertex_num};
      #else
      vid_to_mullevelIndex_.reset(real_vector_size);
      #endif
      if (FLAGS_richgraph_verbose) {
        printf(" levelIndex logical space: %.2fGB\n",
               (real_vector_size * sizeof(MulLevelIndex))
               /1024.0/1024/1024);
      }
    }
    compactor_.init(vertex_futexes_, vid_to_levelIndex_,
                    vertex_rwlocks_,
                    vertex_max_level_,
                    vertex_lock_count_,
#if defined(MMAP_DIFF_SIZE_LEVEL_INDEX) || defined(MMAP_COLD_HOT_LEVEL_INDEX)
                    &vid_to_mullevelIndex_
#elif defined(MMAP_LEVEL_INDEX)
                    vid_to_mullevelIndex_
#else
                    &vid_to_mullevelIndex_
#endif
                    );

    // init memtable
    memTable_list_.reserve(memtable_num);
    for (int i = 0; i < memtable_num; i++) {
      MemTable* tb = new MemTable(level_0_mux_, max_vertex_num, vertex_id_,
                                    vertex_futexes_,
                                    vertex_lock_count_,
                                    vertex_max_level_,
                                    l0_versionset_,
                                    compactor_, sv_, sstdata_manager_,
                                    del_record_manager_,
                                    global_version_id_,
                                    memtable_size_);
      memTable_list_.emplace_back(tb);
      free_menTables.push(tb);
    }
    MemTable* mem = get_newmemTable();
    memTable_.store(mem, std::memory_order_release);
    mem->SetLive(true);
    mem->SetFid(__sync_fetch_and_add(&currentTime, 1));
    mem->SetStartTime(
      automic_get_global_seq(mem->GetMaxEdgeNum()));
    
    // init superversion
    l0_versionset_->VersionLock();
    std::shared_ptr<VersionAndMemTable> new_vms
        = std::make_shared<VersionAndMemTable>();
    new_vms->set_vs(l0_versionset_->GetCurrent());
    new_vms->insert_tb(mem);
    #ifndef VM_RW_LOCK
    std::atomic_store(&sv_.version_memtable, new_vm);
    #else
    {
      std::unique_lock w_lock(sv_.vm_rw_mtx);
      sv_.version_memtable = new_vms;
    }
    #endif
    global_version_id_.fetch_add(1, std::memory_order_acquire);
    l0_versionset_->VersionUnLock();

    // init fileMetaCache
    fileMetaCache.reserve(MAX_LEVEL);
    for (int i = 0; i < MAX_LEVEL; i++) {
      fileMetaCache.push_back(new std::vector<SSTableCache*>());
    }

    // load old data for db
    if (FLAGS_LOAD_OLD_DATA == true) {
      if (FLAGS_richgraph_verbose) {
        std::cout << " *load fileMetaCache from existed SSTables..."
                  << std::endl;
      }

      std::string file_info_path = dataDir + "/file.info";
      std::ifstream file(file_info_path, std::ios::binary);
      std::vector<SSTableCache*> recovered_caches;
      SequenceNumber_t recovered_next_sequence = 0;

      // load fileMetaCache from existed SSTables
      if (file) {
        int levelNum = 0;
        int level_file_num = 0;
        uint64_t fid = 0;
        VertexId_t max_vertex_num = 0;
        file.read((char*)&max_vertex_num, sizeof(max_vertex_num));
        file.read((char*)&levelNum, sizeof(levelNum));
        // The first field is the next allocatable vertex id.  Restore all
        // per-vertex state before publishing recovered SST metadata.
        InitVerticesUpTo(max_vertex_num);
        if (FLAGS_richgraph_verbose) {
          std::cout << " levelNum=" << levelNum << std::endl;
        }

        if (levelNum > 0) {
          for(int level = 0; level < levelNum; ++level) {
            file.read((char*)&level_file_num, sizeof(level_file_num));
            // check if the level directory exists
            if (level_file_num > 0) {
              for(int j = 0; j < level_file_num; ++j) {
                file.read((char*)&fid, sizeof(uint64_t));
                std::string tableName = std::to_string(fid) + ".sst";
                if (FLAGS_richgraph_verbose) {
                  std::cout << " load level=" << level
                            << " file=" << (dataDir + "/" + tableName)
                            << std::endl;
                }
                SSTableCache* curCache = new SSTableCache(dataDir + "/"
                                          + tableName, sstdata_manager_);
                uint64_t curTime = (curCache->header).timeStamp;
                if (level == 0) {
                  for (size_t index_id = 0;
                       index_id + 1 < curCache->indexes.size();
                       ++index_id) {
                    const VertexId_t vertex = curCache->indexes[index_id].key;
                    if (vertex < max_vertex_num_) {
                      write_max(&vertex_max_level_[vertex], Level_t(1));
                    }
                  }
                }
                if (level >= 1) {
                  // build file levelindex
                  for(int32_t i = 0; i < curCache->indexes.size() - 1; ++i) {
                    const VertexId_t vertex = curCache->indexes[i].key;
                    if (vertex < max_vertex_num_) {
                      write_max(&vertex_max_level_[vertex],
                                static_cast<Level_t>(level + 1));
                    }
                    // 需要重置level层的索引为无效值，并更新level+1层的索引
                    if (FLAGS_support_mulversion== false) {
                      int index_id = curCache->indexes[i].key * LEVEL_INDEX_SIZE
                                  + level - 1;
                      LevelIndex& findex = vid_to_levelIndex_[index_id];
                      uint64_t fid = curCache->header.timeStamp;
                      // 需要加锁
                      findex.set_fileID(fid);
                      findex.set_offset(curCache->indexes[i].offset);
                      findex.set_next_offset(curCache->indexes[i+1].offset);
                    } else {
                      int index_id = curCache->indexes[i].key;
#if defined(MMAP_DIFF_SIZE_LEVEL_INDEX) || defined(MMAP_COLD_HOT_LEVEL_INDEX) || defined(MMAP_LEVEL_INDEX)

#else 
                      MulLevelIndex& findex = vid_to_mullevelIndex_[index_id];
                      uint64_t fid = curCache->header.timeStamp;
                      // 需要加锁
                      findex.set_fileID(level - 1, fid);
                      findex.set_offset(level - 1,
                                        curCache->indexes[i].offset);
                      findex.set_next_offset(level - 1,
                                             curCache->indexes[i+1].offset);
#endif
                    }
                  }
                  vector<Index>().swap(curCache->indexes);
                }
                fileMetaCache[level]->push_back(curCache);
                recovered_caches.push_back(curCache);
                if(curTime > currentTime)
                  currentTime = curTime;
              }
              if (level == 0) {
                // make sure the timeStamp of cache is decending
                std::sort(fileMetaCache[level]->begin(),
                          fileMetaCache[level]->end(),
                          cacheTimeCompare);
              } else {
                std::sort(fileMetaCache[level]->begin(),
                          fileMetaCache[level]->end(),
                          cacheKeyCompare);
              }
            }
          }

          uint64_t sequence_magic = 0;
          SequenceNumber_t persisted_next_sequence = 0;
          file.read(reinterpret_cast<char*>(&sequence_magic),
                    sizeof(sequence_magic));
          file.read(reinterpret_cast<char*>(&persisted_next_sequence),
                    sizeof(persisted_next_sequence));
          if (file && sequence_magic == kFileInfoSequenceMagic &&
              persisted_next_sequence >= 0) {
            recovered_next_sequence = persisted_next_sequence;
            for (auto* cache : recovered_caches) {
              RestoreConservativeNewestEdge(
                  cache, sstdata_manager_, recovered_next_sequence);
            }
          } else {
            if (!recovered_caches.empty()) {
              std::cerr
                  << "[RECOVERY] legacy file.info has no topology sequence; "
                  << "scanning SST edge bodies once: " << dataDir
                  << std::endl;
            }
            for (auto* cache : recovered_caches) {
              recovered_next_sequence = std::max(
                  recovered_next_sequence,
                  ScanSstNextTopologySequence(cache, sstdata_manager_));
            }
          }
        } else {
          utils::mkdir((dataDir).c_str());
        }
        file.close();
      } else {
        utils::mkdir((dataDir).c_str());
      }
      currentTime++;
      next_topology_sequence_.store(recovered_next_sequence,
                                    std::memory_order_release);
      global_seq.store(recovered_next_sequence, std::memory_order_relaxed);

      // The active buffers were allocated before recovery and initially used
      // fid/sequence zero. They are still empty, so move them beyond all
      // recovered identifiers before publishing any new write.
      mem->SetFid(__sync_fetch_and_add(&currentTime, 1));
      mem->SetStartTime(automic_get_global_seq(mem->GetMaxEdgeNum()));
      if (FLAGS_richgraph_verbose) {
        std::cout << "finish load head and index of each SSTableCache"
                  << std::endl;
      }

      // Rebuild the published level-0 Version from the recovered metadata.
      // Loading fileMetaCache alone is insufficient in multi-version mode:
      // point reads and iterators acquire their SST list through SuperVersion.
      if (FLAGS_support_mulversion && !fileMetaCache[0]->empty()) {
        auto* recovered_level_zero = fileMetaCache[0];
        VersionEdit edit;
        for (auto* cache : *recovered_level_zero) {
          edit.AddFile(cache);
        }

        auto* recovered_version = new Version(l0_versionset_);
        l0_versionset_->VersionLock();
        l0_versionset_->LogAndApply(edit, recovered_version);
        fileMetaCache[0] =
            l0_versionset_->GetCurrent()->GetLevel0Files();

        std::shared_ptr<VersionAndMemTable> old_view;
        {
          std::shared_lock lock(sv_.vm_rw_mtx);
          old_view = sv_.version_memtable;
        }
        auto new_view =
            std::make_shared<VersionAndMemTable>();
        new_view->batch_insert_tb(old_view->menTables);
        new_view->set_vs(l0_versionset_->GetCurrent());
        {
          std::unique_lock lock(sv_.vm_rw_mtx);
          sv_.version_memtable = std::move(new_view);
        }
        global_version_id_.fetch_add(1, std::memory_order_release);
        l0_versionset_->VersionUnLock();

        // The recovered caches remain owned by their base reference and the
        // published Version.  Only the temporary vector container is obsolete.
        delete recovered_level_zero;
      }
    }

    if (FLAGS_richgraph_verbose) {
      // Legacy diagnostic dump, intentionally opt-in for library users.
      std::cout << "-------------------------config------------------------------"
              << std::endl;
    std::cout << "MAX_TABLE_SIZE=" << MAX_TABLE_SIZE << std::endl;
    std::cout << "MAX_LEVEL=" << MAX_LEVEL << std::endl;
    std::cout << "BODY_BUFFER_SIZE=" << BODY_BUFFER_SIZE << std::endl;
    std::cout << "PROPERTY_BUFFER_SIZE=" << PROPERTY_BUFFER_SIZE << std::endl;
    std::cout << "MAX_EFILE_SiZE=" << MAX_EFILE_SiZE << std::endl;
    std::cout << "MAX_INDEX_NUM=" << MAX_INDEX_NUM << std::endl;
    std::cout << "LEVEL_FILE_RATIO=" << LEVEL_FILE_RATIO << std::endl;
    // Iterate over all GFlags variables and output their names and values
    std::vector<google::CommandLineFlagInfo> all_flags;
    GetAllFlags(&all_flags);
    for (const gflags::CommandLineFlagInfo& flag_info : all_flags) {
        std::cout << "FLAGS_" << flag_info.name
                  << "=" << flag_info.current_value << std::endl;
    }
    std::cout << "-------------------------------------------------------------"
              << std::endl;
    std::cout << "--------------------size-------------------------------------"
              << std::endl;
    std::cout << " sizeof(Futex)=" << sizeof(Futex) << std::endl;
    std::cout << " sizeof(RWLock_t)=" << sizeof(RWLock_t) << std::endl;
    std::cout << " sizeof(LevelIndex)=" << sizeof(LevelIndex) << std::endl;
    std::cout << " sizeof(MulLevelIndex)=" << sizeof(MulLevelIndex) << std::endl;
    std::cout << " sizeof(SSTableCache)=" << sizeof(SSTableCache) << std::endl;
    std::cout << " sizeof(Edge)=" << sizeof(Edge) << std::endl;
    std::cout << " sizeof(NeighBors)=" << sizeof(NeighBors) << std::endl;
      std::cout << "-------------------------------------------------------------"
                << std::endl;
    }

    if (property_update_options_.enabled) {
      std::string error;
      property_delta_store_ = PropertyDeltaStore::Open(
          dataDir + "/property-delta",
          property_update_options_.delta_chain_merge_threshold,
          property_update_options_.durability,
          [this](const PropertyDeltaTarget& target,
                 const std::shared_ptr<const MappedPropertyFile>& current_base,
                 const std::vector<std::shared_ptr<PropertyDeltaFile>>& deltas,
                 uint64_t output_generation,
                 std::shared_ptr<MappedPropertyFile>* merged_base,
                 std::string* merge_error) {
            return MergePropertyDeltaColumn(target,
                                            current_base,
                                            deltas,
                                            output_generation,
                                            merged_base,
                                            merge_error);
          },
          &error);
      if (property_delta_store_ == nullptr) {
        throw std::runtime_error("open property delta store: " + error);
      }
      compactor_.SetPreparationCallback(
          [this](std::string* error) {
            return PreparePropertyDeltasForCompaction(error);
          });
    }

    // CSR 模式下启用专用重建线程：
    // 1) flush 线程仅投递重建请求，不做重活；
    // 2) worker 串行重建，避免重建请求丢失；
    // 3) 启动后先投递一次请求，构建初始 CSR 视图。
    if (use_csr_disk_) {
      csr_rebuild_worker_ = std::thread(&LSMStore::CSRRebuildWorkerMain, this);
      MaybeRebuildCSRFromDisk();
    }
}

PropertyDeltaTarget LSMStore::MakePropertyDeltaTarget(
    FileId_t base_file_id, int property_id) const {
  PropertyDeltaTarget target;
  target.kind = property_object_kind_;
  target.shard_id = property_shard_id_;
  target.base_file_id = base_file_id;
  // File IDs are monotonically allocated and retained when a MemTable is
  // flushed. They therefore also serve as the current base generation.
  target.base_generation = base_file_id;
  target.property_id = static_cast<uint32_t>(property_id);
  return target;
}

std::shared_ptr<const PropertyDeltaReadView>
LSMStore::LoadPropertyDeltaView(FileId_t base_file_id,
                                int property_id) const {
  if (property_delta_store_ == nullptr || property_id < 0 ||
      property_id >= static_cast<int>(sub_property_lengths_.size())) {
    return nullptr;
  }
  return property_delta_store_->ReadView(
      MakePropertyDeltaTarget(base_file_id, property_id));
}

Status LSMStore::AttachPropertyDelta(FileId_t target_fid, int property_id,
                                     bool target_is_persistent,
                                     std::vector<PropertyDeltaRecord> records) {
  if (property_delta_store_ == nullptr || records.empty() ||
      target_fid == INVALID_File_ID || property_id < 0 ||
      property_id >= static_cast<int>(sub_property_lengths_.size())) {
    return Status::kNotFound;
  }
  std::shared_lock<std::shared_mutex> compaction_lock(
      property_delta_compaction_mutex_);

  // The update manager may queue a batch while its original SST is being
  // compacted. Attachment waits for the compaction barrier above, then
  // resolves every logical topology record again. This prevents a delayed
  // buffer flush from publishing a delta against an SST that compaction has
  // already retired. The immutable base sequence guards against accidentally
  // rebinding an update to a newer duplicate edge with the same endpoints.
  struct ReboundBatch {
    bool target_is_persistent{false};
    std::vector<PropertyDeltaRecord> records;
  };
  std::unordered_map<FileId_t, ReboundBatch> rebound;
  for (auto& record : records) {
    LSMEdgeLocation location;
    const Status locate_status = LocateEdge(record.src, record.dst, &location,
                                            record.is_out, record.edge_type);
    if (locate_status != Status::kOk ||
        location.sequence != record.base_sequence ||
        location.target_id == INVALID_File_ID) {
      std::cerr << "[PROPERTY_DELTA_REBIND_ERROR] requested_fid=" << target_fid
                << " src=" << record.src << " dst=" << record.dst
                << " base_sequence=" << record.base_sequence << std::endl;
      return locate_status == Status::kOk ? Status::kCorruption : locate_status;
    }
    auto& batch = rebound[location.target_id];
    batch.target_is_persistent =
        batch.target_is_persistent || !location.in_memtable;
    batch.records.push_back(std::move(record));
  }

  for (auto& item : rebound) {
    std::string error;
    if (!property_delta_store_->Attach(
            MakePropertyDeltaTarget(item.first, property_id),
            std::move(item.second.records), item.second.target_is_persistent,
            &error)) {
      std::cerr << "[PROPERTY_DELTA_ATTACH_ERROR] " << error << std::endl;
      return Status::kBackgroundError;
    }
    if (FLAGS_richgraph_verbose && item.first != target_fid) {
      std::cout << "[PROPERTY_DELTA_REBOUND] old_fid=" << target_fid
                << " new_fid=" << item.first
                << " originally_persistent=" << target_is_persistent
                << std::endl;
    }
  }
  return Status::kOk;
}

void LSMStore::MarkPropertyDeltaTargetPersistent(FileId_t base_file_id) {
  if (property_delta_store_ == nullptr) return;
  for (int property_id = 0;
       property_id < static_cast<int>(sub_property_lengths_.size());
       ++property_id) {
    std::string error;
    if (!property_delta_store_->MarkTargetPersistent(
            MakePropertyDeltaTarget(base_file_id, property_id), &error)) {
      std::cerr << "[PROPERTY_DELTA_PERSIST_ERROR] " << error << std::endl;
    }
  }
}

bool LSMStore::WaitPropertyDeltaIdle(std::string* error) {
  return property_delta_store_ == nullptr ||
         property_delta_store_->WaitForIdle(error);
}

PropertyDeltaStoreStats LSMStore::GetPropertyDeltaStats() const {
  return property_delta_store_ == nullptr ? PropertyDeltaStoreStats{}
                                          : property_delta_store_->Stats();
}

PropertyCommitSequence LSMStore::GetMaxPropertyCommitSequence() const {
  return property_delta_store_ == nullptr
             ? 0
             : property_delta_store_->MaxCommitSequence();
}

bool LSMStore::LocateSstEdgeOrdinal(
    FileId_t target_fid,
    const PropertyDeltaRecord& record,
    size_t* edge_ordinal) const {
  if (edge_ordinal == nullptr) return false;
  SSTDataCache* data = nullptr;
  try {
    data = const_cast<SSTDataManager&>(sstdata_manager_).get_data(target_fid);
  } catch (const std::exception&) {
    return false;
  }
  if (data == nullptr) return false;
  auto* cache = reinterpret_cast<SSTableCache*>(data->GetSSTableCache());
  if (cache == nullptr) return false;
  const int pos = cache->get(record.src);
  if (pos < 0 || pos + 1 >= static_cast<int>(cache->indexes.size())) {
    return false;
  }
  const int begin = static_cast<int>(cache->indexes[pos].offset);
  const int end = static_cast<int>(cache->indexes[pos + 1].offset);
  const EdgeLookupResult lookup = FindEdgeByDstDirectionAndType(
      data->GetEdgeData(), begin, end, record.dst,
      record.is_out, record.edge_type);
  if (lookup.status != Status::kOk || lookup.body_offset < 0) return false;
  if (get_seq(data->GetEdgeData() + lookup.body_offset) !=
      record.base_sequence) {
    return false;
  }
  *edge_ordinal = GetEdgeOrdinalFromBodyOffset(
      static_cast<uint32_t>(lookup.body_offset));
  return true;
}

bool LSMStore::MergePropertyDeltaColumn(
    const PropertyDeltaTarget& target,
    const std::shared_ptr<const MappedPropertyFile>& current_base,
    const std::vector<std::shared_ptr<PropertyDeltaFile>>& deltas,
    uint64_t output_generation,
    std::shared_ptr<MappedPropertyFile>* merged_base,
    std::string* error) {
  if (merged_base == nullptr || deltas.empty() ||
      target.kind != property_object_kind_ ||
      target.shard_id != property_shard_id_ ||
      target.property_id >= sub_property_lengths_.size()) {
    SetPropertyDeltaError(error, "invalid property delta merge request");
    return false;
  }

  const ScopedFixedPropertyLayout property_layout(&sub_property_lengths_);
  const ScopedDbPathOverride db_path_override(&dataDir);
  SSTDataCache* data = nullptr;
  try {
    data = sstdata_manager_.get_data(target.base_file_id);
  } catch (const std::exception& exception) {
    SetPropertyDeltaError(error,
                          "open merge target " +
                              std::to_string(target.base_file_id) + ": " +
                              exception.what());
    return false;
  }
  if (data == nullptr) {
    SetPropertyDeltaError(error, "property delta merge target is unavailable");
    return false;
  }

  const int property_id = static_cast<int>(target.property_id);
  const std::byte* base_data = current_base == nullptr
      ? reinterpret_cast<const std::byte*>(data->GetPropertyData(property_id))
      : current_base->data();
  const size_t base_size = current_base == nullptr
      ? data->GetPropertySize(property_id)
      : current_base->size();
  if (base_data == nullptr || base_size == 0) {
    SetPropertyDeltaError(error, "property delta merge base is empty");
    return false;
  }

  const std::string final_path = pFileName_with_id(
      pFileName(target.base_file_id), property_id);
  const std::string temporary_path = final_path + ".merge." +
                                     std::to_string(output_generation) +
                                     ".tmp";
  const int fd = ::open(temporary_path.c_str(),
                        O_CREAT | O_EXCL | O_RDWR,
                        0644);
  if (fd < 0) {
    SetPropertyDeltaError(error,
                          PropertyDeltaErrno("open", temporary_path));
    return false;
  }

  bool ok = true;
  if (::ftruncate(fd, static_cast<off_t>(base_size)) != 0) {
    SetPropertyDeltaError(error,
                          PropertyDeltaErrno("ftruncate", temporary_path));
    ok = false;
  }
  void* mapping = MAP_FAILED;
  if (ok) {
    mapping = ::mmap(nullptr,
                     base_size,
                     PROT_READ | PROT_WRITE,
                     MAP_SHARED,
                     fd,
                     0);
    if (mapping == MAP_FAILED) {
      SetPropertyDeltaError(error,
                            PropertyDeltaErrno("mmap", temporary_path));
      ok = false;
    }
  }

  if (ok) {
    std::memcpy(mapping, base_data, base_size);
    auto* output = static_cast<char*>(mapping);
    const size_t slot_length = GetSubPropertyFixedLength(property_id);
    for (const auto& delta : deltas) {
      if (!delta->ForEachRecord([&](const PropertyDeltaRecord& record) {
            size_t edge_ordinal = 0;
            if (!LocateSstEdgeOrdinal(target.base_file_id,
                                      record,
                                      &edge_ordinal)) {
              SetPropertyDeltaError(
                  error,
                  "delta record does not match its base topology: fid=" +
                      std::to_string(target.base_file_id) + ", src=" +
                      std::to_string(record.src) + ", dst=" +
                      std::to_string(record.dst));
              return false;
            }
            const size_t offset = edge_ordinal * slot_length;
            if (offset > base_size || slot_length > base_size - offset) {
              SetPropertyDeltaError(error,
                                    "delta record property offset is invalid");
              return false;
            }
            WritePaddedSubPropertySlot(output + offset,
                                       record.value.data(),
                                       record.value.size(),
                                       property_id);
            return true;
          })) {
        ok = false;
        break;
      }
    }
  }

  if (mapping != MAP_FAILED) {
    if (ok && property_update_options_.durability ==
                  DeltaDurability::kProcessCrashSafe &&
        ::msync(mapping, base_size, MS_SYNC) != 0) {
      SetPropertyDeltaError(error,
                            PropertyDeltaErrno("msync", temporary_path));
      ok = false;
    }
    ::munmap(mapping, base_size);
  }
  if (ok && property_update_options_.durability ==
                DeltaDurability::kProcessCrashSafe &&
      ::fdatasync(fd) != 0) {
    SetPropertyDeltaError(error,
                          PropertyDeltaErrno("fdatasync", temporary_path));
    ok = false;
  }
  if (::close(fd) != 0 && ok) {
    SetPropertyDeltaError(error,
                          PropertyDeltaErrno("close", temporary_path));
    ok = false;
  }
  if (!ok) {
    ::unlink(temporary_path.c_str());
    return false;
  }
  if (::rename(temporary_path.c_str(), final_path.c_str()) != 0) {
    SetPropertyDeltaError(error,
                          PropertyDeltaErrno("rename", final_path));
    ::unlink(temporary_path.c_str());
    return false;
  }
  if (property_update_options_.durability ==
          DeltaDurability::kProcessCrashSafe &&
      !SyncPropertyDeltaParent(final_path, error)) {
    return false;
  }

  *merged_base = MappedPropertyFile::Open(final_path,
                                           output_generation,
                                           error);
  return *merged_base != nullptr;
}

std::shared_ptr<void> LSMStore::PreparePropertyDeltasForCompaction(
    std::string* error) {
  auto guard = std::make_shared<std::unique_lock<std::shared_mutex>>(
      property_delta_compaction_mutex_);
  if (property_delta_store_ != nullptr &&
      !property_delta_store_->MergeAllPersistent(error)) {
    return nullptr;
  }
  return std::static_pointer_cast<void>(guard);
}

void LSMStore::ScheduleCSRRebuild() {
  if (!use_csr_disk_) {
    return;
  }
  {
    std::lock_guard<std::mutex> lock(csr_rebuild_state_mutex_);
    if (csr_rebuild_worker_stop_) {
      return;
    }
    // coalescing：多次请求合并成“至少一次”重建。
    csr_rebuild_pending_ = true;
  }
  csr_rebuild_cv_.notify_all();
}

void LSMStore::CSRRebuildWorkerMain() {
  if (!use_csr_disk_) {
    return;
  }
  const ScopedFixedPropertyLayout property_layout(&sub_property_lengths_);
  const ScopedDbPathOverride db_path_override(&dataDir);
  while (true) {
    {
      std::unique_lock<std::mutex> lock(csr_rebuild_state_mutex_);
      csr_rebuild_cv_.wait(lock, [&] {
        return csr_rebuild_worker_stop_ || csr_rebuild_pending_;
      });
      if (csr_rebuild_worker_stop_ && !csr_rebuild_pending_) {
        break;
      }
      if (!csr_rebuild_pending_) {
        continue;
      }
      csr_rebuild_pending_ = false;
      csr_rebuild_running_ = true;
    }

    try {
      RebuildCSRFromVisibleSSTs();
    } catch (const std::exception& e) {
      std::cerr << "[CSR] rebuild failed with exception: " << e.what()
                << std::endl;
    } catch (...) {
      std::cerr << "[CSR] rebuild failed with unknown exception." << std::endl;
    }

    {
      std::lock_guard<std::mutex> lock(csr_rebuild_state_mutex_);
      csr_rebuild_running_ = false;
    }
    csr_rebuild_cv_.notify_all();
  }
}

void LSMStore::MarkCSRFlushJobDone() {
  if (!use_csr_disk_) {
    return;
  }
  const uint64_t prev = csr_flush_jobs_inflight_.fetch_sub(
      1, std::memory_order_acq_rel);
  assert(prev > 0);
  csr_rebuild_cv_.notify_all();
}

void LSMStore::BeginBackgroundFlushJob() {
  background_flush_jobs_inflight_.fetch_add(1, std::memory_order_acq_rel);
}

void LSMStore::MarkBackgroundFlushJobDone() {
  const uint64_t prev = background_flush_jobs_inflight_.fetch_sub(
      1, std::memory_order_acq_rel);
  assert(prev > 0);
}

void LSMStore::MaybeRebuildCSRFromDisk() {
  if (!use_csr_disk_) {
    return;
  }
  ScheduleCSRRebuild();
}

void LSMStore::WaitCSRUpToDate() {
  if (!use_csr_disk_) {
    return;
  }

  // 先投递一次重建请求，覆盖“调用时刻之前”已经可见的磁盘文件。
  ScheduleCSRRebuild();

  std::unique_lock<std::mutex> lock(csr_rebuild_state_mutex_);
  csr_rebuild_cv_.wait(lock, [&] {
    return !csr_rebuild_pending_
           && !csr_rebuild_running_
           && csr_flush_jobs_inflight_.load(std::memory_order_acquire) == 0;
  });
}

bool LSMStore::HasBackgroundWork() {
  if (background_flush_jobs_inflight_.load(std::memory_order_acquire) != 0 ||
      compactor_.GetState()) {
    return true;
  }
  if (use_csr_disk_) {
    std::lock_guard<std::mutex> lock(csr_rebuild_state_mutex_);
    return csr_rebuild_pending_ || csr_rebuild_running_ ||
           csr_flush_jobs_inflight_.load(std::memory_order_acquire) != 0;
  }
  return false;
}

void LSMStore::WaitBackgroundIdle() {
  if (use_csr_disk_) {
    WaitCSRUpToDate();
  }
  while (HasBackgroundWork()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  if (property_delta_store_ != nullptr) {
    std::string error;
    if (!property_delta_store_->WaitForIdle(&error)) {
      throw std::runtime_error("property delta background work failed: " +
                               error);
    }
  }
}

bool LSMStore::ForceCompactAllL0ToL1() {
  WaitBackgroundIdle();
  const ScopedFixedPropertyLayout property_layout(&sub_property_lengths_);
  const ScopedDbPathOverride db_path_override(&dataDir);
  return compactor_.ForceCompactAllL0ToL1();
}

void LSMStore::RebuildCSRFromVisibleSSTs() {
  std::vector<CSRSourceFileView> source_files;
  std::unordered_set<FileId_t> visited_fids;

  auto add_source_file = [&](SSTableCache* cache) {
    if (cache == nullptr) {
      return;
    }
    const FileId_t fid = static_cast<FileId_t>(cache->header.timeStamp);
    if (!visited_fids.insert(fid).second) {
      return;
    }
    try {
      SSTDataCache* data = sstdata_manager_.get_data(fid);
      source_files.push_back({cache, data});
    } catch (const std::exception&) {
      // 若对应 mmap 缓存不存在，则跳过该文件，避免中断在线写入。
    }
  };

  {
    SuperVersion snapshot;
    get_superversion(snapshot);
    if (snapshot.version_memtable != nullptr &&
        snapshot.version_memtable->current_ != nullptr) {
      auto* level0 =
          snapshot.version_memtable->current_->GetLevel0Files();
      if (level0 != nullptr) {
        for (auto* cache : *level0) {
          add_source_file(cache);
        }
      }
    }
    snapshot.version_memtable = nullptr;
  }

  // 兼容“启动时加载旧数据”的场景：将 level>=1 的文件也纳入重建输入。
  for (size_t level = 1; level < fileMetaCache.size(); ++level) {
    if (fileMetaCache[level] == nullptr) {
      continue;
    }
    for (auto* cache : *fileMetaCache[level]) {
      add_source_file(cache);
    }
  }

  std::sort(source_files.begin(), source_files.end(),
            [](const CSRSourceFileView& a, const CSRSourceFileView& b) {
              return a.cache->header.timeStamp > b.cache->header.timeStamp;
            });

  if (source_files.empty()) {
    std::atomic_store(&csr_read_snapshot_,
                      std::shared_ptr<const CSRReadSnapshot>{});
    return;
  }

  std::vector<VertexId_t> all_src;
  all_src.reserve(1024);
  for (const auto& view : source_files) {
    if (view.cache == nullptr || view.cache->indexes.empty()) {
      continue;
    }
    for (size_t i = 0; i + 1 < view.cache->indexes.size(); ++i) {
      const VertexId_t src = view.cache->indexes[i].key;
      if (src != INVALID_VERTEX_ID) {
        all_src.push_back(src);
      }
    }
  }
  if (all_src.empty()) {
    std::atomic_store(&csr_read_snapshot_,
                      std::shared_ptr<const CSRReadSnapshot>{});
    return;
  }
  std::sort(all_src.begin(), all_src.end());
  all_src.erase(std::unique(all_src.begin(), all_src.end()), all_src.end());

  const size_t topo_soft_limit =
      std::max<size_t>(1, static_cast<size_t>(FLAGS_csr_topo_soft_limit_mb))
      * (1ull << 20);

  struct CSRBuildState {
    FileId_t fid = INVALID_File_ID;
    VertexId_t min_key = INVALID_VERTEX_ID;
    VertexId_t max_key = INVALID_VERTEX_ID;
    SequenceNumber_t newest_edge = 0;
    std::vector<EdgeBody_t> edge_bodies;
    std::vector<Index> indexes;  // 不含哨兵
    std::vector<std::unique_ptr<FileIO>> property_files;
    bool opened = false;
    bool overflowed = false;

    bool empty() const { return indexes.empty(); }

    size_t topo_bytes_if_finalized() const {
      const size_t edge_num_with_sentinel = edge_bodies.size() + 1;
      const size_t index_num_with_sentinel = indexes.size() + 1;
      return edge_num_with_sentinel * static_cast<size_t>(EDGEBODY_SIZE)
           + index_num_with_sentinel * sizeof(Index)
           + static_cast<size_t>(HEADER_SIZE);
    }

    void reset() {
      fid = INVALID_File_ID;
      min_key = INVALID_VERTEX_ID;
      max_key = INVALID_VERTEX_ID;
      newest_edge = 0;
      edge_bodies.clear();
      indexes.clear();
      property_files.clear();
      opened = false;
      overflowed = false;
    }
  };

  const int sub_property_num = GetActiveSubPropertyNum();
  std::vector<std::string> zero_slots(sub_property_num);
  for (int pid = 0; pid < sub_property_num; ++pid) {
    zero_slots[pid].assign(GetSubPropertyFixedLength(pid), '\0');
  }

  struct CSRBatchBuildResult {
    bool ok = true;
    std::string error;
    std::vector<SSTableCache*> active_files;
  };

  auto open_builder = [&](CSRBuildState& builder) -> bool {
    if (builder.opened) {
      return true;
    }
    builder.fid = __sync_fetch_and_add(&currentTime, 1);
    builder.property_files.reserve(sub_property_num);
    for (int pid = 0; pid < sub_property_num; ++pid) {
      const std::string p_name =
          pFileName_with_id(pFileName(builder.fid), pid);
      auto p_file = std::make_unique<FileIO>(p_name);
      if (!p_file->Open(O_WRONLY | O_CREAT | O_TRUNC, 0666)) {
        std::cerr << "[CSR] failed to open property file for writing: "
                  << p_name << std::endl;
        return false;
      }
      builder.property_files.emplace_back(std::move(p_file));
    }
    builder.opened = true;
    return true;
  };

  auto finalize_builder = [&](CSRBuildState& builder,
                             CSRBatchBuildResult& result) -> bool {
    if (!builder.opened || builder.empty()) {
      builder.reset();
      return true;
    }

    for (auto& p_file : builder.property_files) {
      if (p_file != nullptr) {
        p_file->Close();
      }
    }

    const size_t real_edge_num = builder.edge_bodies.size();
    const EdgeOffset_t edge_end_offset =
        static_cast<EdgeOffset_t>(real_edge_num * EDGEBODY_SIZE);
    const EdgePropertyOffset_t end_payload =
        static_cast<EdgePropertyOffset_t>(real_edge_num
                                          * GetSubPropertyFixedLength(0));
    EdgeBody_t sentinel_edge(INVALID_VERTEX_ID, INVALID_VERTEX_ID,
                             end_payload, false, true, 0);

    std::vector<Index> indexes_with_sentinel = builder.indexes;
    indexes_with_sentinel.emplace_back(INVALID_VERTEX_ID, edge_end_offset);

    Header header;
    header.timeStamp = builder.fid;
    header.size = real_edge_num + 1;
    header.index_size = indexes_with_sentinel.size();
    header.minKey = builder.min_key;
    header.maxKey = builder.max_key;

    const size_t e_file_size = static_cast<size_t>(header.size) * EDGEBODY_SIZE
                             + static_cast<size_t>(header.index_size) * sizeof(Index)
                             + HEADER_SIZE;
    std::vector<char> e_buffer(e_file_size, 0);

    char* cursor = e_buffer.data();
    if (!builder.edge_bodies.empty()) {
      std::memcpy(cursor, builder.edge_bodies.data(),
                  builder.edge_bodies.size() * EDGEBODY_SIZE);
    }
    cursor += builder.edge_bodies.size() * EDGEBODY_SIZE;
    std::memcpy(cursor, &sentinel_edge, EDGEBODY_SIZE);
    cursor += EDGEBODY_SIZE;
    std::memcpy(cursor, indexes_with_sentinel.data(),
                indexes_with_sentinel.size() * sizeof(Index));
    std::memcpy(e_buffer.data() + e_file_size - HEADER_SIZE,
                &header, HEADER_SIZE);

    const std::string e_name = eFileName(builder.fid);
    FileIO e_file(e_name);
    if (!e_file.Open(O_WRONLY | O_CREAT | O_TRUNC, 0666)) {
      std::cerr << "[CSR] failed to open topology file for writing: "
                << e_name << std::endl;
      return false;
    }
    const ssize_t write_bytes = e_file.Write(e_buffer.data(), e_file_size);
    if (write_bytes != static_cast<ssize_t>(e_file_size)) {
      std::cerr << "[CSR] failed to write topology file: " << e_name
                << ", expected=" << e_file_size
                << ", actual=" << write_bytes << std::endl;
      return false;
    }
    e_file.Close();

    auto* cache = new SSTableCache(sstdata_manager_, builder.newest_edge);
    cache->header = header;
    cache->indexes = std::move(indexes_with_sentinel);
    cache->path = e_name;
    cache->seq_ = 0;
    cache->newest_edge = builder.newest_edge;
    sstdata_manager_.put_data(cache->header.timeStamp,
                              cache->header.size,
                              reinterpret_cast<uintptr_t>(cache),
                              cache->newest_edge);

    result.active_files.push_back(cache);

    builder.reset();
    return true;
  };

  // 每个 src 的候选边收集是彼此独立的。
  // 为了提升 CPU 利用率，这里采用“按 src 连续区间分 batch”：
  // 1) 每个线程负责一个 batch（固定区间）；
  // 2) 在线程内完成 collect + build + write（完整流水）；
  // 3) 最后主线程汇总各 batch 的元数据并原子切换 CSR 视图。
  auto collect_selected_edges_for_src =
      [&](VertexId_t src, std::vector<CSRSelectedEdgeRef>& out) {
        out.clear();

        for (const auto& view : source_files) {
          SSTableCache* cache = view.cache;
          if (cache == nullptr || view.sst_data == nullptr) {
            continue;
          }
          if (src < cache->header.minKey || src > cache->header.maxKey) {
            continue;
          }
          const int pos = cache->get(src);
          if (pos < 0) {
            continue;
          }
          const uint32_t s_offset = cache->indexes[pos].offset;
          const uint32_t e_offset = cache->indexes[pos + 1].offset;
          char* body_buffer = view.sst_data->GetEdgeData();
          for (uint32_t offset = s_offset; offset < e_offset;
               offset += EDGEBODY_SIZE) {
            const auto* body =
                reinterpret_cast<const EdgeBody_t*>(body_buffer + offset);
            if (body->get_marker()) {
              continue;
            }
            CSRSelectedEdgeRef selected;
            selected.dst = body->get_dst();
            selected.seq = body->get_seq();
            selected.is_out = body->get_is_out();
            selected.edge_type = body->get_edge_type();
            selected.sst_data = view.sst_data;
            selected.body_offset = offset;
            out.push_back(selected);
          }
        }

        if (!out.empty()) {
          std::sort(out.begin(), out.end(), CSRSelectedEdgeRefLess);
        }
      };

  // 线程策略：
  // - 用户给系统 16 线程时，这里最多用 8 线程做 CSR 合并；
  // - 其他情况按 min(8, FLAGS_thread_num/2) 自动收敛；
  // - 数据量小时会进一步收敛到 <= src 数量，避免空转线程。
  int csr_merge_threads =
      std::max<int>(1, static_cast<int>(FLAGS_thread_num) / 2);
  csr_merge_threads = std::min<int>(8, csr_merge_threads);
  csr_merge_threads = std::min<int>(csr_merge_threads,
                                    static_cast<int>(all_src.size()));

  const int batch_num = csr_merge_threads;
  std::vector<std::pair<size_t, size_t>> batch_ranges(batch_num);
  for (int batch_id = 0; batch_id < batch_num; ++batch_id) {
    const size_t start = (all_src.size() * static_cast<size_t>(batch_id))
                       / static_cast<size_t>(batch_num);
    const size_t end = (all_src.size() * static_cast<size_t>(batch_id + 1))
                     / static_cast<size_t>(batch_num);
    batch_ranges[batch_id] = {start, end};
  }

  std::vector<CSRBatchBuildResult> batch_results(batch_num);

  #pragma omp parallel for schedule(static) num_threads(csr_merge_threads)
  for (int batch_id = 0; batch_id < batch_num; ++batch_id) {
    const ScopedFixedPropertyLayout property_layout(&sub_property_lengths_);
    const ScopedDbPathOverride db_path_override(&dataDir);
    auto& result = batch_results[batch_id];
    const size_t begin = batch_ranges[batch_id].first;
    const size_t end = batch_ranges[batch_id].second;
    if (begin >= end) {
      continue;
    }

    CSRBuildState builder;
    std::vector<CSRSelectedEdgeRef> selected_edges;
    selected_edges.reserve(256);

    auto close_builder_files = [&](CSRBuildState& state) {
      for (auto& p_file : state.property_files) {
        if (p_file != nullptr) {
          p_file->Close();
        }
      }
      state.reset();
    };

    for (size_t i = begin; i < end; ++i) {
      const VertexId_t src = all_src[i];
      collect_selected_edges_for_src(src, selected_edges);
      if (selected_edges.empty()) {
        continue;
      }

      if (builder.opened && builder.overflowed && !builder.empty()) {
        if (!finalize_builder(builder, result)) {
          result.ok = false;
          result.error = "finalize builder failed";
          close_builder_files(builder);
          break;
        }
      }
      if (!open_builder(builder)) {
        result.ok = false;
        result.error = "open builder failed";
        close_builder_files(builder);
        break;
      }

      if (builder.empty()) {
        builder.min_key = src;
      }
      builder.max_key = src;
      builder.indexes.emplace_back(
          src, static_cast<EdgeOffset_t>(builder.edge_bodies.size() * EDGEBODY_SIZE));

      for (const auto& edge : selected_edges) {
        const EdgePropertyOffset_t payload =
            static_cast<EdgePropertyOffset_t>(builder.edge_bodies.size()
                                              * GetSubPropertyFixedLength(0));
        builder.edge_bodies.emplace_back(edge.dst, edge.seq, payload,
                                         false, edge.is_out, edge.edge_type);
        builder.newest_edge = std::max(builder.newest_edge, edge.seq);

        const size_t edge_ordinal =
            GetEdgeOrdinalFromBodyOffset(edge.body_offset);
        for (int pid = 0; pid < sub_property_num; ++pid) {
          const uint32_t slot_len = GetSubPropertyFixedLength(pid);
          if (slot_len == 0) {
            continue;
          }
          const char* slot_ptr = nullptr;
          if (edge.sst_data != nullptr) {
            char* p_buffer = edge.sst_data->GetPropertyData(pid);
            if (p_buffer != nullptr) {
              const size_t src_offset =
                  GetSubPropertyOffsetByEdgeOrdinal(edge_ordinal, pid);
              slot_ptr = p_buffer + src_offset;
            }
          }
          if (slot_ptr == nullptr) {
            slot_ptr = zero_slots[pid].data();
          }
          const ssize_t write_size =
              builder.property_files[pid]->Write(slot_ptr, slot_len);
          if (write_size != static_cast<ssize_t>(slot_len)) {
            result.ok = false;
            result.error = "write property slot failed";
            close_builder_files(builder);
            break;
          }
        }
        if (!result.ok) {
          break;
        }
      }

      if (!result.ok) {
        break;
      }

      if (builder.topo_bytes_if_finalized() > topo_soft_limit) {
        builder.overflowed = true;
      }
    }

    if (result.ok) {
      if (!finalize_builder(builder, result)) {
        result.ok = false;
        result.error = "finalize tail builder failed";
        close_builder_files(builder);
      }
    }
  }

  std::vector<SSTableCache*> new_active_files;
  bool build_ok = true;

  for (auto& result : batch_results) {
    if (!result.ok) {
      build_ok = false;
      break;
    }
    new_active_files.insert(new_active_files.end(),
                            result.active_files.begin(),
                            result.active_files.end());
  }

  if (!build_ok) {
    // 任一 batch 失败则放弃本轮切换，避免发布不完整 CSR 视图。
    // 同时回收本轮已创建的缓存对象，减少失败重试造成的资源累积。
    for (const auto& result : batch_results) {
      for (auto* cache : result.active_files) {
        if (cache == nullptr) {
          continue;
        }
        cache->Unref();
      }
    }
    return;
  }

  // 构建不可变读快照：
  // - src_index 直接按 src 定位到 {cache, s_offset, e_offset}；
  // - 发布后读线程仅 atomic_load，无锁访问；
  // - 旧快照在最后一个读者离开后析构，触发 Unref 删除旧文件。
  auto new_snapshot = std::make_shared<CSRReadSnapshot>();
  new_snapshot->active_files = new_active_files;
  new_snapshot->src_index.resize(max_vertex_num_);
  for (auto* cache : new_active_files) {
    if (cache == nullptr || cache->indexes.size() < 2) {
      continue;
    }
    for (size_t i = 0; i + 1 < cache->indexes.size(); ++i) {
      const VertexId_t src = cache->indexes[i].key;
      if (src == INVALID_VERTEX_ID || src >= max_vertex_num_) {
        continue;
      }
      auto& entry = new_snapshot->src_index[static_cast<size_t>(src)];
      entry.cache = cache;
      entry.s_offset = cache->indexes[i].offset;
      entry.e_offset = cache->indexes[i + 1].offset;
    }
  }
  std::atomic_store(&csr_read_snapshot_,
                    std::shared_ptr<const CSRReadSnapshot>(new_snapshot));
}

Status LSMStore::find_edge_in_csr_disk(VertexId_t src, VertexId_t dst,
                                       std::string* property, int property_id,
                                       bool is_out, uint8_t edge_type) {
  if (property == nullptr || property_id < 0
      || property_id >= GetActiveSubPropertyNum()) {
    return Status::kNotFound;
  }

  auto snapshot = LoadCSRReadSnapshot();
  const auto* entry = FindCSRIndexEntry(snapshot.get(), src);
  if (entry == nullptr || entry->cache == nullptr) {
    return Status::kNotFound;
  }
  const FileId_t fid = entry->cache->header.timeStamp;
  const uint32_t s_offset = entry->s_offset;
  const uint32_t e_offset = entry->e_offset;

  // 复用现有的 SST 查询实现，保持与 LSM 路径一致：
  // 1) 方向/类型过滤；
  // 2) 定长属性读取；
  // 3) property-delta 覆盖语义。
  if (FLAGS_support_mulversion == true) {
    get_superversion(local_sv_);
  }
  Status rs = find_edge_from_sstdata_cache(src, dst, s_offset, e_offset,
                                           fid, property, property_id,
                                           is_out, edge_type);
  if (FLAGS_support_mulversion == true) {
    local_sv_.version_memtable = nullptr;
  }
  return rs;
}

Status LSMStore::find_edge_seq_in_csr_disk(VertexId_t src, VertexId_t dst,
                                           FileId_t& fid,
                                           SequenceNumber_t& seq) {
  auto snapshot = LoadCSRReadSnapshot();
  const auto* entry = FindCSRIndexEntry(snapshot.get(), src);
  if (entry == nullptr || entry->cache == nullptr) {
    return Status::kNotFound;
  }
  fid = entry->cache->header.timeStamp;
  const uint32_t s_offset = entry->s_offset;
  const uint32_t e_offset = entry->e_offset;
  SSTDataCache* sstcache = nullptr;
  try {
    sstcache = sstdata_manager_.get_data(fid);
  } catch (const std::exception&) {
    return Status::kNotFound;
  }
  if (sstcache == nullptr) {
    return Status::kNotFound;
  }

  char* body_buffer = sstcache->GetEdgeData();
  int low = static_cast<int>(s_offset);
  int high = static_cast<int>(e_offset);
  const int step = static_cast<int>(EDGEBODY_SIZE);
  while (low < high) {
    const int mid = low + (high - low) / 2 / step * step;
    const auto* body =
        reinterpret_cast<const EdgeBody_t*>(body_buffer + mid);
    if (body->get_dst() >= dst) {
      high = mid;
    } else {
      low = mid + step;
    }
  }

  for (int offset = low; offset < static_cast<int>(e_offset); offset += step) {
    const auto* body =
        reinterpret_cast<const EdgeBody_t*>(body_buffer + offset);
    if (body->get_dst() != dst) {
      break;
    }
    if (body->get_marker()) {
      return Status::kDelete;
    }
    seq = body->get_seq();
    return Status::kOk;
  }
  return Status::kNotFound;
}

VertexId_t LSMStore::new_vertex(bool use_recycled_vertex) {
  VertexId_t vertex_id = vertex_id_.fetch_add(1, std::memory_order_relaxed);

  // Initialize all resources related to this vertex
  // must be initialized manually
  if (FLAGS_support_mulversion == false) {
    for (int level_id = 0; level_id < LEVEL_INDEX_SIZE; level_id++) {
      vid_to_levelIndex_[vertex_id * LEVEL_INDEX_SIZE + level_id].init();
    }
  } else if (FLAGS_LOAD_OLD_DATA == false) {
#if defined(MMAP_DIFF_SIZE_LEVEL_INDEX) || defined(MMAP_COLD_HOT_LEVEL_INDEX)
    vid_to_mullevelIndex_.init(vertex_id);
#else
    vid_to_mullevelIndex_.init(vertex_id);
#endif
  }

  atomic_store_value(&vertex_max_level_[vertex_id], Level_t{-1});

  for (auto tb : memTable_list_) {
    tb->init_vertex_adjs(vertex_id);
  }
  return vertex_id;
}

void LSMStore::InitVerticesUpTo(VertexId_t next_vertex_id) {
  VertexId_t current = vertex_id_.load(std::memory_order_relaxed);
  if (next_vertex_id <= current) {
    return;
  }
  for (VertexId_t vertex_id = current; vertex_id < next_vertex_id;
       ++vertex_id) {
    if (FLAGS_support_mulversion == false) {
      for (int level_id = 0; level_id < LEVEL_INDEX_SIZE; ++level_id) {
        vid_to_levelIndex_[vertex_id * LEVEL_INDEX_SIZE + level_id].init();
      }
    } else if (FLAGS_LOAD_OLD_DATA == false) {
#if defined(MMAP_DIFF_SIZE_LEVEL_INDEX) || defined(MMAP_COLD_HOT_LEVEL_INDEX)
      vid_to_mullevelIndex_.init(vertex_id);
#else
      vid_to_mullevelIndex_.init(vertex_id);
#endif
    }
    atomic_store_value(&vertex_max_level_[vertex_id], Level_t{-1});
    for (auto tb : memTable_list_) {
      tb->init_vertex_adjs(vertex_id);
    }
  }
  vertex_id_.store(next_vertex_id, std::memory_order_relaxed);
}

void LSMStore::put_vertex(VertexId_t vertex_id, std::string_view data) {
  // TODO(correctness): Implement vertex-property writes or make this legacy
  // entry point report that the operation is unsupported.

}

LSMStore::~LSMStore() {
    if (FLAGS_richgraph_verbose) {
      std::cout << "in lsmstore ~" << std::endl;
    }
    const ScopedFixedPropertyLayout property_layout(&sub_property_lengths_);
    const ScopedDbPathOverride db_path_override(&dataDir);

    try {
      WaitBackgroundIdle();
    } catch (const std::exception& error) {
      std::cerr << "[LSMStore] background shutdown barrier failed: "
                << error.what() << std::endl;
    }

    if (use_csr_disk_) {
      // Stop CSR rebuilding before releasing resources used by the worker.
      WaitCSRUpToDate();
      {
        std::lock_guard<std::mutex> lock(csr_rebuild_state_mutex_);
        csr_rebuild_worker_stop_ = true;
      }
      csr_rebuild_cv_.notify_all();
      if (csr_rebuild_worker_.joinable()) {
        csr_rebuild_worker_.join();
      }
    }

    while (GetCompactionState()) {
      if (FLAGS_richgraph_verbose) {
        std::cout << " wait for the compaction to complete..." << std::endl;
      }
      std::this_thread::sleep_for(std::chrono::seconds(1)); 
    }

    if (FLAGS_richgraph_verbose) {
      std::cout << "compaction completed" << std::endl;

      static_edge_distribution();

    std::cout << "vertex_id_=" << vertex_id_.load() << std::endl;

    if (FLAGS_support_mulversion== false) {
      int real_vector_size = vertex_id_.load() * LEVEL_INDEX_SIZE;
      printf(" @levelIndex space: %.2fGB\n",
             (real_vector_size * sizeof(LevelIndex))
             /1024.0/1024/1024);
    } else {
      int real_vector_size = vertex_id_.load();
#if !defined(MMAP_LEVEL_INDEX) && !defined(MMAP_DIFF_SIZE_LEVEL_INDEX) && \
    !defined(MMAP_COLD_HOT_LEVEL_INDEX)
      printf(" @levelIndex logical space: %.2fGB\n",
             (real_vector_size * sizeof(MulLevelIndex))
             /1024.0/1024/1024);
      printf(" @levelIndex allocated space: %.2fGB\n",
             vid_to_mullevelIndex_.allocated_bytes() / 1024.0 / 1024 / 1024);
#else
      printf(" @levelIndex space: %.2fGB\n",
             (real_vector_size * sizeof(MulLevelIndex))
             /1024.0/1024/1024);
#endif
    }

    printf(" @Futex space: %0.2fGB\n", vertex_lock_count_ * sizeof(Futex)
           /1024.0/1024/1024);
    printf(" @RWLock_t space: %0.2fGB\n", vertex_lock_count_ * sizeof(RWLock_t)
           /1024.0/1024/1024);
    std::cout << " @sizeof(Edge): " << sizeof(Edge) << std::endl;
    std::cout << " @sizeof(MulLevelIndex): " 
              << sizeof(MulLevelIndex) << std::endl;
    std::cout << " @MAX_LEVEL: " << MAX_LEVEL << std::endl;
        std::cout << " @NeighBors: " << typeid(NeighBors).name() << std::endl;
    std::cout << " @EdgeIterator: " << typeid(EdgeIterator).name() << std::endl;
#ifdef OPT_MERGE_MULTI_LEVEL
    std::cout << " @MULTI_LEVEL: open" << std::endl;
#else
    std::cout << " @MULTI_LEVEL: close" << std::endl;
#endif
#ifdef FULL_FILE_INDEX
    std::cout << " @MMAP_LEVEL_INDEX: full_read_from_file" << std::endl;
#elif defined(MMAP_LEVEL_INDEX)
    std::cout << " @MMAP_LEVEL_INDEX: open_allmmap" << std::endl;
#elif defined(MMAP_DIFF_SIZE_LEVEL_INDEX)
    std::cout << " @MMAP_LEVEL_INDEX: open_diffsize" << std::endl;
#elif defined(MMAP_COLD_HOT_LEVEL_INDEX)
    std::cout << " @MMAP_LEVEL_INDEX: open_hot" << std::endl;
#else
    std::cout << " @MMAP_LEVEL_INDEX: close" << std::endl;
#endif
#ifdef USE_MADVISE
    std::cout << " @USE_MADVISE: open" << std::endl;
#endif
#ifdef MERGE_LARGE_VERTEX
    std::cout << " @MERGE_LARGE_VERTEX: open" << std::endl;
#endif
#ifdef USE_DIRECTEDIO
    std::cout << " @USE_DIRECTEDIO: open" << std::endl;
#else
    std::cout << " @USE_DIRECTEDIO: close" << std::endl;
#endif

    {
      std::cout << "-------------------- start count mem cost------------------"
                << std::endl;
      double all_mem_cost = 0;
      const VertexId_t max_vertex_num = vertex_id_.load();

      // memtable
      double memtable_cost = 0; // 只统计了固定大小没算上skiplist中动态申请的节点
      size_t max_edge_num = memtable_size_;
      size_t skiplist = sizeof(NeighBors)
                        + (sizeof(Edge) + 8) * FLAGS_reserve_node; // reserve-node
      size_t a_memtable_size = max_edge_num * skiplist // all skiplist
                             + max_vertex_num * 8;  // node point -> opt: 并行hashmap
      memtable_cost = a_memtable_size * memTable_list_.size();

      // cache
      double fileMateCache_cost = 0; // 没有包括其他level缓存的信息
      double level_0 = max_edge_num * sizeof(Index); // index
      fileMateCache_cost = level_0 * memTable_list_.size();

      // compaction
      double compact_cost = 1280 * (1 << 20);

      // lock
      double lock_cost = 0;
      lock_cost = vertex_lock_count_ * sizeof(Futex)
                + vertex_lock_count_ * sizeof(RWLock_t);

      // level_index
      double level_index_cost = 0;
#if !defined(MMAP_LEVEL_INDEX) && !defined(MMAP_DIFF_SIZE_LEVEL_INDEX) && \
    !defined(MMAP_COLD_HOT_LEVEL_INDEX)
      level_index_cost = vid_to_mullevelIndex_.allocated_bytes();
#else
      level_index_cost = max_vertex_num * sizeof(MulLevelIndex);
#endif

      all_mem_cost = memtable_cost
                   + fileMateCache_cost
                   + compact_cost
                   + lock_cost
                   + level_index_cost;
      printf(" @all_mem_cost: %0.3fGB\n", all_mem_cost/1024.0/1024/1024);
      printf("  memtable_cost: %0.3fGB\n", memtable_cost/1024.0/1024/1024);
      printf("  fileMateCache_cost: %0.3fGB\n",
             fileMateCache_cost/1024.0/1024/1024);
      printf("  compact_cost: %0.3fGB\n", compact_cost/1024.0/1024/1024);
      printf("  lock_cost: %0.3fGB\n", lock_cost/1024.0/1024/1024);
      printf("  level_index real cost: %0.3fGB\n", level_index_cost/1024.0/1024/1024);
      std::cout << "-------------------- end count mem cost--------------------"
                << std::endl;
    }
    }

    MemTable* mem_ = memTable_.load(std::memory_order_relaxed);
    if(mem_->GetListLength() > 0) {
      if (FLAGS_richgraph_verbose) {
        std::cout << " save memtable data before close db." << std::endl;
      }
      mem_->save2eSSTable(dataDir, currentTime, fileMetaCache[0]);
      MarkPropertyDeltaTargetPersistent(mem_->GetFid());
    }

    // Saving the active MemTable may schedule compaction.  Do not tear down
    // metadata or mapped files until that work and any resulting delta merge
    // have completed.
    try {
      WaitBackgroundIdle();
    } catch (const std::exception& error) {
      std::cerr << "[LSMStore] post-flush shutdown barrier failed: "
                << error.what() << std::endl;
    }

    if (property_delta_store_ != nullptr) {
      std::string delta_error;
      if (!property_delta_store_->WaitForIdle(&delta_error)) {
        std::cerr << "[PROPERTY_DELTA_SHUTDOWN_ERROR] " << delta_error
                  << std::endl;
      }
      property_delta_store_.reset();
    }


    // Persist the current storage version on every clean shutdown. A reopened
    // database may flush or compact additional files; keeping the original
    // file.info would make the next restart reference files already retired
    // by that compaction.
    {
      if (FLAGS_support_mulversion == true) {
        // update version
        l0_versionset_->VersionLock();
        fileMetaCache[0] = l0_versionset_->GetCurrent()->GetLevel0Files();
        l0_versionset_->VersionUnLock();
      }

      std::string filename = dataDir + "/file.info";
      if (FLAGS_richgraph_verbose) {
        std::cout << "write to: " << filename << std::endl;
      }
      std::ofstream outFile(filename, std::ios::binary | std::ios::out);

      int levelNum = fileMetaCache.size();
      int file_num = 0;
      const VertexId_t max_vertex_num = vertex_id_.load();
      outFile.write(reinterpret_cast<const char *>(&max_vertex_num),
                    sizeof(max_vertex_num));
      outFile.write(reinterpret_cast<char *>(&levelNum), sizeof(levelNum));
      size_t offset_part_size = 0;
      for(int i = 0; i < levelNum; ++i) {
        int level_file_num = fileMetaCache[i]->size();
        outFile.write(reinterpret_cast<char *>(&level_file_num),
                      sizeof(level_file_num));
        for(auto it = fileMetaCache[i]->begin();
            it != fileMetaCache[i]->end(); ++it) {
          offset_part_size += (*it)->header.index_size * sizeof(Index);
          outFile.write(reinterpret_cast<char *>(&(*it)->header.timeStamp),
                        sizeof(uint64_t));
          ++file_num;
        }
      }
      const uint64_t sequence_magic = kFileInfoSequenceMagic;
      const SequenceNumber_t next_topology_sequence =
          next_topology_sequence_.load(std::memory_order_acquire);
      outFile.write(reinterpret_cast<const char*>(&sequence_magic),
                    sizeof(sequence_magic));
      outFile.write(reinterpret_cast<const char*>(&next_topology_sequence),
                    sizeof(next_topology_sequence));
      if (FLAGS_richgraph_verbose) {
        std::cout << "@file_num: " << file_num << std::endl;
        std::cout << "@offset_part_size: " << offset_part_size << std::endl;
      }
      outFile.close();
    }

    // save file information of every level
    {
      if (FLAGS_richgraph_verbose && FLAGS_support_mulversion == true) {
        std::cout << "----------------- information of every level---------------"
                  << std::endl;
        std::cout << "\ncurr_level0_version:" << std::endl;
        int file_num = 0;
        for (auto filemeta : *l0_versionset_->GetCurrent()->GetLevel0Files()) {
          std::cout << " level=" << 0
                    << " minkey=" << filemeta->header.minKey
                    << " maxkey=" << filemeta->header.maxKey
                    << " fid=" << filemeta->header.timeStamp
                    << " ref=" << filemeta->Getref()
                    << std::endl;
          ++file_num;
        }
        std::cout << " level_id=" << 0
                  << " file_num=" << file_num
                  << std::endl;
      }
    }
    // Snapshot and Version objects carry intrusive references to SST metadata.
    // Release those references before destroying the base metadata objects.
    // Calling a normal member's destructor explicitly here used to result in
    // SuperVersion being destroyed twice.
    std::unordered_set<SSTableCache*> caches_to_destroy;
    for (auto* level : fileMetaCache) {
      if (level == nullptr) {
        continue;
      }
      caches_to_destroy.insert(level->begin(), level->end());
    }
    auto* version_level_zero =
        l0_versionset_ == nullptr
            ? nullptr
            : l0_versionset_->GetCurrent()->GetLevel0Files();

    // 释放 CSR 读快照；GraphDb 析构要求外部读者已全部退出。
    std::atomic_store(&csr_read_snapshot_,
                      std::shared_ptr<const CSRReadSnapshot>{});

    {
      std::unique_lock lock(sv_.vm_rw_mtx);
      sv_.version_memtable.reset();
    }
    delete l0_versionset_;
    l0_versionset_ = nullptr;

    for (auto* cache : caches_to_destroy) {
      if (cache == nullptr) {
        continue;
      }
      if (cache->Getref() == 1) {
        // The remaining reference is the fileMetaCache ownership.  Delete the
        // in-memory metadata only; persisted SST files must survive shutdown.
        delete cache;
      } else {
        std::cerr << "[LSMStore] SST metadata still referenced during shutdown: "
                  << cache->header.timeStamp
                  << " refs=" << cache->Getref() << std::endl;
      }
    }

    for (auto* level : fileMetaCache) {
      // Level zero aliases Version::l0_filemetas_ in multi-version mode and
      // was already destroyed with VersionSet.  Other vectors were allocated
      // by LSMStore and remain its responsibility.
      if (level != nullptr && level != version_level_zero) {
        delete level;
      }
    }
    fileMetaCache.clear();

    for (auto tb : memTable_list_) {
      delete tb;
    }
    memTable_list_.clear();

    // clear file handle
    for (auto& f_handle : file_handle_cache_) {
      f_handle.second->close();
      delete f_handle.second;
      if (FLAGS_richgraph_verbose) {
        std::cout << " close file handle: " << f_handle.first << std::endl;
      }
    }

    auto futex_allocater =
        std::allocator_traits<decltype(array_allocator)>::rebind_alloc<Futex>(
            array_allocator);
    for (size_t lock_id = 0; lock_id < vertex_lock_count_; ++lock_id) {
      std::allocator_traits<decltype(futex_allocater)>::destroy(
          futex_allocater, vertex_futexes_ + lock_id);
    }
    futex_allocater.deallocate(vertex_futexes_, vertex_lock_count_);

    auto rwlock_allocater = std::allocator_traits<
        decltype(array_allocator)>::rebind_alloc<RWLock_t>(array_allocator);
    for (size_t lock_id = 0; lock_id < vertex_lock_count_; ++lock_id) {
      std::allocator_traits<decltype(rwlock_allocater)>::destroy(
          rwlock_allocater, vertex_rwlocks_ + lock_id);
    }
    rwlock_allocater.deallocate(vertex_rwlocks_, vertex_lock_count_);

    auto vertex_max_level_allocater =
        std::allocator_traits<decltype(array_allocator)>::rebind_alloc<Level_t>(
            array_allocator);
    vertex_max_level_allocater.deallocate(vertex_max_level_, max_vertex_num_);

    if (FLAGS_support_mulversion == false) {
      auto levelIndex_allocater = std::allocator_traits<
          decltype(array_allocator)>::rebind_alloc<LevelIndex>(array_allocator);
      levelIndex_allocater.deallocate(vid_to_levelIndex_,
                                      max_vertex_num_ * LEVEL_INDEX_SIZE);
    } else {
    }

#ifdef DEBUG_COST
    std::cout << "\nCount Time:" << std::endl;
    std::cout << "  save2eSSTable_time=" << MemTable::save2eSSTable_time << " sec" << std::endl;
    std::cout << "  put_flush_waite_time=" << MemTable::put_flush_waite_time << " sec" << std::endl;
    std::cout << "  put_memtable_time=" << MemTable::put_memtable_time << " sec" << std::endl;
    std::cout << "  put_memtable_time_per=" << put_memtable_time_per << " sec" << std::endl;
    std::cout << "  put_memtable_time_sum=" << put_memtable_time_sum << " sec" << std::endl;
    std::cout << "  query_time_mem=" << query_time_mem << " sec" << std::endl;
    std::cout << "  query_time_file=" << query_time_file << " sec" << std::endl;
    std::cout << "    query_time_find_index=" << query_time_find_index << " sec" << std::endl;
    std::cout << "    query_time_find_edge=" << query_time_find_edge << " sec" << std::endl;
    std::cout << "    query_time_find_property=" << query_time_find_property << " sec" << std::endl;
    std::cout << "    find_all_iterator=" << find_all_iterator << " sec" << std::endl;
    std::cout << "  compaction_time=" << compaction_time << " sec" << std::endl;
    std::cout << "  get_curr_version_time=" << get_curr_version_time << " sec" << std::endl;
    std::cout << "  iterate_memtable=" << iterate_memtable
              << " sec" << std::endl;
    std::cout << "  iterate_level_0=" << iterate_level_0 << " sec" << std::endl;
    std::cout << "  iterate_level_1=" << iterate_level_1 << " sec" << std::endl;
    std::cout << "  iterate_find_first=" << iterate_find_first << " sec" << std::endl;
    std::cout << "  iterate_check_entry_valid=" << iterate_check_entry_valid << " sec" << std::endl;
    #endif
    if (FLAGS_richgraph_verbose) {
      std::cout << "closed LSMStore." << std::endl;
    }
}

static void Sleep(double t) {
  timespec req;
  req.tv_sec = (int) t;
  req.tv_nsec = (int64_t)(1e9 * (t - (int64_t) t));
  assert(req.tv_nsec >= 0);
  nanosleep(&req, NULL);
}

void LSMStore::put_edge(VertexId_t src, VertexId_t dst,
                        const EdgeProperty_t &s,
                        EdgeInsertMode insert_mode,
                        bool is_out,
                        uint8_t edge_type,
                        SequenceNumber_t explicit_seq) {
  if (insert_mode == EdgeInsertMode::kBidirectional) {
    // 双向插入语义：
    // 1) 写入 (src,dst) 本边；
    // 2) 额外写入 (dst,src) 的反向边，并翻转方向位。
    put_edge(src, dst, s, EdgeInsertMode::kSingle, is_out, edge_type,
             explicit_seq);
    put_edge(dst, src, s, EdgeInsertMode::kSingle, !is_out, edge_type,
             explicit_seq);
    return;
  }

  Marker_t marker = false;  //delete flag：如果是false表示不是删除操作
  if (s == "~DELETED~") {
      marker = true;
  }



#ifdef WRITE_STALL
  AwaitWrite();
#endif

  int64_t remain_capacity = 0;

  MemTable* old_mem = memTable_.load(std::memory_order_acquire);
  remain_capacity = __sync_fetch_and_sub(&old_mem->remain_capacity, 1);
  auto timeout = std::chrono::milliseconds(10);
  while (remain_capacity <= 0) {
    std::unique_lock<std::mutex> locker(memtable_insert_mux_);
    old_mem = memTable_.load(std::memory_order_acquire);
    remain_capacity = __sync_fetch_and_sub(&old_mem->remain_capacity, 1);
    if (remain_capacity > 0) {
      break;
    }
    memtable_cv_.wait_for(locker, timeout);  // 可能在解锁，等待之间有线程发起了唤醒，那么它就错过了
    old_mem = memTable_.load(std::memory_order_acquire);
    remain_capacity = __sync_fetch_and_sub(&old_mem->remain_capacity, 1);
  }

  assert(remain_capacity > 0);
  size_t id = old_mem->GetMaxEdgeNum() - remain_capacity;
  SequenceNumber_t seq =
      explicit_seq >= 0 ? explicit_seq : old_mem->GetStartTime() + id;
  ObserveTopologySequence(seq);

  // insert edge
  old_mem->put_edge(src, dst, s, marker, seq, id, is_out, edge_type);    //EdgeProperty_t &s

  if (remain_capacity > 1) {
    return ;
  }

  #ifdef DEBUG_COST
  MemTable* mem = memTable_.load(std::memory_order_relaxed);
  std::cout << " ---memTable.edge_num=" << mem->GetListLength() << std::endl;
  #endif

  // switch memtable to im_mem

  {
    // count insert memtable time
    #ifdef DEBUG_COST
    if (put_memtable_time_per < 0) {
      put_memtable_time_per = utils::GetCurrentTime();
    } else {
      double t = utils::GetCurrentTime() - put_memtable_time_per;
      std::cout << "------------insert_time=" << t << std::endl;
      put_memtable_time_per = utils::GetCurrentTime();
      put_memtable_time_sum += t;
    }
    #endif
  }

  MemTable* null_memtable = get_newmemTable();
  null_memtable->SetStartTime(
      automic_get_global_seq(null_memtable->GetMaxEdgeNum()));
  null_memtable->SetLive(true);
  null_memtable->SetFid(__sync_fetch_and_add(&currentTime, 1));
  memTable_.store(null_memtable, std::memory_order_release);

#ifdef DEL_EDGE_SEPARATE
{
  // TODO(correctness): Deletion metadata must outlive every reader that can
  // still observe the corresponding SST generation.
  del_record_manager_.clean();
}
#endif

  memtable_cv_.notify_all();


  l0_versionset_->VersionLock();
  #ifndef VM_RW_LOCK
  std::shared_ptr<VersionAndMemTable>
                  old_vm = std::atomic_load(&sv_.version_memtable);
  #else
  std::shared_ptr<VersionAndMemTable> old_vms;
  {
    std::shared_lock r_lock(sv_.vm_rw_mtx);
    old_vms = sv_.version_memtable;
  }
  #endif
  std::shared_ptr<VersionAndMemTable> new_vms
      = std::make_shared<VersionAndMemTable>();
  new_vms->batch_insert_tb(old_vms->menTables);
  new_vms->set_vs(old_vms->current_);
  new_vms->insert_tb(null_memtable);
  #ifndef VM_RW_LOCK
  std::atomic_store(&sv_.version_memtable, new_vm);
  #else
  {
    std::unique_lock w_lock(sv_.vm_rw_mtx);
    sv_.version_memtable = new_vms;
  }
  #endif
  global_version_id_.fetch_add(1, std::memory_order_acquire);
  l0_versionset_->VersionUnLock();

  bool open_extra_compression_thread = true;
	  if (open_extra_compression_thread) {
	    BeginBackgroundFlushJob();
	    if (use_csr_disk_) {
	      csr_flush_jobs_inflight_.fetch_add(1, std::memory_order_acq_rel);
	    }
	    auto save = [this] (MemTable* immemTable) {
	      struct BackgroundJobGuard {
	        LSMStore* store;
	        ~BackgroundJobGuard() { store->MarkBackgroundFlushJobDone(); }
	      } background_guard{this};
	      const ScopedFixedPropertyLayout property_layout(&sub_property_lengths_);
	      const ScopedDbPathOverride db_path_override(&dataDir);
	      immemTable->save2eSSTable(dataDir, currentTime, fileMetaCache[0]);
      MarkPropertyDeltaTargetPersistent(immemTable->GetFid());
      recycle_memTable(immemTable);
      if (use_csr_disk_) {
        // CSR 单层模式：每次 flush 后重建一次单层 CSR 视图。
        MaybeRebuildCSRFromDisk();
        MarkCSRFlushJobDone();
      } else {
        compactor_.MaybeScheduleCompaction();
      }
    };
    worker_pool.enqueue(save, old_mem);
  } else {
    const ScopedFixedPropertyLayout property_layout(&sub_property_lengths_);
    const ScopedDbPathOverride db_path_override(&dataDir);
    if (use_csr_disk_) {
      csr_flush_jobs_inflight_.fetch_add(1, std::memory_order_acq_rel);
    }
    old_mem->save2eSSTable(dataDir, currentTime, fileMetaCache[0]);
    MarkPropertyDeltaTargetPersistent(old_mem->GetFid());
    recycle_memTable(old_mem);
    if (use_csr_disk_) {
      MaybeRebuildCSRFromDisk();
      MarkCSRFlushJobDone();
    } else {
      compactor_.MaybeScheduleCompaction();
    }
  }
}

void LSMStore::update_edge(VertexId_t src, VertexId_t dis, const EdgeProperty_t &s,
                           Marker_t mk, SequenceNumber_t sq, bool is_out,
                           uint8_t edge_type){
  if (mk) {
    put_edge(src, dis, "~DELETED~", EdgeInsertMode::kSingle,
             is_out, edge_type, sq);
  } else {
    put_edge(src, dis, s, EdgeInsertMode::kSingle, is_out, edge_type, sq);
  }
}

Status LSMStore::find_edge(VertexId_t src, VertexId_t dst,
                           std::string* property, SSTableCache* it,
                           int property_id, bool is_out, uint8_t edge_type) {
  #ifdef DEBUG_COST
    std::chrono::steady_clock::time_point query_time_find_indext1 = std::chrono::steady_clock::now();
  #endif
  Status rs = Status::kNotFound;
  int pos = it->get(src, dst);
  #ifdef DEBUG_COST
    std::chrono::steady_clock::time_point query_time_find_indext2 = std::chrono::steady_clock::now();
    std::chrono::duration<double> query_time_find_indextime_span = std::chrono::duration_cast<std::chrono::duration<double>>(query_time_find_indext2 - query_time_find_indext1);
    write_add(&query_time_find_index, query_time_find_indextime_span.count());
  #endif
  if(pos < 0)
      return rs;

  uint32_t offset = (it->indexes)[pos].offset;
  uint32_t next_offset = (it->indexes)[pos+1].offset;


  if (FLAGS_OPEN_SSTDATA_CACHE == true) {
    rs = find_edge_from_sstdata_cache(src, dst, offset, next_offset,
                                      it->header.timeStamp, property, property_id,
                                      is_out, edge_type);
  } else {
    std::ifstream* file;
    if (file_handle_cache_.find(it->path) != file_handle_cache_.end()) {
      file = file_handle_cache_[it->path];
    } else {
      file = new std::ifstream(it->path, std::ios::binary|std::ios::in);
      file_handle_cache_.emplace(it->path, file);
    }
    if(!file) {
      printf("Lost file: %s", (it->path).c_str());
      exit(-1);
    }

    rs = find_edge_from_file_with_cache(dst, offset, next_offset, file,
                                        it->path, property, property_id, is_out,
                                        edge_type);
  }
  return rs;
}

Status LSMStore::find_edge(VertexId_t src, VertexId_t dst,
                           SequenceNumber_t& seq, SSTableCache* it) {
  #ifdef DEBUG_COST
    std::chrono::steady_clock::time_point query_time_find_indext1 = std::chrono::steady_clock::now();
  #endif
  Status rs = Status::kNotFound;
  int pos = it->get(src, dst);
  #ifdef DEBUG_COST
    std::chrono::steady_clock::time_point query_time_find_indext2 = std::chrono::steady_clock::now();
    std::chrono::duration<double> query_time_find_indextime_span = std::chrono::duration_cast<std::chrono::duration<double>>(query_time_find_indext2 - query_time_find_indext1);
    write_add(&query_time_find_index, query_time_find_indextime_span.count());
  #endif
  if(pos < 0)
      return rs;

  uint32_t offset = (it->indexes)[pos].offset;
  uint32_t next_offset = (it->indexes)[pos+1].offset;


  if (FLAGS_OPEN_SSTDATA_CACHE == true) {
    rs = find_edge_from_sstdata_cache(src, dst, offset, next_offset,
                                      it->header.timeStamp, seq);
  } else {
    std::ifstream* file;
    if (file_handle_cache_.find(it->path) != file_handle_cache_.end()) {
      file = file_handle_cache_[it->path];
    } else {
      file = new std::ifstream(it->path, std::ios::binary|std::ios::in);
      file_handle_cache_.emplace(it->path, file);
    }
    if(!file) {
      printf("Lost file: %s", (it->path).c_str());
      exit(-1);
    }

    rs = find_edge_from_file_with_cache(dst, offset, next_offset, file,
                                        it->path, seq);
  }
  return rs;
}

Status LSMStore::find_edge_in_memtable(VertexId_t src, VertexId_t dst,
  std::string* property, int property_id, bool is_out, uint8_t edge_type){

    get_superversion(local_sv_);
    
    Status rs = Status::kNotFound;
    for(auto tb : local_sv_.version_memtable->menTables) {
      FileId_t fid = INVALID_File_ID;
      SequenceNumber_t base_sequence = MAX_SEQ_ID;
      rs = tb->get(src, dst, is_out, edge_type, fid, base_sequence);
      if(rs != Status::kNotFound) {
        if (rs == Status::kOk) {
          const auto view = LoadPropertyDeltaView(fid, property_id);
          if (view != nullptr &&
              view->Lookup(src,
                           dst,
                           base_sequence,
                           is_out,
                           edge_type,
                           kLatestPropertyCommit,
                           property)) {
            local_sv_.version_memtable = nullptr;
            return Status::kOk;
          }
          std::string payload;
          rs = tb->get(src, dst, is_out, edge_type, &payload);
          if (rs == Status::kOk) {
            assert(property_id < GetActiveSubPropertyNum());
            *property = ExtractSubProperty(payload, property_id);
          }
        }
        local_sv_.version_memtable = nullptr;
        return rs;
      }

    }
    return rs;
  }

Status LSMStore::find_edge_in_SStableCache(VertexId_t src, VertexId_t dst,
    std::string* property, int property_id, bool is_out, uint8_t edge_type){
      Status rs = Status::kNotFound;
      // Search level 0 before the indexed nonzero levels.
    get_superversion(local_sv_);
    
    vertex_rwlocks_[VertexLockSlot(src, vertex_lock_count_)].ReadLock();
    memcpy(&local_sv_.findex, &vid_to_mullevelIndex_[src], sizeof(MulLevelIndex));
    vertex_rwlocks_[VertexLockSlot(src, vertex_lock_count_)].ReadUnlock();
        
        FileId_t min_level_0_fid = local_sv_.findex.get_min_level_0_fid();
        bool found = false;
        for (auto it : *(local_sv_.version_memtable->current_->GetLevel0Files())) {
          if (it->header.timeStamp >= 0
              && src <= it->header.maxKey && src >= it->header.minKey) {
            rs = find_edge_in_lonely_SStableCache(src, dst, property, property_id,
                                                  it, is_out, edge_type);
            if (rs == Status::kNotFound) { // not found in the edge list of this file
              continue;
            } else {
              local_sv_.version_memtable = nullptr;
              return rs;
            }
          }
        }
      

      
      rs = find_edge_by_levelindex(src, dst, property, local_sv_, property_id,
                                   is_out, edge_type);
      local_sv_.version_memtable = nullptr;
      return rs;
    }

Status LSMStore::find_edge_in_lonely_SStableCache(VertexId_t src, VertexId_t dst,
          std::string* property, int property_id, SSTableCache* it,
          bool is_out, uint8_t edge_type){

    Status rs = Status::kNotFound;
    int pos = it->get(src, dst);

    if(pos < 0)
      return rs;

    uint32_t offset = (it->indexes)[pos].offset;
    uint32_t next_offset = (it->indexes)[pos+1].offset;

    SSTDataCache* sstcache = sstdata_manager_.get_data(it->header.timeStamp);
    
    char *body_buffer = sstcache->GetEdgeData();
    const EdgeLookupResult lookup = FindEdgeByDstDirectionAndType(
        body_buffer, offset, next_offset, dst, is_out, edge_type);
    rs = lookup.status;
    const int low = lookup.body_offset;
    const auto delta_view = LoadPropertyDeltaView(
        it->header.timeStamp, property_id);
    if (rs == Status::kOk && delta_view != nullptr &&
        delta_view->Lookup(src,
                           dst,
                           get_seq(body_buffer + low),
                           is_out,
                           edge_type,
                           kLatestPropertyCommit,
                           property)) {
      return Status::kOk;
    }
    // Fall back to the base SST property.
    if (rs == Status::kOk) {
      const std::byte* property_buffer = delta_view == nullptr
          ? reinterpret_cast<const std::byte*>(
                sstcache->GetPropertyData(property_id))
          : delta_view->BaseDataOr(sstcache->GetPropertyData(property_id));

      const auto edge_ordinal = GetEdgeOrdinalFromBodyOffset(low);
      const auto property_offset = GetSubPropertyOffsetByEdgeOrdinal(edge_ordinal, property_id);
      const uint32_t length = GetSubPropertyFixedLength(property_id);


      assert(length < 1024);
      if (length > 0) {
        ReadSubPropertyFromSlot(
            reinterpret_cast<const char*>(property_buffer + property_offset),
            property_id,
            property);
      }
    }
    return rs;
 }

Status LSMStore::find_edge_by_levelindex(VertexId_t src, VertexId_t dst,
                                         std::string* property,
                                         SuperVersion& local_sv,
                                         int property_id, bool is_out,
                                         uint8_t edge_type) {
  Status rs = Status::kNotFound;

  uint32_t fileID = 0;
  uint32_t offset = 0;
  uint32_t next_offset = 0;

  MulLevelIndex& findex = local_sv.findex;
  
  for (int levelID = 0; levelID < LEVEL_INDEX_SIZE; levelID++) {
    fileID = findex.get_fileID(levelID);

      
    
    if (fileID == INVALID_File_ID) {
      continue;
    }
    offset = findex.get_offset(levelID);
    next_offset = findex.get_next_offset(levelID);
    assert(next_offset >= offset);
    rs = find_edge_from_sstdata_cache(src, dst, offset, next_offset, fileID,
                                      property, property_id, is_out, edge_type);
    if (rs != Status::kNotFound) { // not found in the edge list of this file
      break;
    }
  }

  return rs;
}

Status LSMStore::find_edge_by_levelindex(VertexId_t src, VertexId_t dst,
                                         FileId_t& fid, SequenceNumber_t& seq,
                                         SuperVersion& local_sv) {
  #ifdef DEBUG_COST
    std::chrono::steady_clock::time_point query_time_find_indext1 = std::chrono::steady_clock::now();
  #endif
  Status rs = Status::kNotFound;

  uint32_t fileID = 0;
  uint32_t offset = 0;
  uint32_t next_offset = 0;

  MulLevelIndex& findex = local_sv.findex;

  for (int levelID = 0; levelID < LEVEL_INDEX_SIZE; levelID++) {
    if (FLAGS_support_mulversion== false) {
      int index_id = src * LEVEL_INDEX_SIZE + levelID;
      LevelIndex& findex = vid_to_levelIndex_[index_id];
      fileID = findex.get_fileID();

      if (fileID == INVALID_File_ID) {
        continue;
      }

      offset = findex.get_offset();
      next_offset = findex.get_next_offset();
      assert(next_offset >= offset);
    } else {
      fileID = findex.get_fileID(levelID);

      if (fileID == INVALID_File_ID) {
        continue;
      }

      offset = findex.get_offset(levelID);
      next_offset = findex.get_next_offset(levelID);
      assert(next_offset >= offset);
    }

    fid = fileID;

    if (FLAGS_OPEN_SSTDATA_CACHE == true) {
#ifdef USE_DIRECTEDIO
      rs = find_edge_from_file_with_cache_by_directed_IO(src, dst, offset, 
                                                      next_offset, fileID, seq);
#else
      rs = find_edge_from_sstdata_cache(src, dst, offset, next_offset, fileID, seq);
#endif
    } else {
      std::string path = dataDir + "/" + std::to_string(fileID) + ".sst";
      std::ifstream* file;
      if (file_handle_cache_.find(path) != file_handle_cache_.end()) {
        file = file_handle_cache_[path];
      } else {
        file = new std::ifstream(path, std::ios::binary|std::ios::in);
        file_handle_cache_.emplace(path, file);
      }
      if(!file) {
        printf("Lost file: %s", path.c_str());
        exit(-1);
      }

      rs = find_edge_from_file_with_cache(dst, offset, next_offset, file, path,
                                          seq);
    }
    if (rs != Status::kNotFound) { // not found in the edge list of this file
      break;
    }
  }

  #ifdef DEBUG_COST
    std::chrono::steady_clock::time_point query_time_find_indext2 = std::chrono::steady_clock::now();
    std::chrono::duration<double> query_time_find_indextime_span = std::chrono::duration_cast<std::chrono::duration<double>>(query_time_find_indext2 - query_time_find_indext1);
    write_add(&query_time_find_index, query_time_find_indextime_span.count());
  #endif

  return rs;
}

Status LSMStore::find_edge_by_levelindex(VertexId_t src, VertexId_t dst,
                                         FileId_t& fid, SequenceNumber_t& seq,
                                         SuperVersion& local_sv, bool is_out,
                                         uint8_t edge_type) {
#ifdef DEBUG_COST
  std::chrono::steady_clock::time_point query_time_find_indext1 = std::chrono::steady_clock::now();
#endif
  Status rs = Status::kNotFound;

  uint32_t fileID = 0;
  uint32_t offset = 0;
  uint32_t next_offset = 0;

  MulLevelIndex& findex = local_sv.findex;
  for (int levelID = 0; levelID < LEVEL_INDEX_SIZE; levelID++) {
    if (FLAGS_support_mulversion== false) {
      int index_id = src * LEVEL_INDEX_SIZE + levelID;
      LevelIndex& level_index = vid_to_levelIndex_[index_id];
      fileID = level_index.get_fileID();
      if (fileID == INVALID_File_ID) {
        continue;
      }
      offset = level_index.get_offset();
      next_offset = level_index.get_next_offset();
      assert(next_offset >= offset);
    } else {
      fileID = findex.get_fileID(levelID);
      if (fileID == INVALID_File_ID) {
        continue;
      }
      offset = findex.get_offset(levelID);
      next_offset = findex.get_next_offset(levelID);
      assert(next_offset >= offset);
    }

    if (FLAGS_OPEN_SSTDATA_CACHE == true) {
      rs = find_edge_from_sstdata_cache(src, dst, offset, next_offset,
                                        fileID, seq, is_out, edge_type);
    } else {
      std::string path = dataDir + "/" + std::to_string(fileID) + ".sst";
      std::ifstream* file;
      if (file_handle_cache_.find(path) != file_handle_cache_.end()) {
        file = file_handle_cache_[path];
      } else {
        file = new std::ifstream(path, std::ios::binary|std::ios::in);
        file_handle_cache_.emplace(path, file);
      }
      if(!file) {
        printf("Lost file: %s", path.c_str());
        exit(-1);
      }
      rs = find_edge_from_file_with_cache(dst, offset, next_offset,
                                          file, path, seq);
    }
    if (rs != Status::kNotFound) {
      fid = fileID;
      break;
    }
  }

#ifdef DEBUG_COST
  std::chrono::steady_clock::time_point query_time_find_indext2 = std::chrono::steady_clock::now();
  std::chrono::duration<double> query_time_find_indextime_span =
      std::chrono::duration_cast<std::chrono::duration<double>>(query_time_find_indext2 - query_time_find_indext1);
  write_add(&query_time_find_index, query_time_find_indextime_span.count());
#endif

  return rs;
}

void LSMStore::get_superversion(SuperVersion& local_sv) {

  // way3: atomic
  #ifndef VM_RW_LOCK
  local_sv.version_memtable = std::atomic_load(&sv_.version_memtable);
  #else
  {
    std::shared_lock r_lock(sv_.vm_rw_mtx);
    local_sv.version_memtable = sv_.version_memtable;
  }
  #endif
}

void LSMStore::get_superversion() {
  // way1: local
  SequenceNumber_t v_id = global_version_id_.load(memory_order_relaxed);
  if (local_version_id_ < v_id) {
    local_version_id_ = v_id;
    #ifndef VM_RW_LOCK
    local_sv_.version_memtable = std::atomic_load(&sv_.version_memtable);
    #else
    {
      std::shared_lock r_lock (sv_.vm_rw_mtx);
      local_sv_.version_memtable = sv_.version_memtable;
    }
    #endif
  }

  // way2: atomic
}


/**
 * Returns the property of the given edge.
 * An empty string indicates not found.
 */
Status LSMStore::get_edge(VertexId_t src, VertexId_t dst,
                          std::string* property, int property_id, bool is_out,
                          uint8_t edge_type)
{


    if (FLAGS_support_mulversion== true) {
      get_superversion(local_sv_); // 这里发生在读写期间，如果使用本地version它不释放，将导致memtable一直被引用，而无法释放，系统将阻塞, 因此要么自己构建一个局部变量local_sv_, 或者使用全局变量然后在返回的时候手动释放占用的引用，当然这依然无法充分利用到local_version的本来的好处-即不需要每次都获取新的version,仅仅在flush时更新。
    }

    #ifdef DEBUG_COST
      std::chrono::steady_clock::time_point t1 = std::chrono::steady_clock::now();
    #endif

    Status rs = kNotFound;

    if (FLAGS_support_mulversion== false) {
      MemTable* mem = memTable_.load(std::memory_order_acquire);
      Status rs = mem->get(src, dst, is_out, edge_type, property);
      if(rs != Status::kNotFound) {

          return rs;
      }
    } else {
      bool found = false;
      for (auto tb : local_sv_.version_memtable->menTables) {
        if (found == false) {
          rs = tb->get(src, dst, is_out, edge_type, property);
          if(rs != Status::kNotFound) {
            found = true;
          }
        }
      }
      if (rs != Status::kNotFound) {
        local_sv_.version_memtable = nullptr;
        return rs;
      }
    }

    #ifdef DEBUG_COST
      std::chrono::steady_clock::time_point t2 = std::chrono::steady_clock::now();
      std::chrono::duration<double> time_span = std::chrono::duration_cast<std::chrono::duration<double>>(t2 - t1);
      write_add(&query_time_mem, time_span.count());
    #endif

    // TODO(correctness): Include immutable memtables in this read path.


    // CSR 单层模式下，磁盘阶段直接走 CSR 查询路径。
    if (use_csr_disk_) {
      if (FLAGS_support_mulversion == true) {
        local_sv_.version_memtable = nullptr;
      }
      return find_edge_in_csr_disk(src, dst, property, property_id,
                                   is_out, edge_type);
    }

    // When cannot find in memTable, try to find in SSTables.
    // level-0
    if (FLAGS_support_mulversion== false) {
      int levelNum = fileMetaCache.size();
      for (auto it = fileMetaCache[0]->begin();
           it != fileMetaCache[0]->end(); ++it) {
        if(src <= ((*it)->header).maxKey && src >= ((*it)->header).minKey) {
          rs = find_edge(src, dst, property, *it, property_id, is_out, edge_type);
          if (rs == Status::kNotFound) { // not found in the edge list of this file
            continue;
          } else {
            return rs;
          }
        }
      }
    } else {
      {
        vertex_rwlocks_[VertexLockSlot(src, vertex_lock_count_)].ReadLock();
#ifdef MMAP_DIFF_SIZE_LEVEL_INDEX
        int level_num = vid_to_mullevelIndex_.get_level_num_by_vid(src);
        if (level_num > 0) {
          char* ptr = vid_to_mullevelIndex_.get_level_index_ptr_by_vid(src);
          MulLevelIndexWithDiffSize level_index = MulLevelIndexWithDiffSize(ptr, level_num);
          level_index.copy_to_old_index(local_sv_.findex);
        } else {
          local_sv_.findex.set_min_level_0_fid(0);
          local_sv_.findex.set_level_num(0);
        }
#elif defined(MMAP_COLD_HOT_LEVEL_INDEX)
        VertexId_t array_id= 0;
        int old_level_num = 0;
        char* ptr = vid_to_mullevelIndex_.
            get_level_index_ptr_by_vid(src, old_level_num, array_id);
        if (ptr != nullptr) {
          MulLevelIndexWithDiffSize level_index 
              = MulLevelIndexWithDiffSize(ptr, old_level_num);
          level_index.copy_to_old_index(local_sv_.findex);
        } else {
          local_sv_.findex.set_min_level_0_fid(0);
          local_sv_.findex.set_level_num(0);
        }
#else
        memcpy(&local_sv_.findex, &vid_to_mullevelIndex_[src], sizeof(MulLevelIndex));
#endif
        vertex_rwlocks_[VertexLockSlot(src, vertex_lock_count_)].ReadUnlock();
      }
      FileId_t min_level_0_fid = local_sv_.findex.get_min_level_0_fid();
      bool found = false;
      for (auto it : *(local_sv_.version_memtable->current_->GetLevel0Files())) {
        if (it->header.timeStamp >= min_level_0_fid
            && src <= it->header.maxKey && src >= it->header.minKey) {
          rs = find_edge(src, dst, property, it, property_id, is_out, edge_type);
          if (rs == Status::kNotFound) { // not found in the edge list of this file
            continue;
          } else {
            local_sv_.version_memtable = nullptr;
            return rs;
          }
        }
      }
    }

    // level>=1
    local_sv_.version_memtable = nullptr;
    rs = find_edge_by_levelindex(src, dst, property, local_sv_, property_id,
                                 is_out, edge_type);
    return rs;
}

Status LSMStore::GetEdge(VertexId_t src, VertexId_t dst, std::string* property,
                         int property_id, bool is_out, uint8_t edge_type){
  // GraphDb resolves property-buffer updates before reaching this shard. The
  // storage engine searches the active memtables before its disk layout.
  Status rs = find_edge_in_memtable(src, dst, property, property_id, is_out,
                                    edge_type);
  if(rs != Status::kNotFound){
    return rs;
  }
  if (use_csr_disk_) {
    rs = find_edge_in_csr_disk(src, dst, property, property_id,
                               is_out, edge_type);
  } else {
    rs = find_edge_in_SStableCache(src, dst, property, property_id,
                                   is_out, edge_type);
  }
  return rs;
}

Status LSMStore::LocateEdge(VertexId_t src, VertexId_t dst,
                            LSMEdgeLocation* location,
                            bool is_out, uint8_t edge_type) {
  if (location == nullptr) {
    return Status::kNotFound;
  }
  *location = LSMEdgeLocation{};

  if (FLAGS_support_mulversion == true) {
    get_superversion(local_sv_);
  }

  Status rs = Status::kNotFound;
  FileId_t fid = INVALID_File_ID;
  SequenceNumber_t seq = MAX_SEQ_ID;

  if (FLAGS_support_mulversion == false) {
    MemTable* mem = memTable_.load(std::memory_order_acquire);
    rs = mem->get(src, dst, is_out, edge_type, fid, seq);
    if (rs != Status::kNotFound) {
      location->in_memtable = true;
      location->target_id = fid;
      location->sequence = seq;
      return rs;
    }
  } else {
    for (auto tb : local_sv_.version_memtable->menTables) {
      rs = tb->get(src, dst, is_out, edge_type, fid, seq);
      if (rs != Status::kNotFound) {
        local_sv_.version_memtable = nullptr;
        location->in_memtable = true;
        location->target_id = fid;
        location->sequence = seq;
        return rs;
      }
    }
  }

  if (use_csr_disk_) {
    if (FLAGS_support_mulversion == true) {
      local_sv_.version_memtable = nullptr;
    }
    rs = find_edge_seq_in_csr_disk(src, dst, fid, seq);
    if (rs != Status::kNotFound) {
      location->in_memtable = false;
      location->target_id = fid;
      location->sequence = seq;
    }
    return rs;
  }

  if (FLAGS_support_mulversion == false) {
    for (auto it = fileMetaCache[0]->begin();
         it != fileMetaCache[0]->end(); ++it) {
      if(src <= ((*it)->header).maxKey && src >= ((*it)->header).minKey) {
        const int pos = (*it)->get(src, dst);
        if (pos < 0) {
          continue;
        }
        rs = find_edge_from_sstdata_cache(src, dst,
                                          (*it)->indexes[pos].offset,
                                          (*it)->indexes[pos + 1].offset,
                                          ((*it)->header).timeStamp,
                                          seq,
                                          is_out,
                                          edge_type);
        if (rs != Status::kNotFound) {
          location->in_memtable = false;
          location->target_id = ((*it)->header).timeStamp;
          location->sequence = seq;
          return rs;
        }
      }
    }
  } else {
    {
      vertex_rwlocks_[VertexLockSlot(src, vertex_lock_count_)].ReadLock();
#ifdef MMAP_DIFF_SIZE_LEVEL_INDEX
      int level_num = vid_to_mullevelIndex_.get_level_num_by_vid(src);
      if (level_num > 0) {
        char* ptr = vid_to_mullevelIndex_.get_level_index_ptr_by_vid(src);
        MulLevelIndexWithDiffSize level_index = MulLevelIndexWithDiffSize(ptr, level_num);
        level_index.copy_to_old_index(local_sv_.findex);
      } else {
        local_sv_.findex.set_min_level_0_fid(0);
        local_sv_.findex.set_level_num(0);
      }
#elif defined(MMAP_COLD_HOT_LEVEL_INDEX)
      VertexId_t array_id= 0;
      int old_level_num = 0;
      char* ptr = vid_to_mullevelIndex_.
          get_level_index_ptr_by_vid(src, old_level_num, array_id);
      if (ptr != nullptr) {
        MulLevelIndexWithDiffSize level_index =
            MulLevelIndexWithDiffSize(ptr, old_level_num);
        level_index.copy_to_old_index(local_sv_.findex);
      } else {
        local_sv_.findex.set_min_level_0_fid(0);
        local_sv_.findex.set_level_num(0);
      }
#else
      memcpy(&local_sv_.findex, &vid_to_mullevelIndex_[src], sizeof(MulLevelIndex));
#endif
      vertex_rwlocks_[VertexLockSlot(src, vertex_lock_count_)].ReadUnlock();
    }

    const FileId_t min_level_0_fid = local_sv_.findex.get_min_level_0_fid();
    for (auto it : *(local_sv_.version_memtable->current_->GetLevel0Files())) {
      if (it->header.timeStamp >= min_level_0_fid
          && src <= it->header.maxKey && src >= it->header.minKey) {
        const int pos = it->get(src, dst);
        if (pos < 0) {
          continue;
        }
        rs = find_edge_from_sstdata_cache(src, dst,
                                          it->indexes[pos].offset,
                                          it->indexes[pos + 1].offset,
                                          it->header.timeStamp,
                                          seq,
                                          is_out,
                                          edge_type);
        if (rs != Status::kNotFound) {
          local_sv_.version_memtable = nullptr;
          location->in_memtable = false;
          location->target_id = it->header.timeStamp;
          location->sequence = seq;
          return rs;
        }
      }
    }
  }

  if (FLAGS_support_mulversion == true) {
    local_sv_.version_memtable = nullptr;
  }
  rs = find_edge_by_levelindex(src, dst, fid, seq, local_sv_, is_out, edge_type);
  if (rs != Status::kNotFound) {
    location->in_memtable = false;
    location->target_id = fid;
    location->sequence = seq;
  }
  return rs;
}

/**
 * Returns the fid and seq(eid) of the given edge.
 */
Status LSMStore::get_edge(VertexId_t src, VertexId_t dst,
                          FileId_t& fid, SequenceNumber_t& seq) {


    if (FLAGS_support_mulversion== true) {
      get_superversion(local_sv_); // 这里发生在读写期间，如果使用本地version它不释放，将导致memtable一直被引用，而无法释放，系统将阻塞
    }

    #ifdef DEBUG_COST
      std::chrono::steady_clock::time_point t1 = std::chrono::steady_clock::now();
    #endif

    Status rs = kNotFound;

    if (FLAGS_support_mulversion== false) {
      MemTable* mem = memTable_.load(std::memory_order_acquire);
      Status rs = mem->get(src, dst, fid, seq);
      if(rs != Status::kNotFound) {
          return rs;
      }
    } else {
      bool found = false;
      for (auto tb : local_sv_.version_memtable->menTables) {
        if (found == false) {
          rs = tb->get(src, dst, fid, seq);
          if(rs != Status::kNotFound) {
            found = true;
          }
        }
      }
      if (rs != Status::kNotFound) {
        local_sv_.version_memtable = nullptr;
        return rs;
      }
    }

    #ifdef DEBUG_COST
      std::chrono::steady_clock::time_point t2 = std::chrono::steady_clock::now();
      std::chrono::duration<double> time_span = std::chrono::duration_cast<std::chrono::duration<double>>(t2 - t1);
      write_add(&query_time_mem, time_span.count());
    #endif

    // TODO(correctness): Include immutable memtables in this read path.


    // CSR 单层模式下，磁盘阶段直接走 CSR 查询路径。
    if (use_csr_disk_) {
      if (FLAGS_support_mulversion == true) {
        local_sv_.version_memtable = nullptr;
      }
      return find_edge_seq_in_csr_disk(src, dst, fid, seq);
    }

    // When cannot find in memTable, try to find in SSTables.
    // level-0
    if (FLAGS_support_mulversion== false) {
      int levelNum = fileMetaCache.size();
      for (auto it = fileMetaCache[0]->begin();
           it != fileMetaCache[0]->end(); ++it) {
        if(src <= ((*it)->header).maxKey && src >= ((*it)->header).minKey) {
          fid = ((*it)->header).timeStamp;
          rs = find_edge(src, dst, seq, *it);
          if (rs == Status::kNotFound) { // not found in the edge list of this file
            continue;
          } else {
            return rs;
          }
        }
      }
    } else {
      {
        vertex_rwlocks_[VertexLockSlot(src, vertex_lock_count_)].ReadLock();
#ifdef MMAP_DIFF_SIZE_LEVEL_INDEX
        int level_num = vid_to_mullevelIndex_.get_level_num_by_vid(src);
        if (level_num > 0) {
          char* ptr = vid_to_mullevelIndex_.get_level_index_ptr_by_vid(src);
          MulLevelIndexWithDiffSize level_index = MulLevelIndexWithDiffSize(ptr, level_num);
          level_index.copy_to_old_index(local_sv_.findex);
        } else {
          local_sv_.findex.set_min_level_0_fid(0);
          local_sv_.findex.set_level_num(0);
        }
#elif defined(MMAP_COLD_HOT_LEVEL_INDEX)
        VertexId_t array_id= 0;
        int old_level_num = 0;
        char* ptr = vid_to_mullevelIndex_.
            get_level_index_ptr_by_vid(src, old_level_num, array_id);
        if (ptr != nullptr) {
          MulLevelIndexWithDiffSize level_index 
              = MulLevelIndexWithDiffSize(ptr, old_level_num);
          level_index.copy_to_old_index(local_sv_.findex);
        } else {
          local_sv_.findex.set_min_level_0_fid(0);
          local_sv_.findex.set_level_num(0);
        }
#else
        memcpy(&local_sv_.findex, &vid_to_mullevelIndex_[src], sizeof(MulLevelIndex));
#endif
        vertex_rwlocks_[VertexLockSlot(src, vertex_lock_count_)].ReadUnlock();
      }
      FileId_t min_level_0_fid = local_sv_.findex.get_min_level_0_fid();
      bool found = false;
      for (auto it : *(local_sv_.version_memtable->current_->GetLevel0Files())) {
        if (it->header.timeStamp >= 0
            && src <= it->header.maxKey && src >= it->header.minKey) {
          fid = it->header.timeStamp;
          rs = find_edge(src, dst, seq, it);
          if (rs == Status::kNotFound) { // not found in the edge list of this file
            continue;
          } else {
            local_sv_.version_memtable = nullptr;
            return rs;
          }
        }
      }
    }

    // level>=1
    local_sv_.version_memtable = nullptr;
    rs = find_edge_by_levelindex(src, dst, fid, seq, local_sv_);
    return rs;
}

/// Binary search for the target destination vertex in the body part of the edge file.
bool LSMStore::find(VertexId_t target, uint32_t s_offset, uint32_t e_offset,
          uint32_t step_offset, std::ifstream& file,
          uint32_t& obj_offset) {
  if(s_offset >= e_offset)
      return false;
  obj_offset = e_offset;
  uint32_t mid_offset;
  VertexId_t temp_k;
  while (s_offset < e_offset){
      mid_offset = s_offset
                   + (e_offset - s_offset)/2
                      /step_offset*step_offset; // Guaranteed to round down
      file.seekg(mid_offset);
      file.read((char*)(&temp_k), sizeof(VertexId_t));
      if (temp_k == target) {
          e_offset = mid_offset;
      } else if (temp_k < target) {
          s_offset = mid_offset + step_offset;
      } else if (temp_k > target) {
          e_offset = mid_offset;
      }
  }
  // bound is left closed right open: [)
  if (s_offset >= obj_offset) return false;
  VertexId_t dis_ = 0;
  file.seekg(s_offset);
  file.read((char*)(&dis_), sizeof(VertexId_t));
  if (dis_ != target) return false;
  obj_offset = s_offset;
  return true;
}

Status LSMStore::find_edge_from_sstdata_cache(VertexId_t src, VertexId_t dst,
                                    uint32_t s_offset, uint32_t e_offset,
                                    uint32_t fileID, SequenceNumber_t& seq) {
  #ifdef DEBUG_COST
    std::chrono::steady_clock::time_point query_time_find_edget1 = std::chrono::steady_clock::now();
  #endif
  Status result = Status::kNotFound;
  SSTDataCache* sstcache = sstdata_manager_.get_data(fileID);

  uint32_t edge_body_size = EDGEBODY_SIZE;
  uint32_t adj_size = (e_offset - s_offset) / edge_body_size;
  char *body_buffer = sstcache->GetEdgeData();
  int low = s_offset;
  int high = e_offset;
  assert(low >= 0);
  assert(high >= 0);


  if (adj_size < BINARY_THRESHOLD_FIND_EDGE) {  // 二分的阈值
    for ( ; low < high; low+=edge_body_size) {
      char* body =body_buffer + low;
      if (get_dst(body) == dst) {
        if(!get_marker(body)) {
          seq = get_seq(body);
          result = Status::kOk;
        } else {
          result = Status::kDelete;
        }
        break;
      }
    }
  } else {
    int mid = 0;
    while (low < high) {
        mid = low + (high - low) / 2 / edge_body_size * edge_body_size; // 保证取整
        if (get_dst(body_buffer+mid) >= dst)
            high = mid;
        else {
            low = mid + edge_body_size;
        }
    }
    if (low < e_offset) {
      char* body =body_buffer+low;
      if (get_dst(body) == dst) {
        if(!get_marker(body)) {
          seq = get_seq(body);
          result = Status::kOk;
        } else {
          result = Status::kDelete;
        }
      }
    }
  }

  #ifdef DEBUG_COST
    std::chrono::steady_clock::time_point query_time_find_edget2 = std::chrono::steady_clock::now();
    std::chrono::duration<double> query_time_find_edgetime_span
        = std::chrono::duration_cast<std::chrono::duration<double>>(query_time_find_edget2 - query_time_find_edget1);
    write_add(&query_time_find_edge, query_time_find_edgetime_span.count());
  #endif

  return result;
}

Status LSMStore::find_edge_from_sstdata_cache(VertexId_t src, VertexId_t dst,
                                    uint32_t s_offset, uint32_t e_offset,
                                    uint32_t fileID, SequenceNumber_t& seq,
                                    bool is_out, uint8_t edge_type) {
  Status result = Status::kNotFound;
  SSTDataCache* sstcache = sstdata_manager_.get_data(fileID);
  char *body_buffer = sstcache->GetEdgeData();
  const EdgeLookupResult lookup = FindEdgeByDstDirectionAndType(
      body_buffer, static_cast<int>(s_offset), static_cast<int>(e_offset),
      dst, is_out, edge_type);
  result = lookup.status;
  if (result == Status::kOk) {
    seq = get_seq(body_buffer + lookup.body_offset);
  }
  return result;
}

Status LSMStore::find_edge_from_sstdata_cache(VertexId_t src, VertexId_t dst,
                                    uint32_t s_offset, uint32_t e_offset,
                                    uint32_t fileID, std::string* property,
                                    int property_id, bool is_out,
                                    uint8_t edge_type) {
  #ifdef DEBUG_COST
    std::chrono::steady_clock::time_point query_time_find_edget1 = std::chrono::steady_clock::now();
  #endif

  Status result = Status::kNotFound;
  SSTDataCache* sstcache = sstdata_manager_.get_data(fileID);

  char *body_buffer = sstcache->GetEdgeData();
  const EdgeLookupResult lookup = FindEdgeByDstDirectionAndType(
      body_buffer, static_cast<int>(s_offset), static_cast<int>(e_offset),
      dst, is_out, edge_type);
  result = lookup.status;
  const int low = lookup.body_offset;
  const auto delta_view = LoadPropertyDeltaView(fileID, property_id);

  if (result == Status::kOk && delta_view != nullptr &&
      delta_view->Lookup(src,
                         dst,
                         get_seq(body_buffer + low),
                         is_out,
                         edge_type,
                         kLatestPropertyCommit,
                         property)) {
    return Status::kOk;
  }

  #ifdef DEBUG_COST
    std::chrono::steady_clock::time_point query_time_find_edget2 = std::chrono::steady_clock::now();
    std::chrono::duration<double> query_time_find_edgetime_span
        = std::chrono::duration_cast<std::chrono::duration<double>>(query_time_find_edget2 - query_time_find_edget1);
    write_add(&query_time_find_edge, query_time_find_edgetime_span.count());
  #endif

  #ifdef DEBUG_COST
    std::chrono::steady_clock::time_point query_time_find_propertyt1 = std::chrono::steady_clock::now();
  #endif

  // load property
  if (result == Status::kOk) {
    const std::byte* property_buffer = delta_view == nullptr
        ? reinterpret_cast<const std::byte*>(
              sstcache->GetPropertyData(property_id))
        : delta_view->BaseDataOr(sstcache->GetPropertyData(property_id));

    const auto edge_ordinal = GetEdgeOrdinalFromBodyOffset(low);
    const auto property_offset = GetSubPropertyOffsetByEdgeOrdinal(edge_ordinal, property_id);
    const uint32_t length = GetSubPropertyFixedLength(property_id);
    assert(length < 1024);
    if (length > 0) {
      ReadSubPropertyFromSlot(
          reinterpret_cast<const char*>(property_buffer + property_offset),
          property_id,
          property);
    }
  }


  #ifdef DEBUG_COST
    std::chrono::steady_clock::time_point query_time_find_propertyt2 = std::chrono::steady_clock::now();
    std::chrono::duration<double> query_time_find_propertytime_span
        = std::chrono::duration_cast<std::chrono::duration<double>>(query_time_find_propertyt2 - query_time_find_propertyt1);
    write_add(&query_time_find_property, query_time_find_propertytime_span.count());
  #endif

  return result;
}



Status LSMStore::find_edges_from_sstdata_cache(uint32_t s_offset,
                                               uint32_t e_offset,
                                               uint32_t fileID,
                                               std::vector<Edge>& edges) {
  #ifdef DEBUG_COST
    std::chrono::steady_clock::time_point query_time_find_edget1 = std::chrono::steady_clock::now();
  #endif

  Status result = Status::kNotFound;
  SSTDataCache* sstcache = sstdata_manager_.get_data(fileID);
  assert(sstcache != nullptr);
  uint32_t edge_body_size = EDGEBODY_SIZE;

  uint32_t adj_size = (e_offset - s_offset) / edge_body_size;
  char *body_buffer = sstcache->GetEdgeData();
  int low = s_offset;
  int high = e_offset;
  assert(low >= 0);
  assert(high >= 0);

  VertexId_t saved_key = INVALID_VERTEX_ID;
  bool skipping = false;
  edges.reserve(edges.size() + adj_size);
  for ( ; low < high; low+=edge_body_size) {
    char* body = body_buffer+low;
    if (!(skipping && get_dst(body) == saved_key)) {
      if (get_marker(body) == 0) {
        edges.emplace_back(*body);
      } else {
        saved_key = get_dst(body);
        skipping = true;
      }
    }
  }

  #ifdef DEBUG_COST
    std::chrono::steady_clock::time_point query_time_find_edget2 = std::chrono::steady_clock::now();
    std::chrono::duration<double> query_time_find_edgetime_span
        = std::chrono::duration_cast<std::chrono::duration<double>>(query_time_find_edget2 - query_time_find_edget1);
    write_add(&query_time_find_edge, query_time_find_edgetime_span.count());
  #endif

  // load property


  return result;
}

/// Binary search for the target destination vertex in the body part of the edge file.
/// Note: lower_bound
/// Range: [s_offset, e_offset)
Status LSMStore::find_edge_from_file_with_cache(VertexId_t target,
                                                uint32_t s_offset,
                                                uint32_t e_offset,
                                                std::ifstream* file,
                                                std::string& path,
                                                std::string* property,
                                                int property_id,
                                                bool is_out,
                                                uint8_t edge_type) {
  #ifdef DEBUG_COST
    std::chrono::steady_clock::time_point query_time_find_edget1 = std::chrono::steady_clock::now();
  #endif

  if(s_offset >= e_offset)
      return Status::kNotFound;
  uint32_t edge_body_size = EDGEBODY_SIZE;

  // load whole adjlist of the target node
  uint32_t adj_size = (e_offset - s_offset) / edge_body_size;
  EdgeBody_t *body_buffer = new EdgeBody_t[adj_size + 1];
  auto need_bytes = (adj_size + 1) * edge_body_size;
  file->seekg(s_offset); // 这里和下面的read如果多线程读取同一个数据时可能会有问题
  auto read_bytes = file->readsome((char*)body_buffer, need_bytes);  // cache all adj edges of the target node
  assert(need_bytes == read_bytes);

  Status result = Status::kNotFound;
  int low = 0;
  int high = adj_size;
  while (low < high) {
    const int mid = (low + high) / 2;
    if (body_buffer[mid].get_dst() >= target) {
      high = mid;
    } else {
      low = mid + 1;
    }
  }
  for (int pos = low; pos < static_cast<int>(adj_size); ++pos) {
    if (body_buffer[pos].get_dst() != target) {
      break;
    }
    if (body_buffer[pos].get_is_out() != is_out
        || body_buffer[pos].get_edge_type() != edge_type) {
      continue;
    }
    low = pos;
    result = body_buffer[pos].get_marker() ? Status::kDelete : Status::kOk;
    break;
  }

  #ifdef DEBUG_COST
    std::chrono::steady_clock::time_point query_time_find_edget2 = std::chrono::steady_clock::now();
    std::chrono::duration<double> query_time_find_edgetime_span = std::chrono::duration_cast<std::chrono::duration<double>>(query_time_find_edget2 - query_time_find_edget1);
    write_add(&query_time_find_edge, query_time_find_edgetime_span.count());
  #endif

  #ifdef DEBUG_COST
    std::chrono::steady_clock::time_point query_time_find_propertyt1 = std::chrono::steady_clock::now();
  #endif

  // load property
  if (result == Status::kOk) {
    std::ifstream* p_file;
    std::string p_path = path +  + "_p";
    if (file_handle_cache_.find(p_path) != file_handle_cache_.end()) {
      p_file = file_handle_cache_[p_path];
    } else {
      p_file = new std::ifstream(p_path, std::ios::binary|std::ios::in);
      file_handle_cache_.emplace(p_path, p_file);
    }

    const auto edge_ordinal = GetEdgeOrdinalFromBodyOffset(s_offset) + static_cast<size_t>(low);
    const auto property_offset = GetSubPropertyOffsetByEdgeOrdinal(edge_ordinal, property_id);
    const uint32_t length = GetSubPropertyFixedLength(property_id);

    assert(length < 1024);
    if (length > 0) {
      property->resize(length);            // 先按定长读取
      p_file->seekg(property_offset);
      p_file->read(&(*property)[0], length);  // 从文件中读取指定数量的字符
      TrimTrailingZero(property); // 再恢复为逻辑字符串（去尾部补零）
    }
  }


  #ifdef DEBUG_COST
    std::chrono::steady_clock::time_point query_time_find_propertyt2 = std::chrono::steady_clock::now();
    std::chrono::duration<double> query_time_find_propertytime_span = std::chrono::duration_cast<std::chrono::duration<double>>(query_time_find_propertyt2 - query_time_find_propertyt1);
    write_add(&query_time_find_property, query_time_find_propertytime_span.count());
  #endif

  delete[] body_buffer;
  body_buffer = nullptr;
  return result;
}

Status LSMStore::find_edge_from_file_with_cache(VertexId_t target,
                                                uint32_t s_offset,
                                                uint32_t e_offset,
                                                std::ifstream* file,
                                                std::string& path,
                                                SequenceNumber_t& seq) {
  #ifdef DEBUG_COST
    std::chrono::steady_clock::time_point query_time_find_edget1 = std::chrono::steady_clock::now();
  #endif

  if(s_offset >= e_offset)
      return Status::kNotFound;
  uint32_t edge_body_size = EDGEBODY_SIZE;

  // load whole adjlist of the target node
  uint32_t adj_size = (e_offset - s_offset) / edge_body_size;
  EdgeBody_t *body_buffer = new EdgeBody_t[adj_size + 1];
  auto need_bytes = (adj_size + 1) * edge_body_size;
  file->seekg(s_offset); // 这里和下面的read如果多线程读取同一个数据时可能会有问题
  auto read_bytes = file->readsome((char*)body_buffer, need_bytes);  // cache all adj edges of the target node
  assert(need_bytes == read_bytes);

  Status result = Status::kNotFound;
  int low = 0;

  if (adj_size < BINARY_THRESHOLD_FIND_EDGE) {  // 二分的阈值
    for (low = 0; low < adj_size; low++) {
      if (body_buffer[low].get_dst() == target) {
        if(!body_buffer[low].get_marker()) {
          seq = body_buffer[low].get_seq();
          result = Status::kOk;
        } else {
          result = Status::kDelete;
        }
        break;
      }
    }
  } else {
    int high = adj_size;
    int mid = 0;
    while (low < high) {
        mid = (low + high )/2;
        if ( body_buffer[mid].get_dst() >= target)
            high = mid;
        else {
            low = mid + 1;
        }
    }
    if (low < adj_size && body_buffer[low].get_dst() == target) {
      if(!body_buffer[low].get_marker()) {
        seq = body_buffer[low].get_seq();
        result = Status::kOk;
      } else {
        result = Status::kDelete;
      }
    }
  }

  #ifdef DEBUG_COST
    std::chrono::steady_clock::time_point query_time_find_edget2 = std::chrono::steady_clock::now();
    std::chrono::duration<double> query_time_find_edgetime_span = std::chrono::duration_cast<std::chrono::duration<double>>(query_time_find_edget2 - query_time_find_edget1);
    write_add(&query_time_find_edge, query_time_find_edgetime_span.count());
  #endif

  delete[] body_buffer;
  body_buffer = nullptr;
  return result;
}

Status LSMStore::find_edge_from_file_with_cache_by_directed_IO(
                                    VertexId_t src, VertexId_t dst,
                                    uint32_t s_offset, uint32_t e_offset,
                                    uint32_t fileID, SequenceNumber_t& seq) {
  #ifdef DEBUG_COST
    std::chrono::steady_clock::time_point query_time_find_edget1 = std::chrono::steady_clock::now();
  #endif
  if(s_offset >= e_offset)
      return Status::kNotFound;
  Status result = Status::kNotFound;
  SSTDataCache* sstcache = sstdata_manager_.get_data(fileID);

  int fd = sstcache->GetEFileFD();
  if (fd == -1) {
    // 输出错误信息，文件不存在
    throw std::runtime_error("read file not exit: fileID=" + std::to_string(fileID) 
      + " offset=" + std::to_string(s_offset) + " " + std::to_string(e_offset));
  }

  uint32_t edge_body_size = EDGEBODY_SIZE;
  // load whole adjlist of the target node
  uint32_t adj_size = (e_offset - s_offset) / edge_body_size;
  EdgeBody_t *body_buffer = new EdgeBody_t[adj_size + 1];
  auto need_bytes = (adj_size + 1) * edge_body_size;

  auto read_bytes = pread(fd, body_buffer, need_bytes, s_offset);
  if (read_bytes != need_bytes) {
    throw std::runtime_error("read data size error: fileID=" + std::to_string(fileID) 
      + " read_bytes=" + std::to_string(read_bytes) 
      + " need_bytes=" + std::to_string(need_bytes));
  }


  int low = 0;

  if (adj_size < BINARY_THRESHOLD_FIND_EDGE) {  // 二分的阈值
    for (low = 0; low < adj_size; low++) {
      if (body_buffer[low].get_dst() == dst) {
        if(!body_buffer[low].get_marker()) {
          seq = body_buffer[low].get_seq();
          result = Status::kOk;
        } else {
          result = Status::kDelete;
        }
        break;
      }
    }
  } else {
    int high = adj_size;
    int mid = 0;
    while (low < high) {
        mid = (low + high )/2;
        if ( body_buffer[mid].get_dst() >= dst)
            high = mid;
        else {
            low = mid + 1;
        }
    }
    if (low < adj_size && body_buffer[low].get_dst() == dst) {
      if(!body_buffer[low].get_marker()) {
        seq = body_buffer[low].get_seq();
        result = Status::kOk;
      } else {
        result = Status::kDelete;
      }
    }
  }

  #ifdef DEBUG_COST
    std::chrono::steady_clock::time_point query_time_find_edget2 = std::chrono::steady_clock::now();
    std::chrono::duration<double> query_time_find_edgetime_span = std::chrono::duration_cast<std::chrono::duration<double>>(query_time_find_edget2 - query_time_find_edget1);
    write_add(&query_time_find_edge, query_time_find_edgetime_span.count());
  #endif

  delete[] body_buffer;
  body_buffer = nullptr;
  return result;
}

/// Binary search for the target destination vertex in the body part of the edge file.
/// Note: lower_bound
/// Range: [s_offset, e_offset)
Status LSMStore::find_edges_from_file_with_cache(uint32_t s_offset,
                                                 uint32_t e_offset,
                                                 std::ifstream* file,
                                                 SSTableCache* sst,
                                                 std::vector<Edge>& edges) {

  if(s_offset >= e_offset)
      return Status::kNotFound;
  uint32_t edge_body_size = EDGEBODY_SIZE;
  uint32_t last_edge_end_offset = edge_body_size * sst->header.size;
  bool not_file_end = (e_offset < last_edge_end_offset);

  // load whole adjlist of the target node
  uint32_t adj_size = (e_offset - s_offset) / edge_body_size;
  EdgeBody_t *body_buffer = new EdgeBody_t[adj_size + not_file_end];
  auto need_bytes = (adj_size + not_file_end) * edge_body_size;
  file->seekg(s_offset);
  auto read_bytes = file->readsome((char*)body_buffer, need_bytes);  // cache all adj edges of the target node
  assert(need_bytes == read_bytes);



  for (auto& e : edges) {
    e.print();
  }

  return Status::kOk;
}

/**
 * Returns an array of all outgoing edges of a vertex.
 */
bool LSMStore::get_edges(VertexId_t src, std::vector<Edge>& edges)
{
    MemTable* mem = memTable_.load(std::memory_order_acquire);
    mem->get_edges(src, edges);

    int levelNum = fileMetaCache.size();
    for(int i = 0; i < levelNum; ++i) {
        // find keys in SSTableCaches, from the one with biggest timestamp
        for(auto it = fileMetaCache[i]->begin();
            it != fileMetaCache[i]->end(); ++it) {
            // check if the src is in the range of the sstablecache
            if(src <= ((*it)->header).maxKey && src >= ((*it)->header).minKey) {
                int pos = (*it)->get(src);
                if(pos < 0 && i == 0) {
                  continue;
                } else if (pos < 0) {
                  break;
                }

                Status rs;
                uint32_t offset = ((*it)->indexes)[pos].offset;
                uint32_t next_offset = ((*it)->indexes)[pos+1].offset;

                if (FLAGS_OPEN_SSTDATA_CACHE == true) {
                  find_edges_from_sstdata_cache(offset, next_offset,
                                                (*it)->header.timeStamp, edges);
                } else {
                  std::ifstream* file;
                  if (file_handle_cache_.find((*it)->path)
                      != file_handle_cache_.end()) {
                    file = file_handle_cache_[(*it)->path];
                  } else {
                    file = new std::ifstream((*it)->path,
                                                 std::ios::binary|std::ios::in);
                    file_handle_cache_.emplace((*it)->path, file);
                  }
                  if(!file) {
                    printf("Lost file: %s", ((*it)->path).c_str());
                    exit(-1);
                  }

                   find_edges_from_file_with_cache(offset, next_offset, file,
                                                   *it, edges);
                }

                if (it + 1 != fileMetaCache[i]->end()
                    && src == ((*(it+1))->header).minKey) { // The edges of the large vertex may be squeezed to the next sst.
                  continue;
                } else {
                  break;
                }
            }

        }
    }
    return true;
}

EdgeIterator LSMStore::get_edges(VertexId_t src, const SequenceNumber_t seq,
                                 int sub_property_id, bool is_out,
                                 uint8_t edge_type) {
#ifdef DEBUG_COST
  std::chrono::steady_clock::time_point get_curr_version_timet1 = std::chrono::steady_clock::now();
#endif

  if (use_csr_disk_) {
    // CSR fast 路径：
    // - 仅构造“当前 src 的内存层迭代器 + 1个CSR磁盘迭代器”；
    // - 避开通用路径里 level-0 候选文件的扫描与迭代器构造开销。
    get_superversion(local_sv_);
    std::vector<std::shared_ptr<EdgeIteratorBase>> prepared_iters;
    prepared_iters.reserve(local_sv_.get_memtable().size() + 1);

    for (auto* tb : local_sv_.get_memtable()) {
      if (seq <= tb->GetStartTime()) {
        continue;
      }
      auto mt_it = std::shared_ptr<EdgeIteratorBase>(
          new NeighBors::MemEdgeIterator(
              tb->get_vertex_adj(src), tb->GetFid(), tb->newest_edge));
      if (mt_it->valid()) {
        const auto delta_view = LoadPropertyDeltaView(
            tb->GetFid(), sub_property_id);
        if (delta_view != nullptr) {
          mt_it = std::make_shared<PropertyDeltaOverlayIterator>(
              src, mt_it, delta_view);
        }
        prepared_iters.emplace_back(std::move(mt_it));
      }
    }

    auto snapshot = LoadCSRReadSnapshot();
    const auto* entry = FindCSRIndexEntry(snapshot.get(), src);
    if (entry != nullptr && entry->cache != nullptr) {
      SSTDataCache* sstcache = nullptr;
      try {
        sstcache = sstdata_manager_.get_data(entry->cache->header.timeStamp);
      } catch (const std::exception&) {
        sstcache = nullptr;
      }
      if (sstcache != nullptr) {
        const int effective_pid =
            (sub_property_id >= 0) ? sub_property_id : 0;
        const uint32_t slot_len = GetSubPropertyFixedLength(effective_pid);
        const uint32_t adj_size =
            (entry->e_offset - entry->s_offset) / EDGEBODY_SIZE;
        const auto delta_view = LoadPropertyDeltaView(
            entry->cache->header.timeStamp, effective_pid);
        const std::byte* property_data = delta_view == nullptr
            ? reinterpret_cast<const std::byte*>(
                  sstcache->GetPropertyData(effective_pid))
            : delta_view->BaseDataOr(
                  sstcache->GetPropertyData(effective_pid));
        auto disk_it = std::shared_ptr<EdgeIteratorBase>(
            new SSTEdgeIterator(
                reinterpret_cast<EdgeBody_t*>(sstcache->GetEdgeData()
                                              + entry->s_offset),
                const_cast<char*>(
                    reinterpret_cast<const char*>(property_data)),
                adj_size,
                entry->cache->header.timeStamp,
                entry->cache->newest_edge,
                GetSubPropertyOffsetByEdgeOrdinal(
                    GetEdgeOrdinalFromBodyOffset(entry->s_offset), effective_pid),
                slot_len));
        if (delta_view != nullptr) {
          disk_it = std::make_shared<PropertyDeltaOverlayIterator>(
              src, disk_it, delta_view);
        }
        if (disk_it->valid()) {
          prepared_iters.emplace_back(std::move(disk_it));
        }
      }
    }

    return EdgeIterator{std::move(prepared_iters),
                        &local_sv_,
                        seq, is_out, edge_type,
                        std::shared_ptr<const void>(snapshot)};
  }

  MemTable* mem = nullptr;
  int max_level = -1;
  if (FLAGS_support_mulversion== true) {
    {
      // Force initialization of the thread-local SuperVersion. The normal
      // refresh fast path can leave it empty on a thread's first read when
      // local and global version numbers already match.
      get_superversion(local_sv_);
      vertex_rwlocks_[VertexLockSlot(src, vertex_lock_count_)].ReadLock();
#ifdef MMAP_DIFF_SIZE_LEVEL_INDEX
        int level_num = vid_to_mullevelIndex_.get_level_num_by_vid(src);
        if (level_num > 0) {
          char* ptr = vid_to_mullevelIndex_.get_level_index_ptr_by_vid(src);
          MulLevelIndexWithDiffSize level_index = MulLevelIndexWithDiffSize(ptr, level_num);
          level_index.copy_to_old_index(local_sv_.findex);
        } else {
          local_sv_.findex.set_min_level_0_fid(0);
          local_sv_.findex.set_level_num(0);
        }
#elif defined(MMAP_COLD_HOT_LEVEL_INDEX)
        VertexId_t array_id= 0;
        int old_level_num = 0;
        char* ptr = vid_to_mullevelIndex_.
            get_level_index_ptr_by_vid(src, old_level_num, array_id);
        if (ptr != nullptr) {
          MulLevelIndexWithDiffSize level_index =
              MulLevelIndexWithDiffSize(ptr, old_level_num);
          level_index.copy_to_old_index(local_sv_.findex);
        } else {
          local_sv_.findex.set_min_level_0_fid(0);
          local_sv_.findex.set_level_num(0);
        }
#else
#ifdef FULL_FILE_INDEX
      vid_to_mullevelIndex_.copy_index_from_file(src, &local_sv_.findex);
#else
      memcpy(&local_sv_.findex, &vid_to_mullevelIndex_[src],
             sizeof(MulLevelIndex));
#endif
#endif
        vertex_rwlocks_[VertexLockSlot(src, vertex_lock_count_)].ReadUnlock();
        max_level = atomic_load_value(&vertex_max_level_[src]);
    }
  } else {
    mem = memTable_.load(std::memory_order_acquire);
  }


#ifdef DEBUG_COST
  std::chrono::steady_clock::time_point get_curr_version_timet2 =
      std::chrono::steady_clock::now();
  std::chrono::duration<double> get_curr_version_timetime_span =
      std::chrono::duration_cast<std::chrono::duration<double>>(
          get_curr_version_timet2 - get_curr_version_timet1);
  write_add(&get_curr_version_time, get_curr_version_timetime_span.count());
#endif

  return EdgeIterator{src,
                      sstdata_manager_,
                      vid_to_levelIndex_,
                      &local_sv_,
                      max_level,
                      seq,
                      sub_property_id,
                      is_out,
                      edge_type,
                      [this](FileId_t fid, int property_id) {
                        return LoadPropertyDeltaView(fid, property_id);
                      }};
}

EdgeIterator LSMStore::Get_Edges(VertexId_t src, const SequenceNumber_t seq,
                                 int sub_property_id, bool is_out,
                                 uint8_t edge_type) {
  #ifdef DEBUG_COST
    std::chrono::steady_clock::time_point get_curr_version_timet1 = std::chrono::steady_clock::now();
  #endif

    if (use_csr_disk_) {
      get_superversion(local_sv_);
      std::vector<std::shared_ptr<EdgeIteratorBase>> prepared_iters;
      prepared_iters.reserve(local_sv_.get_memtable().size() + 1);

      for (auto* tb : local_sv_.get_memtable()) {
        if (seq <= tb->GetStartTime()) {
          continue;
        }
        auto mt_it = std::shared_ptr<EdgeIteratorBase>(
            new NeighBors::MemEdgeIterator(
                tb->get_vertex_adj(src), tb->GetFid(), tb->newest_edge));
        if (mt_it->valid()) {
          const auto delta_view = LoadPropertyDeltaView(
              tb->GetFid(), sub_property_id);
          if (delta_view != nullptr) {
            mt_it = std::make_shared<PropertyDeltaOverlayIterator>(
                src, mt_it, delta_view);
          }
          prepared_iters.emplace_back(std::move(mt_it));
        }
      }

      auto snapshot = LoadCSRReadSnapshot();
      const auto* entry = FindCSRIndexEntry(snapshot.get(), src);
      if (entry != nullptr && entry->cache != nullptr) {
        SSTDataCache* sstcache = nullptr;
        try {
          sstcache = sstdata_manager_.get_data(entry->cache->header.timeStamp);
        } catch (const std::exception&) {
          sstcache = nullptr;
        }
        if (sstcache != nullptr) {
          const int effective_pid =
              (sub_property_id >= 0) ? sub_property_id : 0;
          const uint32_t slot_len = GetSubPropertyFixedLength(effective_pid);
          const uint32_t adj_size =
              (entry->e_offset - entry->s_offset) / EDGEBODY_SIZE;
          const auto delta_view = LoadPropertyDeltaView(
              entry->cache->header.timeStamp, effective_pid);
          const std::byte* property_data = delta_view == nullptr
              ? reinterpret_cast<const std::byte*>(
                    sstcache->GetPropertyData(effective_pid))
              : delta_view->BaseDataOr(
                    sstcache->GetPropertyData(effective_pid));
          auto disk_it = std::shared_ptr<EdgeIteratorBase>(
              new SSTEdgeIterator(
                  reinterpret_cast<EdgeBody_t*>(sstcache->GetEdgeData()
                                                + entry->s_offset),
                  const_cast<char*>(
                      reinterpret_cast<const char*>(property_data)),
                  adj_size,
                  entry->cache->header.timeStamp,
                  entry->cache->newest_edge,
                  GetSubPropertyOffsetByEdgeOrdinal(
                      GetEdgeOrdinalFromBodyOffset(entry->s_offset), effective_pid),
                  slot_len));
          if (delta_view != nullptr) {
            disk_it = std::make_shared<PropertyDeltaOverlayIterator>(
                src, disk_it, delta_view);
          }
          if (disk_it->valid()) {
            prepared_iters.emplace_back(std::move(disk_it));
          }
        }
      }

      return EdgeIterator{std::move(prepared_iters),
                          &local_sv_,
                          seq, is_out, edge_type,
                          std::shared_ptr<const void>(snapshot)};
    }
  
    MemTable* mem = nullptr;
    int max_level = -1;
    if (FLAGS_support_mulversion== true) {

      {
        // 同上：scan 迭代器路径需要保证 local_sv_ 已实值化，不能依赖懒刷新。
        get_superversion(local_sv_);
        vertex_rwlocks_[VertexLockSlot(src, vertex_lock_count_)].ReadLock();
  #ifdef MMAP_DIFF_SIZE_LEVEL_INDEX
          int level_num = vid_to_mullevelIndex_.get_level_num_by_vid(src);
          if (level_num > 0) {
            char* ptr = vid_to_mullevelIndex_.get_level_index_ptr_by_vid(src);
            MulLevelIndexWithDiffSize level_index = MulLevelIndexWithDiffSize(ptr, level_num);
            level_index.copy_to_old_index(local_sv_.findex);
          } else {
            local_sv_.findex.set_min_level_0_fid(0);
            local_sv_.findex.set_level_num(0);
          }
  #elif defined(MMAP_COLD_HOT_LEVEL_INDEX)
          VertexId_t array_id= 0;
          int old_level_num = 0;
          char* ptr = vid_to_mullevelIndex_.
              get_level_index_ptr_by_vid(src, old_level_num, array_id);
          if (ptr != nullptr) {
            MulLevelIndexWithDiffSize level_index 
                = MulLevelIndexWithDiffSize(ptr, old_level_num);
            level_index.copy_to_old_index(local_sv_.findex);
          } else {
            local_sv_.findex.set_min_level_0_fid(0);
            local_sv_.findex.set_level_num(0);
          }
#else
#ifdef FULL_FILE_INDEX
        vid_to_mullevelIndex_.copy_index_from_file(src, &local_sv_.findex);
#else
        memcpy(&local_sv_.findex, &vid_to_mullevelIndex_[src],
               sizeof(MulLevelIndex));
#endif
#endif
          vertex_rwlocks_[VertexLockSlot(src, vertex_lock_count_)].ReadUnlock();
          max_level = atomic_load_value(&vertex_max_level_[src]);
      }
    } else {
      mem = memTable_.load(std::memory_order_acquire);
    }


#ifdef DEBUG_COST
    std::chrono::steady_clock::time_point get_curr_version_timet2 =
        std::chrono::steady_clock::now();
    std::chrono::duration<double> get_curr_version_timetime_span =
        std::chrono::duration_cast<std::chrono::duration<double>>(
            get_curr_version_timet2 - get_curr_version_timet1);
    write_add(&get_curr_version_time, get_curr_version_timetime_span.count());
  #endif
  
    return EdgeIterator{src,
                        sstdata_manager_,
                        vid_to_levelIndex_,
                        &local_sv_,
                        max_level,
                        seq, sub_property_id, is_out, edge_type,
                        [this](FileId_t fid, int property_id) {
                          return LoadPropertyDeltaView(fid, property_id);
                        }};
  }

/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
Status LSMStore::del_edge(VertexId_t src, VertexId_t dis, int property_id)
{
    std::string res_property;
    Status res = get_edge(src, dis, &res_property, property_id);
    if(res != Status::kOk) {
      return res;
    }
    res_property = "~DELETED~";
    put_edge(src, dis, res_property);
    return Status::kOk;
}

// Insert and delete edges separately
Status LSMStore::del_edge_sep(VertexId_t src, VertexId_t dis) {
    FileId_t fid = INVALID_File_ID;
    SequenceNumber_t eid = MAX_SEQ_ID;
    // get eid, fid
    Status res = get_edge(src, dis, fid, eid);
    if(res != Status::kOk) {
      return res;
    }

    SequenceNumber_t del_time = automic_get_global_seq(1);


    del_record_manager_.put_fid_and_record(fid, eid, del_time);

    return Status::kOk;
}

void LSMStore::print_memTable(std::string label) {
  MemTable* mem = memTable_.load(std::memory_order_acquire);
  mem->print(label);
}

SequenceNumber_t LSMStore::automic_get_global_seq(SequenceNumber_t num) {
  return global_seq.fetch_add(num, std::memory_order_acq_rel);
}

SequenceNumber_t LSMStore::LastSequence() {
  return global_seq.load(std::memory_order_acquire);
}

void LSMStore::SetLastSequence(SequenceNumber_t last_sequence) {
  global_seq.store(last_sequence, std::memory_order_release);
}

void LSMStore::ObserveTopologySequence(SequenceNumber_t sequence) {
  if (sequence < 0 || sequence == MAX_GLOBAL_SEQ) return;
  const SequenceNumber_t candidate = sequence + 1;
  SequenceNumber_t observed =
      next_topology_sequence_.load(std::memory_order_relaxed);
  while (observed < candidate &&
         !next_topology_sequence_.compare_exchange_weak(
             observed, candidate, std::memory_order_release,
             std::memory_order_relaxed)) {
  }
}

MemTable* LSMStore::get_newmemTable() {
  while (true) {
    std::unique_lock<std::mutex> lk(memtable_mux_);
    if (!free_menTables.empty()) {
      auto table = free_menTables.front();
      // 此时，从队列中拿出来，意味着是被回收的im_mem，即不会有新的query读到，只有有
      // 事务读完释放，所以只要读到0就可以释放, 即被重新使用
      if (FLAGS_support_mulversion == true) {
        if ((table->IsFlash() == true || table->IsLive() == false) &&
            table->Getref() == 0) {
          table->reset();  // 确保没有读任务
          free_menTables.pop();
          return table;
        }
      } else {
        table->reset();
        free_menTables.pop();
        return table;
      }
    }
    lk.unlock();
    Sleep(0.0000001);
  }
}

void LSMStore::recycle_memTable(MemTable* table) {
  {
    std::unique_lock<std::mutex> lk(memtable_mux_);
    free_menTables.push(table);
  }
}

void LSMStore::check_vertex_id(VertexId_t vertex_id) {
  if (vertex_id >= vertex_id_.load(std::memory_order_relaxed))
    throw std::invalid_argument("The vertex id is invalid.");
}

// open
void LSMGraph::open(const std::string &dir,
                    size_t max_vertex_num,
                    LSMGraph **db) {
    *db = new LSMStore(dir, max_vertex_num, 1, FLAGS_memtable_num);
}


void LSMStore::static_edge_distribution() {
  std::cout << "-------------------------------------------------" << std::endl;
  // in file
  std::vector<int> filenum_of_vertex;
  std::vector<int> vertex_num_of_level(MAX_LEVEL+2, 0);
  int memtable_vertex_num = 0;
  MemTable* mem = memTable_.load(std::memory_order_acquire);
  for (VertexId_t vid = 0; vid < vertex_id_.load(std::memory_order_relaxed);
       vid++) {
    int cnt = 0;
    // memtable
    if (mem->get_vertex_adj(vid) != nullptr) {
      memtable_vertex_num++;
      cnt++;
      vertex_num_of_level[0]++;
    }
    // level-0
    for (auto it =fileMetaCache[0]->begin(); it !=fileMetaCache[0]->end();
         ++it) {
      if(vid <= ((*it)->header).maxKey && vid >= ((*it)->header).minKey) {
        int rs = (*it)->find(vid, 0, (*it)->indexes.size()-1);
        if (rs != -1) {
          cnt++;
          vertex_num_of_level[1]++;
        }
      }
    }
    // level>=1
    if (FLAGS_support_mulversion== false) {
      for (int levelID = 0; levelID < LEVEL_INDEX_SIZE; levelID++) {
        int index_id = vid * LEVEL_INDEX_SIZE + levelID;
        LevelIndex& findex = vid_to_levelIndex_[index_id];
        FileId_t fileID = findex.get_fileID();
        if (fileID == INVALID_File_ID) {
          continue;
        }
        cnt++;
        vertex_num_of_level[1 + levelID + 1]++;
      }
    } else {
#ifdef MMAP_DIFF_SIZE_LEVEL_INDEX
      char* ptr = vid_to_mullevelIndex_.get_level_index_ptr_by_vid(vid);
      int level_num = vid_to_mullevelIndex_.get_level_num_by_vid(vid);
      if (level_num > 0) {
        MulLevelIndexWithDiffSize level_index 
            = MulLevelIndexWithDiffSize(ptr, level_num);
        for (int i = 0; i < level_num; i++) {
          int levelID = level_index.get_level_id(i);
          vertex_num_of_level[1 + levelID]++;
        }
        cnt += level_num;
      }
#elif defined(MMAP_COLD_HOT_LEVEL_INDEX)
        VertexId_t array_id= 0;
        int old_level_num = 0;
        char* ptr = vid_to_mullevelIndex_.
            get_level_index_ptr_by_vid(vid, old_level_num, array_id);
        if (ptr != nullptr) {
          MulLevelIndexWithDiffSize level_index 
              = MulLevelIndexWithDiffSize(ptr, old_level_num);
          for (int i = 0; i < old_level_num; i++) {
            if (level_index.get_fileID(i) != INVALID_File_ID) {
              int levelID = level_index.get_level_id(i);
              vertex_num_of_level[1 + levelID]++;
              cnt += 1;
            }
          }
        }
#else
      MulLevelIndex findex = vid_to_mullevelIndex_[vid];
      for (int levelID = 0; levelID < LEVEL_INDEX_SIZE; levelID++) {
        FileId_t fileID = findex.get_fileID(levelID);
        if (fileID == INVALID_File_ID) {
          continue;
        }
        cnt++;
        vertex_num_of_level[1 + levelID + 1]++;
      }
#endif
    }
    if (cnt >= filenum_of_vertex.size()) {
      filenum_of_vertex.resize(cnt+1);
    }
    filenum_of_vertex[cnt]++;
  }
  std::cout << " file num of each vertex:" << std::endl;
  for (int i = 0; i < filenum_of_vertex.size(); i++) {
    std::cout << "   file_num=" << i
              << " vertex_num=" << filenum_of_vertex[i]
              << " rate=" << (filenum_of_vertex[i] * 1.0 /
              vertex_id_.load(std::memory_order_relaxed))
              << std::endl;
  }
  std::cout << "   memtable/all_vertex="
            << (memtable_vertex_num * 1.0 /
               vertex_id_.load(std::memory_order_relaxed))
            << std::endl;
  
  std::cout << "\n vertex num of each level: (-1 is memtable):" << std::endl;
  for (int i = 0; i < vertex_num_of_level.size(); i++) {
    std::cout << "   level=" << (i - 1)
              << " vertex_num=" << vertex_num_of_level[i] << " rate="
              << (vertex_num_of_level[i] * 1.0 /
                  vertex_id_.load(std::memory_order_relaxed))
              << std::endl;
  }

  // count max level
  {
    std::cout << "\n max_level info of each vertex:" << std::endl;
    std::vector<VertexId_t> cnt_max_level(MAX_LEVEL + 1, 0);
    for (VertexId_t i = 0; i < vertex_id_; i++) {
      cnt_max_level[atomic_load_value(&vertex_max_level_[i]) + 1]++;
    }
    for (int i = 0; i < MAX_LEVEL + 1; i++) {
      std::cout << "   max_level=" << (i - 1) << " num=" << cnt_max_level[i]
                << " rate="
                << cnt_max_level[i] * 1.0 /
                       vertex_id_.load(std::memory_order_relaxed)
                << std::endl;
    }
  }

  std::cout << "-------------------------------------------------" << std::endl;
}

// Returns the largest vertex ID currently in use.
VertexId_t LSMStore::get_max_vertex_num() {
  std::cout << "vertex_id_=" << vertex_id_.load(std::memory_order_relaxed)
            << std::endl;
  return vertex_id_.load(std::memory_order_relaxed);
}

bool LSMStore::GetCompactionState() {
  return compactor_.GetState();
}

void LSMStore::print_all_file_info() {
  int levelNum =fileMetaCache.size();
  int file_num = 0;
  for(int i = 0; i < levelNum; ++i) {
    int level_file_num =fileMetaCache[i]->size();
    std::cout << " level_id=" << i
              << " file_num=" <<fileMetaCache[i]->size()
              << std::endl;
    for(auto it =fileMetaCache[i]->begin(); it !=fileMetaCache[i]->end();
        ++it) {
      std::cout << "  level=" << i
                << " minkey=" << (*it)->header.minKey
                << " maxkey=" << (*it)->header.maxKey
                << " fid=" << (*it)->header.timeStamp
                << std::endl;
      ++file_num;
    }
  }
  std::cout << "@file_num: " << file_num << std::endl;
}

SequenceNumber_t LSMStore::get_sequence() const {
  MemTable* old_mem = memTable_.load(std::memory_order_acquire);
  return (old_mem->GetStartTime() + old_mem->GetMaxEdgeNum() - old_mem->remain_capacity + 1);
}

void LSMStore::debug() {
#ifdef MMAP_DIFF_SIZE_LEVEL_INDEX
  // 测试将所有Index压缩成最紧凑情况下的性能情况:
  std::chrono::steady_clock::time_point debug_t1 = 
      std::chrono::steady_clock::now();

  MulLevelIndexArrayWrapper temp = MulLevelIndexArrayWrapper{"./", MMAP_INITIAL_SIZE >> 6};
  for (VertexId_t vid = 0; vid < vertex_id_.load(std::memory_order_relaxed); vid++) {
    temp.init(vid);
    int level_num = vid_to_mullevelIndex_.get_level_num_by_vid(vid);
    if (level_num > 0) {
      char* ptr = vid_to_mullevelIndex_.get_level_index_ptr_by_vid(vid);
      MulLevelIndexWithDiffSize level_index = MulLevelIndexWithDiffSize(ptr, level_num);

      VertexId_t array_id = 0;
      char* temp_ptr = 
        temp.get_new_level_index_ptr_by_vid_and_level_num(vid, level_num, array_id);
      MulLevelIndexWithDiffSize new_level_index = MulLevelIndexWithDiffSize(temp_ptr, level_num);
      for (int i = 0; i < level_num; i++) {
        memcpy(new_level_index.get_level_data_ptr(i),
                     level_index.get_level_data_ptr(i),
                     sizeof(MulLevelIndexWithDiffSize::LevelData));
      }
      temp.set_level_num_by_vid(vid, level_num);
      temp.set_array_id_by_vid(vid, array_id);
      new_level_index.set_min_level_0_fid(level_index.get_min_level_0_fid());
    }
  }
  vid_to_mullevelIndex_ = temp;

  std::chrono::steady_clock::time_point debug_t2 = 
      std::chrono::steady_clock::now();
  std::chrono::duration<double> debug_span =    
      std::chrono::duration_cast<std::chrono::duration<double>>(
        debug_t2 - debug_t1);
  std::cout << " debug sort index time=" << debug_span.count() << std::endl;

  std::cout << "finish clear level index......" << std::endl;
#endif
}

void LSMGraph::breakdown(const std::string& label) {
#ifdef DEBUG_COST
    // sec:
    std::cout << ("  @" + label + " query_time_mem: ") 
        << query_time_mem << std::endl;
    std::cout << ("  @" + label + "  query_time_file: ")  
        << query_time_file << std::endl;
    std::cout << ("  @" + label + "    query_time_find_index: ")  
        << query_time_find_index << std::endl;
    std::cout << ("  @" + label + "    query_time_find_edge: ")  
        << query_time_find_edge << std::endl;
    std::cout << ("  @" + label + "    query_time_find_property: ")  
        << query_time_find_property << std::endl;
    std::cout << ("  @" + label + "    find_all_iterator: ")  
        << find_all_iterator << std::endl;
    std::cout << ("  @" + label + "  compaction_time: ")  
        << compaction_time << std::endl;
    std::cout << ("  @" + label + "  get_curr_version_time: ")  
        << get_curr_version_time << std::endl;
    std::cout << ("  @" + label + "  iterate_memtable: ")  
        << iterate_memtable
        << std::endl;
    std::cout << ("  @" + label + "  iterate_level_0: ")  
        << iterate_level_0 << std::endl;
    std::cout << ("  @" + label + "  iterate_level_1: ")  
        << iterate_level_1 << std::endl;
    std::cout << ("  @" + label + "  iterate_find_first: ")  
        << iterate_find_first << std::endl;
    std::cout << ("  @" + label + "  iterate_check_entry_valid: ")  
        << iterate_check_entry_valid << std::endl;
#endif
}


void LSMStore::AwaitWrite() {
  if (CheckState()) {
    // busy loop
    for (uint32_t tries = 0; tries < 120; ++tries) {
      if (!CheckState()) {
        return ;
      }
      asm volatile("pause");
    }

  }
}

bool LSMStore::CheckState() const {
  return fileMetaCache[0]->size() >= 2;
}



SSTDataManager*  LSMStore::GetSSTDataManager(){
  return &sstdata_manager_;
}


void LSMStore::Debug(){
  std::cout<<"sst_dst" << sst_dst<<"--\n";
  std::cout<<"block_cnt"<<block_cnt_<<"--\n";

  write_min(&sst_dst, 0.0);
  write_min(&block_cnt_, 0);
  
}

void LSMStore::un_map(){
  sstdata_manager_.un_map();
}

void LSMStore::clean(){
  fileMetaCache.clear();
  std::vector<std::vector<SSTableCache*>*>().swap(fileMetaCache);

  std::atomic_store(&csr_read_snapshot_,
                    std::shared_ptr<const CSRReadSnapshot>{});

  compactor_.clean();


}


}  // namespace lsmgraph
