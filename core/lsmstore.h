#ifndef LSMGRAPH_KVSTORE_H
#define LSMGRAPH_KVSTORE_H

#include "core/superversion.h"
#include "core/LSMGraph.h"
#include "core/SSTable.h"
#include <climits>
#include <vector>
#include "core/types.h"
#include "util/arena.h"
#include "util/lock.h"
#include "util/worker_pool.h"
#include "core/graph/edge.h"
#include <limits>
#include "core/utils.h"
#include <string>
#include <algorithm>
#include <fstream>
#include <iostream>
#include "core/MemTable.h"
#include <thread>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <map>
#include <memory>
#include <unordered_map>
#include <shared_mutex>
#include <utility>
#include <tbb/concurrent_queue.h>
#include <mutex>
#include "core/cache/BlockManager.h"
#include "core/cache/SSTDataCache.h"
#include "core/compaction.h"
#include "core/version_set.h"
#include "core/superversion.h"
#include "util/livegraph/allocator.hpp"
#include "util/livegraph/futex.hpp"
#include "util/leveldb/port/port_stdcxx.h"
#include "util/leveldb/port/mutexlock.h"
#include "core/cache/SSTDataManager.h"
#include "core/del_record_manage.h"
#include "core/property_delta_store.h"
#include "richgraph/options.h"

using Futex = livegraph::Futex;



namespace lsmgraph {

class LSMStore : public LSMGraph {
private:
    std::atomic<MemTable*> memTable_;
    leveldb::port::Mutex mu_;
    leveldb::port::CondVar memtabl_state_cv_ GUARDED_BY(mu_);

    std::vector<MemTable*> memTable_list_;

    std::queue<MemTable*> free_menTables;

    std::mutex memtable_mux_; // 仅仅用于管理memtable的空闲列表
    std::mutex memtable_insert_mux_;  // 用于管理多线程插入memtable
    std::condition_variable memtable_cv_; // 等待插入新memtable

    // 保证mem, im_mem, versionset 是同一个版本
    std::mutex level_0_mux_; // update level-0 file in fileMetaCache[0]
    std::vector<std::vector<SSTableCache*>*>
        fileMetaCache;  // Each level has some SSTableCache
    // Returns pointers in timestamp order.
    std::vector<std::list<SSTableCache*>> index2cache_;
    std::vector<
        std::unordered_map<SSTableCache*, std::list<SSTableCache*>::iterator>>
        cache2index_;
    uint64_t currentTime;  // file id
    std::string dataDir;
    std::atomic<SequenceNumber_t> global_seq{0};  // edge time
    // First topology sequence that has not yet been used by this shard.
    // Kept separately from global_seq because the latter reserves whole
    std::atomic<SequenceNumber_t> next_topology_sequence_{0};
    SequenceNumber_t MAX_GLOBAL_SEQ =
        std::numeric_limits<SequenceNumber_t>::max();
    ThreadPool worker_pool;
    const size_t max_vertex_num_;  // max vertex_id supported by the system
    const size_t memtable_size_;
    std::atomic<VertexId_t> vertex_id_;  // max vertex_id actually used
    std::unordered_map<std::string, std::ifstream*> file_handle_cache_;  // cache read file head
    BlockManager block_manager_;
    SSTDataManager sstdata_manager_;

    DelRecordManage del_record_manager_;

    PropertyObjectKind property_object_kind_{PropertyObjectKind::kEdge};
    uint32_t property_shard_id_{0};
    PropertyUpdateOptions property_update_options_{};
    std::unique_ptr<PropertyDeltaStore> property_delta_store_;
    // Attachments take a shared lock. Compaction takes the exclusive lock,
    // drains every durable chain, and retains it through publication.
    mutable std::shared_mutex property_delta_compaction_mutex_;

    PropertyDeltaTarget MakePropertyDeltaTarget(FileId_t base_file_id,
                                                int property_id) const;
    void ObserveTopologySequence(SequenceNumber_t sequence);
    std::shared_ptr<const PropertyDeltaReadView> LoadPropertyDeltaView(
        FileId_t base_file_id, int property_id) const;
    void MarkPropertyDeltaTargetPersistent(FileId_t base_file_id);
    bool LocateSstEdgeOrdinal(FileId_t target_fid,
                              const PropertyDeltaRecord& record,
                              size_t* edge_ordinal) const;
    bool MergePropertyDeltaColumn(
        const PropertyDeltaTarget& target,
        const std::shared_ptr<const MappedPropertyFile>& current_base,
        const std::vector<std::shared_ptr<PropertyDeltaFile>>& deltas,
        uint64_t output_generation,
        std::shared_ptr<MappedPropertyFile>* merged_base,
        std::string* error);
    std::shared_ptr<void> PreparePropertyDeltasForCompaction(
        std::string* error);

    //level file index
    livegraph::SparseArrayAllocator<void> array_allocator;
    Futex *vertex_futexes_;
    size_t vertex_lock_count_ = 0;
    RWLock_t *vertex_rwlocks_;            // read and update level_index
    LevelIndex *vid_to_levelIndex_;       // vid to level_index: v_num*(level-1)
  #ifdef MMAP_LEVEL_INDEX
    MulLevelIndexSharedArray vid_to_mullevelIndex_;
  #elif defined(MMAP_DIFF_SIZE_LEVEL_INDEX)
    MulLevelIndexArrayWrapper vid_to_mullevelIndex_;
  #elif defined(MMAP_COLD_HOT_LEVEL_INDEX)
    MulLevelIndexWrapper vid_to_mullevelIndex_;
  #else
    LazyMulLevelIndexArray vid_to_mullevelIndex_; // sparse vid to mullevel_index
  #endif

    // vertex max level
    Level_t* vertex_max_level_;  // max level of a vertex's edge exist, here, mem is level-0, eq., L_0=1

    VersionSet *l0_versionset_;
    SuperVersion sv_;

    std::atomic<SequenceNumber_t> global_version_id_ = 0;
    static thread_local SequenceNumber_t local_version_id_;
    static thread_local SuperVersion local_sv_;

    std::vector<uint32_t> sub_property_lengths_;

    // about compacton
    Compaction compactor_;

    // Single-level CSR disk mode. The multilevel LSM path remains available
    // when this option is disabled.
    bool use_csr_disk_ = false;
    // Immutable CSR snapshots are atomically published for lock-free reads.
    // Snapshot destruction releases its file references.
    struct CSRReadIndexEntry {
      SSTableCache* cache = nullptr;
      uint32_t s_offset = 0;
      uint32_t e_offset = 0;
    };
    struct CSRReadSnapshot {
      std::vector<SSTableCache*> active_files;
      std::vector<CSRReadIndexEntry> src_index;
      ~CSRReadSnapshot();
    };
    std::shared_ptr<const CSRReadSnapshot> csr_read_snapshot_;

    // CSR 重建调度状态（单 worker + pending 合并）：
    // - flush 线程只负责“投递重建请求”，不直接做重建；
    // - worker 串行执行重建，避免多个重建互相覆盖；
    // - WaitCSRUpToDate() 通过该状态等待“flush + 重建”全部追平。
    std::mutex csr_rebuild_state_mutex_;
    std::condition_variable csr_rebuild_cv_;
    std::thread csr_rebuild_worker_;
    bool csr_rebuild_worker_stop_ = false;
    bool csr_rebuild_pending_ = false;
    bool csr_rebuild_running_ = false;
	    std::atomic<uint64_t> csr_flush_jobs_inflight_{0};
	    std::atomic<uint64_t> background_flush_jobs_inflight_{0};
    bool is_csr_ = false;
    uint32_t csr_l0_max_sst_num_ = 8;
    uint32_t csr_l1_max_sst_num_ = 10000;

    std::shared_ptr<const CSRReadSnapshot> LoadCSRReadSnapshot() const;
    const CSRReadIndexEntry* FindCSRIndexEntry(const CSRReadSnapshot* snapshot,
                                               VertexId_t src) const;

	    void ScheduleCSRRebuild();
	    void CSRRebuildWorkerMain();
	    void MarkCSRFlushJobDone();
	    void BeginBackgroundFlushJob();
	    void MarkBackgroundFlushJobDone();
	    void MaybeRebuildCSRFromDisk();
    void RebuildCSRFromVisibleSSTs();
    Status find_edge_in_csr_disk(VertexId_t src, VertexId_t dst,
                                 std::string* property, int property_id,
                                 bool is_out, uint8_t edge_type);
    Status find_edge_seq_in_csr_disk(VertexId_t src, VertexId_t dst,
                                     FileId_t& fid, SequenceNumber_t& seq);


public:
    LSMStore(const std::string &dir, const size_t max_vertex_num,
             int num_threads, int memtable_num,
             std::vector<uint32_t> sub_property_lengths = {},
             size_t memtable_size = FLAGS_memtable_size,
             bool is_csr = false,
             uint32_t csr_l0_max_sst_num = 8,
             uint32_t csr_l1_max_sst_num = 10000,
             PropertyObjectKind property_object_kind =
                 PropertyObjectKind::kEdge,
             uint32_t property_shard_id = 0,
             PropertyUpdateOptions property_update_options = {});

    ~LSMStore();

    // Waits for committed memtables to flush and for all resulting CSR
    // rebuilds to finish. This provides a deterministic write-then-read
    // barrier without a fixed sleep.
    void WaitCSRUpToDate();

    Status del_edge(VertexId_t src, VertexId_t dst, int property_id) override;

    Status del_edge_sep(VertexId_t src, VertexId_t dis) override;

    VertexId_t new_vertex(bool use_recycled_vertex = false) override;

    // 预初始化 [0, next_vertex_id) 的顶点辅助结构，便于外部先完成 re-id
    // 再直接按显式顶点 id 写入。
    void InitVerticesUpTo(VertexId_t next_vertex_id);

    void put_vertex(VertexId_t vertex_id, std::string_view data) override;

    void put_edge(VertexId_t src, VertexId_t dst, const EdgeProperty_t &s,
                  EdgeInsertMode insert_mode = EdgeInsertMode::kSingle,
                  bool is_out = true,
                  uint8_t edge_type = 0,
                  SequenceNumber_t sq = -1) override;

    void update_edge(VertexId_t src, VertexId_t dis, const EdgeProperty_t &s,
                     Marker_t mk = -1, SequenceNumber_t sq = -1,
                     bool is_out = true,
                     uint8_t edge_type = 0) override;

    Status get_edge(VertexId_t src, VertexId_t dst, std::string* property,
                    int property_id = -1, bool is_out = true,
                    uint8_t edge_type = 0) override;

    Status GetEdge(VertexId_t src, VertexId_t dst, std::string* property,
                   int property_id = -1, bool is_out = true,
                   uint8_t edge_type = 0) override;

    Status LocateEdge(VertexId_t src, VertexId_t dst,
                      LSMEdgeLocation* location,
                      bool is_out = true,
                      uint8_t edge_type = 0) override;

    Status get_edge(VertexId_t src, VertexId_t dst,
                              FileId_t& fid, SequenceNumber_t& seq);

    Status find_edge(VertexId_t src, VertexId_t dst, std::string* property,
                     SSTableCache* it, int property_id,
                     bool is_out = true,
                     uint8_t edge_type = 0) override;

    Status find_edge(VertexId_t src, VertexId_t dst,
                               SequenceNumber_t& seq, SSTableCache* it);

    Status find_edge_in_memtable(VertexId_t src, VertexId_t dst,
                                 std::string* property, int property_id,
                                 bool is_out = true,
                                 uint8_t edge_type = 0) override;

    Status find_edge_in_SStableCache(VertexId_t src, VertexId_t dst,
                                     std::string* property, int property_id,
                                     bool is_out = true,
                                     uint8_t edge_type = 0) override;
    
    Status find_edge_in_lonely_SStableCache(VertexId_t src, VertexId_t dst,
                                            std::string* property, int property_id,
                                            SSTableCache* it,
                                            bool is_out = true,
                                            uint8_t edge_type = 0) override;

    Status find_edge_by_levelindex(VertexId_t src, VertexId_t dst,
                                   std::string* property, SuperVersion& sv,
                                   int property_id, bool is_out = true,
                                   uint8_t edge_type = 0) override;

    Status find_edge_by_levelindex(VertexId_t src, VertexId_t dst,
                                         FileId_t& fid, SequenceNumber_t& seq,
                                         SuperVersion& local_sv);
    Status find_edge_by_levelindex(VertexId_t src, VertexId_t dst,
                                   FileId_t& fid, SequenceNumber_t& seq,
                                   SuperVersion& local_sv, bool is_out,
                                   uint8_t edge_type);
    
                                         
    bool get_edges(VertexId_t src, std::vector<Edge>& edges) override;

    EdgeIterator get_edges(VertexId_t src, SequenceNumber_t seq = MAX_SEQ_ID,
                           int sub_property_id = -1,
                           bool is_out = true,
                           uint8_t edge_type = 0) override;

    EdgeIterator Get_Edges(VertexId_t src, SequenceNumber_t seq = MAX_SEQ_ID,
                           int sub_property_id = -1,
                           bool is_out = true,
                           uint8_t edge_type = 0) override;

    void print_memTable(std::string label) override;

    SequenceNumber_t automic_get_global_seq(SequenceNumber_t num);

    SequenceNumber_t LastSequence();

    void SetLastSequence(SequenceNumber_t last_sequence);

    SequenceNumber_t NextTopologySequence() const {
      return next_topology_sequence_.load(std::memory_order_acquire);
    }

    bool find(VertexId_t target, uint32_t s_offset, uint32_t e_offset,
            uint32_t step_offset, std::ifstream& file,
            uint32_t& obj_offset) override;

    Status find_edge_from_file_with_cache(VertexId_t target, uint32_t s_offset,
            uint32_t e_offset, std::ifstream* file,
            std::string& path, std::string* property, int property_id,
            bool is_out, uint8_t edge_type);

    Status find_edge_from_file_with_cache_by_directed_IO(
                                    VertexId_t src, VertexId_t dst,
                                    uint32_t s_offset, uint32_t e_offset,
                                    uint32_t fileID, SequenceNumber_t& seq);

    Status find_edge_from_file_with_cache(VertexId_t target,
                                                uint32_t s_offset,
                                                uint32_t e_offset,
                                                std::ifstream* file,
                                                std::string& path,
                                                SequenceNumber_t& seq);

    Status find_edges_from_file_with_cache(uint32_t s_offset, uint32_t e_offset,
            std::ifstream* file, SSTableCache* sst, std::vector<Edge>& edges);

    Status find_edge_from_sstdata_cache(VertexId_t src, VertexId_t dst, uint32_t s_offset,
            uint32_t e_offset, uint32_t fileID, std::string* property,
            int property_id, bool is_out, uint8_t edge_type);

    Status find_edge_from_sstdata_cache(VertexId_t src, VertexId_t dst,
                                    uint32_t s_offset, uint32_t e_offset,
                                    uint32_t fileID, SequenceNumber_t& seq);
    Status find_edge_from_sstdata_cache(VertexId_t src, VertexId_t dst,
                                    uint32_t s_offset, uint32_t e_offset,
                                    uint32_t fileID, SequenceNumber_t& seq,
                                    bool is_out, uint8_t edge_type);

    Status find_edges_from_sstdata_cache(uint32_t s_offset, uint32_t e_offset,
            uint32_t fileID, std::vector<Edge>& edges);

    MemTable* get_newmemTable();

    void recycle_memTable(MemTable* table);

    void check_vertex_id(VertexId_t vertex_id);


    VertexId_t get_max_vertex_num() override;

    void compact();

	    bool GetCompactionState() override;

	    bool HasBackgroundWork() override;

	    void WaitBackgroundIdle() override;

    bool ForceCompactAllL0ToL1() override;

    void  print_all_file_info();

    void static_edge_distribution();

    void get_superversion(SuperVersion& sv);

    void get_superversion();

    SequenceNumber_t get_sequence() const override;

    void debug() override;


    void AwaitWrite();

    bool CheckState() const;

    SSTDataManager * GetSSTDataManager() override;


    void Debug() override;

    void un_map() override;

    void clean() override;

    Status AttachPropertyDelta(FileId_t target_fid,
                               int property_id,
                               bool target_is_persistent,
                               std::vector<PropertyDeltaRecord> records);
    bool WaitPropertyDeltaIdle(std::string* error = nullptr);
    PropertyDeltaStoreStats GetPropertyDeltaStats() const;
    PropertyCommitSequence GetMaxPropertyCommitSequence() const;

};

}  // namespace lsmgraph
#endif
