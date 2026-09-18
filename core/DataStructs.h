#ifndef DATASTRUCTS_H
#define DATASTRUCTS_H

#include <unistd.h>
#include <atomic>
#include <string>
#include <sys/mman.h>
#include <fcntl.h>
#include <csignal>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <vector>
#include <queue>
#include "core/graph/edge.h"
#include "util/ConcurrentHashMap.h"
#include "util/ConcurrentUnorderedMap.h"

#define likely(x) __builtin_expect(!!(x), 1)       
#define unlikely(x) __builtin_expect(!!(x), 0)
#define LEVEL_NUM_MASK 0xE000000000000000
#define INDEX_MASK 0x1FFFFFFFFFFFFFFF

namespace lsmgraph {

#define MAX_LEVEL 5  // level-4: file_num=20000 capcity= 1388.875 G
#define LEVEL_INDEX_SIZE (MAX_LEVEL - 1)
#define INVALID_OFFSET 0xFFFFFFFF
#define MMAP_INITIAL_SIZE (1ul << 20)
#define MMAP_CHUNK_SIZE (1ul << 29)


#define INDEX_MAP_TYPE 1  // 0: array, 1: hashmap, 2: unordermap
#define COLD_LEVEL_NUM 2  // 放在磁盘上的层数
#define HOT_LEVEL_NUM 4   // 放在内存中的层数
    struct Header {
      uint64_t timeStamp;  // file id
      uint64_t size;       // edge num
      uint64_t index_size; // src  num
      uint64_t minKey, maxKey;

      Header() : timeStamp(0), size(0), index_size(0), minKey(0), maxKey(0) {}
    };

    struct __attribute__ ((__packed__)) Index {
      VertexId_t key;
      EdgeOffset_t offset;

      Index(uint64_t k = 0, uint32_t o = 0) : key(k), offset(o) {}
    };

    struct __attribute__ ((__packed__)) File_Index {
      uint32_t fileID; // 前4bit为levelID
      uint32_t offset;
      uint32_t next_offset;

      File_Index(uint32_t _fileID = 0,
                 EdgeOffset_t _offset = 0,
                 EdgeOffset_t _next_offset = 0)
              : fileID(_fileID), offset(_offset), next_offset(_next_offset) {}
    };

    struct Range {
      uint64_t min, max;

      Range(const uint64_t &i, const uint64_t &a) : min(i), max(a) {}
    };

    class LevelIndex {
    public:
      LevelIndex() : fileID(INVALID_File_ID),
                     offset(INVALID_OFFSET),
                     next_offset(INVALID_OFFSET) {}

      LevelIndex(uint32_t _fid,
                 uint32_t _offset,
                 uint32_t _next_offset) : fileID(_fid),
                                          offset(_offset),
                                          next_offset(_next_offset) {}

      void init() {
        fileID = INVALID_File_ID;
        offset = INVALID_OFFSET;
        next_offset = INVALID_OFFSET;
      }

      void set_fileID(uint32_t fid) {
        this->fileID = fid;
      }

      uint32_t get_fileID() const {
        return this->fileID;
      }

      void set_offset(uint32_t offset) {
        this->offset = offset;
      }

      uint32_t get_offset() const {
        return this->offset;
      }

      void set_next_offset(uint32_t next_offset) {
        this->next_offset = next_offset;
      }

      uint32_t get_next_offset() const {
        return this->next_offset;
      }

    private:
      uint32_t fileID = INVALID_File_ID; // levelid从数组下标推断
      uint32_t offset = INVALID_OFFSET;
      uint32_t next_offset = INVALID_OFFSET;
    };

    class MulLevelIndex_old {
    public:
      void init() {
        min_level_0_fid = 0;
        for (int i = 0; i < LEVEL_INDEX_SIZE; i++) {
          fileID[i] = INVALID_File_ID;
          offset[i] = INVALID_OFFSET;
          next_offset[i] = INVALID_OFFSET;
        }
      }

      void set_min_level_0_fid(FileId_t fid) {
        min_level_0_fid = fid;
      }

      FileId_t get_min_level_0_fid() {
        return min_level_0_fid;
      }

      void set_fileID(int level, FileId_t fid) {
        this->fileID[level] = fid;
      }

      FileId_t get_fileID(int level) const {
        return this->fileID[level];
      }

      void set_offset(int level, uint32_t offset) {
        this->offset[level] = offset;
      }

      uint32_t get_offset(int level) const {
        return this->offset[level];
      }

      void set_next_offset(int level, uint32_t next_offset) {
        this->next_offset[level] = next_offset;
      }

      uint32_t get_next_offset(int level) const {
        return this->next_offset[level];
      }

      void print() {
        for (int i = 0; i < LEVEL_INDEX_SIZE; i++) {
          std::cout << " i=" << i
                    << " fid=" << get_fileID(i)
                    << " offset=" << get_offset(i)
                    << " next_offset=" << get_next_offset(i)
                    << std::endl;
        }
      }

    private:
      FileId_t min_level_0_fid = 0;       // minimum fid for readability
      FileId_t fileID[LEVEL_INDEX_SIZE];  // levelid从数组下标推断
      uint32_t offset[LEVEL_INDEX_SIZE];
      uint32_t next_offset[LEVEL_INDEX_SIZE];
    };

#ifdef OPT_MERGE_MULTI_LEVEL
    static const uint32_t COPY_FLAG_MASK = 0x80000000; // Highest bit set
    static const uint32_t FID_MASK = 0x7FFFFFFF; // All bits except the highest
#endif

    class MulLevelIndex {
    public:
      friend class MulLevelIndexWithDiffSize;

      struct LevelData {
        FileId_t fileID = INVALID_File_ID;
        uint32_t offset = INVALID_OFFSET;
        uint32_t next_offset = INVALID_OFFSET;
      };

      void init() {
        min_level_0_fid = 0;
        for (int i = 0; i < LEVEL_INDEX_SIZE; i++) {
          set_fileID(i, INVALID_File_ID);
#ifdef OPT_MERGE_MULTI_LEVEL
          setCopyFlag(i, INVALID_OFFSET);
#endif
          levels[i].offset = INVALID_OFFSET;
          levels[i].next_offset = INVALID_OFFSET;
        }
      }

      void set_level_data(int level, FileId_t fid,
                          uint32_t offset, uint32_t next_offset) {
        levels[level].fileID = fid;
        levels[level].offset = offset;
        levels[level].next_offset = next_offset;
      }

      const LevelData &get_level_data(int level) const {
        return levels[level];
      }

      const LevelData *get_level_data_ptr(int level) const {
        return levels;
      }

      void set_min_level_0_fid(FileId_t fid) {
        min_level_0_fid = fid;
      }

      const FileId_t get_min_level_0_fid() const {
        return min_level_0_fid;
      }

#ifdef OPT_MERGE_MULTI_LEVEL
      void set_fileID(int level, FileId_t fid) {
          // Clear the lower 31 bits and then set the value
          levels[level].fileID = (levels[level].fileID & COPY_FLAG_MASK) | (fid & FID_MASK);
      }

      const FileId_t get_fileID(int level) const {
          return levels[level].fileID & FID_MASK;
      }

      // Set the copy flag
      void setCopyFlag(int level, bool value) {
          if (value) {
              levels[level].fileID |= COPY_FLAG_MASK; // Set the highest bit
          } else {
              levels[level].fileID &= FID_MASK; // Clear the highest bit
          }
      }

      // Get the copy flag
      bool getCopyFlag(int level) const {
          return (levels[level].fileID & COPY_FLAG_MASK) != 0;
      }

      bool readable(int level) const {
        return !getCopyFlag(level) && get_fileID(level) != INVALID_File_ID;
      }

#else

      void set_fileID(int level, FileId_t fid) {
        levels[level].fileID = fid;
      }

      const FileId_t get_fileID(int level) const {
        return levels[level].fileID;
      }

      bool readable(int level) const {
        return get_fileID(level) != INVALID_File_ID;
      }

#endif

      void set_offset(int level, uint32_t offset) {
        levels[level].offset = offset;
      }

      const uint32_t get_offset(int level) const {
        return levels[level].offset;
      }

      void set_next_offset(int level, uint32_t next_offset) {
        levels[level].next_offset = next_offset;
      }

      const uint32_t get_next_offset(int level) const {
        return levels[level].next_offset;
      }

      void copy_from_ptr(MulLevelIndex *ptr, int level = LEVEL_INDEX_SIZE) {
        memcpy(this, ptr,
               sizeof(FileId_t)
               + level * sizeof(MulLevelIndex::LevelData));
      }

      void copy_from_obj(MulLevelIndex &obj, int level = LEVEL_INDEX_SIZE) {
        set_min_level_0_fid(obj.get_min_level_0_fid());
        memcpy(this,
               obj.get_level_data_ptr(0),
               level * sizeof(MulLevelIndex::LevelData));
      }

#if defined(MMAP_DIFF_SIZE_LEVEL_INDEX) || defined(MMAP_COLD_HOT_LEVEL_INDEX)
      void set_level_num(int _level_num) {
        level_num = _level_num;
      }

      int get_level_num() {
        return level_num;
      }
#endif

      void print() const {
        for (int i = 0; i < LEVEL_INDEX_SIZE; i++) {
          std::cout << " i=" << i
                    #ifdef OPT_MERGE_MULTI_LEVEL
                    << " getCopyFlag=" << getCopyFlag(i)
                    #endif
                    << " fid=" << get_fileID(i)
                    << " offset=" << get_level_data(i).offset
                    << " next_offset=" << get_level_data(i).next_offset
                    << std::endl;
        }
      }

    private:
      // Reserved to preserve the persisted 64-byte MulLevelIndex layout.
      int *level_fd;
      LevelData levels[LEVEL_INDEX_SIZE];
      uint32_t min_level_0_fid = 0; // minimum fid for readability
#if defined(MMAP_DIFF_SIZE_LEVEL_INDEX) || defined(MMAP_COLD_HOT_LEVEL_INDEX) 
      int level_num = 0;
#endif
    };

    static_assert(std::is_same<FileId_t, uint32_t>::value,
                  "FileId_t must be a uint32_t");
    static_assert(sizeof(MulLevelIndex) == 64,
                  "MulLevelIndex size exceeds the expected limit!");

    class LazyMulLevelIndexArray {
     public:
      LazyMulLevelIndexArray() = default;
      explicit LazyMulLevelIndexArray(uint64_t max_vertex_num) {
        ResetStorage(max_vertex_num);
      }

      LazyMulLevelIndexArray(const LazyMulLevelIndexArray&) = delete;
      LazyMulLevelIndexArray& operator=(const LazyMulLevelIndexArray&) = delete;

      void reset(uint64_t max_vertex_num) {
        std::lock_guard<std::mutex> lock(mutex_);
        ResetStorage(max_vertex_num);
      }

      void init(VertexId_t /*vid*/) {
        // Sparse reset path: a missing entry is already logically initialized.
      }

      MulLevelIndex& operator[](VertexId_t vid) {
        const size_t block_id = static_cast<size_t>(vid >> kBlockShift);
        const size_t offset = static_cast<size_t>(vid & (kBlockSize - 1));
        MulLevelIndex* block = EnsureBlock(block_id);
        return block[offset];
      }

      size_t allocated_blocks() const {
        size_t count = 0;
        for (size_t block_id = 0; block_id < block_count_; ++block_id) {
          count += published_blocks_[block_id].load(
                       std::memory_order_acquire) != nullptr;
        }
        return count;
      }

      size_t allocated_bytes() const {
        return allocated_blocks() * kBlockSize * sizeof(MulLevelIndex);
      }

     private:
      static constexpr size_t kBlockShift = 20;
      static constexpr size_t kBlockSize = size_t{1} << kBlockShift;

      static size_t BlockCount(uint64_t max_vertex_num) {
        return static_cast<size_t>((max_vertex_num + kBlockSize - 1) >>
                                   kBlockShift);
      }

      void ResetStorage(uint64_t max_vertex_num) {
        // reset() is a configuration-time operation. Callers must ensure no
        // concurrent index access while replacing the backing arrays.
        max_vertex_num_ = max_vertex_num;
        block_count_ = BlockCount(max_vertex_num_);
        owned_blocks_.clear();
        owned_blocks_.resize(block_count_);
        if (block_count_ == 0) {
          published_blocks_.reset();
          return;
        }
        published_blocks_ =
            std::make_unique<std::atomic<MulLevelIndex*>[]>(block_count_);
        for (size_t block_id = 0; block_id < block_count_; ++block_id) {
          published_blocks_[block_id].store(nullptr, std::memory_order_relaxed);
        }
      }

      MulLevelIndex* EnsureBlock(size_t block_id) {
        if (unlikely(block_id >= block_count_)) {
          throw std::out_of_range("vertex id exceeds configured maximum");
        }
        MulLevelIndex* block =
            published_blocks_[block_id].load(std::memory_order_acquire);
        if (likely(block != nullptr)) {
          return block;
        }

        std::lock_guard<std::mutex> lock(mutex_);
        block = published_blocks_[block_id].load(std::memory_order_relaxed);
        if (block == nullptr) {
          owned_blocks_[block_id] =
              std::make_unique<MulLevelIndex[]>(kBlockSize);
          block = owned_blocks_[block_id].get();
          published_blocks_[block_id].store(block, std::memory_order_release);
        }
        return block;
      }

      uint64_t max_vertex_num_ = 0;
      size_t block_count_ = 0;
      std::unique_ptr<std::atomic<MulLevelIndex*>[]> published_blocks_;
      std::vector<std::unique_ptr<MulLevelIndex[]>> owned_blocks_;
      mutable std::mutex mutex_;
    };

    /// @brief way1: 一种Level-index的n层固定大小数组实现的level-index
    /// @details n个顶点级别的数组，每个数组存储n层的索引
    class MulLevelIndexSharedArray {
    private:

      struct MulLevelIndexArray {
        uint64_t vertex_num_;
        MulLevelIndex *data_;
        int fd_;
        std::mutex mutex;
        std::string db_dir_;


        MulLevelIndexArray(const std::string &db_dir, uint64_t vertex_num) 
            : vertex_num_(vertex_num), 
              data_(nullptr), 
              fd_(-1), mutex() {

          db_dir_ = db_dir + "/multi-level";
          size_t capacity = vertex_num_ * sizeof(MulLevelIndex);

          printf(" multi-level index path=%s vertex_num=%ld\n", 
                 (db_dir + "/multi-level").c_str(), vertex_num);

#ifdef MEM_MALLOC
          data_ = new MulLevelIndex[vertex_num_];
#else
          fd_ = open((db_dir + "/multi-level").c_str(), O_RDWR | O_CREAT, 0666);
          if (fd_ == -1) {
            perror("Open: ");
            throw std::runtime_error("Unable to open file");
          }

          lseek(fd_, 0, SEEK_SET);
          if (ftruncate(fd_, capacity) == -1) {
            close(fd_);
            perror("Adjust mmap file error: ");
            throw std::runtime_error("Error adjusting file size");
          }

          
          // 参数:
          //   MAP_SHARED
          data_ = static_cast<MulLevelIndex *>(mmap(nullptr, 
                                                    capacity,
                                                    PROT_READ | PROT_WRITE,
                                                    MAP_SHARED, fd_,
                                                    0));
          if (data_ == MAP_FAILED) {
            close(fd_);
            perror("mmap: ");
            throw std::runtime_error("Error mapping file to memory");
          }

#ifdef MMAP_MLOCK
          // 锁定内存区域
          if (mlock(data_, capacity) == -1) {
              perror("mlock");
              exit(0);
          }
#endif

          if (madvise(data_, capacity, MADV_WILLNEED) != 0)
                throw std::runtime_error("madvise block error.");
#endif
        }

        MulLevelIndexArray &operator=(const MulLevelIndexArray &rhs) = delete;

        MulLevelIndexArray &operator=(MulLevelIndexArray &&rhs) = delete;

        ~MulLevelIndexArray() {
#ifdef MEM_MALLOC
          delete[] data_;
#else
          if (data_ != nullptr) {
            msync(data_, vertex_num_ * sizeof(MulLevelIndex), MS_SYNC);
            munmap(data_, vertex_num_ * sizeof(MulLevelIndex));
          }
          if (fd_ != -1) {
            close(fd_);
          }
#endif
#ifdef MMAP_MLOCK
          printf("@mlock: open\n");
#endif
          printf("Size of MulLevelIndexSharedArray mmap: %lu MB\n", 
                 ((vertex_num_ * sizeof(MulLevelIndex)) >> 20));
        }

        void adjust_file_size(uint64_t new_size) {
          std::lock_guard<std::mutex> lock(mutex);
          if (new_size <= vertex_num_) {
            return ;
          }
          size_t length = vertex_num_ * sizeof(MulLevelIndex);
          size_t new_length = new_size * sizeof(MulLevelIndex);

          vertex_num_ = new_size;
#ifdef MEM_MALLOC
          MulLevelIndex* data_2_ = new MulLevelIndex[new_size];
          memcpy(data_2_, data_, length);
          delete[] data_;
          data_ = data_2_;
#else
          if (fd_ != -1 && 
              ftruncate(fd_, new_length) != 0) {
            close(fd_);
            perror("Adjust mmap file error: ");
            throw std::runtime_error("Error adjusting file size");
          }

#ifdef MMAP_MLOCK
          // 锁定内存区域
          if (munlock(data_, length) == -1) {
              perror("munlock");
              exit(0);
          }
#endif

          data_ = static_cast<MulLevelIndex *>(mmap(nullptr, new_length,
                                                    PROT_READ | PROT_WRITE,
                                                    MAP_SHARED, fd_,
                                                    0));
          if (data_ == MAP_FAILED) {
            close(fd_);
            perror("mmap: ");
            throw std::runtime_error("Error mapping file to memory");
          }
#ifdef MMAP_MLOCK
          // 锁定新扩展的内存区域
          if (mlock(data_, new_length) == -1) {
              perror("mlock");
              exit(0);
          }
#endif
#endif

          printf("Adjusting Level Index File Size to %lu MB\n", 
                 ((new_length) >> 20));
        }

        void copy_index_from_file(VertexId_t src, MulLevelIndex *index) {
          size_t index_offset = sizeof(MulLevelIndex) * src;

          int fd = open(db_dir_.c_str(), O_RDONLY);
          if (fd == -1) {
            perror("open file: ");
            throw std::runtime_error(db_dir_);
          }

          ssize_t size = pread(fd, reinterpret_cast<char *>(index), sizeof(MulLevelIndex), index_offset);

          if (size <= 0) {
            close(fd_);
            perror("mmap: ");
            throw std::runtime_error("Error index.sieze <= 0");
          }

          close(fd);
        }

      };

      std::shared_ptr<MulLevelIndexArray> arr_ptr_;


    public:

      MulLevelIndexSharedArray() : arr_ptr_(nullptr) {}

      MulLevelIndexSharedArray(const std::string &db_dir, uint64_t vertex_num) :
              arr_ptr_(new MulLevelIndexArray(db_dir, vertex_num)) {}


      MulLevelIndex &operator[](size_t index) {
        if (unlikely(index >= arr_ptr_->vertex_num_)) {
          uint64_t file_size = ((index << 6) / MMAP_CHUNK_SIZE + 1) * MMAP_CHUNK_SIZE; // vertex_num << 6 = file size
          arr_ptr_->adjust_file_size(file_size >> 6); // file_size >> 6 = vertex_num
        }
        return arr_ptr_->data_[index];
      }

      void copy_index_from_file(VertexId_t src, MulLevelIndex *index) {
        arr_ptr_->copy_index_from_file(src, index);
      }


    };

    // 这是一个存储level_index的数据类，包含level_0_fid和level_data
    // 其中level_data存储了index的具体数据，index的size可以不同
    // level=i的数据存储在level=i
    class MulLevelIndexWithDiffSize {
      public:
        struct LevelData {
          FileId_t fileID = INVALID_File_ID;
          uint32_t offset = INVALID_OFFSET;      // 前3位表示level_id
          uint32_t next_offset = INVALID_OFFSET;
        };

        MulLevelIndexWithDiffSize(char* ptr, int _level_num) : 
          level_data(reinterpret_cast<LevelData*>(ptr+sizeof(min_level_0_fid))), 
          level_num(_level_num),
          min_level_0_fid(*reinterpret_cast<uint32_t*>(ptr)){}

        void init() {
          min_level_0_fid = 0;
          level_num = 0;
          level_data = nullptr;
        }

        void init(int level) {
          min_level_0_fid = 0;
          level_num = level;
          for (int i = 0; i < level_num; i++) {
            set_level_id(i, 0);
            set_fileID(i, INVALID_File_ID);
            set_offset(i, INVALID_OFFSET);
            set_next_offset(i, INVALID_OFFSET);
          }
        }

        void set_level_data(int i, int level_id, FileId_t fid,
                            uint32_t offset, uint32_t next_offset) {
          set_level_id(i, level_id);
          set_fileID(i, fid);
          set_offset(i, offset);
          set_next_offset(i, next_offset);
        }

        // LevelData内部数据进行了编码，不建议在外部使用
        LevelData *get_level_data_ptr(int i) const {
          return &level_data[i];
        }


        // 获得数组第i个元素存储的Level_id
        const uint32_t get_level_id(int i) const {
          return (level_data[i].offset >> 29) & 0x07;
        }

        void set_level_id(uint32_t i, uint32_t level_id) {
          assert(i < level_num);
          level_data[i].offset &= 0x1FFFFFFF;
          level_data[i].offset |= (level_id & 0x07) << 29;
        }

        const uint32_t get_min_level_0_fid() const {
          return min_level_0_fid;
        }

        void set_min_level_0_fid(uint32_t minLevel0Fid) {

          min_level_0_fid = minLevel0Fid;
        }

        const uint32_t get_offset(int i) const {
          return level_data[i].offset & 0x1FFFFFFF;
        }

        void set_offset(int i, uint32_t offset) {
          assert(i < level_num);
          level_data[i].offset &= 0xE0000000;
          level_data[i].offset |= offset & 0x1FFFFFFF;
        }

  #ifdef OPT_MERGE_MULTI_LEVEL
        void set_fileID(int i, FileId_t fid) {
            // Clear the lower 31 bits and then set the value
            level_data[i].fileID = (level_data[i].fileID & COPY_FLAG_MASK) | (fid & FID_MASK);
        }

        const FileId_t get_fileID(int level) const {
            return level_data[level].fileID & FID_MASK;
        }

        // Set the copy flag
        void setCopyFlag(int level, bool value) {
            if (value) {
                level_data[level].fileID |= COPY_FLAG_MASK; // Set the highest bit
            } else {
                level_data[level].fileID &= FID_MASK; // Clear the highest bit
            }
        }

        // Get the copy flag
        bool getCopyFlag(int level) const {
            return (level_data[level].fileID & COPY_FLAG_MASK) != 0;
        }

        bool readable(int level) const {
          return !getCopyFlag(level) && get_fileID(level) != INVALID_File_ID;
        }

  #else

        void set_fileID(int i, FileId_t fid) {
          assert(i < level_num);
          level_data[i].fileID = fid;
        }

        const FileId_t get_fileID(int level) const {
          return level_data[level].fileID;
        }

        bool readable(int level) const {
          return get_fileID(level) != INVALID_File_ID;
        }

  #endif

        void set_next_offset(int level, uint32_t next_offset) {
          level_data[level].next_offset = next_offset;
        }

        const uint32_t get_next_offset(int level) const {
          return level_data[level].next_offset;
        }

        void copy_to_old_index(MulLevelIndex& index_old) {
          index_old.min_level_0_fid = this->min_level_0_fid;
          index_old.init();
          index_old.min_level_0_fid = min_level_0_fid;
#if defined(MMAP_DIFF_SIZE_LEVEL_INDEX) || defined(MMAP_COLD_HOT_LEVEL_INDEX)
          index_old.level_num = level_num;
#endif
          for (int i = 0; i < level_num; i++) {
            index_old.set_level_data(i,
                                     get_fileID(i), 
                                     get_offset(i), get_next_offset(i));
          }
        }

        static size_t get_element_size(int _level_num) {
          return sizeof(min_level_0_fid) 
                 + _level_num * sizeof(LevelData);
        }

        int get_level_num() {
          return level_num;
        }

        void print() const {
          std::cout << "\n min_level_0_fid=" << get_min_level_0_fid() 
                    << " level_num=" << level_num
                    << std::endl;
          for (int i = 0; i < level_num; i++) {
            std::cout << " i=" << i
                      << " level_id=" << get_level_id(i)
                      #ifdef OPT_MERGE_MULTI_LEVEL
                      << " getCopyFlag=" << getCopyFlag(i)
                      #endif
                      << " fid=" << get_fileID(i)
                      << " offset=" << get_offset(i)
                      << " next_offset=" << get_next_offset(i)
                      << std::endl;
          }
        }

      private:
        uint32_t& min_level_0_fid;  // 前三位表示层数
        LevelData* level_data = nullptr;
        int level_num = 0;
    };


    /// @brief MMAP实现的数组
    /// @details 可以存储指定层数的level-index
    /// 通过优先队列回收废弃数组空间
    struct MulLevelIndexMMapArray {
      struct CompareVertexId {
        bool operator()(const VertexId_t a, const VertexId_t b) const {
          return a > b;  // 返回 a > b，实现最小元素优先
        }
      };

      VertexId_t vertex_sum_num_ = 0;
      VertexId_t vertex_use_num_ = 0;
      std::priority_queue<VertexId_t, std::vector<VertexId_t>, 
                          CompareVertexId> free_array_id_queue_;
      char *data_;
      int fd_;
      std::mutex mutex;
      int level_num_ = 0;
      int element_size_ = 0;

      int batch_size_ = 1280; //80;
      int add_each_time_ = 10; //10;


      MulLevelIndexMMapArray(const std::string &db_dir, VertexId_t vertex_num,
                          int level_num) 
                          : vertex_sum_num_(vertex_num), data_(nullptr), 
                            fd_(-1), level_num_(level_num), mutex() {
        element_size_ = MulLevelIndexWithDiffSize::get_element_size(level_num_);

        std::string file_name = db_dir 
                              + "/multi-level_" + std::to_string(level_num_);
        size_t capacity = vertex_sum_num_ * element_size_;

        printf("multi-level index file:\n \
                path=%s vertex_num=%ld capacit=%ld\n", 
                file_name.c_str(), vertex_sum_num_, capacity);

        std::cout << " sizeof(MulLevelIndexWithDiffSize)="
                  << element_size_ 
                  << " level_num=" << level_num_
                  << std::endl;
        fd_ = open((file_name).c_str(), O_RDWR | O_CREAT, 0666);
        if (fd_ == -1) {
          perror("Open: ");
          throw std::runtime_error("Unable to open file");
        }
        lseek(fd_, 0, SEEK_SET);
        if (ftruncate(fd_, capacity) == -1) {
          close(fd_);
          perror("Adjust mmap file error: ");
          throw std::runtime_error("Error adjusting file size");
        }
        data_ = static_cast<char *>(mmap(nullptr, capacity,
                                                  PROT_READ | PROT_WRITE,
                                                  MAP_SHARED, fd_,
                                                  0));
        if (data_ == MAP_FAILED) {
          close(fd_);
          perror("mmap: ");
          throw std::runtime_error("Error mapping file to memory");
        }

        // 锁定内存区域

        if (madvise(data_, capacity, MADV_WILLNEED) != 0)
              throw std::runtime_error("madvise block error.");

        for (int i = 0; i < vertex_sum_num_; ++i) {
          free_array_id_queue_.push(i);
        }
      }

      char* get_level_index_ptr_by_array_id(VertexId_t array_id) {
        return data_ + array_id * element_size_;
      }

      char* get_level_index_by_array_id(VertexId_t array_id) {
        return data_ + array_id * element_size_;
      }

      char* get_new_array_id_and_ptr(VertexId_t& array_id) {
        std::lock_guard<std::mutex> lock(mutex);
        if (free_array_id_queue_.empty()) {
          adjust_file_size();
        } 
        array_id = free_array_id_queue_.top();
        free_array_id_queue_.pop();
        vertex_use_num_++;
        return data_ + array_id * element_size_;
      }

      void get_batch_new_array_id_and_ptr(
          std::vector<std::pair<VertexId_t, char*>>& free_ids, int batch_size) {
        std::lock_guard<std::mutex> lock(mutex);
        while (free_array_id_queue_.size() < batch_size) {
          adjust_file_size();
        } 
        for (int i = 0; i < batch_size; ++i) {
          VertexId_t array_id = free_array_id_queue_.top();
          free_array_id_queue_.pop();
          vertex_use_num_++;
          free_ids.push_back(
              std::make_pair(array_id, data_ + array_id * element_size_));
        }
      }

      void recycle_array_id(VertexId_t array_id) {
        std::lock_guard<std::mutex> lock(mutex);
        free_array_id_queue_.push(array_id);
        vertex_use_num_--;

      }

      MulLevelIndexMMapArray &operator=(const MulLevelIndexMMapArray &rhs) = delete;

      MulLevelIndexMMapArray &operator=(MulLevelIndexMMapArray &&rhs) = delete;

      ~MulLevelIndexMMapArray() {
        if (data_ != nullptr) {
          msync(data_, vertex_sum_num_ * element_size_, MS_SYNC);
          munmap(data_, vertex_sum_num_ * element_size_);
        }
        if (fd_ != -1) {
          close(fd_);
        }
        printf("~Size of MulLevelIndexMMapArray: %lu MB \t", 
                ((vertex_sum_num_ * element_size_) >> 20));
        printf("Utilization: %ld/%ld=%.2f%%", vertex_use_num_, vertex_sum_num_,
                vertex_use_num_*1.0/vertex_sum_num_*100);
        printf("\tfree_id_num: %ld, mem=%ld(M)\n", free_array_id_queue_.size(), 
                  free_array_id_queue_.size() * sizeof(VertexId_t) /1024/1024);
      }

      void adjust_file_size() {
        VertexId_t new_size = vertex_sum_num_ + batch_size_ * add_each_time_;
        size_t length = vertex_sum_num_ * element_size_;
        size_t new_length = new_size * element_size_;

        if (fd_ != -1 && 
            ftruncate(fd_, new_length) != 0) {
          close(fd_);
          perror("Adjust mmap file error: ");
          throw std::runtime_error("Error adjusting file size");
        }


        data_ = static_cast<char *>(mmap(nullptr, new_length,
                                                  PROT_READ | PROT_WRITE,
                                                  MAP_SHARED, fd_,
                                                  0));
        // 重新映射文件
        if (data_ == MAP_FAILED) {
          close(fd_);
          perror("mremap of mmap: ");
          throw std::runtime_error("Error mapping file to memory");
        }


        printf("Adjusting Level Index File Size to %lu MB\n", 
                ((new_length) >> 20));

        for (VertexId_t i = vertex_sum_num_; i < new_size; i++) {
          free_array_id_queue_.push(i);
        }
        vertex_sum_num_ = new_size;

        printf("Adjusting Level Index File Size to %lu MB of level_%d\n", 
                ((new_length) >> 20), level_num_);
      }

      size_t get_vertex_sum_num() const {
        return vertex_sum_num_;
      }

      size_t get_vertex_use_num() const {
        return vertex_use_num_;
      }
    };


    /// @brief way2: 一种Level-index的不同大小数组实现
    /// @details 每一层一种大小的数组，每个顶点的index通过一个顶点
    ///  即级别的数组进行索引，通过索引的前三位决定去那个大小的数组中去数据
    class MulLevelIndexArrayWrapper {
    private:
      struct MulLevelIndexArray;


      size_t vertex_num_;
      std::vector<uint64_t> vid_to_array_index_;
      std::vector<std::shared_ptr<MulLevelIndexArray>> arr_ptrs_;
      
      struct MulLevelIndexArray {
        struct CompareVertexId {
          bool operator()(const VertexId_t a, const VertexId_t b) const {
            return a > b;  // 返回 a > b，实现最小元素优先
          }
        };

        VertexId_t vertex_sum_num_ = 0;
        VertexId_t vertex_use_num_ = 0;
        std::priority_queue<VertexId_t, std::vector<VertexId_t>, 
                            CompareVertexId> free_array_id_queue_;
        char *data_;
        int fd_;
        std::mutex mutex;
        int level_num_ = 0;
        int element_size_ = 0;


        MulLevelIndexArray(const std::string &db_dir, VertexId_t vertex_num,
                           int level_num) 
                            : vertex_sum_num_(vertex_num), data_(nullptr), 
                              fd_(-1), level_num_(level_num), mutex() {
          element_size_ = MulLevelIndexWithDiffSize::get_element_size(level_num_);

          std::string file_name = db_dir 
                                + "/multi-level_" + std::to_string(level_num_);
          size_t capacity = vertex_sum_num_ * element_size_;

          printf("multi-level index file:\n \
                  path=%s vertex_num=%ld capacit=%ld\n", 
                  file_name.c_str(), vertex_sum_num_, capacity);

          std::cout << " sizeof(MulLevelIndexWithDiffSize)="
                    << element_size_ 
                    << " level_num=" << level_num_
                    << std::endl;
          fd_ = open((file_name).c_str(), O_RDWR | O_CREAT, 0666);
          if (fd_ == -1) {
            perror("Open: ");
            throw std::runtime_error("Unable to open file");
          }
          lseek(fd_, 0, SEEK_SET);
          if (ftruncate(fd_, capacity) == -1) {
            close(fd_);
            perror("Adjust mmap file error: ");
            throw std::runtime_error("Error adjusting file size");
          }
          data_ = static_cast<char *>(mmap(nullptr, capacity,
                                                    PROT_READ | PROT_WRITE,
                                                    MAP_SHARED, fd_,
                                                    0));
          if (data_ == MAP_FAILED) {
            close(fd_);
            perror("mmap: ");
            throw std::runtime_error("Error mapping file to memory");
          }

          // 锁定内存区域

          if (madvise(data_, capacity, MADV_WILLNEED) != 0)
                throw std::runtime_error("madvise block error.");

          for (int i = 0; i < vertex_sum_num_; ++i) {
            free_array_id_queue_.push(i);
          }
        }

        char* get_level_index_ptr_by_array_id(VertexId_t array_id) {
          return data_ + array_id * element_size_;
        }

        char* get_new_array_id_and_ptr(VertexId_t& array_id) {
          std::lock_guard<std::mutex> lock(mutex);
          if (free_array_id_queue_.empty()) {
            adjust_file_size();
          } 
          array_id = free_array_id_queue_.top();
          free_array_id_queue_.pop();
          vertex_use_num_++;
          return data_ + array_id * element_size_;
        }

        void get_batch_new_array_id_and_ptr(
            std::vector<std::pair<VertexId_t, char*>>& free_ids, int batch_size) {
          std::lock_guard<std::mutex> lock(mutex);
          while (free_array_id_queue_.size() < batch_size) {
            adjust_file_size();
          } 
          for (int i = 0; i < batch_size; ++i) {
            VertexId_t array_id = free_array_id_queue_.top();
            free_array_id_queue_.pop();
            vertex_use_num_++;
            free_ids.push_back(
                std::make_pair(array_id, data_ + array_id * element_size_));
          }
        }

        void recycle_array_id(VertexId_t array_id) {
          std::lock_guard<std::mutex> lock(mutex);
          free_array_id_queue_.push(array_id);
          vertex_use_num_--;

        }

        MulLevelIndexArray &operator=(const MulLevelIndexArray &rhs) = delete;

        MulLevelIndexArray &operator=(MulLevelIndexArray &&rhs) = delete;

        ~MulLevelIndexArray() {
          if (data_ != nullptr) {
            msync(data_, vertex_sum_num_ * element_size_, MS_SYNC);
            munmap(data_, vertex_sum_num_ * element_size_);
          }
          if (fd_ != -1) {
            close(fd_);
          }
          printf("~Size of Level Index mmap: %lu MB \t", 
                  ((vertex_sum_num_ * element_size_) >> 20));
          printf("Utilization: %ld/%ld=%.2f%%", vertex_use_num_, vertex_sum_num_,
                  vertex_use_num_*1.0/vertex_sum_num_*100);
          printf("\tfree_id_num: %ld, mem=%ld(M)\n", free_array_id_queue_.size(), 
                   free_array_id_queue_.size() * sizeof(VertexId_t) /1024/1024);
        }

        void adjust_file_size() {
          VertexId_t new_size = vertex_sum_num_ * 1.2;
          size_t length = vertex_sum_num_ * element_size_;
          size_t new_length = new_size * element_size_;

          if (fd_ != -1 && 
              ftruncate(fd_, new_length) != 0) {
            close(fd_);
            perror("Adjust mmap file error: ");
            throw std::runtime_error("Error adjusting file size");
          }


          data_ = static_cast<char *>(mmap(nullptr, new_length,
                                                    PROT_READ | PROT_WRITE,
                                                    MAP_SHARED, fd_,
                                                    0));
          // 重新映射文件
          if (data_ == MAP_FAILED) {
            close(fd_);
            perror("mremap of mmap: ");
            throw std::runtime_error("Error mapping file to memory");
          }


          printf("Adjusting Level Index File Size to %lu MB\n", 
                 ((new_length) >> 20));

          for (VertexId_t i = vertex_sum_num_; i < new_size; i++) {
            free_array_id_queue_.push(i);
          }
          vertex_sum_num_ = new_size;

          printf("Adjusting Level Index File Size to %lu MB of level_%d\n", 
                  ((new_length) >> 20), level_num_);
        }

        size_t get_vertex_sum_num() const {
          return vertex_sum_num_;
        }

        size_t get_vertex_use_num() const {
          return vertex_use_num_;
        }
      };

    public:
      MulLevelIndexArrayWrapper() {}

      MulLevelIndexArrayWrapper(const std::string &db_dir, 
                                      VertexId_t vertex_num) 
                  : vertex_num_(vertex_num) {
        vid_to_array_index_.resize(vertex_num, 0);

        for (int i = 0; i < MAX_LEVEL - 1; i++) {
          arr_ptrs_.emplace_back(
            std::shared_ptr<MulLevelIndexArray>(
              new MulLevelIndexArray(db_dir, vertex_num_, i+1))); // i=0, 表示levle_1
        }
      }

      void init(VertexId_t vid) {
        if (vid >= vertex_num_) {
          vertex_num_ *= 1.2;
          vid_to_array_index_.resize(vertex_num_, 0);
        }
      }

      int _get_level_num(VertexId_t index) {
          return (index & LEVEL_NUM_MASK) >> 61;
      }

      void _set_level_num(VertexId_t& index, int level_num) {
          index &= INDEX_MASK; // 清除高三位
          index |= static_cast<VertexId_t>(level_num) << 61; // 设置新的高三位
      }

      // 从index 中获得 MulLevelIndexWithDiffSize数据 中的array_id
      VertexId_t _get_array_id(VertexId_t index) {
          return index & INDEX_MASK;
      }

      void _set_array_id(VertexId_t& index, VertexId_t new_index) {
          index &= LEVEL_NUM_MASK; // 清除低位
          index |= new_index; // 设置新的低位
      }

      void set_array_id_by_vid (VertexId_t v_id, VertexId_t array_id) {
        assert(v_id < vertex_num_);
        _set_array_id(vid_to_array_index_[v_id], array_id);
      }

      int get_level_num_by_vid (VertexId_t v_id) {
        assert(v_id < vertex_num_);
        return _get_level_num(vid_to_array_index_[v_id]);
      }

      void set_level_num_by_vid (VertexId_t v_id, int level_num) {
        assert(v_id < vertex_num_);
        _set_level_num(vid_to_array_index_[v_id], level_num);
      }

      // 注意: 只返回一个新的level_index,并为将其添加到vid_to_array_index_中进行索引
      // 需要调用者在填充内容后，调用设置函数，将其array_id加入vid_to_array_index_
      char* get_new_level_index_ptr_by_vid_and_level_num (VertexId_t v_id, 
                                                          int level_num,
                                                          VertexId_t& array_id) {
        if (MAX_LEVEL <= level_num || level_num == 0) {
          throw std::runtime_error("Error level_num="+std::to_string(level_num));
        }
        char* ptr = arr_ptrs_[level_num-1]->get_new_array_id_and_ptr(array_id);
        return ptr;
      }

      // 批量获取
      void get_batch_new_level_index_ptr_by_level_num (
          std::vector<std::pair<VertexId_t, char*>>& free_ids, int level_num,
          int batch_size) {
        if (level_num <= 0) {
          throw std::runtime_error("Error level_num="+std::to_string(level_num));
        }
        arr_ptrs_[level_num-1]->get_batch_new_array_id_and_ptr(free_ids, batch_size);
      }

      char* get_level_index_ptr_by_vid (VertexId_t v_id) {
        assert(v_id < vertex_num_);
        VertexId_t index = vid_to_array_index_[v_id];
        int level_num = _get_level_num(index);
        if (level_num == 0) {
          return nullptr;
        }
        VertexId_t array_id = _get_array_id(index);

        if (arr_ptrs_[level_num-1]->vertex_sum_num_ <= array_id) {
          printf("array_id: %lu, vertex_num: %lu, level_num=%d\n", 
                  array_id, arr_ptrs_[level_num-1]->vertex_sum_num_,
                  level_num);
          throw std::runtime_error("Index error.");
        }
        return arr_ptrs_[level_num-1]->get_level_index_ptr_by_array_id(array_id);
      }

      void recycle_array_id(VertexId_t v_id, int level_num) {
        assert(v_id < vertex_num_);
        assert(level_num > 0);
        VertexId_t index = vid_to_array_index_[v_id];
        VertexId_t array_id = _get_array_id(index);
        arr_ptrs_[level_num-1]->recycle_array_id(array_id);
        vid_to_array_index_[v_id] = 0;
      }

      void recycle_array_id_by_array_id(int level_num, VertexId_t array_id) {
        arr_ptrs_[level_num-1]->recycle_array_id(array_id);
      }

      ~MulLevelIndexArrayWrapper () {
        arr_ptrs_.clear();
        printf("~MulLevelIndexArrayWrapper\n");
      }

    };


    /// @brief way3: 一种Level-index的分层实现
    /// @details 采用分层设计，一个cold_level_index位与磁盘文件，一个hot_level常驻内存
    /// 通过一个bitmap/hashmap来定位
    class MulLevelIndexWrapper {
      private:
        struct Hot_index_array_wrapper;
        struct Cold_index_array_wrapper;

        #if INDEX_MAP_TYPE == 0
            std::vector<VertexId_t> vertex_to_hot_array_;
        #elif INDEX_MAP_TYPE == 1
            ConcurrentHashMap<VertexId_t, VertexId_t> vertex_to_hot_array_;
        #elif INDEX_MAP_TYPE == 2
            ConcurrentUnorderedMap<VertexId_t, VertexId_t> vertex_to_hot_array_;
        #else
        #endif


#ifdef MMAP_HOT
        std::shared_ptr<MulLevelIndexMMapArray> hot_index_array_wrapper_;
#else
        std::shared_ptr<Hot_index_array_wrapper> hot_index_array_wrapper_;
#endif
        std::shared_ptr<Cold_index_array_wrapper> cold_index_array_wrapper_;



        // 期待常驻内存
        struct Hot_index_array_wrapper {
          struct CompareVertexId {
            bool operator()(const VertexId_t a, const VertexId_t b) const {
              return a > b;  // 返回 a > b，实现最小元素优先
            }
          };

          #define HOT_INDEX_SIZE 52  // 4(fid) + 4*12(level_data)

          std::vector<std::vector<char[HOT_INDEX_SIZE]>*> index_array_;
          VertexId_t vertex_sum_num_ = 0;
          VertexId_t vertex_use_num_ = 0;
          std::priority_queue<VertexId_t, std::vector<VertexId_t>, 
                              CompareVertexId> free_array_id_queue_;
          std::mutex mutex;
          int batch_size_ = 1280; //80;
          int add_each_time_ = 10; //10;
          int element_size_ = HOT_INDEX_SIZE;

          Hot_index_array_wrapper() : vertex_sum_num_(0), vertex_use_num_(0) {

          }

          char* get_level_index_by_array_id(VertexId_t array_id) {
            return (*(index_array_[array_id/batch_size_]))[array_id%batch_size_];
          }

          char* get_new_array_id_and_ptr(VertexId_t& array_id) {
            std::lock_guard<std::mutex> lock(mutex);
            if (free_array_id_queue_.empty()) {
              adjust_file_size();
            } 
            array_id = free_array_id_queue_.top();
            assert(array_id < vertex_sum_num_);
            free_array_id_queue_.pop();
            vertex_use_num_++;
            return (*(index_array_[array_id/batch_size_]))[array_id%batch_size_];
          }

          void get_batch_new_array_id_and_ptr(
              std::vector<std::pair<VertexId_t, char*>>& free_ids, int batch_size) {
            std::lock_guard<std::mutex> lock(mutex);
            while (free_array_id_queue_.size() < batch_size) {
              adjust_file_size();
            }
            for (int i = 0; i < batch_size; ++i) {
              VertexId_t array_id = free_array_id_queue_.top();
              free_array_id_queue_.pop();
              vertex_use_num_++;
              free_ids.push_back(
                std::make_pair(array_id, 
                (*(index_array_[array_id/batch_size_]))[array_id%batch_size_]));
            }
          }

          void recycle_array_id(VertexId_t array_id) {
            assert(array_id < vertex_sum_num_);
            std::lock_guard<std::mutex> lock(mutex);
            free_array_id_queue_.push(array_id);
            vertex_use_num_--;
          }

          size_t get_vertex_sum_num() const {
            return vertex_sum_num_;
          }

          size_t get_vertex_use_num() const {
            return vertex_use_num_;
          }

          Hot_index_array_wrapper &operator=(const Hot_index_array_wrapper &rhs) = delete;

          Hot_index_array_wrapper &operator=(Hot_index_array_wrapper &&rhs) = delete;

          ~Hot_index_array_wrapper() {
            for (auto& index : index_array_) {
              delete index;
            }
            #ifdef HOT_LEVEL_NUM
              printf("HOT_LEVEL_NUM=%d\n", HOT_LEVEL_NUM);
            #endif
            printf("~Size of Hot Level Index mmap: %lu MB \t", 
                    ((vertex_sum_num_ * element_size_) >> 20));
            printf("Utilization: %ld/%ld=%.2f%%", vertex_use_num_, vertex_sum_num_,
                    vertex_use_num_*1.0/vertex_sum_num_*100);
            printf("\tfree_id_num: %ld, mem=%ld(M)\n", free_array_id_queue_.size(), 
                      free_array_id_queue_.size() * sizeof(VertexId_t) /1024/1024);
            printf("@Utilization: %ld/%ld=%.2f%%\n", vertex_use_num_, vertex_sum_num_,
                    vertex_use_num_*1.0/vertex_sum_num_*100);
          }

          void adjust_file_size() {
            assert(batch_size_ > 0);
            VertexId_t new_size = vertex_sum_num_ + batch_size_ * add_each_time_;
            size_t new_length = new_size * element_size_;

            for (int i = 0; i < add_each_time_; ++i) {
              std::vector<char[HOT_INDEX_SIZE]>* ptr 
                  = new std::vector<char[HOT_INDEX_SIZE]>(batch_size_);
              index_array_.push_back(ptr);
            }

            printf("Adjusting Level Index File Size to %lu MB, vertex_num=%lu\n", 
                    ((new_length) >> 20), new_size);

            for (VertexId_t i = vertex_sum_num_; i < new_size; i++) {
              free_array_id_queue_.push(i);
            }
            vertex_sum_num_ = new_size;

            printf("Adjusting Level Index File Size to %lu MB of level_hot\n", 
                    ((new_length) >> 20));
          }

        };

        // 映射到磁盘文件
        // vid == array_id
        struct Cold_index_array_wrapper {
          char *data_;
          int fd_;
          std::mutex mutex;
          int level_num_ = 0;
          int element_size_ = 0;
          size_t vertex_sum_num_ = 0;


          Cold_index_array_wrapper(const std::string &db_dir, VertexId_t vertex_num,
                              int level_num) 
                              : vertex_sum_num_(vertex_num), data_(nullptr), 
                                fd_(-1), level_num_(level_num), mutex() {
            element_size_ = MulLevelIndexWithDiffSize::get_element_size(level_num_);

            std::string file_name = db_dir 
                                  + "/multi-level_" + std::to_string(level_num_);
            size_t capacity = vertex_sum_num_ * element_size_;

            printf(" Cold multi-level index file:\n \
                    path=%s vertex_num=%ld capacit=%ld\n", 
                    file_name.c_str(), vertex_sum_num_, capacity);

            std::cout << " sizeof(MulLevelIndexWithDiffSize)="
                      << element_size_ 
                      << " level_num=" << level_num_
                      << std::endl;

#ifdef MEM_MALLOC
            data_ = new char[capacity];
#else       
            fd_ = open((file_name).c_str(), O_RDWR | O_CREAT, 0666);
            if (fd_ == -1) {
              perror("Open: ");
              throw std::runtime_error("Unable to open file");
            }
            lseek(fd_, 0, SEEK_SET);
            if (ftruncate(fd_, capacity) == -1) {
              close(fd_);
              perror("Adjust mmap file error: ");
              throw std::runtime_error("Error adjusting file size");
            }
            data_ = static_cast<char *>(mmap(nullptr, capacity,
                                                      PROT_READ | PROT_WRITE,
                                                      MAP_SHARED, fd_,
                                                      0));
            if (data_ == MAP_FAILED) {
              close(fd_);
              perror("mmap: ");
              throw std::runtime_error("Error mapping file to memory");
            }

#ifdef MMAP_MLOCK
            // 锁定内存区域
            if (mlock(data_, capacity) == -1) {
                perror("mlock");
                exit(0);
            }
#endif

            if (madvise(data_, capacity, MADV_WILLNEED) != 0)
                  throw std::runtime_error("madvise block error.");
#endif       
          }

          char* get_level_index_ptr_by_array_id(VertexId_t array_id) {
            if (array_id >= vertex_sum_num_) {
              std::lock_guard<std::mutex> lock(mutex);
              if (array_id >= vertex_sum_num_) {
                adjust_file_size();
              }
            }
            return data_ + array_id * element_size_;
          }

          Cold_index_array_wrapper &operator=(const Cold_index_array_wrapper &rhs) = delete;

          Cold_index_array_wrapper &operator=(Cold_index_array_wrapper &&rhs) = delete;

          ~Cold_index_array_wrapper() {
#ifdef MEM_MALLOC
          delete[] data_;
#else
            if (data_ != nullptr) {
              msync(data_, vertex_sum_num_ * element_size_, MS_SYNC);
              munmap(data_, vertex_sum_num_ * element_size_);
            }
            if (fd_ != -1) {
              close(fd_);
            }
#endif
#ifdef MMAP_MLOCK
            printf("@mlock: open\n");
#endif
#ifdef MEM_MALLOC
            printf("@memlock: open\n");
#endif
            printf("~Size of Cold Level Index mmap: %lu MB \n", 
                    ((vertex_sum_num_ * element_size_) >> 20));
          }

          void adjust_file_size() {
            VertexId_t new_size = vertex_sum_num_ * 1.2;
            size_t length = vertex_sum_num_ * element_size_;
            size_t new_length = new_size * element_size_;

#ifdef MEM_MALLOC
          char* data_2_ = new char[new_length];
          memcpy(data_2_, data_, length);
          delete[] data_;
          data_ = data_2_;
#else
            if (fd_ != -1 && 
                ftruncate(fd_, new_length) != 0) {
              close(fd_);
              perror("Adjust mmap file error: ");
              throw std::runtime_error("Error adjusting file size");
            }

#ifdef MMAP_MLOCK
            // 锁定内存区域
            if (munlock(data_, length) == -1) {
                perror("munlock");
                exit(0);
            }
#endif

            data_ = static_cast<char *>(mmap(nullptr, new_length,
                                                      PROT_READ | PROT_WRITE,
                                                      MAP_SHARED, fd_,
                                                      0));
            // 重新映射文件
            if (data_ == MAP_FAILED) {
              close(fd_);
              perror("mremap of mmap: ");
              throw std::runtime_error("Error mapping file to memory");
            }

#ifdef MMAP_MLOCK
            // 锁定新扩展的内存区域
            if (mlock(data_, new_length) == -1) {
                perror("mlock");
                exit(0);
            }
#endif
#endif

            vertex_sum_num_ = new_size;

            printf("Adjusting Level Index File Size to %lu MB of level_%d\n", 
                    ((new_length) >> 20), level_num_);
          }

          size_t get_vertex_sum_num() const {
            return vertex_sum_num_;
          }
        };



      public:
        MulLevelIndexWrapper()
#if INDEX_MAP_TYPE == 1 || INDEX_MAP_TYPE == 2
                      : vertex_to_hot_array_(NULLPOINTER)
#endif 
                      {}

        MulLevelIndexWrapper(const std::string &db_dir, 
                             VertexId_t vertex_num) 
                    : 
#if INDEX_MAP_TYPE == 1 || INDEX_MAP_TYPE == 2
                      vertex_to_hot_array_(NULLPOINTER),
#endif
                      cold_index_array_wrapper_(
                        new Cold_index_array_wrapper(db_dir, vertex_num,
                                                     COLD_LEVEL_NUM)),
#ifdef MMAP_HOT
                      hot_index_array_wrapper_(
                        new MulLevelIndexMMapArray(db_dir, 128, HOT_LEVEL_NUM)) {
#else
                      hot_index_array_wrapper_(
                        new Hot_index_array_wrapper()) {
#endif
#if INDEX_MAP_TYPE == 0
          vertex_to_hot_array_.resize(vertex_num, 0);
#endif
        }

        void init(VertexId_t v_id) {
#if INDEX_MAP_TYPE == 0
          if (v_id >= vertex_to_hot_array_.size()) {
            vertex_to_hot_array_.resize(vertex_to_hot_array_.size() * 1.2, 0);
          }
#endif
          char* ptr = cold_index_array_wrapper_
            ->get_level_index_ptr_by_array_id(v_id);
          MulLevelIndexWithDiffSize level_index 
            = MulLevelIndexWithDiffSize(ptr, COLD_LEVEL_NUM);
          level_index.init(COLD_LEVEL_NUM);
        }

        // 注意: 只返回一个新的level_index,并为将其添加到vid_to_array_index_中进行索引
        // 需要调用者在填充内容后，调用设置函数，将其array_id加入vid_to_array_index_
        char* get_new_level_index_ptr_by_vid_and_level_num (VertexId_t v_id, 
                                                            int level_num,
                                                            VertexId_t& array_id) {
          if (level_num != COLD_LEVEL_NUM && level_num != HOT_LEVEL_NUM) {
            throw std::runtime_error("Error level_num="+std::to_string(level_num));
          }
          char* ptr = nullptr;
          if (level_num == COLD_LEVEL_NUM) {
            ptr = cold_index_array_wrapper_->get_level_index_ptr_by_array_id(v_id);
            array_id = v_id;
          } else {
            ptr = hot_index_array_wrapper_->get_new_array_id_and_ptr(array_id);
          }
          return ptr;
        }

        // 批量获取
        void get_batch_new_level_index_ptr_by_level_num (
            std::vector<std::pair<VertexId_t, char*>>& free_ids,
            int batch_size) {
          hot_index_array_wrapper_->get_batch_new_array_id_and_ptr(free_ids, batch_size);
        }
    
        char* get_level_index_ptr_by_vid (VertexId_t v_id,
                                          int& level_num,
                                          VertexId_t& array_id) {
          level_num = COLD_LEVEL_NUM;
          if (vertex_to_hot_array_.find(v_id, array_id)) {
            level_num = HOT_LEVEL_NUM;
          }

 
          char* ptr = nullptr;
          if (level_num == COLD_LEVEL_NUM) {
            ptr = cold_index_array_wrapper_->get_level_index_ptr_by_array_id(v_id);
            array_id = v_id;
          } else {
            ptr = hot_index_array_wrapper_->get_level_index_by_array_id(array_id);
          }
          return ptr;
        }

        void insert_hot_hash(VertexId_t v_id, VertexId_t array_id) {
          vertex_to_hot_array_.insert(v_id, array_id);
        }

        void recycle_array_id_by_array_id(VertexId_t v_id, int array_id, 
                                          int level_num, bool have_vid) {
          assert(level_num == HOT_LEVEL_NUM);

          hot_index_array_wrapper_->recycle_array_id(array_id);
          if (have_vid) {
            vertex_to_hot_array_.erase(v_id);
          }
        }

        ~MulLevelIndexWrapper () {

          printf("~MulLevelIndexWrapper\n");
        }
    };


};  // namespace lsmgraph


#endif // DATASTRUCTS_H
