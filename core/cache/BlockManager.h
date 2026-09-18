#pragma once

#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

namespace lsmgraph {
class BlockManager {
 public:
  constexpr static uintptr_t NULLPOINTER = 0;  // UINTPTR_MAX;

#if defined(RICHGRAPH_THREAD_SANITIZER)
  // ThreadSanitizer reserves a large shadow-address region. Reserving the
  // production 1 TiB sparse arena can collide with that region before a
  // test starts, so sanitizer builds use a bounded test arena.
  constexpr static size_t DEFAULT_CAPACITY = 1ul << 30;
#else
  constexpr static size_t DEFAULT_CAPACITY = 1ul << 40;
#endif

  BlockManager(std::string path, size_t _capacity = DEFAULT_CAPACITY)
      : capacity(_capacity), mutex() {
    if (path.empty()) {
      fd = EMPTY_FD;
      data = mmap(nullptr, capacity, PROT_READ | PROT_WRITE,
                  MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
      if (data == MAP_FAILED) throw std::runtime_error("mmap block error.");
    } else {
      std::cout << "\n mmap_path=" << path << std::endl;
      fd = open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0640);
      if (fd == EMPTY_FD) {
        throw std::runtime_error("open block file error. path=" + path);
      }
      if (ftruncate(fd, FILE_TRUNC_SIZE) != 0)
        throw std::runtime_error("ftruncate block file error.");
      data = mmap(nullptr, capacity, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
      if (data == MAP_FAILED) throw std::runtime_error("mmap block error.");
    }

    if (madvise(data, capacity, MADV_RANDOM) != 0)
      throw std::runtime_error("madvise block error.");

    file_size = FILE_TRUNC_SIZE;
    used_size = 0;
  }

  ~BlockManager() {
    msync(data, capacity, MS_SYNC);
    munmap(data, capacity);
    if (fd != EMPTY_FD) close(fd);
  }

  uintptr_t alloc(size_t block_size) {
    uintptr_t pointer = used_size.fetch_add(block_size);

    if (pointer + block_size >= file_size) {
      auto new_file_size =
          ((pointer + block_size) / FILE_TRUNC_SIZE + 1) * FILE_TRUNC_SIZE;
      std::lock_guard<std::mutex> lock(mutex);
      if (new_file_size >= file_size) {
        if (fd != EMPTY_FD) {
          if (ftruncate(fd, new_file_size) != 0)
            throw std::runtime_error("ftruncate block file error.");
        }
        file_size = new_file_size;
      }
    }

    return pointer;
  }

  void* get_data() { return data; }

 private:
  const size_t capacity;
  int fd;
  void* data;
  std::mutex mutex;
  std::atomic<size_t> used_size, file_size;

  constexpr static int EMPTY_FD = -1;
  constexpr static size_t FILE_TRUNC_SIZE = 1ul << 30;  // 1GB
};

}  // namespace lsmgraph
