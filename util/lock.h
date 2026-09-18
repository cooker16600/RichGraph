#pragma once
#include <assert.h>

#include <atomic>
#include <climits>
#include <condition_variable>
#include <cstdlib>
#include <mutex>
#include <shared_mutex>
#include <thread>

class Spinlock {
 private:
  std::atomic_flag flag = ATOMIC_FLAG_INIT;

 public:
  void lock() {
    while (flag.test_and_set(std::memory_order_acquire)) {
      // std::this_thread::yield();
      // //这里应注释掉，否则会引起线程切换，成本较高达不到自 旋锁要的效率
    }
  }

  void unlock() { flag.clear(std::memory_order_release); }
};

class SharedRWClock {
 public:
  void ReadLock() {
    rwLock.lock_shared();
  }
  void WriteLock() {
    rwLock.lock();
  }
  void ReadUnlock() { rwLock.unlock_shared(); }
  void WriteUnlock() {
    rwLock.unlock();
  }

 private:
  std::shared_mutex rwLock;  // 读写锁
};

class CASRWLock {
 public:
  CASRWLock() : lock_(0) {}
  void ReadLock() {
    uint64_t i, n;
    for (;;) {
      uint64_t old_readers = lock_.load(std::memory_order_relaxed);
      if (old_readers != WLOCK &&
          lock_.compare_exchange_weak(old_readers, old_readers + 1,
                                      std::memory_order_acquire,
                                      std::memory_order_relaxed)) {
        return;
      }
      for (n = 1; n < SPIN; n <<= 1) {
        for (i = 0; i < n; i++) {
          __asm__("pause");
        }
        old_readers = lock_.load(std::memory_order_relaxed);
        if (old_readers != WLOCK &&
            lock_.compare_exchange_weak(old_readers, old_readers + 1,
                                        std::memory_order_acquire,
                                        std::memory_order_relaxed)) {
          return;
        }
      }
      sched_yield();
    }
  }
  void WriteLock() {
    uint64_t i, n;
    for (;;) {
      uint64_t expected = 0;
      if (lock_.compare_exchange_weak(expected, WLOCK,
                                      std::memory_order_acquire,
                                      std::memory_order_relaxed)) {
        return;
      }
      for (n = 1; n < SPIN; n <<= 1) {
        for (i = 0; i < n; i++) {
          __asm__("pause");
        }
        expected = 0;
        if (lock_.compare_exchange_weak(expected, WLOCK,
                                        std::memory_order_acquire,
                                        std::memory_order_relaxed)) {
          return;
        }
      }
      sched_yield();
    }
  }
  void ReadUnlock() {
    const uint64_t old_readers = lock_.fetch_sub(1, std::memory_order_release);
    assert(old_readers != 0 && old_readers != WLOCK);
  }
  void WriteUnlock() {
    uint64_t expected = WLOCK;
    const bool unlocked = lock_.compare_exchange_strong(
        expected, 0, std::memory_order_release, std::memory_order_relaxed);
    assert(unlocked);
  }

 private:
  static const uint64_t SPIN = 2048;
  static const uint64_t WLOCK = ((unsigned long)-1);
  std::atomic<uint64_t> lock_;
};
