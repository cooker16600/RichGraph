/*
Copyright (c) 2015-2016 Xiaowei Zhu, Tsinghua University

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

#ifndef ATOMIC_HPP
#define ATOMIC_HPP

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

#include <atomic>
#include <type_traits>

template <class T>
inline T atomic_load_value(const T* ptr) {
  static_assert(std::is_trivially_copyable<T>::value,
                "atomic helper requires a trivially copyable type");
  T value;
  __atomic_load(ptr, &value, __ATOMIC_ACQUIRE);
  return value;
}

template <class T>
inline void atomic_store_value(T* ptr, T value) {
  static_assert(std::is_trivially_copyable<T>::value,
                "atomic helper requires a trivially copyable type");
  __atomic_store(ptr, &value, __ATOMIC_RELEASE);
}

template <class T>
inline bool cas(T* ptr, T old_val, T new_val) {
  static_assert(std::is_trivially_copyable<T>::value,
                "atomic helper requires a trivially copyable type");
  return __atomic_compare_exchange(ptr, &old_val, &new_val, false,
                                   __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE);
}

template <class T>
inline bool write_min(T* ptr, T val) {
  T current = atomic_load_value(ptr);
  while (current > val) {
    if (cas(ptr, current, val)) {
      return true;
    }
    current = atomic_load_value(ptr);
  }
  return false;
}

template <class T>
inline bool write_max(T* ptr, T val) {
  T current = atomic_load_value(ptr);
  while (current < val) {
    if (cas(ptr, current, val)) {
      return true;
    }
    current = atomic_load_value(ptr);
  }
  return false;
}

template <class T>
inline bool write_min(std::atomic<T>* ptr, T val) {
  T current = ptr->load(std::memory_order_acquire);
  while (current > val) {
    if (ptr->compare_exchange_weak(current, val, std::memory_order_acq_rel,
                                   std::memory_order_acquire)) {
      return true;
    }
  }
  return false;
}

template <class T>
inline bool write_max(std::atomic<T>* ptr, T val) {
  T current = ptr->load(std::memory_order_acquire);
  while (current < val) {
    if (ptr->compare_exchange_weak(current, val, std::memory_order_acq_rel,
                                   std::memory_order_acquire)) {
      return true;
    }
  }
  return false;
}

template <class T>
inline void write_add(T* ptr, T val) {
  T old_val = atomic_load_value(ptr);
  while (!cas(ptr, old_val, static_cast<T>(old_val + val))) {
    old_val = atomic_load_value(ptr);
  }
}

#endif
