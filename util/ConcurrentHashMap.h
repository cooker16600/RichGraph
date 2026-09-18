/*
Copyright (c) 2023 The LSMGraph Authors, Northeastern University

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
#pragma once

#include<map>
#include<string>
#include<vector>
#include<fstream>
#include<iostream>
#include <tbb/concurrent_hash_map.h>

template <typename KeyType, typename ValueType>
class ConcurrentHashMap {
  private:
    typedef tbb::concurrent_hash_map<KeyType, ValueType> HashMap;
    typedef typename HashMap::const_accessor HashMapConstAccessor;
    typedef typename HashMap::accessor HashMapAccessor;
    typedef typename HashMap::iterator HashMapIterator;
    typedef typename HashMap::value_type HashMapValuePair;

  public:
    ConcurrentHashMap (ValueType default_value) : DEFAULT_VALUE(default_value) {
    }

    size_t size() {
      return hashMap.size();
    }

    bool insert(const KeyType& key, const ValueType& value) {
      HashMapAccessor accessor;
      bool inserted = hashMap.insert(accessor, key);
      if (inserted) {
        accessor->second = value;
      }
      return inserted;
    }

    bool erase(const KeyType& key) {
      return hashMap.erase(key);
    }

    bool find(const KeyType& key, ValueType& value) {
      HashMapConstAccessor accessor;
      if (hashMap.find(accessor, key)) {
        value = accessor->second;
        return true;
      }
      value = DEFAULT_VALUE;
      return false;
    }

    void clear() {
      hashMap.clear();
    }

    // ValueType& operator[](const KeyType& key) {
    //   HashMapAccessor accessor;
    //   if (!hashMap.insert(accessor, key)) {
    //     return accessor->second;
    //   }
    //   accessor->second = DEFAULT_VALUE;
    //   return accessor->second;
    // }

    inline const ValueType& operator[](const KeyType& key) const {
      HashMapConstAccessor accessor;
      hashMap.find(accessor, key);
      if (hashMap.find(accessor, key)) {
        return accessor->second;
      }
      return DEFAULT_VALUE;
    }

    HashMapIterator begin() {
      return hashMap.begin();
    }

    HashMapIterator end() {
      return hashMap.end();
    }
    
  private:
    HashMap hashMap;
    ValueType DEFAULT_VALUE;
};
