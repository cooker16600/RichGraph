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

#include <tbb/concurrent_unordered_map.h>

template <typename KeyType, typename ValueType>
class ConcurrentUnorderedMap {
private:
    typedef tbb::concurrent_unordered_map<KeyType, ValueType> HashMap;
    typedef typename HashMap::iterator HashMapIterator;
    typedef typename HashMap::const_iterator HashMapConstIterator;
    typedef typename HashMap::value_type HashMapValuePair;

public:
    ConcurrentUnorderedMap (ValueType default_value) : DEFAULT_VALUE(default_value) {
    }

    size_t size() {
        return hashMap.size();
    }

    bool insert(const KeyType& key, const ValueType& value) {
        std::pair<HashMapIterator, bool> result = hashMap.insert(HashMapValuePair(key, value));
        return result.second;
    }

    bool erase(const KeyType& key) {
        printf("Not support concurrent erasure\n");
        return false;
    }

    void clear() {
        hashMap.clear();
    }

    bool find(const KeyType& key, ValueType& value) {
        HashMapConstIterator it = hashMap.find(key);
        if (it != hashMap.end()) {
            value = it->second;
            return true;
        }
        value = DEFAULT_VALUE;
        return false;
    }

    inline const ValueType& operator[](const KeyType& key) const {
        HashMapConstIterator it = hashMap.find(key);
        if (it != hashMap.end()) {
            return it->second;
        }
        return DEFAULT_VALUE;
    }

    // ValueType& operator[](const KeyType& key) {
    //     return hashMap[key];
    // }

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