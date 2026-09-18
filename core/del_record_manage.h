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
#include <tbb/concurrent_hash_map.h>
#include <cassert>
#include "core/types.h"

namespace lsmgraph {

using EidToTimeMap = tbb::concurrent_hash_map<SequenceNumber_t, 
                                              SequenceNumber_t>;
using FidToRecordMap = tbb::concurrent_hash_map<FileId_t, EidToTimeMap*>;


class DelRecordManage {
private:
  FidToRecordMap fidMap;

public:
  DelRecordManage() {

  }

  ~DelRecordManage() {
    for (auto& pair : fidMap) {
      delete pair.second;
    }
  }

  size_t size() {
    return fidMap.size();
  }

  void put_fid_and_record(FileId_t fid, SequenceNumber_t eid, 
                          SequenceNumber_t time) {
      FidToRecordMap::accessor a; // For thread-safe access
      bool isNewInsertion = fidMap.insert(a, fid);
      if (isNewInsertion) {
          // New element inserted, create new EidToTimeMap
          a->second = new EidToTimeMap();
      }
      // Insert record into the EidToTimeMap
      a->second->insert(std::make_pair(eid, time));
  }

  bool find_eidmap(FileId_t fid, EidToTimeMap* &mp) {
    FidToRecordMap::const_accessor ca;
    if(!fidMap.find(ca, fid) || ca->second->size() == 0) {
      return false;
    }
    mp = ca->second;
    return true;
  }

  bool get_time(EidToTimeMap* mp, SequenceNumber_t eid, 
                SequenceNumber_t& time) {
    assert(mp != nullptr);
    EidToTimeMap::const_accessor ca;

    if(!mp->find(ca, eid)) {
      return false;
    }
    time = ca->second;
    return true;
  }

  void del_eid_frome_eidmap(EidToTimeMap* mp, SequenceNumber_t eid) {
    assert(mp != nullptr);
    mp->erase(eid);
  }

  void clean() {
  }

  void prinf() {
    size_t fild_size = 0;
    size_t eid_size = 0;
    for (auto it = fidMap.begin(); it != fidMap.end(); ++it) {
      FidToRecordMap::const_accessor ca;
      fild_size++;
      if (fidMap.find(ca, it->first)) {
        eid_size += ca->second->size();
      }
    }
    std::cout << "fild_size=" << fild_size
              << " eid_size=" << eid_size
              << std::endl;
  }

};

} // end lsmgraph
