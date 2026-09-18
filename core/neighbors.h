#pragma once

#include <cstdlib>
#include <atomic>
#include <set>
#include <utility>
#include "core/DataStructs.h"
#include "core/types.h"
#include "core/config.h"
#include "core/graph/edge.h"
#include "core/edge_iterator_base.h"
#include "ThreadLocalRandom.h"


namespace lsmgraph {

    // Ordered array backed by a tree after reaching capacity. Array movement
    // prevents concurrent reads and writes.
    class NewEdge;

    // Array backed by a skip list after reaching capacity. The unsorted mode
    // supports concurrent access; only the skip-list suffix is ordered.
    class NewEdgeSL;

    // Dynamically growing array without concurrent read/write support.
    class NewEdgeArray;

    // Dynamically growing array with partial locking for concurrent access.
    class NewEdgeArrayEntry;

    // Ordered skip list supporting concurrent reads and writes.
    template<typename K, class Cmp>
    class SkipList;

    // Linked list supporting concurrent reads and writes.
    class List;

    // Hybrid array, list, and skip-list representation.
    class NewEdgeHybrid;

    // The unsorted representation supports concurrent reads and writes;
    // ordered array variants move elements and do not.
    using NeighBors = NewEdgeSL;

    const uint16_t kMaxHeight_ = 4; // SKIPLIST_MAX_HEIGHT
    const uint16_t kBranching_ = 4;
    const uint32_t kScaledInverseBranching_ = 2147483647L / kBranching_;

    class NewEdge {
    private:
      bool full_;
      Edge *dst_;
      int cnt_ = 0;
      size_t property_size_ = 0;
      std::set<Edge> *list_;
    public:
      class MemEdgeIterator;

      NewEdge() {
        full_ = false;
        if (FLAGS_reserve_node > 0) {
          dst_ = new Edge[FLAGS_reserve_node];
        }
      }

      ~NewEdge() {
        if (FLAGS_reserve_node > 0) {
          delete[] dst_;
        }
        if (full_) {
          delete list_;
        }
      }

      void put_edge(VertexId_t dst, SequenceNumber_t seq,
                    Marker_t marker, const EdgeProperty_t &property,
                    bool is_out = true) {

        Edge edge(dst, seq, marker, property);

        if (full_ == false) {
          if (cnt_ == FLAGS_reserve_node) {
            full_ = true;
            list_ = new std::set<Edge>;
            for (int i = 0; i < FLAGS_reserve_node; i++) {
              list_->insert(this->dst_[i]);
            }
            list_->insert(edge);
          } else {
            int i;
            for (i = 0; i < this->cnt_; i++) {
              if (edge < this->dst_[i]) {
                for (int j = cnt_; j > i; j--) {
                  dst_[j] = dst_[j - 1];
                }
                break;
              }
            }
            dst_[i] = edge;
            cnt_++;
          }
        } else {
          list_->insert(edge);
        }
        property_size_ += property.size();
      }

      Status get(VertexId_t dst, std::string *property) {
        if (!full_) {
          int i = 0;
          while (i < this->cnt_) {
            if (this->dst_[i].destination() == dst) {
              if (this->dst_[i].marker())
                return Status::kDelete;
              property->assign(this->dst_[i].property().data(),
                               this->dst_[i].property().size());
              return Status::kOk;
            }
            i++;
          }
          return Status::kNotFound;
        } else {
          std::set<Edge>::iterator temp;

          if ((temp = list_->lower_bound(Edge(dst, MAX_SEQ_ID)))
              == list_->end()) {
            return Status::kNotFound;
          } else if (!temp->Equal(Edge(dst, MAX_SEQ_ID)))
            return Status::kNotFound;
          else if (temp->marker())
            return Status::kDelete;
          else {
            property->assign(temp->property().data(), temp->property().size());
            return Status::kOk;
          }
        }
      }

      Status get(VertexId_t dst, SequenceNumber_t &seq) {
        if (!full_) {
          int i = 0;
          while (i < this->cnt_) {
            if (this->dst_[i].destination() == dst) {
              if (this->dst_[i].marker())
                return Status::kDelete;
              seq = this->dst_[i].sequence();
              return Status::kOk;;
            }
            i++;
          }
          return Status::kNotFound;
        } else {
          std::set<Edge>::iterator temp;

          if ((temp = list_->lower_bound(Edge(dst, MAX_SEQ_ID)))
              == list_->end()) {
            return Status::kNotFound;
          } else if (!temp->Equal(Edge(dst, MAX_SEQ_ID)))
            return Status::kNotFound;
          else if (temp->marker())
            return Status::kDelete;
          else {
            seq = temp->sequence();
            return Status::kOk;;
          }
        }
      }

      void get_edges(VertexId_t src, std::vector<Edge> &edges) {
        edges.reserve(edges.size() + this->cnt_);
        if (!full_) {
          VertexId_t saved_key = INVALID_VERTEX_ID;
          bool skipping = false;
          for (int i = 0; i < this->cnt_; i++) {
            if (!(skipping && this->dst_[i].destination() == saved_key)) {
              if (this->dst_[i].marker() == 0) {
                edges.emplace_back(this->dst_[i]);
              } else {
                saved_key = this->dst_[i].destination();
                skipping = true;
              }
            }
          }
        } else {
          VertexId_t saved_key = INVALID_VERTEX_ID;
          bool skipping = false;
          for (auto &edge: *list_) {
            if (!(skipping && edge.destination() == saved_key)) {
              if (edge.marker() == 0) {
                edges.emplace_back(edge);
              } else {
                saved_key = edge.destination();
                skipping = true;
              }
            }
          }
        }
      }

      Edge getvector(int i) {
        if (i > cnt_ - 1) {
          std::cout << "error";
          return Edge(0, 0);
        } else return dst_[i];
      }

      int getVectorNum() const {
        return cnt_;
      }

      int getEdgeNum() const {
        if (!full_) {
          return cnt_;
        } else {
          return list_->size();
        }
      }

      size_t getPropertySize() const {
        return property_size_;
      }

      bool isfull() const { return full_; }

      std::set<Edge>::iterator getListHead() const {
        return list_->begin();
      }

      std::set<Edge>::iterator getListEnd() const {
        return list_->end();
      }

      const Edge *getArrayHead() const {
        return dst_;
      }

      void reset() {
        if (full_) {
          delete list_;
        }
        full_ = false;
        cnt_ = 0;
        property_size_ = 0;
      }

      void sort() {
        // no need to sort.
      }
    };

    class NewEdge::MemEdgeIterator : public EdgeIteratorBase {
    public:
      // Initialize an iterator over the specified list.
      // The returned iterator is not valid.
      explicit MemEdgeIterator(const NewEdge *edges, FileId_t fid = INVALID_File_ID)
              : fid_(fid) {
        init(edges);
      }

      void init(const NewEdge *edges) {
        edges_ = edges;
        if (edges_ == nullptr) {
          return;
        }
        if (!edges_->isfull()) {
          array_it = edges_->getArrayHead();
        } else {
          set_it = edges_->getListHead();
        }
      }

      bool valid() const override {
        if (edges_ == nullptr) {
          return false;
        }
        if (!edges_->isfull()) {
          return array_it != edges_->getArrayHead() + edges_->getVectorNum();
        } else {
          return !(set_it == edges_->getListEnd());
        }
      }

      // Returns the edge at the current position.
      const Edge &key() {
        assert(valid());
        if (!edges_->isfull()) {
          return *array_it;
        } else {
          return *set_it;
        }
      }

      void next() {
        if (!edges_->isfull()) {
          array_it++;
        } else {
          set_it++;
        }
      }

      VertexId_t dst_id() const override {
        if (!edges_->isfull()) {
          return array_it->destination();
        } else {
          return set_it->destination();
        }
      }

      Marker_t marker() const override {
        if (!edges_->isfull()) {
          return array_it->marker();
        } else {
          return set_it->marker();
        }
      }

      SequenceNumber_t sequence() const override {
        if (!edges_->isfull()) {
          return array_it->sequence();
        } else {
          return set_it->sequence();
        }
      }

      EdgeProperty_t edge_data(int inplace) const override {
        if (!edges_->isfull()) {
          return array_it->property();
        } else {
          return set_it->property();
        }
      }

      bool empty() const override {
        return !edges_->getEdgeNum();
      }

      size_t size() const override {
        return edges_->getEdgeNum();
      }

      #ifndef  NO_VIRTUAL

      bool IsMemTable() const override {
        return is_mem_table;
      }

      #endif

      FileId_t get_fid() const override {
        return fid_;
      }

    private:
      const NewEdge *edges_;
      std::set<Edge>::iterator set_it;
      const Edge *array_it;
      FileId_t fid_;
      #ifndef NO_VIRTUAL
      bool is_mem_table = true;
      #endif
    };

    template<typename K, class Cmp>
    class SkipList {
    public:
      class MemEdgeIterator;

      explicit SkipList() :
              head_(NewNode(0, kMaxHeight_)),
              max_height_(1),
              size_(0),
              property_size_(0),
              prev_height_(1) {
        assert(kMaxHeight_ > 0 && kMaxHeight_ == static_cast<uint32_t>(kMaxHeight_));
        assert(kBranching_ > 0 && kBranching_ == static_cast<uint32_t>(kBranching_));
        assert(kScaledInverseBranching_ > 0);
        prev_ = (Node **) malloc(sizeof(Node *) * kMaxHeight_);
        for (int i = 0; i < kMaxHeight_; i++) {
          head_->SetNext(i, nullptr);
          prev_[i] = head_;
        }
        if (FLAGS_reserve_node > 0) {
          dst_ = new Node[FLAGS_reserve_node];
        }
      }

      ~SkipList() {
        if (FLAGS_reserve_node > 0) {
          delete[] dst_;
        }
        // 释放 prev_ 的内存
        if (prev_ != nullptr) {
          free(prev_);
        }
        // 释放所有node节点的空间
        for (auto mem: recycle_mem) {
          if (mem != nullptr) {
            clear_node_key(mem);
            free(mem);
          }
        }
      }

      void clear_node_key(char *mem) {
        Node *temp_node = reinterpret_cast<Node *>(mem);
        temp_node->~Node();
      }

      void reset() {
        max_height_ = 1;
        size_ = 0;
        property_size_ = 0;
        prev_height_ = 1;
        for (int i = 0; i < kMaxHeight_; i++) {
          head_->SetNext(i, nullptr);
          prev_[i] = head_;
        }
        // 释放所有node节点的空间
        for (auto mem: recycle_mem) {
          if (mem != nullptr && mem != reinterpret_cast<char *>(head_)) {
            clear_node_key(mem);
            free(mem);
            mem = nullptr;
          }
        }
        recycle_mem.clear();
      }

      SkipList(const SkipList &other) = delete;

      void operator=(const SkipList &other) = delete;

      Status get(VertexId_t dst, std::string *property) {
        Edge key = Edge(dst, MAX_SEQ_ID);
        Node *x = FindGreaterOrEqual(key);
        if (x != nullptr && key.destination() == x->key.destination()) {
          if (x->key.marker()) {
            return Status::kDelete;
          }
          property->assign(x->key.property().data(), x->key.property().size());
          return Status::kOk;
        } else {
          return Status::kNotFound;
        }
      }

      // 按方向读取：
      // 在同一个 (src,dst) 上如果同时存在入/出两种记录，
      // 这里会按 seq 从新到旧扫描，返回第一条方向匹配的记录。
      Status get(VertexId_t dst, bool is_out, uint8_t edge_type,
                 std::string *property) {
        Edge key = Edge(dst, MAX_SEQ_ID);
        Node *x = FindGreaterOrEqual(key);
        while (x != nullptr && x->key.destination() == key.destination()) {
          if (x->key.is_out() == is_out && x->key.edge_type() == edge_type) {
            if (x->key.marker()) {
              return Status::kDelete;
            }
            property->assign(x->key.property().data(), x->key.property().size());
            return Status::kOk;
          }
          x = x->Next(0);
        }
        return Status::kNotFound;
      }

      Status get(VertexId_t dst, SequenceNumber_t &seq) {
        Edge key = Edge(dst, MAX_SEQ_ID);
        Node *x = FindGreaterOrEqual(key);
        if (x != nullptr && key.destination() == x->key.destination()) {
          if (x->key.marker()) {
            return Status::kDelete;
          }
          seq = x->key.sequence();
          return Status::kOk;;
        } else {
          return Status::kNotFound;
        }
      }

      Status get(VertexId_t dst, bool is_out, uint8_t edge_type,
                 SequenceNumber_t &seq) {
        Edge key = Edge(dst, MAX_SEQ_ID);
        Node *x = FindGreaterOrEqual(key);
        while (x != nullptr && key.destination() == x->key.destination()) {
          if (x->key.is_out() == is_out && x->key.edge_type() == edge_type) {
            if (x->key.marker()) {
              return Status::kDelete;
            }
            seq = x->key.sequence();
            return Status::kOk;
          }
          x = x->Next(0);
        }
        return Status::kNotFound;
      }

      void get_edges(VertexId_t src, std::vector<Edge> &edges) {
        edges.resize(size_);
        uint32_t idx(0);
        Node *x = head_;
        int level = GetMaxHeight() - 1;
        while (true) {
          Node *next = x->Next(level);
          if (next == nullptr) {
            if (level == 0) {
              return;
            } else {
              // Switch to next list
              level--;
            }
          } else {
            edges[idx++] = x->key;
            x = next;
          }
        }
      }

      void update_node(Edge edge){
        // Updating the newest matching edge does not change its sort order.
        Edge key = Edge(edge.destination(), MAX_SEQ_ID);
      }

      void put_edge(VertexId_t dst, SequenceNumber_t seq,
                    Marker_t marker, const EdgeProperty_t &property,
                    bool is_out = true, uint8_t edge_type = 0) {

        int height = 1;
        Node *x;
        if (size_ < FLAGS_reserve_node) {
          x = GetOldNode(size_);
          x->SetNext(0, nullptr);
        } else {
          height = RandomHeight();
          x = GetNewNode(height);
        }

        Edge &key = x->key;
        key.set_destination(dst);
        key.set_sequence(seq);
        key.set_marker(marker);
        key.set_property(property);
        key.set_is_out(is_out);
        key.set_edge_type(edge_type);

        // fast path for sequential insertion
        if (!KeyIsAfterNode(key, prev_[0]->NoBarrier_Next(0)) &&
            (prev_[0] == head_ || KeyIsAfterNode(key, prev_[0]))) {
          assert(prev_[0] != head_ || (prev_height_ == 1 && GetMaxHeight() == 1));

          for (int i = 1; i < prev_height_; i++) {
            prev_[i] = prev_[0];
          }
        } else {
          FindLessThan(key, prev_);
        }

        // Our data structure does not allow duplicate insertion
        assert(prev_[0]->Next(0) == nullptr || !Equal(key, prev_[0]->Next(0)->key));
        if (height > GetMaxHeight()) {
          for (int i = GetMaxHeight(); i < height; i++) {
            prev_[i] = head_;
          }
          // It is ok to mutate max_height_ without any synchronization
          // with concurrent readers.
          max_height_.store(height, std::memory_order_relaxed);
        }

        for (int i = 0; i < height; i++) {
          x->NoBarrier_SetNext(i, prev_[i]->NoBarrier_Next(i));
          prev_[i]->SetNext(i, x);
        }
        prev_[0] = x;
        prev_height_ = height;
        size_++;
        property_size_ += property.size();
      }

      bool Contains(const K &key) const {
        Node *x = FindGreaterOrEqual(key);
        if (x != nullptr && Equal(key, x->key)) {
          return true;
        } else {
          return false;
        }
      }

      uint64_t EstimateCount(const K &key) const {
        uint64_t count = 0;

        Node *x = head_;
        int level = GetMaxHeight() - 1;
        while (true) {
          assert(x == head_ || Cmp::compare(x->key, key) < 0);
          Node *next = x->Next(level);
          if (next == nullptr || Cmp::compare(next->key, key) >= 0) {
            if (level == 0) {
              return count;
            } else {
              // Switch to next list
              count *= kBranching_;
              level--;
            }
          } else {
            x = next;
            count++;
          }
        }
      }

      size_t getEdgeNum() const {
        return size_;
      }

      size_t getPropertySize() const {
        return property_size_;
      }

      void print() {
        printf("-----------------print list----------------\n");
        printf("---%ld:\n", getEdgeNum());
        MemEdgeIterator it = MemEdgeIterator(this);
        for (; it.valid(); it.next()) {
          std::cout << " read next edge..." << std::endl;
          it.key().print();
        }
        printf("-------------------------------------------\n");
      }

      void prefetch() {
        __builtin_prefetch(head_, 0, 0);
      }

      size_t size() const {
        return size_;
      }

      MemEdgeIterator begin(const FileId_t fid) const {
        return MemEdgeIterator{this, fid};
      }

      void sort() {
        // no need to sort.
      }


    private:
      struct Node;

      // 可以在此维护一个本skiplist的node_free_list来重复利用node
      std::vector<char *> recycle_mem;

      Node *const head_;

      // Used for optimizing sequential insert patterns.
      Node **prev_;

      std::atomic<int> max_height_;  // Height of the entire list

      int32_t prev_height_;

      int32_t size_;
      int32_t property_size_;
      Node *dst_;

      Node *NewNode(const K &key, int height) {
        char *mem = (char *) malloc(sizeof(Node) + sizeof(std::atomic<Node *>) * (height - 1));
        recycle_mem.push_back(mem);
        return new(mem) Node(key);
      }

      Node *GetNewNode(const int height) {
        char *mem = (char *) malloc(sizeof(Node) + sizeof(std::atomic<Node *>) * (height - 1));
        recycle_mem.push_back(mem);
        return new(mem) Node();
      }

      Node *GetOldNode(int id) {
        return dst_ + id;
      }

      int RandomHeight() {
        int height = 1;
        while (height < kMaxHeight_ && rand() < kScaledInverseBranching_) height++;


        assert(height > 0);
        assert(height <= kMaxHeight_);
        return height;
      }

      bool KeyIsAfterNode(const K &key, Node *n) const {
        // nullptr n is considered infinite
        return (n != nullptr) && (Cmp::compare(n->key, key) < 0);
      }

      inline int GetMaxHeight() const {
        return max_height_.load(std::memory_order_relaxed);
      }

      bool Equal(const K &a, const K &b) const { return (Cmp::compare(a, b) == 0); }

      bool LessThan(const K &a, const K &b) const {
        return (Cmp::compare(a, b) < 0);
      }

      // Returns the earliest node with a key >= key.
      // Return nullptr if there is no such node.
      Node *FindGreaterOrEqual(const K &key) const {
        Node *x = head_;
        int level = GetMaxHeight() - 1;
        Node *last_bigger = nullptr;
        while (true) {
          assert(x != nullptr);
          Node *next = x->Next(level);
          // Make sure the lists are sorted
          assert(x == head_ || next == nullptr || KeyIsAfterNode(next->key, x));
          // Make sure we haven't overshot during our search
          assert(x == head_ || KeyIsAfterNode(key, x));
          int cmp = (next == nullptr || next == last_bigger) ? 1 : Cmp::compare(next->key, key);
          if (cmp == 0 || (cmp > 0 && level == 0)) {
            return next;
          } else if (cmp < 0) {
            // Keep searching in this list
            x = next;
          } else {
            // Switch to next list, reuse Cmp::compare() result
            last_bigger = next;
            level--;
          }
        }
      }

      public:
      void UpdateEdge(VertexId_t dst, SequenceNumber_t seq, Marker_t marker,
                        const EdgeProperty_t &property) {
        Node *x = FindGreaterOrEqual(Edge(dst, 0x7fffffff)); 
        if (x!= nullptr) {
          std::string p = x->key.property();
          std::vector<std::string>p_tmp;
          p_tmp.resize(FLAGS_sub_property_num, "");
          int now_id = 0;
          for(auto c:p){
            if(c == '|'){
              now_id++;
            } else {
              p_tmp[now_id] += c;
            }
          }
          now_id = 0;
          std::string tmp_s;
          for(auto c:property){
              if(c == '|'){
                if(p_tmp[now_id] == "~INPLACED~" || tmp_s != "~INPLACED~"){
                  p_tmp[now_id] = tmp_s;
                }
                now_id++;
                tmp_s = "";
              } else {
                tmp_s += c;
              }
          }
          if(p_tmp[now_id] == "~INPLACED~" || tmp_s != "~INPLACED~"){
            p_tmp[now_id] = tmp_s;
          }
          
          std::string ans = p_tmp[0];
          for(int i = 1; i < FLAGS_sub_property_num; i++){
            ans += "|" + p_tmp[i] ;
          }
          x->key.set_marker(marker);
          x->key.set_property(ans);
          x->key.set_sequence(seq);
        }
      }
      
      // Return the latest node with a key < key.
      // Return head_ if there is no such node.
      Node *FindLessThan(const K &key, Node **prev = nullptr) const {
        Node *x = head_;
        int level = GetMaxHeight() - 1;
        // KeyIsAfter(key, last_not_after) is definitely false
        Node *last_not_after = nullptr;
        while (true) {
          assert(x != nullptr);
          Node *next = x->Next(level);
          assert(x == head_ || next == nullptr || KeyIsAfterNode(next->key, x));
          assert(x == head_ || KeyIsAfterNode(key, x));
          if (next != last_not_after && KeyIsAfterNode(key, next)) {
            // Keep searching in this list
            x = next;
          } else {
            if (prev != nullptr) {
              prev[level] = x;
            }
            if (level == 0) {
              return x;
            } else {
              // Switch to next list, reuse KeyIUsAfterNode() result
              last_not_after = next;
              level--;
            }
          }
        }
      }

      Node *FindLast() const {
        Node *x = head_;
        int level = GetMaxHeight() - 1;
        while (true) {
          Node *next = x->Next(level);
          if (next == nullptr) {
            if (level == 0) {
              return x;
            } else {
              // Switch to next list
              level--;
            }
          } else {
            x = next;
          }
        }
      }
    };

    template<typename K, class Cmp>
    struct SkipList<K, Cmp>::Node {
      K key;

      explicit Node(K k) : key(std::move(k)), next_{nullptr} {}

      explicit Node() : key(), next_{nullptr} {}

      ~Node() {
        next_[0] = nullptr;
      }

      void cleal_Key() {
        key.~K();
      }

      Node *Next(int n) {
        assert(n >= 0);
        // Use an 'acquire load' so that we observe a fully initialized
        // version of the returned Node.
        return next_[n].load(std::memory_order_acquire);
      }

      void SetNext(int n, Node *next) {
        assert(n >= 0);
        // Use a 'release store' so that anybody who reads through this
        // pointer observes a fully initialized version of the inserted node
        next_[n].store(next, std::memory_order_release);
      }

      // No-barrier variants that can be safely used in a few locations.
      Node *NoBarrier_Next(int n) {
        assert(n >= 0);
        return next_[n].load(std::memory_order_relaxed);
      }

      void NoBarrier_SetNext(int n, Node *x) {
        assert(n >= 0);
        next_[n].store(x, std::memory_order_relaxed);
      }

    private:
      // Array of length equal to the node height.  next_[0] is lowest level link.
      std::atomic<Node *> next_[1];
    };

    template<typename K, class Cmp>
    class SkipList<K, Cmp>::MemEdgeIterator : public EdgeIteratorBase {
    public:
      // Initialize an iterator over the specified list.
      // The returned iterator is not valid.
      explicit MemEdgeIterator(const SkipList *list, FileId_t fid = INVALID_File_ID)
              : fid_(fid) {
        SetList(list);
      }

      explicit MemEdgeIterator() : fid_(INVALID_File_ID), list_(nullptr), node_(nullptr) {}

      bool operator==(const MemEdgeIterator &rhs) const {
        return node_ == rhs.node_;
      }

      void SetList(const SkipList *list) {
        list_ = list;
        node_ = nullptr;
        if (list_ == nullptr) {
          return;
        }
        SeekToFirst();
      }

      FileId_t get_fid() const {
        return fid_;
      }

      bool valid() const {
        return node_ != nullptr;
      }

      // Returns the key at the current position.
      const K &key() {
        assert(valid());
        return node_->key;
      }

      VertexId_t dst_id() const {
        assert(valid());
        return node_->key.destination();
      }

      SequenceNumber_t sequence() const {
        assert(valid());
        return node_->key.sequence();
      }

      Marker_t marker() const {
        assert(valid());
        return node_->key.marker();
      }

      bool is_out() const override {
        assert(valid());
        return node_->key.is_out();
      }

      uint8_t edge_type() const override {
        assert(valid());
        return node_->key.edge_type();
      }

      EdgeProperty_t edge_data(int inplace = -1) const {
        assert(valid());
        if(inplace == -1){
          return node_->key.property();
        }
        // Decode the stored property payload.
        int now_id = 0; 
        EdgeProperty_t ans = "";
        for(auto c:node_->key.property()){
          if(c == '|'){
            now_id++;
          } else {
            if(now_id == inplace){
              ans += c;
            }
          }
          if(now_id > inplace){
            break;
          }
        }
        return ans;
      }

      bool empty() const {
        assert(valid());
        return list_->getEdgeNum() == 0;
      }

      size_t size() const {
        assert(valid());
        return list_->getEdgeNum();
      }

      void next() {
        if (!valid()) {
          return;
        }
        node_ = node_->Next(0);
      }

      void Prev() {
        assert(valid());
        node_ = list_->FindLessThan(node_->key);
        if (node_ == list_->head_) {
          node_ = nullptr;
        }
      }

      // Advance to the first entry with a key >= target
      void Seek(const K &target) {
        node_ = list_->FindGreaterOrEqual(target);
      }

      // Retreat to the last entry with a key <= target
      void SeekForPrev(const K &target) {
        Seek(target);
        if (!valid()) {
          SeekToLast();
        }
        while (valid() && list_->LessThan(target, key())) {
          Prev();
        }
      }

      void SeekToFirst() {
        node_ = list_->head_->Next(0);
      }

      void SeekToLast() {
        node_ = list_->FindLast();
        if (node_ == list_->head_) {
          node_ = nullptr;
        }
      }

      #ifndef NO_VIRTUAL

      bool IsMemTable() const {
        return is_mem_table;
      }

      #endif

    private:
      const SkipList *list_;
      Node *node_;
      FileId_t fid_;

      #ifndef NO_VIRTUAL
      bool is_mem_table = true;
      #endif
      // Intentionally copyable
    };

    // array + skip list
    class NewEdgeSL {
    private:
      bool full_;
      Edge *dst_;
      int cnt_;
      size_t property_size_;
      SkipList<Edge, EdgeComparator> *list_;
    public:
      class MemEdgeIterator;

      NewEdgeSL() : full_(false), dst_(nullptr), cnt_(0), property_size_(0), list_(nullptr) {
        if (FLAGS_reserve_node > 0) {
          dst_ = new Edge[FLAGS_reserve_node];
        }
      }

      ~NewEdgeSL() {
        if (FLAGS_reserve_node > 0) {
          delete[] dst_;
        }
        if (full_) {
          delete list_;
        }
      }

      // is_out: true 表示按 (src,dst) 语义的出边；false 表示按 (src,dst)
      // 存储但语义为入边（实际边方向为 dst -> src）。
      void put_edge(VertexId_t dst, SequenceNumber_t seq,
                    Marker_t marker, const EdgeProperty_t &property,
                    bool is_out = true, uint8_t edge_type = 0) {


        if (!full_) {
          if (cnt_ == FLAGS_reserve_node) {
            list_ = new SkipList<Edge, EdgeComparator>;
            for (int i = 0; i < FLAGS_reserve_node; i++) {
              list_->put_edge(dst_[i].destination(),
                              dst_[i].sequence(),
                              dst_[i].marker(),
                              dst_[i].property(),
                              dst_[i].is_out(),
                              dst_[i].edge_type());
            }
            list_->put_edge(dst, seq, marker, property, is_out, edge_type);
            full_ = true;
          } else {
            Edge edge(dst, seq, marker, property, is_out, edge_type);
            #ifdef SORTED_ARRAY
            int i;
            for (i = 0; i < this->cnt_; i++) {
              if (edge < this->dst_[i]) {
                for (int j = cnt_; j > i; j--) {
                  dst_[j] = dst_[j - 1];
                }
                break;
              }
            }
            dst_[i] = edge;
            cnt_++;
            #else
            dst_[cnt_++] = edge;
            #endif
          }
        } else {
          list_->put_edge(dst, seq, marker, property, is_out, edge_type);
        }
        property_size_ += property.size();
      }
      
      Status get(VertexId_t dst, std::string *property) {
        if (!full_) {
          int i = this->cnt_ - 1;
          while (i >= 0) {
            if (this->dst_[i].destination() == dst) {
              if (this->dst_[i].marker())
                return Status::kDelete;
              property->assign(this->dst_[i].property().data(),
                               this->dst_[i].property().size());
              return Status::kOk;
            }
            i--;
          }
          return Status::kNotFound;
        } else {
          return list_->get(dst, property);
        }
      }

      Status get(VertexId_t dst, bool is_out, uint8_t edge_type,
                 std::string *property) {
        if (!full_) {
          int i = this->cnt_ - 1;
          while (i >= 0) {
            if (this->dst_[i].destination() == dst
                && this->dst_[i].is_out() == is_out
                && this->dst_[i].edge_type() == edge_type) {
              if (this->dst_[i].marker()) {
                return Status::kDelete;
              }
              property->assign(this->dst_[i].property().data(),
                               this->dst_[i].property().size());
              return Status::kOk;
            }
            --i;
          }
          return Status::kNotFound;
        }
        return list_->get(dst, is_out, edge_type, property);
      }

      Status get(VertexId_t dst, SequenceNumber_t &seq) {
        if (!full_) {
          int i = 0;
          while (i < this->cnt_) {
            if (this->dst_[i].destination() == dst) {
              if (this->dst_[i].marker())
                return Status::kDelete;
              seq = this->dst_[i].sequence();
              return Status::kOk;;
            }
            i++;
          }
          return Status::kNotFound;
        } else {
          return list_->get(dst, seq);
        }
      }

      Status get(VertexId_t dst, bool is_out, uint8_t edge_type,
                 SequenceNumber_t &seq) {
        if (!full_) {
          int i = this->cnt_ - 1;
          while (i >= 0) {
            if (this->dst_[i].destination() == dst
                && this->dst_[i].is_out() == is_out
                && this->dst_[i].edge_type() == edge_type) {
              if (this->dst_[i].marker()) {
                return Status::kDelete;
              }
              seq = this->dst_[i].sequence();
              return Status::kOk;
            }
            --i;
          }
          return Status::kNotFound;
        }
        return list_->get(dst, is_out, edge_type, seq);
      }

      void get_edges(VertexId_t src, std::vector<Edge> &edges) {
        edges.reserve(edges.size() + this->cnt_);
        if (!full_) {
          VertexId_t saved_key = INVALID_VERTEX_ID;
          bool skipping = false;
          for (int i = 0; i < this->cnt_; i++) {
            if (!(skipping && this->dst_[i].destination() == saved_key)) {
              if (this->dst_[i].marker() == 0) {
                edges.emplace_back(this->dst_[i]);
              } else {
                saved_key = this->dst_[i].destination();
                skipping = true;
              }
            }
          }
        } else {
          list_->get_edges(src, edges);
        }
      }

      void update_edge(VertexId_t dst, 
                        SequenceNumber_t seq, 
                        Marker_t marker,
                        const EdgeProperty_t &property) {
        if(!full_){
          int i = 0;
          while (i < this->cnt_) {
            if (this->dst_[i].destination() == dst) {
                this->dst_[i].set_marker(marker);
                this->dst_[i].set_sequence(seq);
                this->dst_[i].set_marker(marker);
                
                this->dst_[i].set_property(property);
                return;
            }
          }
        } else {
          list_->UpdateEdge(dst, seq, marker, property);
        }                     
      }

      Edge getvector(int i) {
        if (i > cnt_ - 1) {
          std::cout << "error";
          return {0, 0};
        } else return dst_[i];
      }

      int getVectorNum() const {
        return cnt_;
      }

      int getEdgeNum() const {
        if (!full_) {
          return cnt_;
        } else {
          return list_->size();
        }
      }

      size_t getPropertySize() const {
        return property_size_;
      }

      bool isfull() const { return full_; }

      SkipList<Edge, EdgeComparator>::MemEdgeIterator getListHead(const FileId_t fid) const {
        return list_->begin(fid);
      }

      const Edge *getArrayHead() const {
        return dst_;
      }

      void reset() {
        if (full_) {
          delete list_;
        }
        full_ = false;
        cnt_ = 0;
        property_size_ = 0;
      }


      void sort() {
#ifndef SORTED_ARRAY
        if (!full_) {
          std::sort(dst_, dst_ + cnt_, [](const Edge &lhs, const Edge &rhs) -> bool {
            if (lhs.destination() != rhs.destination()) {
              return lhs.destination() < rhs.destination();
            } else {
              return lhs.sequence() > rhs.sequence();
            }
          });
        }
#endif
      }

    };

    class NewEdgeSL::MemEdgeIterator : public EdgeIteratorBase {
    public:
      // Initialize an iterator over the specified list.
      // The returned iterator is not valid.
      explicit MemEdgeIterator(const NewEdgeSL *edges, FileId_t fid = INVALID_File_ID, SequenceNumber_t newest_edge_ = 0)
              : fid_(fid), edges_(edges), newest_edge(newest_edge_ ) {
        init();
      }

      void init() {
        if (edges_ == nullptr) {
          return;
        }
        if (!edges_->isfull()) {
          array_it_ = edges_->getArrayHead();
        } else {
          skip_list_it_ = SkipList<Edge, EdgeComparator>::MemEdgeIterator(edges_->list_, fid_);
        }
      }

      bool valid() const override {
        if (edges_ == nullptr) {
          return false;
        }
        if (!edges_->isfull()) {
          return array_it_ != edges_->getArrayHead() + edges_->getVectorNum();
        } else {
          return skip_list_it_.valid();
        }
      }

      // Returns the edge at the current position.
      const Edge &key() {
        assert(valid());
        if (!edges_->isfull()) {
          return *array_it_;
        } else {
          return skip_list_it_.key();
        }
      }

      void next() {
        if (!valid()) {
          return;
        }
        if (!edges_->isfull()) {
          array_it_++;
        } else {
          skip_list_it_.next();
        }
      }

      VertexId_t dst_id() const override {
        if (!edges_->isfull()) {
          return array_it_->destination();
        } else {
          return skip_list_it_.dst_id();
        }
      }

      Marker_t marker() const override {
        if (!edges_->isfull()) {
          return array_it_->marker();
        } else {
          return skip_list_it_.marker();
        }
      }

      bool is_out() const override {
        if (!edges_->isfull()) {
          return array_it_->is_out();
        }
        return skip_list_it_.is_out();
      }

      uint8_t edge_type() const override {
        if (!edges_->isfull()) {
          return array_it_->edge_type();
        }
        return skip_list_it_.edge_type();
      }

      SequenceNumber_t sequence() const override {
        if (!edges_->isfull()) {
          return array_it_->sequence();
        } else {
          return skip_list_it_.sequence();
        }
      }

      EdgeProperty_t edge_data(int sub_property_id = -1) const override {
        if (!edges_->isfull()) {
            
          
          return array_it_->property(sub_property_id);
        } else {
            
          
          return skip_list_it_.edge_data(sub_property_id);
        }
      }

      bool empty() const override {
        return !edges_->getEdgeNum();
      }

      size_t size() const override {
        return edges_->getEdgeNum();
      }

      #ifndef  NO_VIRTUAL

      bool IsMemTable() const override {
        return is_mem_table;
      }

      #endif

      FileId_t get_fid() const override {
        return fid_;
      }

    private:
      const NewEdgeSL *edges_;
      SkipList<Edge, EdgeComparator>::MemEdgeIterator skip_list_it_;
      const Edge *array_it_;
      FileId_t fid_;
      SequenceNumber_t newest_edge = 0;
      #ifndef NO_VIRTUAL
      bool is_mem_table = true;
      #endif
    };

    class NewEdgeArray {
    private:
      std::vector<Edge *> dst_;
      size_t property_size_ = 0;
    public:
      class MemEdgeIterator;

      NewEdgeArray() {
      }

      ~NewEdgeArray() {
        for (auto &ptr: dst_) {
          delete ptr;
        }
      }

      void put_edge(VertexId_t dst, SequenceNumber_t seq,
                    Marker_t marker, const EdgeProperty_t &property) {

        dst_.emplace_back(new Edge(dst, seq, marker, property));

        property_size_ += property.size();
      }

      Status get(VertexId_t dst, std::string *property) {
        for (auto edge_ptr: dst_) {
          if (edge_ptr->destination() == dst) {
            if (edge_ptr->marker()) {
              return Status::kDelete;
            }
            property->assign(edge_ptr->property().data(), edge_ptr->property().size());
            return Status::kOk;
          }
        }
        return Status::kNotFound;

      }

      Status get(VertexId_t dst, SequenceNumber_t &seq) {
        for (auto edge_ptr: dst_) {
          if (edge_ptr->destination() == dst) {
            if (edge_ptr->marker()) {
              return Status::kDelete;
            }
            seq = edge_ptr->destination();
          }
          return Status::kOk;
        }
        return Status::kNotFound;
      }

      void get_edges(VertexId_t src, std::vector<Edge> &edges) {
        edges.reserve(edges.size());
        VertexId_t saved_key = INVALID_VERTEX_ID;
        bool skipping = false;
        for (uint32_t i = 0; i < dst_.size(); ++i) {
          if (!(skipping && dst_[i]->destination() == saved_key)) {
            if (dst_[i]->marker() == 0) {
              edges.emplace_back(*dst_[i]);
            } else {
              saved_key = dst_[i]->destination();
              skipping = true;
            }
          }
        }
      }

      const Edge& getvector(int i) const {
        assert(i >= 0 && static_cast<std::size_t>(i) < dst_.size());
        return *dst_[i];
      }

      int getVectorNum() const {
        return dst_.size();
      }

      int getEdgeNum() const {
        return dst_.size();
      }

      size_t getPropertySize() const {
        return property_size_;
      }
      std::vector<Edge *>::iterator getArrayHead() {
        return dst_.begin();
      }

      std::vector<Edge *>::iterator getArrayEnd() {
        return dst_.end();
      }

      void sort() {
        std::sort(dst_.begin(), dst_.end(), [](const Edge *lhs, const Edge *rhs) -> bool {
          if (lhs->destination() != rhs->destination()) {
            return lhs->destination() < rhs->destination();
          } else {
            return lhs->sequence() > rhs->sequence();
          }
        });
      }

      void reset() {
        for (auto &ptr: dst_) {
          delete ptr;
        }
        dst_.clear();
        property_size_ = 0;
      }
    };

    class NewEdgeArray::MemEdgeIterator : public EdgeIteratorBase {
    public:
      // Initialize an iterator over the specified list.
      // The returned iterator is not valid.
      explicit MemEdgeIterator(NewEdgeArray *edges, FileId_t fid = INVALID_File_ID)
              : fid_(fid), cur_(0) {
        init(edges);
      }

      void init(NewEdgeArray *edges) {
        edges_ = edges;
        if (edges_ == nullptr) {
          return;
        }
      }

      bool valid() const override {
        if (edges_ == nullptr) {
          return false;
        }
        if (cur_ < edges_->getVectorNum()) {
          return true;
        }
        return false;
      }

      // Returns the edge at the current position.
      const Edge &key() const {
        assert(valid());
        return edges_->getvector(cur_);
      }

      void next() {
        ++cur_;
      }

      VertexId_t dst_id() const override {
        return edges_->getvector(cur_).destination();
      }

      Marker_t marker() const override {
        return edges_->getvector(cur_).marker();
      }

      SequenceNumber_t sequence() const override {
        return edges_->getvector(cur_).sequence();
      }

      EdgeProperty_t edge_data(int inplace) const override {
        return edges_->getvector(cur_).property();
      }

      bool empty() const override {
        return !edges_->getEdgeNum();
      }

      size_t size() const override {
        return edges_->getEdgeNum();
      }

      #ifndef  NO_VIRTUAL

      bool IsMemTable() const override {
        return is_mem_table;
      }

      #endif

      FileId_t get_fid() const override {
        return fid_;
      }

      void sort() {
        edges_->sort();
      }


    private:
      NewEdgeArray *edges_;
      FileId_t fid_;
      uint32_t cur_;
      #ifndef NO_VIRTUAL
      bool is_mem_table = true;
      #endif
    };

    class NewEdgeArrayEntry {
    private:
      std::vector<Edge> dst_;
      size_t property_size_ = 0;
      RWLock_t rw_lock;
    public:
      class MemEdgeIterator;

      NewEdgeArrayEntry() {
      }

      ~NewEdgeArrayEntry() {
      }

      void put_edge(VertexId_t dst, SequenceNumber_t seq,
                    Marker_t marker, const EdgeProperty_t &property) {
        rw_lock.WriteLock();
        dst_.emplace_back(dst, seq, marker, property);
        rw_lock.WriteUnlock();

        property_size_ += property.size();
      }

      Status get(VertexId_t dst, std::string *property) {
        rw_lock.ReadLock();
        for (auto &edge: dst_) {
          if (edge.destination() == dst) {
            if (edge.marker()) {
              return Status::kDelete;
            }
            property->assign(edge.property().data(), edge.property().size());
            return Status::kOk;
          }
        }
        rw_lock.ReadUnlock();
        return Status::kNotFound;

      }

      Status get(VertexId_t dst, SequenceNumber_t &seq) {
        rw_lock.ReadLock();
        for (auto &edge: dst_) {
          if (edge.destination() == dst) {
            if (edge.marker()) {
              return Status::kDelete;
            }
            seq = edge.destination();
          }
          return Status::kOk;
        }
        rw_lock.ReadUnlock();
        return Status::kNotFound;
      }

      void get_edges(VertexId_t src, std::vector<Edge> &edges) {
        rw_lock.ReadLock();
        edges.reserve(edges.size());
        VertexId_t saved_key = INVALID_VERTEX_ID;
        bool skipping = false;
        for (uint32_t i = 0; i < dst_.size(); ++i) {
          if (!(skipping && dst_[i].destination() == saved_key)) {
            if (dst_[i].marker() == 0) {
              edges.emplace_back(dst_[i]);
            } else {
              saved_key = dst_[i].destination();
              skipping = true;
            }
          }
        }
        rw_lock.ReadUnlock();
      }

      const Edge &getvector(int i) const {
        assert(i < dst_.size());
        return dst_[i];
      }

      int getVectorNum() const {
        return dst_.size();
      }

      int getEdgeNum() const {
        return dst_.size();
      }

      size_t getPropertySize() const {
        return property_size_;
      }

      std::vector<Edge>::iterator getArrayHead() {
        return dst_.begin();
      }

      std::vector<Edge>::iterator getArrayEnd() {
        return dst_.end();
      }

      void sort() {
        rw_lock.WriteLock();
        std::sort(dst_.begin(), dst_.end(), [](const Edge lhs, const Edge rhs) -> bool {
          if (lhs.destination() != rhs.destination()) {
            return lhs.destination() < rhs.destination();
          } else {
            return lhs.sequence() > rhs.sequence();
          }
        });
        rw_lock.WriteUnlock();
      }

      void reset() {
        dst_.clear();
        property_size_ = 0;
      }
    };

    class NewEdgeArrayEntry::MemEdgeIterator : public EdgeIteratorBase {
    public:
      // Initialize an iterator over the specified list.
      // The returned iterator is not valid.
      explicit MemEdgeIterator(NewEdgeArrayEntry *edges, FileId_t fid = INVALID_File_ID)
              : fid_(fid), cur_(0) {
        init(edges);
      }

      void init(NewEdgeArrayEntry *edges) {
        edges_ = edges;
        if (edges_ == nullptr) {
          return;
        }
      }

      bool valid() const override {
        if (edges_ == nullptr) {
          return false;
        }
        edges_->rw_lock.ReadLock();
        size_t num = edges_->getVectorNum();
        edges_->rw_lock.ReadUnlock();
        if (cur_ < num) {
          return true;
        }
        return false;
      }

      // Returns the edge at the current position.
      const Edge &key() const {
        assert(valid());
        return edges_->getvector(cur_);
      }

      void next() {
        ++cur_;
      }

      VertexId_t dst_id() const override {
        edges_->rw_lock.ReadLock();
        VertexId_t dst = edges_->getvector(cur_).destination();
        edges_->rw_lock.ReadUnlock();
        return dst;
      }

      Marker_t marker() const override {
        return edges_->getvector(cur_).marker();
      }

      SequenceNumber_t sequence() const override {
        return edges_->getvector(cur_).sequence();
      }

      EdgeProperty_t edge_data(int inplace) const override {
        return edges_->getvector(cur_).property();
      }

      bool empty() const override {
        return !edges_->getEdgeNum();
      }

      size_t size() const override {
        return edges_->getEdgeNum();
      }

      #ifndef  NO_VIRTUAL

      bool IsMemTable() const override {
        return is_mem_table;
      }

      #endif

      FileId_t get_fid() const override {
        return fid_;
      }

      void sort() {
        edges_->sort();
      }


    private:
      NewEdgeArrayEntry *edges_;
      FileId_t fid_;
      uint32_t cur_;
      #ifndef NO_VIRTUAL
      bool is_mem_table = true;
      #endif
    };

    class List {
    public:
      class MemEdgeIterator;

      explicit List() : size_(0) {
        head_ = new Node;
        head_->next_ = nullptr;
      }

      ~List() {
        auto cur = head_;
        size_ = 0;
        while (cur != nullptr) {
          auto tmp = cur;
          cur = cur->next_;
          delete tmp;
        }
        head_ = nullptr;
      }

      List(const List &rhs) = delete;

      List &operator=(const List &rhs) = delete;

      Status get(VertexId_t dst, std::string *property) {
        Node *res = nullptr;
        auto stat = search(dst, &res);
        if (stat == Status::kOk) {
          Edge &e = res->data_;
          property->assign(e.property().data(), e.property().size());
        }
        return stat;
      }

      Status get(VertexId_t dst, SequenceNumber_t &seq) {
        Node *res = nullptr;
        auto stat = search(dst, &res);
        if (stat == Status::kOk) {
          seq = res->data_.sequence();
        }
        return stat;
      }

      void get_edges(VertexId_t dst, std::vector<Edge> &edges) {
        edges.reserve(size_);
        auto cur = head_;
        while (cur != nullptr) {
          edges.emplace_back(cur->data_);
          cur = cur->next_;
        }
      }

      void dump_to_skip_list(SkipList<Edge, EdgeComparator> *sl) {
        Node *cur = head_->next_;
        while (cur != nullptr) {
          Edge &edge = cur->data_;
          sl->put_edge(edge.destination(), edge.sequence(), edge.marker(), edge.property());
          cur = cur->next_;
        }
      }

      void put_edge(VertexId_t dst, SequenceNumber_t seq, Marker_t marker, const EdgeProperty_t &property) {
        ++size_;
        Node *new_node = new Node{{dst, seq, marker, property}, nullptr};
        Node *cur = head_;
        Node *next = head_->next_;

        while (next != nullptr && next->data_.destination() < dst) {
          cur = next;
          next = next->next_;
        }

        new_node->next_ = next;
        while (__sync_val_compare_and_swap(&cur->next_, next, new_node) != next) {
          next = cur->next_;
          while (next != nullptr && next->data_.destination() < dst) {
            cur = next;
            next = next->next_;
          }
          new_node->next_ = next;
        }
      }

      size_t size() const {
        return size_;
      }



    private:
      struct Node {
        Edge data_;
        Node *next_;
      };
      Node *head_;
      std::atomic_size_t size_;

      Status search(VertexId_t dst, Node **res) {
        Node *cur = head_->next_;
        while (cur != nullptr && cur->data_.destination() < dst) {
          cur = cur->next_;
        }
        if (cur != nullptr && cur->data_.destination() == dst) {
          if (cur->data_.marker()) {
            return Status::kDelete;
          }
          *res = cur;
          return Status::kOk;
        }
        return Status::kNotFound;
      }
    };

    class List::MemEdgeIterator : public EdgeIteratorBase {
    public:
      explicit MemEdgeIterator(const List *list, FileId_t fid = INVALID_File_ID) : list_(list), fid_(fid) {
        node_ = list_->head_->next_;
      }

      explicit MemEdgeIterator() : fid_(INVALID_File_ID), list_(nullptr), node_(nullptr) {}


      bool operator==(const MemEdgeIterator &rhs) const {
        return node_ == rhs.node_;
      }


      FileId_t get_fid() const {
        return fid_;
      }

      const Edge &key() {
        assert(valid());
        return node_->data_;
      }

      VertexId_t dst_id() const {
        assert(valid());
        return node_->data_.destination();
      }

      SequenceNumber_t sequence() const {
        assert(valid());
        return node_->data_.sequence();
      }

      Marker_t marker() const {
        assert(valid());
        return node_->data_.marker();
      }

      EdgeProperty_t edge_data(int inplace) const {
        assert(valid());
        return node_->data_.property();
      }

      bool empty() const {
        assert(valid());
        return list_->size() == 0;
      }

      size_t size() const {
        assert(valid());
        return list_->size();
      }

      void next() {
        assert(valid());
        node_ = node_->next_;
      }

      bool valid() const {
        return node_ != nullptr;
      }

#ifndef NO_VIRTUAL
       bool IsMemTable() const {
        return true;
      };
#endif

    private:
      const List *list_;
      Node *node_;
      FileId_t fid_;

      #ifndef NO_VIRTUAL
      bool is_mem_table = true;
      #endif
    };

    //Array + List + SkipList
    class NewEdgeHybrid {
    private:
      Edge *dst_;
      size_t property_size_ = 0;
      List *list_;
      SkipList<Edge, EdgeComparator> *skip_list_;
      int cnt_ = 0;
      uint8_t full_; // 1: array full, 2:list full
    public:
      class MemEdgeIterator;

      NewEdgeHybrid() : list_(nullptr), skip_list_(nullptr), full_(0) {
        if (FLAGS_reserve_node > 0) {
          dst_ = new Edge[FLAGS_reserve_node];
        }
      }

      ~NewEdgeHybrid() {
        switch (full_) {
          case 2:
            delete skip_list_;
          case 1:
            delete list_;
          case 0:
            if (FLAGS_reserve_node > 0) {
              delete[] dst_;
            }
        }

      }

      void put_edge(VertexId_t dst, SequenceNumber_t seq,
                    Marker_t marker, const EdgeProperty_t &property) {

        switch (full_) {
          case 0: {
            if (cnt_ == FLAGS_reserve_node) {
              full_ = 1;
              list_ = new List;
              for (int i = 0; i < FLAGS_reserve_node; i++) {
                list_->put_edge(dst_[i].destination(), dst_[i].sequence(), dst_[i].marker(), dst_[i].property());
              }
              list_->put_edge(dst, seq, marker, property);
            } else {
              Edge edge(dst, seq, marker, property);
              int i;
              for (i = 0; i < this->cnt_; i++) {
                if (edge < this->dst_[i]) {
                  for (int j = cnt_; j > i; j--) {
                    dst_[j] = dst_[j - 1];
                  }
                  break;
                }
              }
              dst_[i] = edge;
              cnt_++;
            }
            break;
          }
          case 1: {
            if (list_->size() == 5) {
              full_ = 2;
              skip_list_ = new SkipList<Edge, EdgeComparator>;
              list_->dump_to_skip_list(skip_list_);
              skip_list_->put_edge(dst, seq, marker, property);
            } else {
              list_->put_edge(dst, seq, marker, property);
            }
            break;
          }
          case 2: {
            skip_list_->put_edge(dst, seq, marker, property);
          }
        }
        property_size_ += property.size();
      }

      Status get(VertexId_t dst, std::string* property) {
        switch (full_) {
          case 0: {
            int i = 0;
            while (i < this->cnt_) {
              if (this->dst_[i].destination() == dst) {
                if (this->dst_[i].marker()) return Status::kDelete;
                property->assign(this->dst_[i].property().data(),
                                 this->dst_[i].property().size());
                return Status::kOk;
              }
              i++;
            }
            return Status::kNotFound;
          }
          case 1: {
            return list_->get(dst, property);
          }
          case 2: {
            return skip_list_->get(dst, property);
          }
        }
        return Status::kNotFound;
      }

      Status get(VertexId_t dst, SequenceNumber_t& seq) {
        switch (full_) {
          case 0: {
            int i = 0;
            while (i < this->cnt_) {
              if (this->dst_[i].destination() == dst) {
                if (this->dst_[i].marker()) return Status::kDelete;
                seq = this->dst_[i].sequence();
                return Status::kOk;
              }
              i++;
            }
            return Status::kNotFound;
          }
          case 1: {
            return list_->get(dst, seq);
          }
          case 2: {
            return skip_list_->get(dst, seq);
          }
        }
        return Status::kNotFound;
      }

      void get_edges(VertexId_t src, std::vector<Edge>& edges) {
        edges.reserve(edges.size() + this->cnt_);
        switch (full_) {
          case 0: {
            VertexId_t saved_key = INVALID_VERTEX_ID;
            bool skipping = false;
            for (int i = 0; i < this->cnt_; i++) {
              if (!(skipping && this->dst_[i].destination() == saved_key)) {
                if (this->dst_[i].marker() == 0) {
                  edges.emplace_back(this->dst_[i]);
                } else {
                  saved_key = this->dst_[i].destination();
                  skipping = true;
                }
              }
            }
          }
          case 1: {
            list_->get_edges(src, edges);
          }
          case 2: {
            skip_list_->get_edges(src, edges);
          }
        }
      }

      Edge getvector(int i) {
        if (i > cnt_ - 1) {
          std::cout << "error";
          return Edge(0, 0);
        } else return dst_[i];
      }

      int getVectorNum() const {
        return cnt_;
      }

      int getEdgeNum() const {
        switch (full_) {
          case 0:
            return cnt_;
          case 1:
            return list_->size();
          case 2:
            return skip_list_->size();
        }
        std::abort();
      }

      size_t getPropertySize() const {
        return property_size_;
      }

      uint8_t isfull() const {
        return full_;
      }

      const Edge *getArrayHead() const {
        return dst_;
      }

      void reset() {
        switch (full_) {
          case 2:
            delete skip_list_;
            skip_list_ = nullptr;
          case 1:
            delete list_;
            list_ = nullptr;
          case 0:
            full_ = 0;
            cnt_ = 0;
            property_size_ = 0;
        }

      }

      void sort() {
#ifndef SORTED_ARRAY
        if (full_ == 0) {
          std::sort(dst_, dst_ + cnt_, [](const Edge &lhs, const Edge &rhs) -> bool {
            if (lhs.destination() != rhs.destination()) {
              return lhs.destination() < rhs.destination();
            } else {
              return lhs.sequence() > rhs.sequence();
            }
          });
        }
#endif
      }
    };

    class NewEdgeHybrid::MemEdgeIterator : public EdgeIteratorBase {
    public:
      // Initialize an iterator over the specified list.
      // The returned iterator is not valid.
      explicit MemEdgeIterator(const NewEdgeHybrid *edges, FileId_t fid = INVALID_File_ID)
              : fid_(fid), edges_(edges) {
        init();
      }

      void init() {
        if (edges_ == nullptr) {
          return;
        }
        switch (edges_->isfull()) {
          case 0:
            array_it_ = edges_->getArrayHead();
            break;
          case 1:
            list_it_ = List::MemEdgeIterator(edges_->list_, fid_);
            break;
          case 2:
            skip_list_it_ = SkipList<Edge, EdgeComparator>::MemEdgeIterator(edges_->skip_list_, fid_);
            break;
        }
      }

      bool valid() const override {
        if (edges_ == nullptr) {
          return false;
        }
        switch (edges_->isfull()) {
          case 0:
            return array_it_ != edges_->getArrayHead() + edges_->getVectorNum();
          case 1:
            return list_it_.valid();
          case 2:
            return skip_list_it_.valid();
        }
        std::abort();
      }

      // Returns the edge at the current position.
      const Edge &key() {
        assert(valid());
        switch (edges_->isfull()) {
          case 0:
            return *array_it_;
          case 1:
            return list_it_.key();
          case 2:
            return skip_list_it_.key();
        }
        std::abort();
      }

      void next() {
        switch (edges_->isfull()) {
          case 0:
            array_it_++;
            break;
          case 1:
            list_it_.next();
            break;
          case 2:
            skip_list_it_.next();
            break;
        }
      }

      VertexId_t dst_id() const override {
        switch (edges_->isfull()) {
          case 0:
            return array_it_->destination();
          case 1:
            return list_it_.dst_id();
          case 2:
            return skip_list_it_.dst_id();
        }
        std::abort();
      }

      Marker_t marker() const override {
        switch (edges_->isfull()) {
          case 0:
            return array_it_->marker();
          case 1:
            return list_it_.marker();
          case 2:
            return skip_list_it_.marker();
        }
        std::abort();
      }

      SequenceNumber_t sequence() const override {
        switch (edges_->isfull()) {
          case 0:
            return array_it_->sequence();
          case 1:
            return list_it_.sequence();
          case 2:
            return skip_list_it_.sequence();
        }
        std::abort();
      }

      EdgeProperty_t edge_data(int inplace) const override {
        switch (edges_->isfull()) {
          case 0:
            return array_it_->property();
          case 1:
            return list_it_.edge_data(inplace);
          case 2:
            return skip_list_it_.edge_data(inplace);
        }
        std::abort();
      }

      bool empty() const override {
        return !edges_->getEdgeNum();
      }

      size_t size() const override {
        return edges_->getEdgeNum();
      }

      #ifndef  NO_VIRTUAL

      bool IsMemTable() const override {
        return is_mem_table;
      }

      #endif

      FileId_t get_fid() const override {
        return fid_;
      }

    private:
      const NewEdgeHybrid *edges_;
      SkipList<Edge, EdgeComparator>::MemEdgeIterator skip_list_it_;
      List::MemEdgeIterator list_it_;
      const Edge *array_it_;
      FileId_t fid_;
      #ifndef NO_VIRTUAL
      bool is_mem_table = true;
      #endif
    };
} // lsmgraph
