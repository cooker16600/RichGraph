#pragma once

#include <pthread.h>
#include <sys/mman.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <condition_variable>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <functional>
#include <fstream>
#include <iostream>
#include <limits>
#include <map>
#include <memory>
#include <memory_resource>
#include <mutex>
#include <optional>
#include <queue>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

namespace prechunk {

constexpr uint32_t kMagicA = 0x48435052;  // RPCH, little endian.
constexpr uint32_t kMagicB = 0x5043484b;  // KHCP, tolerated draft magic.
constexpr char kMagicRichPreprocessed[8] = {'R', 'P', 'R', 'E',
                                            'C', 'H', '1', '\0'};

enum class Stage : uint16_t {
  kUnknown = 0,
  kSnapshotNodes = 1,
  kSnapshotEdges = 2,
  kRemainingNodes = 3,
  kRemainingEdges = 4,
  kMixedOps = 5,
  kFinalQuery = 6,
  kSingleNodeRead = 7,
  kSingleEdgeRead = 8,
  kFinbenchNodeUpdate = 9,
  kFinbenchEdgeUpdate = 10,
};

using FieldString = std::pmr::string;

struct Record {
  uint8_t op_code = 0;
  uint32_t schema_id = 0;
  uint64_t row_index = 0;
  uint64_t event_time_ms = 0;
  std::pmr::vector<FieldString> fields;

  explicit Record(std::pmr::memory_resource* mr =
                      std::pmr::get_default_resource())
      : fields(mr) {}

  Record(const Record&) = delete;
  Record& operator=(const Record&) = delete;
  Record(Record&&) noexcept = default;
  Record& operator=(Record&&) noexcept = default;
};

struct Chunk {
  std::filesystem::path path;
  uint16_t version = 0;
  Stage stage = Stage::kUnknown;
  uint64_t record_count = 0;
  std::pmr::vector<Record> records;
  uint32_t arena_block_id = std::numeric_limits<uint32_t>::max();
  std::pmr::memory_resource* memory_resource = std::pmr::get_default_resource();

  explicit Chunk(std::pmr::memory_resource* mr =
                     std::pmr::get_default_resource())
      : records(mr), memory_resource(mr) {}

  Chunk(const Chunk&) = delete;
  Chunk& operator=(const Chunk&) = delete;
  Chunk(Chunk&&) noexcept = default;
  Chunk& operator=(Chunk&&) noexcept = default;
};

struct SchemaInfo {
  uint32_t id = 0;
  std::string name;
  std::string kind;
  std::vector<std::string> columns;
  std::unordered_map<std::string, std::string> metadata;

  std::string Metadata(std::string_view key) const {
    const auto it = metadata.find(std::string(key));
    return it == metadata.end() ? std::string() : it->second;
  }
};

inline std::unordered_map<std::string, std::string> RecordFieldMap(
    const SchemaInfo& schema,
    const Record& record) {
  std::unordered_map<std::string, std::string> out;
  const size_t n = std::min(schema.columns.size(), record.fields.size());
  out.reserve(n);
  for (size_t i = 0; i < n; ++i) {
    out.emplace(schema.columns[i],
                std::string(record.fields[i].data(), record.fields[i].size()));
  }
  return out;
}

inline std::string RecordField(const SchemaInfo& schema,
                               const Record& record,
                               std::string_view name) {
  for (size_t i = 0; i < schema.columns.size() && i < record.fields.size();
       ++i) {
    if (schema.columns[i] == name) {
      return std::string(record.fields[i].data(), record.fields[i].size());
    }
  }
  return {};
}

inline std::string_view FieldView(const Record& record, size_t idx) {
  if (idx >= record.fields.size()) {
    return {};
  }
  return std::string_view(record.fields[idx].data(), record.fields[idx].size());
}

inline std::vector<std::string> CopyFieldsToStd(const Record& record) {
  std::vector<std::string> out;
  out.reserve(record.fields.size());
  for (const auto& field : record.fields) {
    out.emplace_back(field.data(), field.size());
  }
  return out;
}

inline bool ReadWholeFile(const std::filesystem::path& path,
                          std::string* out,
                          std::string* error) {
  std::ifstream in(path, std::ios::binary);
  if (!in) {
    if (error != nullptr) {
      *error = "open failed: " + path.string();
    }
    return false;
  }
  std::ostringstream ss;
  ss << in.rdbuf();
  *out = ss.str();
  return true;
}

inline void SkipJsonWs(const std::string& s, size_t* pos) {
  while (*pos < s.size() &&
         std::isspace(static_cast<unsigned char>(s[*pos])) != 0) {
    ++(*pos);
  }
}

inline bool ParseJsonStringAt(const std::string& s,
                              size_t* pos,
                              std::string* out) {
  if (*pos >= s.size() || s[*pos] != '"') {
    return false;
  }
  ++(*pos);
  out->clear();
  while (*pos < s.size()) {
    const char ch = s[(*pos)++];
    if (ch == '"') {
      return true;
    }
    if (ch != '\\') {
      out->push_back(ch);
      continue;
    }
    if (*pos >= s.size()) {
      return false;
    }
    const char esc = s[(*pos)++];
    switch (esc) {
      case '"':
      case '\\':
      case '/':
        out->push_back(esc);
        break;
      case 'b':
        out->push_back('\b');
        break;
      case 'f':
        out->push_back('\f');
        break;
      case 'n':
        out->push_back('\n');
        break;
      case 'r':
        out->push_back('\r');
        break;
      case 't':
        out->push_back('\t');
        break;
      case 'u':
        // The generated schema only uses ASCII keys/values needed here.
        if (*pos + 4 > s.size()) {
          return false;
        }
        *pos += 4;
        break;
      default:
        out->push_back(esc);
        break;
    }
  }
  return false;
}

inline size_t FindJsonKey(const std::string& s,
                          std::string_view key,
                          size_t start = 0) {
  const std::string needle = "\"" + std::string(key) + "\"";
  return s.find(needle, start);
}

inline bool FindJsonValueStart(const std::string& s,
                               std::string_view key,
                               size_t* value_pos) {
  size_t pos = FindJsonKey(s, key);
  if (pos == std::string::npos) {
    return false;
  }
  pos += key.size() + 2;
  pos = s.find(':', pos);
  if (pos == std::string::npos) {
    return false;
  }
  ++pos;
  SkipJsonWs(s, &pos);
  *value_pos = pos;
  return true;
}

inline size_t FindJsonMatching(const std::string& s,
                               size_t open_pos,
                               char open_ch,
                               char close_ch) {
  if (open_pos >= s.size() || s[open_pos] != open_ch) {
    return std::string::npos;
  }
  bool in_string = false;
  bool escaped = false;
  int depth = 0;
  for (size_t i = open_pos; i < s.size(); ++i) {
    const char ch = s[i];
    if (in_string) {
      if (escaped) {
        escaped = false;
      } else if (ch == '\\') {
        escaped = true;
      } else if (ch == '"') {
        in_string = false;
      }
      continue;
    }
    if (ch == '"') {
      in_string = true;
      continue;
    }
    if (ch == open_ch) {
      ++depth;
    } else if (ch == close_ch) {
      --depth;
      if (depth == 0) {
        return i;
      }
    }
  }
  return std::string::npos;
}

inline bool ExtractJsonString(const std::string& object,
                              std::string_view key,
                              std::string* out) {
  size_t pos = 0;
  if (!FindJsonValueStart(object, key, &pos)) {
    return false;
  }
  return ParseJsonStringAt(object, &pos, out);
}

inline bool ExtractJsonUint(const std::string& object,
                            std::string_view key,
                            uint32_t* out) {
  size_t pos = 0;
  if (!FindJsonValueStart(object, key, &pos)) {
    return false;
  }
  uint64_t value = 0;
  bool seen = false;
  while (pos < object.size() &&
         std::isdigit(static_cast<unsigned char>(object[pos])) != 0) {
    seen = true;
    value = value * 10ULL + static_cast<uint64_t>(object[pos] - '0');
    if (value > std::numeric_limits<uint32_t>::max()) {
      return false;
    }
    ++pos;
  }
  if (!seen) {
    return false;
  }
  *out = static_cast<uint32_t>(value);
  return true;
}

inline std::vector<std::string> ExtractJsonStringArray(
    const std::string& object,
    std::string_view key) {
  std::vector<std::string> out;
  size_t pos = 0;
  if (!FindJsonValueStart(object, key, &pos) || pos >= object.size() ||
      object[pos] != '[') {
    return out;
  }
  const size_t end = FindJsonMatching(object, pos, '[', ']');
  if (end == std::string::npos) {
    return out;
  }
  ++pos;
  while (pos < end) {
    SkipJsonWs(object, &pos);
    if (pos >= end) {
      break;
    }
    if (object[pos] == ',') {
      ++pos;
      continue;
    }
    std::string value;
    if (!ParseJsonStringAt(object, &pos, &value)) {
      break;
    }
    out.push_back(std::move(value));
  }
  return out;
}

inline std::unordered_map<std::string, std::string> ExtractJsonSimpleObject(
    const std::string& object,
    std::string_view key) {
  std::unordered_map<std::string, std::string> out;
  size_t pos = 0;
  if (!FindJsonValueStart(object, key, &pos) || pos >= object.size() ||
      object[pos] != '{') {
    return out;
  }
  const size_t end = FindJsonMatching(object, pos, '{', '}');
  if (end == std::string::npos) {
    return out;
  }
  ++pos;
  while (pos < end) {
    SkipJsonWs(object, &pos);
    if (pos >= end) {
      break;
    }
    if (object[pos] == ',') {
      ++pos;
      continue;
    }
    std::string k;
    if (!ParseJsonStringAt(object, &pos, &k)) {
      break;
    }
    SkipJsonWs(object, &pos);
    if (pos >= end || object[pos] != ':') {
      break;
    }
    ++pos;
    SkipJsonWs(object, &pos);
    std::string v;
    if (pos < end && object[pos] == '"') {
      if (!ParseJsonStringAt(object, &pos, &v)) {
        break;
      }
    } else {
      const size_t begin = pos;
      while (pos < end && object[pos] != ',' && object[pos] != '}') {
        ++pos;
      }
      v = object.substr(begin, pos - begin);
      while (!v.empty() &&
             std::isspace(static_cast<unsigned char>(v.back())) != 0) {
        v.pop_back();
      }
    }
    out.emplace(std::move(k), std::move(v));
  }
  return out;
}

class SchemaCatalog {
 public:
  bool Load(const std::string& root, std::string* error) {
    std::filesystem::path path = std::filesystem::path(root) / "meta" /
                                 "schema.json";
    std::string json;
    if (!ReadWholeFile(path, &json, error)) {
      return false;
    }
    size_t schemas_pos = 0;
    if (!FindJsonValueStart(json, "schemas", &schemas_pos) ||
        schemas_pos >= json.size() || json[schemas_pos] != '[') {
      if (error != nullptr) {
        *error = "schema.json missing schemas array: " + path.string();
      }
      return false;
    }
    const size_t schemas_end = FindJsonMatching(json, schemas_pos, '[', ']');
    if (schemas_end == std::string::npos) {
      if (error != nullptr) {
        *error = "schema.json bad schemas array: " + path.string();
      }
      return false;
    }
    schemas_.clear();
    by_id_.clear();
    size_t pos = schemas_pos + 1;
    while (pos < schemas_end) {
      SkipJsonWs(json, &pos);
      if (pos >= schemas_end) {
        break;
      }
      if (json[pos] == ',') {
        ++pos;
        continue;
      }
      if (json[pos] != '{') {
        ++pos;
        continue;
      }
      const size_t obj_end = FindJsonMatching(json, pos, '{', '}');
      if (obj_end == std::string::npos || obj_end > schemas_end) {
        break;
      }
      const std::string obj = json.substr(pos, obj_end - pos + 1);
      SchemaInfo info;
      if (ExtractJsonUint(obj, "id", &info.id) &&
          ExtractJsonString(obj, "name", &info.name) &&
          ExtractJsonString(obj, "kind", &info.kind)) {
        info.columns = ExtractJsonStringArray(obj, "columns");
        info.metadata = ExtractJsonSimpleObject(obj, "metadata");
        by_id_[info.id] = schemas_.size();
        schemas_.push_back(std::move(info));
      }
      pos = obj_end + 1;
    }
    if (schemas_.empty()) {
      if (error != nullptr) {
        *error = "schema.json has no parsed schemas: " + path.string();
      }
      return false;
    }
    return true;
  }

  const SchemaInfo* Find(uint32_t id) const {
    const auto it = by_id_.find(id);
    if (it == by_id_.end()) {
      return nullptr;
    }
    return &schemas_[it->second];
  }

  const std::vector<SchemaInfo>& schemas() const { return schemas_; }

 private:
  std::vector<SchemaInfo> schemas_;
  std::unordered_map<uint32_t, size_t> by_id_;
};

struct LoaderOptions {
  std::string root;
  uint32_t threads = 16;
  uint32_t loader_cpu_base = 16;
  uint32_t db_cpu_base = 0;
  uint64_t arena_gb = 40;
  uint32_t queue_blocks = 32;
  uint32_t prefill_blocks = 32;
  uint32_t block_records = 100000;
  bool skip_node_update = false;
  bool skip_edge_update = false;
};

struct LoaderStats {
  bool ok = true;
  uint64_t chunks = 0;
  uint64_t records = 0;
  uint64_t bytes = 0;
  uint64_t arena_peak_bytes = 0;
  uint64_t loader_wait_count = 0;
  uint64_t db_wait_count = 0;
  double loader_wait_sec = 0.0;
  double db_wait_sec = 0.0;
  uint32_t prefill_target_blocks = 0;
  uint32_t prefill_ready_blocks = 0;
  double prefill_wait_sec = 0.0;
  double load_sec = 0.0;
  std::string error;
};

inline bool SetCurrentThreadAffinityRange(uint32_t first_cpu,
                                          uint32_t cpu_count) {
#ifdef __linux__
  if (cpu_count == 0) {
    return true;
  }
  cpu_set_t set;
  CPU_ZERO(&set);
  for (uint32_t i = 0; i < cpu_count; ++i) {
    CPU_SET(first_cpu + i, &set);
  }
  return pthread_setaffinity_np(pthread_self(), sizeof(set), &set) == 0;
#else
  (void)first_cpu;
  (void)cpu_count;
  return true;
#endif
}

class FixedArena {
 public:
  class BlockResource final : public std::pmr::memory_resource {
   public:
    BlockResource() = default;
    BlockResource(char* base, uint64_t size) : base_(base), size_(size) {}

    void Reset() { used_ = 0; }
    uint64_t used() const { return used_; }
    uint64_t max_used() const { return max_used_; }

   private:
    static uint64_t AlignUp(uint64_t value, uint64_t alignment) {
      if (alignment == 0) {
        return value;
      }
      const uint64_t remainder = value % alignment;
      return remainder == 0 ? value : value + alignment - remainder;
    }

    void* do_allocate(size_t bytes, size_t alignment) override {
      const uint64_t aligned =
          AlignUp(used_, static_cast<uint64_t>(alignment));
      if (aligned > size_ || bytes > size_ - aligned) {
        throw std::bad_alloc();
      }
      void* ptr = base_ + aligned;
      used_ = aligned + static_cast<uint64_t>(bytes);
      if (used_ > max_used_) {
        max_used_ = used_;
      }
      return ptr;
    }

    void do_deallocate(void*, size_t, size_t) override {
      // Block arena: memory is reclaimed by Reset() after the whole chunk.
    }

    bool do_is_equal(const std::pmr::memory_resource& other) const noexcept override {
      return this == &other;
    }

    char* base_ = nullptr;
    uint64_t size_ = 0;
    uint64_t used_ = 0;
    uint64_t max_used_ = 0;
  };

  bool Open(uint64_t gib, uint32_t block_count) {
    block_count_ = std::max<uint32_t>(1U, block_count);
    if (gib == 0) {
      return true;
    }
    const uint64_t bytes = gib * 1024ULL * 1024ULL * 1024ULL;
    void* base = mmap(nullptr,
                      static_cast<size_t>(bytes),
                      PROT_READ | PROT_WRITE,
                      MAP_PRIVATE | MAP_ANONYMOUS,
                      -1,
                      0);
    if (base == MAP_FAILED) {
      std::cerr << "[PREPROCESSED_LOADER] mmap arena failed, bytes=" << bytes
                << ", errno=" << errno << std::endl;
      return false;
    }
    base_ = static_cast<char*>(base);
    size_ = bytes;
    block_size_ = size_ / block_count_;
    if (block_size_ == 0) {
      std::cerr << "[PREPROCESSED_LOADER] arena block size is zero"
                << std::endl;
      Close();
      return false;
    }
    const long page_size_long = sysconf(_SC_PAGESIZE);
    const size_t page_size =
        page_size_long <= 0 ? 4096 : static_cast<size_t>(page_size_long);
    const auto t1 = std::chrono::steady_clock::now();
    for (uint64_t offset = 0; offset < size_; offset += page_size) {
      base_[offset] = 0;
    }
    if (size_ > 0) {
      base_[size_ - 1] = 0;
    }
    const auto t2 = std::chrono::steady_clock::now();
    std::cout << "[PREPROCESSED_LOADER] arena_gb: " << gib << std::endl;
    std::cout << "[PREPROCESSED_LOADER] arena_bytes: " << size_ << std::endl;
    std::cout << "[PREPROCESSED_LOADER] arena_blocks: " << block_count_
              << std::endl;
    std::cout << "[PREPROCESSED_LOADER] arena_block_bytes: " << block_size_
              << std::endl;
    std::cout << "[PREPROCESSED_LOADER] arena_touch_time(s): "
              << std::chrono::duration_cast<std::chrono::duration<double>>(t2 - t1)
                     .count()
              << std::endl;
    resources_.reserve(block_count_);
    for (uint32_t i = 0; i < block_count_; ++i) {
      resources_.push_back(std::make_unique<BlockResource>(
          base_ + static_cast<uint64_t>(i) * block_size_, block_size_));
      free_blocks_.push(i);
    }
    return true;
  }

  void Close() {
    {
      std::lock_guard<std::mutex> lock(mu_);
      closed_ = true;
      while (!free_blocks_.empty()) {
        free_blocks_.pop();
      }
    }
    cv_.notify_all();
    resources_.clear();
    if (base_ != nullptr) {
      munmap(base_, static_cast<size_t>(size_));
      base_ = nullptr;
      size_ = 0;
      block_size_ = 0;
    }
  }

  ~FixedArena() {
    Close();
  }

  uint64_t size() const {
    return size_;
  }

  std::pmr::memory_resource* Acquire(uint32_t* block_id) {
    if (block_id != nullptr) {
      *block_id = std::numeric_limits<uint32_t>::max();
    }
    if (size_ == 0) {
      return std::pmr::get_default_resource();
    }
    std::unique_lock<std::mutex> lock(mu_);
    cv_.wait(lock, [&]() { return closed_ || !free_blocks_.empty(); });
    if (closed_ || free_blocks_.empty()) {
      return nullptr;
    }
    const uint32_t id = free_blocks_.front();
    free_blocks_.pop();
    resources_[id]->Reset();
    if (block_id != nullptr) {
      *block_id = id;
    }
    return resources_[id].get();
  }

  void Release(uint32_t block_id) {
    if (size_ == 0 || block_id == std::numeric_limits<uint32_t>::max()) {
      return;
    }
    {
      std::lock_guard<std::mutex> lock(mu_);
      if (closed_ || block_id >= resources_.size()) {
        return;
      }
      resources_[block_id]->Reset();
      free_blocks_.push(block_id);
    }
    cv_.notify_one();
  }

  uint64_t max_used_bytes() const {
    uint64_t total = 0;
    for (const auto& resource : resources_) {
      if (resource != nullptr) {
        total += resource->max_used();
      }
    }
    return total;
  }

 private:
  char* base_ = nullptr;
  uint64_t size_ = 0;
  uint64_t block_size_ = 0;
  uint32_t block_count_ = 1;
  std::vector<std::unique_ptr<BlockResource>> resources_;
  std::queue<uint32_t> free_blocks_;
  mutable std::mutex mu_;
  std::condition_variable cv_;
  bool closed_ = false;
};

inline bool ReadExact(std::ifstream* in, void* dst, size_t bytes) {
  in->read(static_cast<char*>(dst), static_cast<std::streamsize>(bytes));
  return static_cast<size_t>(in->gcount()) == bytes;
}

template <typename T>
bool ReadPod(std::ifstream* in, T* value) {
  return ReadExact(in, value, sizeof(T));
}

inline bool ReadString(std::ifstream* in, FieldString* out) {
  uint32_t len = 0;
  if (!ReadPod(in, &len)) {
    return false;
  }
  out->assign(len, '\0');
  if (len == 0) {
    return true;
  }
  return ReadExact(in, out->data(), len);
}

inline bool ReadChunkFile(const std::filesystem::path& path,
                          Chunk* chunk,
                          std::string* error,
                          std::pmr::memory_resource* mr =
                              std::pmr::get_default_resource()) {
  if (mr == nullptr) {
    mr = std::pmr::get_default_resource();
  }
  *chunk = Chunk(mr);
  std::ifstream in(path, std::ios::binary);
  if (!in) {
    if (error != nullptr) {
      *error = "open failed: " + path.string();
    }
    return false;
  }

  char magic8[8];
  if (!ReadExact(&in, magic8, sizeof(magic8))) {
    if (error != nullptr) {
      *error = "short chunk header: " + path.string();
    }
    return false;
  }

  uint16_t version = 0;
  uint16_t stage = 0;
  uint64_t record_count = 0;
  if (std::memcmp(magic8, kMagicRichPreprocessed, sizeof(magic8)) == 0) {
    uint32_t count32 = 0;
    uint64_t reserved = 0;
    if (!ReadPod(&in, &version) || !ReadPod(&in, &stage) ||
        !ReadPod(&in, &count32) || !ReadPod(&in, &reserved)) {
      if (error != nullptr) {
        *error = "short chunk header: " + path.string();
      }
      return false;
    }
    record_count = count32;
    chunk->path = path;
    chunk->version = version;
    chunk->stage = static_cast<Stage>(stage);
    chunk->record_count = record_count;
  } else {
    uint32_t magic = 0;
    std::memcpy(&magic, magic8, sizeof(magic));
    in.seekg(0, std::ios::beg);
    if (!ReadPod(&in, &magic) || !ReadPod(&in, &version) ||
        !ReadPod(&in, &stage) || !ReadPod(&in, &record_count)) {
      if (error != nullptr) {
        *error = "short chunk header: " + path.string();
      }
      return false;
    }
    if (magic != kMagicA && magic != kMagicB) {
      if (error != nullptr) {
        *error = "bad chunk magic: " + path.string();
      }
      return false;
    }
  }

  chunk->path = path;
  chunk->version = version;
  chunk->stage = static_cast<Stage>(stage);
  chunk->record_count = record_count;
  chunk->records.reserve(static_cast<size_t>(
      std::min<uint64_t>(record_count, 100000ULL)));

  for (uint64_t i = 0; i < record_count; ++i) {
    Record record(mr);
    uint16_t field_count = 0;
    if (std::memcmp(magic8, kMagicRichPreprocessed, sizeof(magic8)) == 0) {
      if (!ReadPod(&in, &record.op_code) ||
          !ReadPod(&in, &record.schema_id) ||
          !ReadPod(&in, &record.row_index) ||
          !ReadPod(&in, &record.event_time_ms) ||
          !ReadPod(&in, &field_count)) {
        if (error != nullptr) {
          *error = "short record header: " + path.string();
        }
        return false;
      }
    } else if (!ReadPod(&in, &record.op_code) ||
               !ReadPod(&in, &field_count)) {
      if (error != nullptr) {
        *error = "short record header: " + path.string();
      }
      return false;
    }
    record.fields.reserve(field_count);
    for (uint32_t f = 0; f < field_count; ++f) {
      FieldString value(mr);
      if (!ReadString(&in, &value)) {
        if (error != nullptr) {
          *error = "short field value: " + path.string();
        }
        return false;
      }
      record.fields.push_back(std::move(value));
    }
    chunk->records.push_back(std::move(record));
  }
  return true;
}

inline std::vector<std::filesystem::path> ListChunkFiles(
    const std::string& root,
    bool skip_node_update = false,
    bool skip_edge_update = false) {
  std::vector<std::filesystem::path> files;
  std::error_code ec;
  if (root.empty() || !std::filesystem::exists(root, ec)) {
    return files;
  }
  for (std::filesystem::recursive_directory_iterator it(root, ec), end;
       !ec && it != end;
       it.increment(ec)) {
    if (ec) {
      break;
    }
    if (!it->is_regular_file(ec)) {
      continue;
    }
    const auto path = it->path();
    if (path.extension() == ".json") {
      continue;
    }
    const std::string parent = path.parent_path().filename().string();
    if (skip_node_update && parent == "finbench_node_update") {
      continue;
    }
    if (skip_edge_update && parent == "finbench_edge_update") {
      continue;
    }
    files.push_back(path);
  }
  auto rank = [](const std::filesystem::path& path) {
    const std::string parent = path.parent_path().filename().string();
    if (parent == "snapshot_nodes") return 1;
    if (parent == "snapshot_edges") return 2;
    if (parent == "remaining_nodes") return 3;
    if (parent == "remaining_edges") return 4;
    if (parent == "mixed_ops") return 5;
    if (parent == "single_node_read") return 6;
    if (parent == "single_edge_read") return 7;
    if (parent == "final_queries") return 8;
    if (parent == "finbench_node_update") return 9;
    if (parent == "finbench_edge_update") return 10;
    return 100;
  };
  std::sort(files.begin(), files.end(), [&](const auto& a, const auto& b) {
    const int ra = rank(a);
    const int rb = rank(b);
    if (ra != rb) {
      return ra < rb;
    }
    return a < b;
  });
  return files;
}

class ChunkQueue {
 public:
  explicit ChunkQueue(size_t cap) : cap_(std::max<size_t>(1, cap)) {}

  bool Push(Chunk chunk, LoaderStats* stats) {
    const auto wait_t1 = std::chrono::steady_clock::now();
    std::unique_lock<std::mutex> lock(mu_);
    not_full_.wait(lock, [&]() { return closed_ || queue_.size() < cap_; });
    const auto wait_t2 = std::chrono::steady_clock::now();
    const double wait_sec =
        std::chrono::duration_cast<std::chrono::duration<double>>(wait_t2 - wait_t1)
            .count();
    if (wait_sec > 0.000001 && stats != nullptr) {
      ++stats->loader_wait_count;
      stats->loader_wait_sec += wait_sec;
    }
    if (closed_) {
      return false;
    }
    queue_.push(std::move(chunk));
    not_empty_.notify_one();
    return true;
  }

  size_t WaitForPrefill(size_t target) {
    if (target == 0) {
      return 0;
    }
    std::unique_lock<std::mutex> lock(mu_);
    not_empty_.wait(lock, [&]() {
      return closed_ || queue_.size() >= target;
    });
    return queue_.size();
  }

  bool Pop(Chunk* chunk, LoaderStats* stats) {
    const auto wait_t1 = std::chrono::steady_clock::now();
    std::unique_lock<std::mutex> lock(mu_);
    not_empty_.wait(lock, [&]() { return closed_ || !queue_.empty(); });
    const auto wait_t2 = std::chrono::steady_clock::now();
    const double wait_sec =
        std::chrono::duration_cast<std::chrono::duration<double>>(wait_t2 - wait_t1)
            .count();
    if (wait_sec > 0.000001 && stats != nullptr) {
      ++stats->db_wait_count;
      stats->db_wait_sec += wait_sec;
    }
    if (queue_.empty()) {
      return false;
    }
    *chunk = std::move(queue_.front());
    queue_.pop();
    not_full_.notify_one();
    return true;
  }

  void Close() {
    std::lock_guard<std::mutex> lock(mu_);
    closed_ = true;
    not_full_.notify_all();
    not_empty_.notify_all();
  }

 private:
  size_t cap_;
  std::mutex mu_;
  std::condition_variable not_full_;
  std::condition_variable not_empty_;
  std::queue<Chunk> queue_;
  bool closed_ = false;
};

using ChunkConsumer = std::function<bool(Chunk*)>;

template <typename Item>
class TypedChunkQueue {
 public:
  explicit TypedChunkQueue(size_t cap) : cap_(std::max<size_t>(1, cap)) {}

  bool Push(uint32_t block_id, Item item, LoaderStats* stats) {
    const auto wait_t1 = std::chrono::steady_clock::now();
    std::unique_lock<std::mutex> lock(mu_);
    not_full_.wait(lock, [&]() { return closed_ || queue_.size() < cap_; });
    const auto wait_t2 = std::chrono::steady_clock::now();
    const double wait_sec =
        std::chrono::duration_cast<std::chrono::duration<double>>(wait_t2 - wait_t1)
            .count();
    if (wait_sec > 0.000001 && stats != nullptr) {
      ++stats->loader_wait_count;
      stats->loader_wait_sec += wait_sec;
    }
    if (closed_) {
      return false;
    }
    queue_.emplace(block_id, std::move(item));
    not_empty_.notify_one();
    return true;
  }

  size_t WaitForPrefill(size_t target) {
    if (target == 0) {
      return 0;
    }
    std::unique_lock<std::mutex> lock(mu_);
    not_empty_.wait(lock, [&]() {
      return closed_ || queue_.size() >= target;
    });
    return queue_.size();
  }

  bool Pop(std::optional<Item>* item,
           uint32_t* block_id,
           LoaderStats* stats) {
    const auto wait_t1 = std::chrono::steady_clock::now();
    std::unique_lock<std::mutex> lock(mu_);
    not_empty_.wait(lock, [&]() { return closed_ || !queue_.empty(); });
    const auto wait_t2 = std::chrono::steady_clock::now();
    const double wait_sec =
        std::chrono::duration_cast<std::chrono::duration<double>>(wait_t2 - wait_t1)
            .count();
    if (wait_sec > 0.000001 && stats != nullptr) {
      ++stats->db_wait_count;
      stats->db_wait_sec += wait_sec;
    }
    if (queue_.empty()) {
      return false;
    }
    const uint32_t queued_block_id = queue_.front().arena_block_id;
    if (block_id != nullptr) {
      *block_id = queued_block_id;
    }
    item->emplace(std::move(queue_.front().item));
    (*item)->arena_block_id = queued_block_id;
    queue_.pop();
    not_full_.notify_one();
    return true;
  }

  void Close() {
    std::lock_guard<std::mutex> lock(mu_);
    closed_ = true;
    not_full_.notify_all();
    not_empty_.notify_all();
  }

 private:
  struct QueuedItem {
    uint32_t arena_block_id = std::numeric_limits<uint32_t>::max();
    Item item;

    QueuedItem(uint32_t id, Item&& value)
        : arena_block_id(id), item(std::move(value)) {}
  };

  size_t cap_;
  std::mutex mu_;
  std::condition_variable not_full_;
  std::condition_variable not_empty_;
  std::queue<QueuedItem> queue_;
  bool closed_ = false;
};

template <typename Item, typename Transform, typename Consumer>
inline bool RunTransformedPreprocessedLoader(const LoaderOptions& options,
                                             const Transform& transform,
                                             const Consumer& consumer,
                                             LoaderStats* out) {
  LoaderStats stats;
  const auto files = ListChunkFiles(options.root,
                                    options.skip_node_update,
                                    options.skip_edge_update);
  if (files.empty()) {
    stats.ok = false;
    stats.error = "no chunk files under " + options.root;
    if (out != nullptr) {
      *out = stats;
    }
    return false;
  }

  FixedArena arena;
  if (!arena.Open(options.arena_gb, options.queue_blocks)) {
    stats.ok = false;
    stats.error = "arena open failed";
    if (out != nullptr) {
      *out = stats;
    }
    return false;
  }
  stats.arena_peak_bytes = arena.size();

  const auto t1 = std::chrono::steady_clock::now();
  TypedChunkQueue<Item> queue(options.queue_blocks);
  std::atomic<size_t> next_file{0};
  std::atomic<uint32_t> active_loaders{0};
  std::atomic<bool> abort_load{false};
  std::mutex stats_mu;
  std::mutex order_mu;
  std::condition_variable order_cv;
  size_t next_to_push = 0;
  std::string first_error;

  const uint32_t loader_threads = std::max<uint32_t>(1, options.threads);
  std::vector<std::thread> threads;
  threads.reserve(loader_threads);
  for (uint32_t tid = 0; tid < loader_threads; ++tid) {
    threads.emplace_back([&, tid]() {
      SetCurrentThreadAffinityRange(options.loader_cpu_base + tid, 1);
      active_loaders.fetch_add(1, std::memory_order_relaxed);
      while (true) {
        if (abort_load.load(std::memory_order_acquire)) {
          break;
        }
        const size_t index = next_file.fetch_add(1, std::memory_order_relaxed);
        if (index >= files.size()) {
          break;
        }
        uint32_t block_id = std::numeric_limits<uint32_t>::max();
        std::pmr::memory_resource* mr = arena.Acquire(&block_id);
        if (mr == nullptr) {
          break;
        }

        Chunk raw(mr);
        raw.arena_block_id = block_id;
        raw.memory_resource = mr;
        std::string error;
        if (!ReadChunkFile(files[index], &raw, &error, mr)) {
          arena.Release(block_id);
          std::lock_guard<std::mutex> lock(stats_mu);
          if (first_error.empty()) {
            first_error = error;
          }
          abort_load.store(true, std::memory_order_release);
          order_cv.notify_all();
          break;
        }

        Item item(mr);
        item.arena_block_id = block_id;
        if (!transform(&raw, mr, &item, &error)) {
          arena.Release(block_id);
          std::lock_guard<std::mutex> lock(stats_mu);
          if (first_error.empty()) {
            first_error = error.empty() ? "transform failed: " + files[index].string()
                                        : error;
          }
          abort_load.store(true, std::memory_order_release);
          order_cv.notify_all();
          break;
        }
        if (item.record_count == 0) {
          item.record_count = raw.record_count;
        }
        {
          std::lock_guard<std::mutex> lock(stats_mu);
          stats.bytes += std::filesystem::file_size(files[index]);
        }
        {
          std::unique_lock<std::mutex> lock(order_mu);
          order_cv.wait(lock, [&]() {
            return abort_load.load(std::memory_order_acquire) ||
                   index == next_to_push;
          });
          if (abort_load.load(std::memory_order_acquire)) {
            arena.Release(block_id);
            break;
          }
        }
        if (!queue.Push(block_id, std::move(item), &stats)) {
          arena.Release(block_id);
          break;
        }
        {
          std::lock_guard<std::mutex> lock(order_mu);
          ++next_to_push;
        }
        order_cv.notify_all();
      }
      if (active_loaders.fetch_sub(1, std::memory_order_acq_rel) == 1) {
        queue.Close();
      }
    });
  }

  const uint32_t prefill_target =
      std::min<uint32_t>(options.prefill_blocks, options.queue_blocks);
  stats.prefill_target_blocks = prefill_target;
  if (prefill_target > 0) {
    const auto prefill_t1 = std::chrono::steady_clock::now();
    stats.prefill_ready_blocks =
        static_cast<uint32_t>(queue.WaitForPrefill(prefill_target));
    const auto prefill_t2 = std::chrono::steady_clock::now();
    stats.prefill_wait_sec =
        std::chrono::duration_cast<std::chrono::duration<double>>(
            prefill_t2 - prefill_t1)
            .count();
  }

  SetCurrentThreadAffinityRange(options.db_cpu_base, 16);
  std::optional<Item> item;
  uint32_t release_id = std::numeric_limits<uint32_t>::max();
  while (queue.Pop(&item, &release_id, &stats)) {
    stats.chunks += 1;
    stats.records += item->record_count;
    if (options.block_records > 0 &&
        item->record_count > options.block_records) {
      std::lock_guard<std::mutex> lock(stats_mu);
      if (first_error.empty()) {
        first_error = "chunk exceeds block_records: " + item->path.string();
      }
    }
    if (!consumer(&*item)) {
      std::lock_guard<std::mutex> lock(stats_mu);
      if (first_error.empty()) {
        first_error = "consumer failed: " + item->path.string();
      }
      queue.Close();
      item.reset();
      arena.Release(release_id);
      break;
    }
    item.reset();
    arena.Release(release_id);
  }

  for (auto& thread : threads) {
    thread.join();
  }
  const auto t2 = std::chrono::steady_clock::now();
  stats.load_sec =
      std::chrono::duration_cast<std::chrono::duration<double>>(t2 - t1)
          .count();
  if (!first_error.empty()) {
    stats.ok = false;
    stats.error = first_error;
  }
  stats.arena_peak_bytes = arena.max_used_bytes();
  if (out != nullptr) {
    *out = stats;
  }
  return stats.ok;
}

inline bool RunPreprocessedLoader(const LoaderOptions& options,
                                  const ChunkConsumer& consumer,
                                  LoaderStats* out) {
  LoaderStats stats;
  const auto files = ListChunkFiles(options.root,
                                    options.skip_node_update,
                                    options.skip_edge_update);
  if (files.empty()) {
    stats.ok = false;
    stats.error = "no chunk files under " + options.root;
    if (out != nullptr) {
      *out = stats;
    }
    return false;
  }

  FixedArena arena;
  if (!arena.Open(options.arena_gb, options.queue_blocks)) {
    stats.ok = false;
    stats.error = "arena open failed";
    if (out != nullptr) {
      *out = stats;
    }
    return false;
  }
  stats.arena_peak_bytes = arena.size();

  const auto t1 = std::chrono::steady_clock::now();
  ChunkQueue queue(options.queue_blocks);
  std::atomic<size_t> next_file{0};
  std::atomic<uint32_t> active_loaders{0};
  std::atomic<bool> abort_load{false};
  std::mutex stats_mu;
  std::mutex order_mu;
  std::condition_variable order_cv;
  size_t next_to_push = 0;
  std::string first_error;

  const uint32_t loader_threads = std::max<uint32_t>(1, options.threads);
  std::vector<std::thread> threads;
  threads.reserve(loader_threads);
  for (uint32_t tid = 0; tid < loader_threads; ++tid) {
    threads.emplace_back([&, tid]() {
      SetCurrentThreadAffinityRange(options.loader_cpu_base + tid, 1);
      active_loaders.fetch_add(1, std::memory_order_relaxed);
      while (true) {
        if (abort_load.load(std::memory_order_acquire)) {
          break;
        }
        const size_t index = next_file.fetch_add(1, std::memory_order_relaxed);
        if (index >= files.size()) {
          break;
        }
        uint32_t block_id = std::numeric_limits<uint32_t>::max();
        std::pmr::memory_resource* mr = arena.Acquire(&block_id);
        if (mr == nullptr) {
          break;
        }
        Chunk chunk(mr);
        chunk.arena_block_id = block_id;
        std::string error;
        if (!ReadChunkFile(files[index], &chunk, &error, mr)) {
          arena.Release(block_id);
          std::lock_guard<std::mutex> lock(stats_mu);
          if (first_error.empty()) {
            first_error = error;
          }
          abort_load.store(true, std::memory_order_release);
          order_cv.notify_all();
          break;
        }
        chunk.arena_block_id = block_id;
        chunk.memory_resource = mr;
        {
          std::lock_guard<std::mutex> lock(stats_mu);
          stats.bytes += std::filesystem::file_size(files[index]);
        }
        {
          std::unique_lock<std::mutex> lock(order_mu);
          order_cv.wait(lock, [&]() {
            return abort_load.load(std::memory_order_acquire) ||
                   index == next_to_push;
          });
          if (abort_load.load(std::memory_order_acquire)) {
            const uint32_t release_id = chunk.arena_block_id;
            chunk = Chunk();
            arena.Release(release_id);
            break;
          }
        }
        if (!queue.Push(std::move(chunk), &stats)) {
          arena.Release(block_id);
          break;
        }
        {
          std::lock_guard<std::mutex> lock(order_mu);
          ++next_to_push;
        }
        order_cv.notify_all();
      }
      if (active_loaders.fetch_sub(1, std::memory_order_acq_rel) == 1) {
        queue.Close();
      }
    });
  }

  const uint32_t prefill_target =
      std::min<uint32_t>(options.prefill_blocks, options.queue_blocks);
  stats.prefill_target_blocks = prefill_target;
  if (prefill_target > 0) {
    const auto prefill_t1 = std::chrono::steady_clock::now();
    stats.prefill_ready_blocks =
        static_cast<uint32_t>(queue.WaitForPrefill(prefill_target));
    const auto prefill_t2 = std::chrono::steady_clock::now();
    stats.prefill_wait_sec =
        std::chrono::duration_cast<std::chrono::duration<double>>(
            prefill_t2 - prefill_t1)
            .count();
  }

  SetCurrentThreadAffinityRange(options.db_cpu_base, 16);
  Chunk chunk;
  while (queue.Pop(&chunk, &stats)) {
    stats.chunks += 1;
    stats.records += chunk.record_count;
    if (options.block_records > 0 &&
        chunk.record_count > options.block_records) {
      std::lock_guard<std::mutex> lock(stats_mu);
      if (first_error.empty()) {
        first_error = "chunk exceeds block_records: " + chunk.path.string();
      }
    }
    if (consumer && !consumer(&chunk)) {
      std::lock_guard<std::mutex> lock(stats_mu);
      if (first_error.empty()) {
        first_error = "consumer failed: " + chunk.path.string();
      }
      queue.Close();
      const uint32_t release_id = chunk.arena_block_id;
      chunk = Chunk();
      arena.Release(release_id);
      break;
    }
    const uint32_t release_id = chunk.arena_block_id;
    chunk = Chunk();
    arena.Release(release_id);
  }

  for (auto& thread : threads) {
    thread.join();
  }
  const auto t2 = std::chrono::steady_clock::now();
  stats.load_sec =
      std::chrono::duration_cast<std::chrono::duration<double>>(t2 - t1)
          .count();
  if (!first_error.empty()) {
    stats.ok = false;
    stats.error = first_error;
  }
  stats.arena_peak_bytes = arena.max_used_bytes();
  if (out != nullptr) {
    *out = stats;
  }
  return stats.ok;
}

inline bool RunPreprocessedLoaderSmoke(const LoaderOptions& options,
                                       LoaderStats* out) {
  return RunPreprocessedLoader(options, ChunkConsumer{}, out);
}

inline void PrintLoaderStats(const LoaderStats& stats) {
  std::cout << "[PREPROCESSED_LOADER] ok: "
            << (stats.ok ? "true" : "false") << std::endl;
  std::cout << "[PREPROCESSED_LOADER] chunks: " << stats.chunks << std::endl;
  std::cout << "[PREPROCESSED_LOADER] records: " << stats.records
            << std::endl;
  std::cout << "[PREPROCESSED_LOADER] bytes: " << stats.bytes << std::endl;
  std::cout << "[PREPROCESSED_LOADER] time(s): " << stats.load_sec
            << std::endl;
  std::cout << "[PREPROCESSED_LOADER] qps(record/s): "
            << (stats.load_sec <= 0.0
                    ? 0.0
                    : static_cast<double>(stats.records) / stats.load_sec)
            << std::endl;
  std::cout << "[PREPROCESSED_LOADER] arena_peak_bytes: "
            << stats.arena_peak_bytes << std::endl;
  std::cout << "[PREPROCESSED_LOADER] prefill_target_blocks: "
            << stats.prefill_target_blocks << std::endl;
  std::cout << "[PREPROCESSED_LOADER] prefill_ready_blocks: "
            << stats.prefill_ready_blocks << std::endl;
  std::cout << "[PREPROCESSED_LOADER] prefill_wait_time(s): "
            << stats.prefill_wait_sec << std::endl;
  std::cout << "[LOADER_STALL] loader_wait_count: "
            << stats.loader_wait_count << std::endl;
  std::cout << "[LOADER_STALL] loader_wait_time(s): "
            << stats.loader_wait_sec << std::endl;
  std::cout << "[LOADER_STALL] db_wait_count: " << stats.db_wait_count
            << std::endl;
  std::cout << "[LOADER_STALL] db_wait_time(s): " << stats.db_wait_sec
            << std::endl;
  if (!stats.error.empty()) {
    std::cout << "[PREPROCESSED_LOADER] error: " << stats.error
              << std::endl;
  }
}

}  // namespace prechunk
