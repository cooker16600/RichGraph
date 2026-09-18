#include "richgraph/graph_db.h"

#include "core/flags.h"
#include "core/fixed_property_layout.h"
#include "core/lsmstore.h"
#include "core/property_update_manager.h"

#include <algorithm>
#include <cctype>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <sstream>
#include <unordered_set>

namespace lsmgraph {

GraphDb::GraphDb() = default;

namespace {

std::string Trim(const std::string& s) {
  size_t l = 0;
  while (l < s.size() && std::isspace(static_cast<unsigned char>(s[l]))) {
    ++l;
  }
  size_t r = s.size();
  while (r > l && std::isspace(static_cast<unsigned char>(s[r - 1]))) {
    --r;
  }
  return s.substr(l, r - l);
}

std::string StripComment(const std::string& line) {
  const size_t p = line.find('#');
  if (p == std::string::npos) {
    return line;
  }
  return line.substr(0, p);
}

bool StartsWith(const std::string& s, const std::string& prefix) {
  return s.size() >= prefix.size() && s.compare(0, prefix.size(), prefix) == 0;
}

std::string Unquote(const std::string& s) {
  if (s.size() >= 2) {
    const char a = s.front();
    const char b = s.back();
    if ((a == '"' && b == '"') || (a == '\'' && b == '\'')) {
      return s.substr(1, s.size() - 2);
    }
  }
  return s;
}

bool ParseBool(const std::string& raw, bool* out) {
  if (out == nullptr) {
    return false;
  }
  const std::string s = Trim(raw);
  if (s == "1" || s == "true" || s == "True" || s == "TRUE") {
    *out = true;
    return true;
  }
  if (s == "0" || s == "false" || s == "False" || s == "FALSE") {
    *out = false;
    return true;
  }
  return false;
}

bool ParseUint64(const std::string& raw, uint64_t* out) {
  if (out == nullptr) {
    return false;
  }
  try {
    const std::string s = Trim(raw);
    if (s.empty()) {
      return false;
    }
    *out = static_cast<uint64_t>(std::stoull(s));
    return true;
  } catch (...) {
    return false;
  }
}

bool ParseUint32(const std::string& raw, uint32_t* out) {
  if (out == nullptr) {
    return false;
  }
  uint64_t tmp = 0;
  if (!ParseUint64(raw, &tmp)) {
    return false;
  }
  if (tmp > static_cast<uint64_t>(std::numeric_limits<uint32_t>::max())) {
    return false;
  }
  *out = static_cast<uint32_t>(tmp);
  return true;
}

bool ParseInt(const std::string& raw, int* out) {
  if (out == nullptr) {
    return false;
  }
  try {
    const std::string s = Trim(raw);
    if (s.empty()) {
      return false;
    }
    *out = std::stoi(s);
    return true;
  } catch (...) {
    return false;
  }
}

// 解析一行内的字符串列表：
// - 支持 [A, B, C]
// - 也支持 A, B, C
// - 元素允许带单双引号。
bool ParseInlineList(const std::string& raw, std::vector<std::string>* out) {
  if (out == nullptr) {
    return false;
  }
  std::string s = Trim(raw);
  if (s.empty()) {
    out->clear();
    return true;
  }
  if (s.front() == '[' && s.back() == ']') {
    s = Trim(s.substr(1, s.size() - 2));
  }

  out->clear();
  std::stringstream ss(s);
  std::string token;
  while (std::getline(ss, token, ',')) {
    token = Unquote(Trim(token));
    if (!token.empty()) {
      out->push_back(token);
    }
  }

  // 兼容只有一个元素且不带逗号的情况。
  if (out->empty()) {
    token = Unquote(Trim(s));
    if (!token.empty()) {
      out->push_back(token);
    }
  }
  return true;
}

bool ParseKeyValue(const std::string& line,
                   std::string* key,
                   std::string* value) {
  const size_t p = line.find(':');
  if (p == std::string::npos) {
    return false;
  }
  if (key != nullptr) {
    *key = Trim(line.substr(0, p));
  }
  if (value != nullptr) {
    *value = Trim(line.substr(p + 1));
  }
  return true;
}

struct PropertyVersionKey {
  VertexId_t dst{0};
  SequenceNumber_t sequence{0};

  bool operator==(const PropertyVersionKey& other) const {
    return dst == other.dst && sequence == other.sequence;
  }
};

struct PropertyVersionKeyHash {
  std::size_t operator()(const PropertyVersionKey& key) const {
    std::size_t hash = std::hash<VertexId_t>{}(key.dst);
    hash ^= std::hash<SequenceNumber_t>{}(key.sequence) +
            0x9e3779b97f4a7c15ULL + (hash << 6U) + (hash >> 2U);
    return hash;
  }
};

using BufferedPropertySnapshot =
    std::unordered_map<PropertyVersionKey,
                       std::string,
                       PropertyVersionKeyHash>;

BufferedPropertySnapshot CaptureBufferedProperties(
    const PropertyUpdateManager* manager,
    PropertyObjectKind kind,
    std::uint32_t shard_id,
    std::uint32_t property_id,
    VertexId_t src,
    bool is_out,
    std::uint8_t edge_type) {
  BufferedPropertySnapshot snapshot;
  if (manager == nullptr) return snapshot;
  for (auto& item : manager->SnapshotForSource(kind,
                                                shard_id,
                                                property_id,
                                                src,
                                                is_out,
                                                edge_type)) {
    snapshot.emplace(PropertyVersionKey{item.dst, item.base_sequence},
                     std::move(item.value));
  }
  return snapshot;
}

std::string ResolveBufferedProperty(const BufferedPropertySnapshot& snapshot,
                                    VertexId_t dst,
                                    SequenceNumber_t sequence,
                                    std::string base_value) {
  const auto it = snapshot.find(PropertyVersionKey{dst, sequence});
  return it == snapshot.end() ? std::move(base_value) : it->second;
}

}  // namespace

Status GraphDb::LoadSchemaFromYaml(const std::string &schema_path,
                                   Schema *out_schema, std::string *err_msg) {
  if (out_schema == nullptr) {
    if (err_msg != nullptr) {
      *err_msg = "out_schema is null";
    }
    return Status::kInvalidArgument;
  }

  std::ifstream in(schema_path);
  if (!in.is_open()) {
    if (err_msg != nullptr) {
      *err_msg = "open schema file failed: " + schema_path;
    }
    return Status::kIOError;
  }

  Schema schema;
  enum class Section {
    kRoot,
    kEdgeShards,
    kNodeDb,
    kPropertyDefs,
  };
  Section section = Section::kRoot;

  EdgeShardSchema curr_edge_shard;
  bool edge_shard_open = false;

  PropertyDef curr_prop_def;
  bool prop_def_open = false;

  auto flush_edge_shard = [&]() {
    if (!edge_shard_open) {
      return;
    }
    if (curr_edge_shard.name.empty()) {
      curr_edge_shard.name =
          "edge_Db" + std::to_string(schema.edge_shards.size());
    }
    schema.edge_shards.push_back(curr_edge_shard);
    curr_edge_shard = EdgeShardSchema{};
    edge_shard_open = false;
  };

  auto flush_prop_def = [&]() {
    if (!prop_def_open) {
      return;
    }
    if (!curr_prop_def.name.empty()) {
      schema.property_defs.push_back(curr_prop_def);
    }
    curr_prop_def = PropertyDef{};
    prop_def_open = false;
  };

  bool has_max_vertex_num = false;

  std::string raw_line;
  size_t line_no = 0;
  while (std::getline(in, raw_line)) {
    ++line_no;
    const std::string no_comment = StripComment(raw_line);
    const std::string line = Trim(no_comment);
    if (line.empty()) {
      continue;
    }

    if (line == "edge_shards:") {
      flush_prop_def();
      flush_edge_shard();
      section = Section::kEdgeShards;
      continue;
    }
    if (line == "node_db:") {
      flush_prop_def();
      flush_edge_shard();
      section = Section::kNodeDb;
      continue;
    }
    if (line == "property_defs:") {
      flush_edge_shard();
      flush_prop_def();
      section = Section::kPropertyDefs;
      continue;
    }

    // 根级通用字段：无论当前 section 在哪，都允许直接覆写。
    {
      std::string key;
      std::string value;
      if (ParseKeyValue(line, &key, &value)) {
        if (key == "max_vertex_num") {
          if (!ParseUint64(value, &schema.max_vertex_num)) {
            if (err_msg != nullptr) {
              *err_msg =
                  "invalid max_vertex_num at line " + std::to_string(line_no);
            }
            return Status::kInvalidArgument;
          }
          has_max_vertex_num = true;
          continue;
        }
        if (key == "use_csr_disk") {
          if (!ParseBool(value, &schema.use_csr_disk)) {
            if (err_msg != nullptr) {
              *err_msg =
                  "invalid use_csr_disk at line " + std::to_string(line_no);
            }
            return Status::kInvalidArgument;
          }
          continue;
        }
        if (key == "sub_property_num") {
          if (!ParseUint32(value, &schema.sub_property_num)) {
            if (err_msg != nullptr) {
              *err_msg =
                  "invalid sub_property_num at line " + std::to_string(line_no);
            }
            return Status::kInvalidArgument;
          }
          continue;
        }
        if (key == "max_property_length") {
          if (!ParseUint32(value, &schema.max_property_length)) {
            if (err_msg != nullptr) {
              *err_msg = "invalid max_property_length at line " +
                         std::to_string(line_no);
            }
            return Status::kInvalidArgument;
          }
          continue;
        }
        if (key == "system_threads") {
          if (!ParseInt(value, &schema.system_threads) ||
              schema.system_threads <= 0) {
            if (err_msg != nullptr) {
              *err_msg =
                  "invalid system_threads at line " + std::to_string(line_no);
            }
            return Status::kInvalidArgument;
          }
          continue;
        }
      }
    }

    if (section == Section::kEdgeShards) {
      if (StartsWith(line, "-")) {
        // 新 shard 开始，先冲刷上一个。
        flush_edge_shard();
        edge_shard_open = true;
        curr_edge_shard = EdgeShardSchema{};

        const std::string remain = Trim(line.substr(1));
        if (remain.empty()) {
          continue;
        }

        std::string key;
        std::string value;
        if (!ParseKeyValue(remain, &key, &value)) {
          if (err_msg != nullptr) {
            *err_msg =
                "invalid edge_shards item at line " + std::to_string(line_no);
          }
          return Status::kInvalidArgument;
        }
        if (key == "name") {
          curr_edge_shard.name = Unquote(value);
        } else if (key == "properties") {
          if (!ParseInlineList(value, &curr_edge_shard.properties)) {
            if (err_msg != nullptr) {
              *err_msg = "invalid edge_shard properties at line " +
                         std::to_string(line_no);
            }
            return Status::kInvalidArgument;
          }
        } else if (key == "memtable_size") {
          if (!ParseUint32(value, &curr_edge_shard.memtable_size)) {
            if (err_msg != nullptr) {
              *err_msg = "invalid edge_shard memtable_size at line " +
                         std::to_string(line_no);
            }
            return Status::kInvalidArgument;
          }
        } else if (key == "is_csr") {
          if (!ParseBool(value, &curr_edge_shard.is_csr)) {
            if (err_msg != nullptr) {
              *err_msg = "invalid edge_shard is_csr at line " +
                         std::to_string(line_no);
            }
            return Status::kInvalidArgument;
          }
        } else if (key == "csr_l0_max_sst_num") {
          if (!ParseUint32(value, &curr_edge_shard.csr_l0_max_sst_num)) {
            if (err_msg != nullptr) {
              *err_msg = "invalid edge_shard csr_l0_max_sst_num at line " +
                         std::to_string(line_no);
            }
            return Status::kInvalidArgument;
          }
        } else if (key == "csr_l1_max_sst_num") {
          if (!ParseUint32(value, &curr_edge_shard.csr_l1_max_sst_num)) {
            if (err_msg != nullptr) {
              *err_msg = "invalid edge_shard csr_l1_max_sst_num at line " +
                         std::to_string(line_no);
            }
            return Status::kInvalidArgument;
          }
        } else {
          if (err_msg != nullptr) {
            *err_msg = "unknown edge_shard key at line " +
                       std::to_string(line_no) + ": " + key;
          }
          return Status::kInvalidArgument;
        }
        continue;
      }

      if (!edge_shard_open) {
        edge_shard_open = true;
        curr_edge_shard = EdgeShardSchema{};
      }

      std::string key;
      std::string value;
      if (!ParseKeyValue(line, &key, &value)) {
        if (err_msg != nullptr) {
          *err_msg = "invalid edge_shards line " + std::to_string(line_no);
        }
        return Status::kInvalidArgument;
      }
      if (key == "name") {
        curr_edge_shard.name = Unquote(value);
      } else if (key == "properties") {
        if (!ParseInlineList(value, &curr_edge_shard.properties)) {
          if (err_msg != nullptr) {
            *err_msg = "invalid edge_shard properties at line " +
                       std::to_string(line_no);
          }
          return Status::kInvalidArgument;
        }
      } else if (key == "memtable_size") {
        if (!ParseUint32(value, &curr_edge_shard.memtable_size)) {
          if (err_msg != nullptr) {
            *err_msg = "invalid edge_shard memtable_size at line " +
                       std::to_string(line_no);
          }
          return Status::kInvalidArgument;
        }
      } else if (key == "is_csr") {
        if (!ParseBool(value, &curr_edge_shard.is_csr)) {
          if (err_msg != nullptr) {
            *err_msg =
                "invalid edge_shard is_csr at line " + std::to_string(line_no);
          }
          return Status::kInvalidArgument;
        }
      } else if (key == "csr_l0_max_sst_num") {
        if (!ParseUint32(value, &curr_edge_shard.csr_l0_max_sst_num)) {
          if (err_msg != nullptr) {
            *err_msg = "invalid edge_shard csr_l0_max_sst_num at line " +
                       std::to_string(line_no);
          }
          return Status::kInvalidArgument;
        }
      } else if (key == "csr_l1_max_sst_num") {
        if (!ParseUint32(value, &curr_edge_shard.csr_l1_max_sst_num)) {
          if (err_msg != nullptr) {
            *err_msg = "invalid edge_shard csr_l1_max_sst_num at line " +
                       std::to_string(line_no);
          }
          return Status::kInvalidArgument;
        }
      } else {
        if (err_msg != nullptr) {
          *err_msg = "unknown edge_shard key at line " +
                     std::to_string(line_no) + ": " + key;
        }
        return Status::kInvalidArgument;
      }
      continue;
    }

    if (section == Section::kNodeDb) {
      std::string key;
      std::string value;
      if (!ParseKeyValue(line, &key, &value)) {
        if (err_msg != nullptr) {
          *err_msg = "invalid node_db line " + std::to_string(line_no);
        }
        return Status::kInvalidArgument;
      }
      if (key == "name") {
        schema.node_db.name = Unquote(value);
      } else if (key == "properties") {
        if (!ParseInlineList(value, &schema.node_db.properties)) {
          if (err_msg != nullptr) {
            *err_msg =
                "invalid node_db properties at line " + std::to_string(line_no);
          }
          return Status::kInvalidArgument;
        }
      } else if (key == "memtable_size") {
        if (!ParseUint32(value, &schema.node_db.memtable_size)) {
          if (err_msg != nullptr) {
            *err_msg = "invalid node_db memtable_size at line " +
                       std::to_string(line_no);
          }
          return Status::kInvalidArgument;
        }
      } else if (key == "is_csr") {
        if (!ParseBool(value, &schema.node_db.is_csr)) {
          if (err_msg != nullptr) {
            *err_msg =
                "invalid node_db is_csr at line " + std::to_string(line_no);
          }
          return Status::kInvalidArgument;
        }
      } else if (key == "csr_l0_max_sst_num") {
        if (!ParseUint32(value, &schema.node_db.csr_l0_max_sst_num)) {
          if (err_msg != nullptr) {
            *err_msg = "invalid node_db csr_l0_max_sst_num at line " +
                       std::to_string(line_no);
          }
          return Status::kInvalidArgument;
        }
      } else if (key == "csr_l1_max_sst_num") {
        if (!ParseUint32(value, &schema.node_db.csr_l1_max_sst_num)) {
          if (err_msg != nullptr) {
            *err_msg = "invalid node_db csr_l1_max_sst_num at line " +
                       std::to_string(line_no);
          }
          return Status::kInvalidArgument;
        }
      } else {
        if (err_msg != nullptr) {
          *err_msg = "unknown node_db key at line " + std::to_string(line_no) +
                     ": " + key;
        }
        return Status::kInvalidArgument;
      }
      continue;
    }

    if (section == Section::kPropertyDefs) {
      if (StartsWith(line, "-")) {
        flush_prop_def();
        prop_def_open = true;
        curr_prop_def = PropertyDef{};

        const std::string remain = Trim(line.substr(1));
        if (remain.empty()) {
          continue;
        }
        std::string key;
        std::string value;
        if (!ParseKeyValue(remain, &key, &value)) {
          if (err_msg != nullptr) {
            *err_msg =
                "invalid property_defs item at line " + std::to_string(line_no);
          }
          return Status::kInvalidArgument;
        }
        if (key == "name") {
          curr_prop_def.name = Unquote(value);
        } else if (key == "length") {
          if (!ParseUint32(value, &curr_prop_def.length)) {
            if (err_msg != nullptr) {
              *err_msg =
                  "invalid property length at line " + std::to_string(line_no);
            }
            return Status::kInvalidArgument;
          }
        } else {
          if (err_msg != nullptr) {
            *err_msg = "unknown property_defs key at line " +
                       std::to_string(line_no) + ": " + key;
          }
          return Status::kInvalidArgument;
        }
        continue;
      }

      if (!prop_def_open) {
        prop_def_open = true;
        curr_prop_def = PropertyDef{};
      }

      std::string key;
      std::string value;
      if (!ParseKeyValue(line, &key, &value)) {
        if (err_msg != nullptr) {
          *err_msg = "invalid property_defs line " + std::to_string(line_no);
        }
        return Status::kInvalidArgument;
      }
      if (key == "name") {
        curr_prop_def.name = Unquote(value);
      } else if (key == "length") {
        if (!ParseUint32(value, &curr_prop_def.length)) {
          if (err_msg != nullptr) {
            *err_msg =
                "invalid property length at line " + std::to_string(line_no);
          }
          return Status::kInvalidArgument;
        }
      } else {
        if (err_msg != nullptr) {
          *err_msg = "unknown property_defs key at line " +
                     std::to_string(line_no) + ": " + key;
        }
        return Status::kInvalidArgument;
      }
      continue;
    }

    if (err_msg != nullptr) {
      *err_msg =
          "unknown or misplaced line " + std::to_string(line_no) + ": " + line;
    }
    return Status::kInvalidArgument;
  }

  flush_edge_shard();
  flush_prop_def();

  if (!has_max_vertex_num || schema.max_vertex_num == 0) {
    if (err_msg != nullptr) {
      *err_msg = "max_vertex_num is required and must be > 0";
    }
    return Status::kInvalidArgument;
  }
  if (schema.edge_shards.empty()) {
    if (err_msg != nullptr) {
      *err_msg = "edge_shards must not be empty";
    }
    return Status::kInvalidArgument;
  }
  if (schema.node_db.name.empty()) {
    schema.node_db.name = "node_Db";
  }
  if (schema.system_threads <= 0) {
    schema.system_threads = 1;
  }

  // Default to the largest shard width. The storage layer still exposes this
  // value through one process-wide flag, so every shard must share it.
  if (schema.sub_property_num == 0) {
    size_t max_cols = schema.node_db.properties.size();
    for (const auto& shard : schema.edge_shards) {
      max_cols = std::max(max_cols, shard.properties.size());
    }
    if (max_cols == 0) {
      max_cols = 1;
    }
    schema.sub_property_num = static_cast<uint32_t>(max_cols);
  }

  *out_schema = schema;
  return Status::kOk;
}

Status GraphDb::OpenFromYaml(const std::string &base_dir,
                             const std::string &schema_path, GraphDb **out_db,
                             std::string *error) {
  return OpenFromYaml(base_dir, schema_path, LegacyOptionsFromFlags(), out_db,
                      error);
}

GraphDbOptions GraphDb::LegacyOptionsFromFlags() {
  GraphDbOptions options;
  options.memtable_count = FLAGS_memtable_num;
  options.default_memtable_capacity = FLAGS_memtable_size;
  options.max_subcompactions = FLAGS_max_subcompactions;
  options.support_multi_version = FLAGS_support_mulversion;
  options.load_existing = FLAGS_LOAD_OLD_DATA;
  options.cache_sst_data = FLAGS_OPEN_SSTDATA_CACHE;
  options.verbose_logging = FLAGS_richgraph_verbose;
  options.mmap_path = FLAGS_mmap_path;
  options.property_updates.enabled = FLAGS_enable_memproperty;
  options.property_updates.buffer_count = FLAGS_memproperty_num;
  options.property_updates.buffer_capacity_records = FLAGS_memproperty_size;
  return options;
}

Status GraphDb::OpenFromYaml(const std::string &base_dir,
                             const std::string &schema_path,
                             const GraphDbOptions &options, GraphDb **out_db,
                             std::string *error) {
  if (out_db == nullptr) {
    if (error != nullptr) {
      *error = "out_db is null";
    }
    return Status::kInvalidArgument;
  }
  *out_db = nullptr;

  std::unique_ptr<GraphDb> db;
  const Status status =
      OpenFromYaml(base_dir, schema_path, options, &db, error);
  if (status == Status::kOk) {
    *out_db = db.release();
  }
  return status;
}

Status GraphDb::OpenFromYaml(const std::string &base_dir,
                             const std::string &schema_path,
                             const GraphDbOptions &options,
                             std::unique_ptr<GraphDb> *out_db,
                             std::string *error) {
  if (out_db == nullptr) {
    if (error != nullptr) {
      *error = "out_db is null";
    }
    return Status::kInvalidArgument;
  }
  out_db->reset();

  const std::string options_error = options.Validate();
  if (!options_error.empty()) {
    if (error != nullptr) {
      *error = options_error;
    }
    return Status::kInvalidArgument;
  }

  Schema schema;
  std::string err_msg;
  Status rs = LoadSchemaFromYaml(schema_path, &schema, &err_msg);
  if (rs != Status::kOk) {
    if (error != nullptr) {
      *error = err_msg;
    }
    return rs;
  }

  // 底层当前使用 gflags 全局参数控制属性列数与 CSR 模式，
  // 因此在创建所有 shard 前统一设置一次。
  FLAGS_sub_property_num = schema.sub_property_num;
  FLAGS_max_property_length = schema.max_property_length;
  FLAGS_use_csr_disk = schema.use_csr_disk;
  // Transitional compatibility bridge. LSMStore/MemTable still contain legacy
  // flag reads; later milestones replace those reads with the stored options.
  FLAGS_memtable_num = options.memtable_count;
  FLAGS_memtable_size =
      static_cast<uint32_t>(options.default_memtable_capacity);
  FLAGS_max_subcompactions = options.max_subcompactions;
  FLAGS_support_mulversion = options.support_multi_version;
  FLAGS_LOAD_OLD_DATA = options.load_existing;
  FLAGS_OPEN_SSTDATA_CACHE = options.cache_sst_data;
  FLAGS_richgraph_verbose = options.verbose_logging;
  FLAGS_mmap_path = options.mmap_path;
  auto db = std::unique_ptr<GraphDb>(new GraphDb());
  rs = db->Init(base_dir, schema, options, &err_msg);
  if (rs != Status::kOk) {
    if (error != nullptr) {
      *error = err_msg;
    }
    return rs;
  }

  *out_db = std::move(db);
  return Status::kOk;
}

Status GraphDb::Init(const std::string &base_dir, const Schema &schema,
                     const GraphDbOptions &options, std::string *err_msg) {
  schema_ = schema;
  options_ = options;
  base_dir_ = base_dir;

  std::error_code ec;
  std::filesystem::create_directories(base_dir_, ec);
  if (ec) {
    if (err_msg != nullptr) {
      *err_msg = "create base dir failed: " + base_dir_ + ", " + ec.message();
    }
    return Status::kIOError;
  }

  edge_shards_.clear();
  edge_property_to_route_.clear();
  node_property_to_route_.clear();

  std::unordered_map<std::string, uint32_t> property_length_by_name;
  for (const auto& def : schema_.property_defs) {
    if (!def.name.empty()) {
      property_length_by_name[def.name] = std::max<uint32_t>(1, def.length);
    }
  }
  const auto resolve_property_length = [&](const std::string& property_name) {
    const auto it = property_length_by_name.find(property_name);
    if (it != property_length_by_name.end()) {
      return it->second;
    }
    return std::max<uint32_t>(1, schema_.max_property_length);
  };
  const auto resolve_memtable_size = [&](uint32_t shard_memtable_size) {
    return static_cast<size_t>(
        std::max<uint32_t>(1, shard_memtable_size == 0
                                  ? static_cast<uint32_t>(
                                        options_.default_memtable_capacity)
                                  : shard_memtable_size));
  };

  const int background_threads =
      options_.background_threads == 0
          ? std::max(1, schema_.system_threads)
          : static_cast<int>(options_.background_threads);

  edge_shards_.reserve(schema_.edge_shards.size());
  for (size_t i = 0; i < schema_.edge_shards.size(); ++i) {
    const auto &s = schema_.edge_shards[i];
    EdgeShardHandle handle;
    handle.name = s.name.empty() ? ("edge_Db" + std::to_string(i)) : s.name;
    handle.properties = s.properties;
    handle.property_lengths.reserve(handle.properties.size());

    const std::string shard_dir = base_dir_ + "/" + handle.name;
    std::filesystem::create_directories(shard_dir, ec);
    if (ec) {
      if (err_msg != nullptr) {
        *err_msg =
            "create edge shard dir failed: " + shard_dir + ", " + ec.message();
      }
      return Status::kIOError;
    }

    // 直接构造 LSMStore，便于把线程数显式配置成 schema 值。
    for (size_t pid = 0; pid < handle.properties.size(); ++pid) {
      const std::string &pname = handle.properties[pid];
      if (pname.empty()) {
        if (err_msg != nullptr) {
          *err_msg = "empty property name in edge shard " + handle.name;
        }
        return Status::kInvalidArgument;
      }
      handle.property_lengths.push_back(resolve_property_length(pname));
      handle.property_to_local_id.emplace(pname, static_cast<int>(pid));
      if (edge_property_to_route_.find(pname) !=
          edge_property_to_route_.end()) {
        if (err_msg != nullptr) {
          *err_msg = "duplicate edge property across shards: " + pname;
        }
        return Status::kInvalidArgument;
      }
      edge_property_to_route_[pname] = PropertyRoute{i, static_cast<int>(pid)};
    }

    handle.db = std::make_unique<LSMStore>(
        shard_dir, schema_.max_vertex_num, background_threads,
        static_cast<int>(options_.memtable_count),
        handle.property_lengths, resolve_memtable_size(s.memtable_size),
        s.is_csr, s.csr_l0_max_sst_num, s.csr_l1_max_sst_num,
        PropertyObjectKind::kEdge, static_cast<uint32_t>(i),
        options_.property_updates);

    edge_shards_.push_back(std::move(handle));
  }

  node_shard_ = NodeShardHandle{};
  node_shard_.name =
      schema_.node_db.name.empty() ? "node_Db" : schema_.node_db.name;
  node_shard_.properties = schema_.node_db.properties;

  const std::string node_dir = base_dir_ + "/" + node_shard_.name;
  std::filesystem::create_directories(node_dir, ec);
  if (ec) {
    if (err_msg != nullptr) {
      *err_msg =
          "create node shard dir failed: " + node_dir + ", " + ec.message();
    }
    return Status::kIOError;
  }
  node_shard_.property_lengths.reserve(node_shard_.properties.size());
  for (const auto &pname : node_shard_.properties) {
    node_shard_.property_lengths.push_back(resolve_property_length(pname));
  }
  node_shard_.db = std::make_unique<LSMStore>(
      node_dir, schema_.max_vertex_num, background_threads,
      static_cast<int>(options_.memtable_count),
      node_shard_.property_lengths,
      resolve_memtable_size(schema_.node_db.memtable_size),
      schema_.node_db.is_csr, schema_.node_db.csr_l0_max_sst_num,
      schema_.node_db.csr_l1_max_sst_num, PropertyObjectKind::kNode, 0,
      options_.property_updates);

  for (size_t pid = 0; pid < node_shard_.properties.size(); ++pid) {
    const std::string &pname = node_shard_.properties[pid];
    if (pname.empty()) {
      if (err_msg != nullptr) {
        *err_msg = "empty property name in node shard";
      }
      return Status::kInvalidArgument;
    }
    node_shard_.property_to_local_id.emplace(pname, static_cast<int>(pid));
    node_property_to_route_[pname] = PropertyRoute{0, static_cast<int>(pid)};
  }

  SequenceNumber_t recovered_next_sequence = 0;
  for (const auto& shard : edge_shards_) {
    const auto* store = static_cast<const LSMStore*>(shard.db.get());
    if (store != nullptr) {
      recovered_next_sequence = std::max(
          recovered_next_sequence, store->NextTopologySequence());
    }
  }
  const auto* recovered_node_store =
      static_cast<const LSMStore*>(node_shard_.db.get());
  if (recovered_node_store != nullptr) {
    recovered_next_sequence = std::max(
        recovered_next_sequence,
        recovered_node_store->NextTopologySequence());
  }
  next_sequence_.store(recovered_next_sequence, std::memory_order_release);

  if (options_.property_updates.enabled) {
    PropertyCommitSequence initial_commit_sequence = 1;
    for (const auto& shard : edge_shards_) {
      const auto* store = static_cast<const LSMStore*>(shard.db.get());
      if (store != nullptr) {
        initial_commit_sequence = std::max(
            initial_commit_sequence,
            store->GetMaxPropertyCommitSequence() + 1);
      }
    }
    const auto* node_store =
        static_cast<const LSMStore*>(node_shard_.db.get());
    if (node_store != nullptr) {
      initial_commit_sequence = std::max(
          initial_commit_sequence,
          node_store->GetMaxPropertyCommitSequence() + 1);
    }
    property_update_manager_ = std::make_unique<PropertyUpdateManager>(
        options_.property_updates,
        [this](const PropertyDeltaTarget& target,
               std::vector<PropertyDeltaRecord> records,
               bool target_is_persistent,
               std::string* error) {
          LSMStore* store = nullptr;
          if (target.kind == PropertyObjectKind::kNode) {
            store = static_cast<LSMStore*>(node_shard_.db.get());
          } else if (target.shard_id < edge_shards_.size()) {
            store = static_cast<LSMStore*>(
                edge_shards_[target.shard_id].db.get());
          }
          if (store == nullptr) {
            if (error != nullptr) {
              *error = "property update target shard is unavailable";
            }
            return false;
          }
          const Status status = store->AttachPropertyDelta(
              target.base_file_id,
              static_cast<int>(target.property_id),
              target_is_persistent,
              std::move(records));
          if (status != Status::kOk && error != nullptr) {
            *error = "attach property delta failed for fid=" +
                     std::to_string(target.base_file_id);
          }
          return status == Status::kOk;
        },
        initial_commit_sequence);
  }

  return Status::kOk;
}

VertexId_t GraphDb::NewVertex(bool use_recycled_vertex) {
  VertexId_t expected = INVALID_VERTEX_ID;

  auto alloc_in_db = [&](LSMGraph* db) {
    if (db == nullptr) {
      return;
    }
    const VertexId_t id = db->new_vertex(use_recycled_vertex);
    if (expected == INVALID_VERTEX_ID) {
      expected = id;
    } else if (expected != id) {
      // 理论上所有 shard 必须严格同步分配顶点 id。
      // 这里保留报警，便于排查调用顺序或外部误用。
      std::cerr << "[GraphDb] WARN: shard vertex id diverged, expected="
                << expected << ", got=" << id << std::endl;
    }
  };

  for (auto& shard : edge_shards_) {
    alloc_in_db(shard.db.get());
  }
  alloc_in_db(node_shard_.db.get());

  return expected;
}

SequenceNumber_t GraphDb::NextSequence() {
  return next_sequence_.fetch_add(1, std::memory_order_relaxed);
}

void GraphDb::ObserveSequence(SequenceNumber_t sequence) {
  if (sequence < 0 || sequence == MAX_SEQ_ID) return;
  const SequenceNumber_t candidate = sequence + 1;
  SequenceNumber_t observed = next_sequence_.load(std::memory_order_relaxed);
  while (observed < candidate &&
         !next_sequence_.compare_exchange_weak(
             observed,
             candidate,
             std::memory_order_release,
             std::memory_order_relaxed)) {
  }
}

void GraphDb::InitVerticesUpTo(VertexId_t next_vertex_id) {
  if (next_vertex_id == INVALID_VERTEX_ID) {
    return;
  }
  for (auto& shard : edge_shards_) {
    if (shard.db != nullptr) {
      static_cast<LSMStore*>(shard.db.get())->InitVerticesUpTo(next_vertex_id);
    }
  }
  if (node_shard_.db != nullptr) {
    static_cast<LSMStore*>(node_shard_.db.get())
        ->InitVerticesUpTo(next_vertex_id);
  }
}

std::string GraphDb::BuildShardPropertyString(
    const std::vector<std::string> &local_property_order,
    const std::unordered_map<std::string, std::string> &incoming_properties)
    const {
  std::string out;
  for (size_t i = 0; i < local_property_order.size(); ++i) {
    if (i > 0) {
      out.push_back('|');
    }
    const auto it = incoming_properties.find(local_property_order[i]);
    if (it != incoming_properties.end()) {
      out += it->second;
    }
  }
  return out;
}

Status GraphDb::ResolveEdgePropertyRoutes(
    const std::vector<std::string> &property_names,
    std::vector<ResolvedPropertyRoute> *out_routes,
    std::string *err_msg) const {
  if (out_routes == nullptr) {
    if (err_msg != nullptr) {
      *err_msg = "out_routes is null";
    }
    return Status::kInvalidArgument;
  }
  out_routes->clear();

  std::unordered_set<std::string> seen;
  for (const auto &p : property_names) {
    if (p.empty() || !seen.insert(p).second) {
      continue;
    }
    const auto it = edge_property_to_route_.find(p);
    if (it == edge_property_to_route_.end()) {
      if (err_msg != nullptr) {
        *err_msg = "unknown edge property: " + p;
      }
      return Status::kInvalidArgument;
    }
    out_routes->push_back(ResolvedPropertyRoute{p, it->second});
  }

  if (out_routes->empty()) {
    if (err_msg != nullptr) {
      *err_msg = "empty edge property list";
    }
    return Status::kInvalidArgument;
  }
  return Status::kOk;
}

Status GraphDb::ResolveNodePropertyRoutes(
    const std::vector<std::string> &property_names,
    std::vector<ResolvedPropertyRoute> *out_routes,
    std::string *err_msg) const {
  if (out_routes == nullptr) {
    if (err_msg != nullptr) {
      *err_msg = "out_routes is null";
    }
    return Status::kInvalidArgument;
  }
  out_routes->clear();

  std::unordered_set<std::string> seen;
  for (const auto &p : property_names) {
    if (p.empty() || !seen.insert(p).second) {
      continue;
    }
    const auto it = node_property_to_route_.find(p);
    if (it == node_property_to_route_.end()) {
      if (err_msg != nullptr) {
        *err_msg = "unknown node property: " + p;
      }
      return Status::kInvalidArgument;
    }
    out_routes->push_back(ResolvedPropertyRoute{p, it->second});
  }

  if (out_routes->empty()) {
    if (err_msg != nullptr) {
      *err_msg = "empty node property list";
    }
    return Status::kInvalidArgument;
  }
  return Status::kOk;
}

Status
GraphDb::PutNode(VertexId_t vertex_id,
                 const std::unordered_map<std::string, std::string> &properties,
                 bool is_out, uint8_t edge_type) {
  if (node_shard_.db == nullptr) {
    return Status::kInvalidArgument;
  }

  bool has_any = false;
  for (const auto &kv : properties) {
    if (node_property_to_route_.find(kv.first) ==
        node_property_to_route_.end()) {
      continue;
    }
    has_any = true;
    break;
  }
  if (!has_any) {
    return Status::kInvalidArgument;
  }

  const std::string property_payload =
      BuildShardPropertyString(node_shard_.properties, properties);
  const std::string node_db_path = base_dir_ + "/" + node_shard_.name;
  const ScopedFixedPropertyLayout property_layout(
      &node_shard_.property_lengths);
  const ScopedDbPathOverride db_path_override(&node_db_path);
  node_shard_.db->put_edge(vertex_id, vertex_id, property_payload,
                           EdgeInsertMode::kSingle, is_out, edge_type,
                           NextSequence());
  return Status::kOk;
}

Status GraphDb::PutNodePayload(VertexId_t vertex_id, const std::string &payload,
                               bool is_out, uint8_t edge_type) {
  if (node_shard_.db == nullptr || payload.empty()) {
    return Status::kInvalidArgument;
  }
  const std::string node_db_path = base_dir_ + "/" + node_shard_.name;
  const ScopedFixedPropertyLayout property_layout(
      &node_shard_.property_lengths);
  const ScopedDbPathOverride db_path_override(&node_db_path);
  node_shard_.db->put_edge(vertex_id, vertex_id, payload,
                           EdgeInsertMode::kSingle, is_out, edge_type,
                           NextSequence());
  return Status::kOk;
}

Status
GraphDb::GetNode(VertexId_t vertex_id,
                 const std::vector<std::string> &property_names,
                 std::unordered_map<std::string, std::string> *out_properties,
                 bool is_out, uint8_t edge_type) const {
  if (out_properties == nullptr || node_shard_.db == nullptr) {
    return Status::kInvalidArgument;
  }

  out_properties->clear();

  std::vector<ResolvedPropertyRoute> routes;
  Status rs = ResolveNodePropertyRoutes(property_names, &routes, nullptr);
  if (rs != Status::kOk) {
    return rs;
  }

  for (const auto &r : routes) {
    std::string value;
    if (property_update_manager_ != nullptr &&
        property_update_manager_->LookupLatest(
            PropertyObjectKind::kNode, 0,
            static_cast<uint32_t>(r.route.local_property_id), vertex_id,
            vertex_id, is_out, edge_type, &value)) {
      GraphDbStorageLocation location;
      if (LocateNodeProperty(vertex_id, r.property_name, &location, is_out,
                             edge_type) == Status::kOk &&
          property_update_manager_->LookupExact(
              PropertyObjectKind::kNode, 0,
              static_cast<uint32_t>(r.route.local_property_id), vertex_id,
              vertex_id, location.sequence, is_out, edge_type, &value)) {
        (*out_properties)[r.property_name] = std::move(value);
        continue;
      }
    }
    const std::string node_db_path = base_dir_ + "/" + node_shard_.name;
    const ScopedFixedPropertyLayout property_layout(
        &node_shard_.property_lengths);
    const ScopedDbPathOverride db_path_override(&node_db_path);
    rs = node_shard_.db->GetEdge(vertex_id, vertex_id, &value,
                                 r.route.local_property_id, is_out, edge_type);
    if (rs != Status::kOk) {
      out_properties->clear();
      return rs;
    }
    (*out_properties)[r.property_name] = std::move(value);
  }
  return Status::kOk;
}

Status GraphDb::UpdateNode(
    VertexId_t vertex_id,
    const std::unordered_map<std::string, std::string> &properties, bool is_out,
    uint8_t edge_type, PropertyCommitSequence *commit_sequence) {
  if (properties.empty() || node_shard_.db == nullptr) {
    return Status::kInvalidArgument;
  }
  std::vector<std::string> names;
  names.reserve(properties.size());
  for (const auto &item : properties)
    names.push_back(item.first);
  std::vector<ResolvedPropertyRoute> routes;
  if (ResolveNodePropertyRoutes(names, &routes, nullptr) != Status::kOk) {
    return Status::kInvalidArgument;
  }

  if (property_update_manager_ == nullptr) {
    const std::string payload =
        BuildShardPropertyString(node_shard_.properties, properties);
    const std::string node_db_path = base_dir_ + "/" + node_shard_.name;
    const ScopedFixedPropertyLayout property_layout(
        &node_shard_.property_lengths);
    const ScopedDbPathOverride db_path_override(&node_db_path);
    node_shard_.db->update_edge(vertex_id, vertex_id, payload, false,
                                NextSequence(), is_out, edge_type);
    return Status::kOk;
  }

  GraphDbStorageLocation location;
  const Status locate_status = LocateNodeProperty(
      vertex_id, routes.front().property_name, &location, is_out, edge_type);
  if (locate_status != Status::kOk) {
    return locate_status;
  }

  std::vector<BufferedPropertyUpdate> updates;
  updates.reserve(routes.size());
  for (const auto &route : routes) {
    BufferedPropertyUpdate update;
    update.target.kind = PropertyObjectKind::kNode;
    update.target.shard_id = 0;
    update.target.base_file_id = location.target_id;
    update.target.base_generation = location.target_id;
    update.target.property_id =
        static_cast<uint32_t>(route.route.local_property_id);
    update.target_is_persistent = !location.in_memtable;
    update.record.src = vertex_id;
    update.record.dst = vertex_id;
    update.record.base_sequence = location.sequence;
    update.record.is_out = is_out;
    update.record.edge_type = edge_type;
    update.record.value = properties.at(route.property_name);
    updates.push_back(std::move(update));
  }
  std::string error;
  if (!property_update_manager_->SubmitBatch(std::move(updates),
                                             commit_sequence,
                                             &error)) {
    std::cerr << "[GraphDb] node property update failed: " << error
              << std::endl;
    return Status::kBackgroundError;
  }
  return Status::kOk;
}

Status GraphDb::LocateNodeProperty(VertexId_t vertex_id,
                                   const std::string& property_name,
                                   GraphDbStorageLocation* location,
                                   bool is_out,
                                   uint8_t edge_type) const {
  if (location == nullptr || node_shard_.db == nullptr) {
    return Status::kInvalidArgument;
  }
  std::vector<ResolvedPropertyRoute> routes;
  Status rs = ResolveNodePropertyRoutes({property_name}, &routes, nullptr);
  if (rs != Status::kOk || routes.empty()) {
    return rs;
  }

  const auto& r = routes.front();
  lsmgraph::LSMEdgeLocation low_level_location;
  const std::string node_db_path = base_dir_ + "/" + node_shard_.name;
  const ScopedFixedPropertyLayout property_layout(&node_shard_.property_lengths);
  const ScopedDbPathOverride db_path_override(&node_db_path);
  rs = node_shard_.db->LocateEdge(vertex_id, vertex_id, &low_level_location,
                                  is_out, edge_type);
  if (rs != Status::kOk) {
    return rs;
  }
  location->in_memtable = low_level_location.in_memtable;
  location->shard_idx = 0;
  location->local_property_id = r.route.local_property_id;
  location->target_id = low_level_location.target_id;
  location->sequence = low_level_location.sequence;
  return Status::kOk;
}

Status GraphDb::PutEdge(VertexId_t src,
                        VertexId_t dst,
                        const std::unordered_map<std::string, std::string>& properties,
                        EdgeInsertMode insert_mode,
                        bool is_out,
                        uint8_t edge_type) {
  if (properties.empty()) {
    return Status::kInvalidArgument;
  }

  // 先按属性名定位“本次需要写入”的 shard。
  std::unordered_set<size_t> touched_shards;
  for (const auto& kv : properties) {
    const auto it = edge_property_to_route_.find(kv.first);
    if (it == edge_property_to_route_.end()) {
      return Status::kInvalidArgument;
    }
    touched_shards.insert(it->second.shard_idx);
  }
  if (touched_shards.empty()) {
    return Status::kInvalidArgument;
  }

  // 每个命中的 shard 都写一条边：
  // - shard 内未提供的列写空；
  // - 没命中的 shard 不写，避免无意义拓扑冗余。
  const SequenceNumber_t sequence = NextSequence();
  for (const size_t shard_idx : touched_shards) {
    auto& shard = edge_shards_[shard_idx];
    const std::string payload =
        BuildShardPropertyString(shard.properties, properties);
    const std::string shard_db_path = base_dir_ + "/" + shard.name;
    const ScopedFixedPropertyLayout property_layout(&shard.property_lengths);
    const ScopedDbPathOverride db_path_override(&shard_db_path);
    shard.db->put_edge(src, dst, payload, insert_mode, is_out, edge_type,
                       sequence);
  }
  return Status::kOk;
}

Status GraphDb::PutEdgePayload(size_t shard_idx,
                               VertexId_t src,
                               VertexId_t dst,
                               const std::string& payload,
                               EdgeInsertMode insert_mode,
                               bool is_out,
                               uint8_t edge_type,
                               SequenceNumber_t sequence) {
  if (payload.empty() || shard_idx >= edge_shards_.size()) {
    return Status::kInvalidArgument;
  }
  if (sequence < 0) {
    sequence = NextSequence();
  } else {
    ObserveSequence(sequence);
  }
  auto& shard = edge_shards_[shard_idx];
  const std::string shard_db_path = base_dir_ + "/" + shard.name;
  const ScopedFixedPropertyLayout property_layout(&shard.property_lengths);
  const ScopedDbPathOverride db_path_override(&shard_db_path);
  shard.db->put_edge(src, dst, payload, insert_mode, is_out, edge_type,
                     sequence);
  return Status::kOk;
}

Status GraphDb::UpdateEdge(VertexId_t src,
                           VertexId_t dst,
                           const std::unordered_map<std::string, std::string>& properties,
                           bool is_out,
                           uint8_t edge_type,
                           PropertyCommitSequence* commit_sequence) {
  if (properties.empty()) {
    return Status::kInvalidArgument;
  }

  std::unordered_set<size_t> touched_shards;
  for (const auto& kv : properties) {
    const auto it = edge_property_to_route_.find(kv.first);
    if (it == edge_property_to_route_.end()) {
      return Status::kInvalidArgument;
    }
    touched_shards.insert(it->second.shard_idx);
  }
  if (touched_shards.empty()) {
    return Status::kInvalidArgument;
  }

  if (property_update_manager_ == nullptr) {
    const SequenceNumber_t sequence = NextSequence();
    for (const size_t shard_idx : touched_shards) {
      auto& shard = edge_shards_[shard_idx];
      const std::string payload =
          BuildShardPropertyString(shard.properties, properties);
      const std::string shard_db_path = base_dir_ + "/" + shard.name;
      const ScopedFixedPropertyLayout property_layout(&shard.property_lengths);
      const ScopedDbPathOverride db_path_override(&shard_db_path);
      shard.db->update_edge(src, dst, payload, false, sequence,
                            is_out, edge_type);
    }
    return Status::kOk;
  }

  std::unordered_map<size_t, GraphDbStorageLocation> locations;
  for (const size_t shard_idx : touched_shards) {
    auto& shard = edge_shards_[shard_idx];
    lsmgraph::LSMEdgeLocation low_level_location;
    const std::string shard_db_path = base_dir_ + "/" + shard.name;
    const ScopedFixedPropertyLayout property_layout(&shard.property_lengths);
    const ScopedDbPathOverride db_path_override(&shard_db_path);
    const Status locate_status = shard.db->LocateEdge(
        src, dst, &low_level_location, is_out, edge_type);
    if (locate_status != Status::kOk) {
      return locate_status;
    }
    GraphDbStorageLocation location;
    location.in_memtable = low_level_location.in_memtable;
    location.shard_idx = shard_idx;
    location.target_id = low_level_location.target_id;
    location.sequence = low_level_location.sequence;
    locations.emplace(shard_idx, location);
  }

  std::vector<BufferedPropertyUpdate> updates;
  updates.reserve(properties.size());
  for (const auto& item : properties) {
    const PropertyRoute route = edge_property_to_route_.at(item.first);
    const auto location_it = locations.find(route.shard_idx);
    if (location_it == locations.end()) return Status::kNotFound;
    const auto& location = location_it->second;
    BufferedPropertyUpdate update;
    update.target.kind = PropertyObjectKind::kEdge;
    update.target.shard_id = static_cast<uint32_t>(route.shard_idx);
    update.target.base_file_id = location.target_id;
    update.target.base_generation = location.target_id;
    update.target.property_id =
        static_cast<uint32_t>(route.local_property_id);
    update.target_is_persistent = !location.in_memtable;
    update.record.src = src;
    update.record.dst = dst;
    update.record.base_sequence = location.sequence;
    update.record.is_out = is_out;
    update.record.edge_type = edge_type;
    update.record.value = item.second;
    updates.push_back(std::move(update));
  }
  std::string error;
  if (!property_update_manager_->SubmitBatch(std::move(updates),
                                             commit_sequence,
                                             &error)) {
    std::cerr << "[GraphDb] edge property update failed: " << error
              << std::endl;
    return Status::kBackgroundError;
  }
  return Status::kOk;
}

Status GraphDb::GetEdge(VertexId_t src,
                        VertexId_t dst,
                        const std::vector<std::string>& property_names,
                        std::unordered_map<std::string, std::string>* out_properties,
                        bool is_out,
                        uint8_t edge_type) const {
  if (out_properties == nullptr) {
    return Status::kInvalidArgument;
  }
  out_properties->clear();

  std::vector<ResolvedPropertyRoute> routes;
  Status rs = ResolveEdgePropertyRoutes(property_names, &routes, nullptr);
  if (rs != Status::kOk) {
    return rs;
  }

  for (const auto& r : routes) {
    const auto& shard = edge_shards_[r.route.shard_idx];
    std::string value;
    if (property_update_manager_ != nullptr &&
        property_update_manager_->LookupLatest(
            PropertyObjectKind::kEdge,
            static_cast<uint32_t>(r.route.shard_idx),
            static_cast<uint32_t>(r.route.local_property_id),
            src,
            dst,
            is_out,
            edge_type,
            &value)) {
      GraphDbStorageLocation location;
      if (LocateEdgeProperty(src,
                             dst,
                             r.property_name,
                             &location,
                             is_out,
                             edge_type) == Status::kOk &&
          property_update_manager_->LookupExact(
              PropertyObjectKind::kEdge,
              static_cast<uint32_t>(r.route.shard_idx),
              static_cast<uint32_t>(r.route.local_property_id),
              src,
              dst,
              location.sequence,
              is_out,
              edge_type,
              &value)) {
        (*out_properties)[r.property_name] = std::move(value);
        continue;
      }
    }
    const std::string shard_db_path = base_dir_ + "/" + shard.name;
    const ScopedFixedPropertyLayout property_layout(&shard.property_lengths);
    const ScopedDbPathOverride db_path_override(&shard_db_path);
    rs = shard.db->GetEdge(src, dst, &value,
                           r.route.local_property_id,
                           is_out,
                           edge_type);
    if (rs != Status::kOk) {
      out_properties->clear();
      return rs;
    }
    (*out_properties)[r.property_name] = std::move(value);
  }

  return Status::kOk;
}

Status GraphDb::LocateEdgeProperty(VertexId_t src,
                                   VertexId_t dst,
                                   const std::string& property_name,
                                   GraphDbStorageLocation* location,
                                   bool is_out,
                                   uint8_t edge_type) const {
  if (location == nullptr) {
    return Status::kInvalidArgument;
  }
  std::vector<ResolvedPropertyRoute> routes;
  Status rs = ResolveEdgePropertyRoutes({property_name}, &routes, nullptr);
  if (rs != Status::kOk || routes.empty()) {
    return rs;
  }

  const auto& r = routes.front();
  const auto& shard = edge_shards_[r.route.shard_idx];
  lsmgraph::LSMEdgeLocation low_level_location;
  const std::string shard_db_path = base_dir_ + "/" + shard.name;
  const ScopedFixedPropertyLayout property_layout(&shard.property_lengths);
  const ScopedDbPathOverride db_path_override(&shard_db_path);
  rs = shard.db->LocateEdge(src, dst, &low_level_location, is_out, edge_type);
  if (rs != Status::kOk) {
    return rs;
  }
  location->in_memtable = low_level_location.in_memtable;
  location->shard_idx = r.route.shard_idx;
  location->local_property_id = r.route.local_property_id;
  location->target_id = low_level_location.target_id;
  location->sequence = low_level_location.sequence;
  return Status::kOk;
}

Status GraphDb::ForEachEdge(VertexId_t src,
                            const std::string& property_name,
                            const GraphDbEdgeCallback& callback,
                            bool is_out,
                            uint8_t edge_type) const {
  if (!callback) {
    return Status::kInvalidArgument;
  }

  std::vector<ResolvedPropertyRoute> routes;
  Status rs = ResolveEdgePropertyRoutes({property_name}, &routes, nullptr);
  if (rs != Status::kOk) {
    return rs;
  }
  if (routes.empty()) {
    return Status::kOk;
  }

  struct EdgeVersionKey {
    VertexId_t dst = 0;
    SequenceNumber_t seq = 0;

    bool operator==(const EdgeVersionKey& other) const {
      return dst == other.dst && seq == other.seq;
    }
  };

  struct SinglePropertyRecord {
    EdgeVersionKey key;
    std::string value;
  };

  const auto& r = routes.front();
  const auto& shard = edge_shards_[r.route.shard_idx];
  const std::string shard_db_path = base_dir_ + "/" + shard.name;
  const ScopedFixedPropertyLayout property_layout(&shard.property_lengths);
  const ScopedDbPathOverride db_path_override(&shard_db_path);

  // Capture the PropertyBuffer before Get_Edges captures immutable delta
  // views.  See PropertyUpdateManager::SnapshotForSource for the hand-off
  // invariant.
  const BufferedPropertySnapshot buffered = CaptureBufferedProperties(
      property_update_manager_.get(),
      PropertyObjectKind::kEdge,
      static_cast<std::uint32_t>(r.route.shard_idx),
      static_cast<std::uint32_t>(r.route.local_property_id),
      src,
      is_out,
      edge_type);

  std::vector<SinglePropertyRecord> records;
  auto it = shard.db->Get_Edges(src,
                                MAX_SEQ_ID,
                                r.route.local_property_id,
                                is_out,
                                edge_type);
  for (; it.valid(); it.next()) {
    if (it.marker()) {
      continue;
    }
    records.push_back(SinglePropertyRecord{
        EdgeVersionKey{it.dst_id(), it.sequence()},
        ResolveBufferedProperty(buffered,
                                it.dst_id(),
                                it.sequence(),
                                it.edge_data(r.route.local_property_id))});
  }

  std::stable_sort(records.begin(),
                   records.end(),
                   [](const SinglePropertyRecord& a,
                      const SinglePropertyRecord& b) {
                     if (a.key.dst != b.key.dst) {
                       return a.key.dst < b.key.dst;
                     }
                     return a.key.seq > b.key.seq;
                   });

  for (size_t i = 0; i < records.size();) {
    size_t last = i;
    while (last + 1 < records.size()
           && records[last + 1].key == records[i].key) {
      ++last;
    }
    const auto& record = records[last];
    if (!callback(record.key.dst, record.key.seq, record.value)) {
      break;
    }
    i = last + 1;
  }

  return Status::kOk;
}

Status GraphDb::ScanEdges(VertexId_t src,
                          const std::vector<std::string>& property_names,
                          std::vector<GraphDbEdgeScanRecord>* out_records,
                          bool is_out,
                          uint8_t edge_type) const {
  if (out_records == nullptr) {
    return Status::kInvalidArgument;
  }
  out_records->clear();

  std::vector<ResolvedPropertyRoute> routes;
  Status rs = ResolveEdgePropertyRoutes(property_names, &routes, nullptr);
  if (rs != Status::kOk) {
    return rs;
  }
  if (routes.empty()) {
    return Status::kOk;
  }

  struct EdgeVersionKey {
    VertexId_t dst = 0;
    SequenceNumber_t seq = 0;

    bool operator==(const EdgeVersionKey& other) const {
      return dst == other.dst && seq == other.seq;
    }
  };

  struct EdgeVersionKeyHash {
    size_t operator()(const EdgeVersionKey& key) const {
      size_t h = std::hash<VertexId_t>{}(key.dst);
      h ^= std::hash<SequenceNumber_t>{}(key.seq) + 0x9e3779b97f4a7c15ULL +
           (h << 6) + (h >> 2);
      return h;
    }
  };

  if (routes.size() == 1) {
    const auto& r = routes.front();
    const auto& shard = edge_shards_[r.route.shard_idx];
    const std::string shard_db_path = base_dir_ + "/" + shard.name;
    const ScopedFixedPropertyLayout property_layout(&shard.property_lengths);
    const ScopedDbPathOverride db_path_override(&shard_db_path);
    const BufferedPropertySnapshot buffered = CaptureBufferedProperties(
        property_update_manager_.get(),
        PropertyObjectKind::kEdge,
        static_cast<std::uint32_t>(r.route.shard_idx),
        static_cast<std::uint32_t>(r.route.local_property_id),
        src,
        is_out,
        edge_type);

    struct SinglePropertyRecord {
      EdgeVersionKey key;
      std::string value;
    };

    std::vector<SinglePropertyRecord> records;
    auto it = shard.db->Get_Edges(src, MAX_SEQ_ID,
                                  r.route.local_property_id,
                                  is_out,
                                  edge_type);
    for (; it.valid(); it.next()) {
      if (it.marker()) {
        continue;
      }
      records.push_back(SinglePropertyRecord{
          EdgeVersionKey{it.dst_id(), it.sequence()},
          ResolveBufferedProperty(buffered,
                                  it.dst_id(),
                                  it.sequence(),
                                  it.edge_data(r.route.local_property_id))});
    }

    std::stable_sort(records.begin(), records.end(),
                     [](const SinglePropertyRecord& a,
                        const SinglePropertyRecord& b) {
                       if (a.key.dst != b.key.dst) {
                         return a.key.dst < b.key.dst;
                       }
                       return a.key.seq > b.key.seq;
                     });

    out_records->reserve(records.size());
    for (size_t i = 0; i < records.size();) {
      size_t last = i;
      while (last + 1 < records.size()
             && records[last + 1].key == records[i].key) {
        ++last;
      }

      GraphDbEdgeScanRecord rec;
      rec.dst = records[last].key.dst;
      rec.sequence = records[last].key.seq;
      rec.properties.reserve(1);
      rec.properties.emplace(r.property_name, std::move(records[last].value));
      out_records->push_back(std::move(rec));
      i = last + 1;
    }
    return Status::kOk;
  }

  struct Candidate {
    std::unordered_map<std::string, std::string> properties;
    std::unordered_set<std::string> hit_properties;
  };

  std::unordered_map<EdgeVersionKey, Candidate, EdgeVersionKeyHash> merged;
  for (const auto& r : routes) {
    const auto& shard = edge_shards_[r.route.shard_idx];
    const std::string shard_db_path = base_dir_ + "/" + shard.name;
    const ScopedFixedPropertyLayout property_layout(&shard.property_lengths);
    const ScopedDbPathOverride db_path_override(&shard_db_path);
    const BufferedPropertySnapshot buffered = CaptureBufferedProperties(
        property_update_manager_.get(),
        PropertyObjectKind::kEdge,
        static_cast<std::uint32_t>(r.route.shard_idx),
        static_cast<std::uint32_t>(r.route.local_property_id),
        src,
        is_out,
        edge_type);
    auto it = shard.db->Get_Edges(src, MAX_SEQ_ID,
                                  r.route.local_property_id,
                                  is_out,
                                  edge_type);
    for (; it.valid(); it.next()) {
      if (it.marker()) {
        continue;
      }
      const EdgeVersionKey key{it.dst_id(), it.sequence()};
      auto& cand = merged[key];
      cand.properties[r.property_name] = ResolveBufferedProperty(
          buffered,
          it.dst_id(),
          it.sequence(),
          it.edge_data(r.route.local_property_id));
      cand.hit_properties.insert(r.property_name);
    }
  }

  // 只返回“命中全部请求属性”的边。
  const size_t need_cnt = routes.size();
  std::vector<EdgeVersionKey> ready_keys;
  ready_keys.reserve(merged.size());
  for (const auto& kv : merged) {
    if (kv.second.hit_properties.size() == need_cnt) {
      ready_keys.push_back(kv.first);
    }
  }
  std::sort(ready_keys.begin(), ready_keys.end(),
            [](const EdgeVersionKey& a, const EdgeVersionKey& b) {
              if (a.dst != b.dst) {
                return a.dst < b.dst;
              }
              return a.seq > b.seq;
            });

  out_records->reserve(ready_keys.size());
  for (const auto& key : ready_keys) {
    auto it = merged.find(key);
    if (it == merged.end()) {
      continue;
    }
    GraphDbEdgeScanRecord rec;
    rec.dst = key.dst;
    rec.sequence = key.seq;
    rec.properties = std::move(it->second.properties);
    out_records->push_back(std::move(rec));
  }

  return Status::kOk;
}

void GraphDb::WaitCSRUpToDate() {
  if (!schema_.use_csr_disk) {
    return;
  }

  for (auto& shard : edge_shards_) {
    if (shard.db == nullptr) {
      continue;
    }
    static_cast<LSMStore*>(shard.db.get())->WaitCSRUpToDate();
  }
  if (node_shard_.db != nullptr) {
    static_cast<LSMStore*>(node_shard_.db.get())->WaitCSRUpToDate();
  }
}

bool GraphDb::HasBackgroundWork() {
  for (auto& shard : edge_shards_) {
    if (shard.db != nullptr && shard.db->HasBackgroundWork()) {
      return true;
    }
  }
  return node_shard_.db != nullptr && node_shard_.db->HasBackgroundWork();
}

void GraphDb::WaitBackgroundIdle() {
  // Property updates are acknowledged while they are still resident in the
  // engine-owned Property Buffer.  Publish that buffer first, then wait for
  // per-shard delta merges and the ordinary LSM background work.  Keeping the
  // ordering here gives callers one well-defined quiescence barrier.
  const Status flush_status = FlushPropertyUpdates();
  if (flush_status != Status::kOk) {
    std::cerr << "[GraphDb] failed to flush property updates while waiting "
                 "for background work"
              << std::endl;
  }
  for (auto& shard : edge_shards_) {
    if (shard.db != nullptr) {
      shard.db->WaitBackgroundIdle();
    }
  }
  if (node_shard_.db != nullptr) {
    node_shard_.db->WaitBackgroundIdle();
  }
}

Status GraphDb::FlushPropertyUpdates() {
  if (property_update_manager_ == nullptr) {
    return Status::kOk;
  }

  std::string error;
  if (!property_update_manager_->FlushAndWait(&error)) {
    std::cerr << "[GraphDb] property buffer flush failed: " << error
              << std::endl;
    return Status::kBackgroundError;
  }

  for (auto& shard : edge_shards_) {
    auto* store = static_cast<LSMStore*>(shard.db.get());
    if (store != nullptr && !store->WaitPropertyDeltaIdle(&error)) {
      std::cerr << "[GraphDb] edge property delta wait failed for shard "
                << shard.name << ": " << error << std::endl;
      return Status::kBackgroundError;
    }
  }
  auto* node_store = static_cast<LSMStore*>(node_shard_.db.get());
  if (node_store != nullptr && !node_store->WaitPropertyDeltaIdle(&error)) {
    std::cerr << "[GraphDb] node property delta wait failed: " << error
              << std::endl;
    return Status::kBackgroundError;
  }
  return Status::kOk;
}

PropertyUpdateManagerStats GraphDb::GetPropertyUpdateStats() const {
  if (property_update_manager_ == nullptr) {
    return {};
  }
  return property_update_manager_->Stats();
}

GraphDb::~GraphDb() {
  // The manager callback references the shard handles, so it must be drained
  // and destroyed before the stores.  No caller may race the GraphDb
  // destructor; after the manager is gone each store is quiesced and released
  // deterministically.
  if (property_update_manager_ != nullptr) {
    const Status status = FlushPropertyUpdates();
    if (status != Status::kOk) {
      std::cerr << "[GraphDb] property updates could not be fully persisted "
                   "during shutdown"
                << std::endl;
    }
    property_update_manager_.reset();
  }

  try {
    for (auto& shard : edge_shards_) {
      if (shard.db != nullptr) {
        shard.db->WaitBackgroundIdle();
      }
    }
    if (node_shard_.db != nullptr) {
      node_shard_.db->WaitBackgroundIdle();
    }
  } catch (const std::exception& error) {
    std::cerr << "[GraphDb] background shutdown barrier failed: "
              << error.what() << std::endl;
  }

  // Destroy stores explicitly to make the callback/lifetime ordering obvious.
  node_shard_.db.reset();
  edge_shards_.clear();
}

Status GraphDb::ForceCompactAllShards() {
  const Status flush_status = FlushPropertyUpdates();
  if (flush_status != Status::kOk) {
    return flush_status;
  }

  bool ok = true;
  for (auto& shard : edge_shards_) {
    if (shard.db == nullptr) {
      ok = false;
      continue;
    }
    shard.db->WaitBackgroundIdle();
    ok = shard.db->ForceCompactAllL0ToL1() && ok;
  }
  if (node_shard_.db == nullptr) {
    ok = false;
  } else {
    node_shard_.db->WaitBackgroundIdle();
    ok = node_shard_.db->ForceCompactAllL0ToL1() && ok;
  }
  return ok ? Status::kOk : Status::kNotFound;
}

bool GraphDb::ForceCompactCsrEdgeShards() {
  bool ok = true;
  bool any = false;
  for (size_t i = 0; i < edge_shards_.size(); ++i) {
    if (i >= schema_.edge_shards.size() || !schema_.edge_shards[i].is_csr) {
      continue;
    }
    auto& shard = edge_shards_[i];
    if (shard.db == nullptr) {
      ok = false;
      continue;
    }
    any = true;
    const auto t1 = std::chrono::steady_clock::now();
    const bool shard_ok = shard.db->ForceCompactAllL0ToL1();
    const auto t2 = std::chrono::steady_clock::now();
    const double sec = std::chrono::duration_cast<std::chrono::duration<double>>(
                           t2 - t1)
                           .count();
    std::cout << "[HOT_EDGE_CSR_COMPACTION] shard: " << shard.name
              << std::endl;
    std::cout << "[HOT_EDGE_CSR_COMPACTION] time(s): " << sec << std::endl;
    std::cout << "[HOT_EDGE_CSR_COMPACTION] success: "
              << (shard_ok ? "true" : "false") << std::endl;
    ok = ok && shard_ok;
  }
  if (!any) {
    std::cout << "[HOT_EDGE_CSR_COMPACTION] enabled: false" << std::endl;
  }
  return ok;
}

}  // namespace lsmgraph
