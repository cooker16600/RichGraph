#include <algorithm>
#include <cctype>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace fs = std::filesystem;

struct Stats {
  uint64_t node_count = 0;
  uint64_t edge_count = 0;
  uint64_t node_type_bytes = 0;
  uint64_t edge_type_bytes = 0;
  uint64_t node_prop_bytes = 0;
  uint64_t edge_prop_bytes = 0;
};

struct RelCounters {
  uint64_t rows = 0;
  uint64_t nonzero = 0;
  uint64_t zero = 0;
};

std::vector<std::string_view> SplitViews(std::string_view line) {
  std::vector<std::string_view> out;
  size_t begin = 0;
  while (begin <= line.size()) {
    const size_t pos = line.find('|', begin);
    if (pos == std::string_view::npos) {
      out.emplace_back(line.substr(begin));
      break;
    }
    out.emplace_back(line.substr(begin, pos - begin));
    begin = pos + 1;
  }
  return out;
}

bool ValidValue(std::string_view v) {
  return !v.empty() && v != "-1";
}

uint64_t PayloadBytes(const std::vector<std::string_view>& values) {
  uint64_t bytes = 0;
  uint64_t count = 0;
  for (const auto v : values) {
    if (!ValidValue(v)) {
      continue;
    }
    bytes += static_cast<uint64_t>(v.size());
    ++count;
  }
  if (count > 1) {
    bytes += count - 1;
  }
  return bytes;
}

uint64_t PayloadBytesByIndex(const std::vector<std::string_view>& fields,
                             const std::vector<int>& indices,
                             uint64_t* nonempty_count = nullptr) {
  uint64_t bytes = 0;
  uint64_t count = 0;
  for (const int idx : indices) {
    if (idx < 0 || static_cast<size_t>(idx) >= fields.size()) {
      continue;
    }
    const auto v = fields[static_cast<size_t>(idx)];
    if (!ValidValue(v)) {
      continue;
    }
    bytes += static_cast<uint64_t>(v.size());
    ++count;
  }
  if (count > 1) {
    bytes += count - 1;
  }
  if (nonempty_count != nullptr) {
    *nonempty_count = count;
  }
  return bytes;
}

std::unordered_map<std::string, int> HeaderIndex(const std::string& header) {
  std::unordered_map<std::string, int> out;
  const auto cols = SplitViews(header);
  for (size_t i = 0; i < cols.size(); ++i) {
    out.emplace(std::string(cols[i]), static_cast<int>(i));
  }
  return out;
}

int ColumnIndex(const std::unordered_map<std::string, int>& idx,
                const std::string& name) {
  const auto it = idx.find(name);
  return it == idx.end() ? -1 : it->second;
}

std::vector<int> ColumnIndices(const std::unordered_map<std::string, int>& idx,
                               const std::vector<std::string>& names) {
  std::vector<int> out;
  out.reserve(names.size());
  for (const auto& name : names) {
    out.push_back(ColumnIndex(idx, name));
  }
  return out;
}

uint64_t CountDataRows(const fs::path& path) {
  std::ifstream in(path, std::ios::binary);
  if (!in.is_open()) {
    std::cerr << "failed to open " << path << "\n";
    std::exit(1);
  }
  std::string line;
  uint64_t rows = 0;
  bool first = true;
  while (std::getline(in, line)) {
    if (first) {
      first = false;
      continue;
    }
    if (!line.empty() && line.back() == '\r') {
      line.pop_back();
    }
    if (!line.empty()) {
      ++rows;
    }
  }
  return rows;
}

uint64_t CountOccurrences(const fs::path& path, const std::string& needle) {
  std::ifstream in(path, std::ios::binary);
  if (!in.is_open()) {
    return 0;
  }
  const std::string quoted = "\"" + needle + "\"";
  std::string s((std::istreambuf_iterator<char>(in)),
                std::istreambuf_iterator<char>());
  uint64_t count = 0;
  size_t pos = 0;
  while ((pos = s.find(quoted, pos)) != std::string::npos) {
    ++count;
    pos += quoted.size();
  }
  return count;
}

std::string FieldAt(const std::vector<std::string_view>& f, size_t idx) {
  if (idx >= f.size()) {
    return {};
  }
  return std::string(f[idx]);
}

std::vector<std::string_view> SplitList(std::string_view v, char sep) {
  std::vector<std::string_view> out;
  size_t begin = 0;
  while (begin <= v.size()) {
    const size_t pos = v.find(sep, begin);
    const auto item = pos == std::string_view::npos
                          ? v.substr(begin)
                          : v.substr(begin, pos - begin);
    if (ValidValue(item)) {
      out.push_back(item);
    }
    if (pos == std::string_view::npos) {
      break;
    }
    begin = pos + 1;
  }
  return out;
}

uint64_t FinBenchColdExtraCount(const fs::path& schema,
                                const std::string& rel) {
  if (schema.empty() || !fs::exists(schema)) {
    return 0;
  }
  const uint64_t occurrences = CountOccurrences(schema, rel);
  return occurrences == 0 ? 0 : occurrences - 1;  // subtract top-level edge_types
}

struct FinBenchBase {
  Stats stats;
  std::unordered_map<std::string, RelCounters> rels;
};

uint64_t FinNodeTypeLength(const std::string& file) {
  if (file == "Person.csv" || file == "AddPersonWrite1.csv") return 6;
  if (file == "Company.csv" || file == "AddCompanyWrite2.csv") return 7;
  if (file == "Account.csv") return 7;
  if (file == "Loan.csv") return 4;
  if (file == "Medium.csv" || file == "AddMediumWrite3.csv") return 6;
  return 0;
}

uint64_t FinExtraNodeTypeLength(const std::string& file) {
  if (file == "AddPersonOwnAccountWrite4.csv" ||
      file == "AddCompanyOwnAccountWrite5.csv") {
    return 7;  // Account
  }
  if (file == "AddPersonApplyLoanWrite6.csv" ||
      file == "AddCompanyApplyLoanWrite7.csv") {
    return 4;  // Loan
  }
  return 0;
}

const std::unordered_map<std::string, std::vector<std::string>>& FinNodeProps() {
  static const std::unordered_map<std::string, std::vector<std::string>> props = {
      {"Person.csv", {"personName", "isBlocked", "createTime", "gender",
                       "birthday", "country", "city"}},
      {"Company.csv", {"companyName", "isBlocked", "createTime", "country",
                        "city", "business", "description", "url"}},
      {"Account.csv", {"createTime", "isBlocked", "accountType", "nickname",
                        "phonenum", "email", "freqLoginType", "lastLoginTime",
                        "accountLevel"}},
      {"Loan.csv", {"loanAmount", "balance", "createTime", "loanUsage",
                     "interestRate"}},
      {"Medium.csv", {"mediumType", "isBlocked", "createTime",
                       "lastLoginTime", "riskLevel"}},
  };
  return props;
}

const std::unordered_map<std::string, std::vector<std::string>>& FinRelProps() {
  static const std::unordered_map<std::string, std::vector<std::string>> props = {
      {"AccountTransferAccount", {"createTime", "amount", "orderNum",
                                   "comment", "payType", "goodsType",
                                   "dependencyTime"}},
      {"AccountWithdrawAccount", {"createTime", "amount", "fromType",
                                   "toType", "comment", "dependencyTime"}},
      {"AccountRepayLoan", {"createTime", "amount", "comment",
                             "dependencyTime"}},
      {"CompanyApplyLoan", {"createTime", "loanAmount", "org", "comment"}},
      {"CompanyGuaranteeCompany", {"createTime", "relation", "comment"}},
      {"CompanyInvestCompany", {"ratio", "createTime", "comment"}},
      {"CompanyOwnAccount", {"createTime", "comment"}},
      {"LoanDepositAccount", {"createTime", "amount", "comment",
                               "dependencyTime"}},
      {"MediumSignInAccount", {"createTime", "location", "comment",
                                "dependencyTime"}},
      {"PersonApplyLoan", {"createTime", "loanAmount", "org", "comment"}},
      {"PersonGuaranteePerson", {"createTime", "relation", "comment"}},
      {"PersonInvestCompany", {"ratio", "createTime", "comment"}},
      {"PersonOwnAccount", {"createTime", "comment"}},
  };
  return props;
}

const std::unordered_map<std::string, std::string>& FinIncRel() {
  static const std::unordered_map<std::string, std::string> rel = {
      {"AddPersonOwnAccountWrite4.csv", "PersonOwnAccount"},
      {"AddCompanyOwnAccountWrite5.csv", "CompanyOwnAccount"},
      {"AddPersonApplyLoanWrite6.csv", "PersonApplyLoan"},
      {"AddCompanyApplyLoanWrite7.csv", "CompanyApplyLoan"},
      {"AddPersonInvestCompanyWrite8.csv", "PersonInvestCompany"},
      {"AddCompanyInvestCompanyWrite9.csv", "CompanyInvestCompany"},
      {"AddPersonGuaranteePersonWrite10.csv", "PersonGuaranteePerson"},
      {"AddPersonGuaranteePersonReadWrite3.csv", "PersonGuaranteePerson"},
      {"AddCompanyGuaranteeCompanyWrite11.csv", "CompanyGuaranteeCompany"},
      {"AddAccountTransferAccountWrite12.csv", "AccountTransferAccount"},
      {"AddAccountTransferAccountReadWrite1.csv", "AccountTransferAccount"},
      {"AddAccountTransferAccountReadWrite2.csv", "AccountTransferAccount"},
      {"AddAccountWithdrawAccountWrite13.csv", "AccountWithdrawAccount"},
      {"AddAccountRepayLoanWrite14.csv", "AccountRepayLoan"},
      {"AddLoanDepositAccountWrite15.csv", "LoanDepositAccount"},
      {"AddMediumSigninAccountWrite16.csv", "MediumSignInAccount"},
  };
  return rel;
}

void ScanCsvProperties(const fs::path& path,
                       const std::vector<std::string>& props,
                       uint64_t* rows,
                       uint64_t* prop_bytes,
                       RelCounters* rel_counters = nullptr) {
  std::ifstream in(path, std::ios::binary);
  if (!in.is_open()) {
    std::cerr << "failed to open " << path << "\n";
    std::exit(1);
  }
  std::string line;
  if (!std::getline(in, line)) {
    return;
  }
  if (!line.empty() && line.back() == '\r') {
    line.pop_back();
  }
  const auto idx = HeaderIndex(line);
  const auto prop_idx = ColumnIndices(idx, props);
  while (std::getline(in, line)) {
    if (!line.empty() && line.back() == '\r') {
      line.pop_back();
    }
    if (line.empty()) {
      continue;
    }
    const auto fields = SplitViews(line);
    uint64_t nonempty = 0;
    const uint64_t bytes = PayloadBytesByIndex(fields, prop_idx, &nonempty);
    ++(*rows);
    *prop_bytes += bytes;
    if (rel_counters != nullptr) {
      ++rel_counters->rows;
      if (nonempty > 0) {
        ++rel_counters->nonzero;
      } else {
        ++rel_counters->zero;
      }
    }
  }
}

FinBenchBase ScanFinBenchBase(const fs::path& root) {
  FinBenchBase out;
  for (const auto& [file, props] : FinNodeProps()) {
    uint64_t rows = 0, bytes = 0;
    ScanCsvProperties(root / "snapshot" / file, props, &rows, &bytes);
    out.stats.node_count += rows;
    out.stats.node_type_bytes += rows * FinNodeTypeLength(file);
    out.stats.node_prop_bytes += bytes;
  }
  for (const auto& [rel, props] : FinRelProps()) {
    uint64_t rows = 0, bytes = 0;
    ScanCsvProperties(root / "snapshot" / (rel + ".csv"), props, &rows,
                      &bytes, &out.rels[rel]);
    out.stats.edge_count += rows;
    out.stats.edge_type_bytes += rows * rel.size();
    out.stats.edge_prop_bytes += bytes;
  }
  const std::unordered_map<std::string, std::vector<std::string>> node_only = {
      {"AddPersonWrite1.csv", {"createTime", "personName", "isBlocked",
                                "gender", "birthday", "country", "city"}},
      {"AddCompanyWrite2.csv", {"createTime", "companyName", "isBlocked",
                                 "country", "city", "business", "description",
                                 "url"}},
      {"AddMediumWrite3.csv", {"createTime", "mediumType", "isBlocked",
                                "lastLoginTime", "riskLevel"}},
  };
  for (const auto& [file, props] : node_only) {
    uint64_t rows = 0, bytes = 0;
    ScanCsvProperties(root / "incremental" / file, props, &rows, &bytes);
    out.stats.node_count += rows;
    out.stats.node_type_bytes += rows * FinNodeTypeLength(file);
    out.stats.node_prop_bytes += bytes;
  }
  const std::unordered_map<std::string, std::vector<std::string>> extra_nodes = {
      {"AddPersonOwnAccountWrite4.csv", {"accountType", "accountBlocked",
                                          "nickname", "phonenum", "email",
                                          "freqLoginType", "lastLoginTime",
                                          "accountLevel"}},
      {"AddCompanyOwnAccountWrite5.csv", {"accountType", "accountBlocked",
                                           "nickname", "phonenum", "email",
                                           "freqLoginType", "lastLoginTime",
                                           "accountLevel"}},
      {"AddPersonApplyLoanWrite6.csv", {"loanAmount", "balance", "loanUsage",
                                         "interestRate"}},
      {"AddCompanyApplyLoanWrite7.csv", {"loanAmount", "balance", "loanUsage",
                                          "interestRate"}},
  };
  for (const auto& [file, rel] : FinIncRel()) {
    const auto& props = FinRelProps().at(rel);
    uint64_t rows = 0, bytes = 0;
    ScanCsvProperties(root / "incremental" / file, props, &rows, &bytes,
                      &out.rels[rel]);
    out.stats.edge_count += rows;
    out.stats.edge_type_bytes += rows * rel.size();
    out.stats.edge_prop_bytes += bytes;
    const auto it = extra_nodes.find(file);
    if (it != extra_nodes.end()) {
      uint64_t node_rows = 0, node_bytes = 0;
      ScanCsvProperties(root / "incremental" / file, it->second, &node_rows,
                        &node_bytes);
      out.stats.node_count += node_rows;
      out.stats.node_type_bytes += node_rows * FinExtraNodeTypeLength(file);
      out.stats.node_prop_bytes += node_bytes;
    }
  }
  return out;
}

uint64_t LogicalBytes(const Stats& s) {
  return s.node_count * 8ULL + s.edge_count * 16ULL + s.node_type_bytes +
         s.edge_type_bytes + s.node_prop_bytes + s.edge_prop_bytes;
}

uint64_t FinBenchVariantBytes(const FinBenchBase& base,
                              const fs::path& dataset_base,
                              const std::string& scale,
                              int target) {
  uint64_t extra = 0;
  if (target > 0) {
    const fs::path schema = dataset_base / ("sf" + scale + "+" +
                                            std::to_string(target)) /
                            "plus_schema.json";
    for (const auto& [rel, counters] : base.rels) {
      const uint64_t k = FinBenchColdExtraCount(schema, rel);
      if (k == 0) {
        continue;
      }
      extra += counters.nonzero * (k * 14ULL + k);
      extra += counters.zero * (k * 14ULL + (k == 0 ? 0 : k - 1ULL));
    }
  }
  return LogicalBytes(base.stats) + extra;
}

struct SnbBase {
  Stats stats;
  std::unordered_map<std::string, RelCounters> explicit_edges;
  std::unordered_map<std::string, uint64_t> implicit_rows;
};

uint64_t SnbNodeTypeLength(const std::string& file) {
  if (file == "static/place_0_0.csv") return 5;          // Place
  if (file == "static/organisation_0_0.csv") return 12; // Organisation
  if (file == "static/tag_0_0.csv") return 3;            // Tag
  if (file == "static/tagclass_0_0.csv") return 8;       // TagClass
  if (file == "dynamic/person_0_0.csv") return 6;        // Person
  if (file == "dynamic/forum_0_0.csv") return 5;         // Forum
  if (file == "dynamic/post_0_0.csv") return 4;          // Post
  if (file == "dynamic/comment_0_0.csv") return 7;       // Comment
  return 0;
}

uint64_t SnbEdgeTypeLength(const std::string& type) {
  return type.size();
}

std::string SnbExplicitTypeFromFile(const std::string& file) {
  std::string type = file;
  const std::string suffix = "_0_0.csv";
  if (type.size() >= suffix.size() &&
      type.compare(type.size() - suffix.size(), suffix.size(), suffix) == 0) {
    type.resize(type.size() - suffix.size());
  }
  return type;
}

const std::unordered_map<std::string, std::vector<std::string>>& SnbNodeProps() {
  static const std::unordered_map<std::string, std::vector<std::string>> props = {
      {"static/place_0_0.csv", {"name", "url", "type"}},
      {"static/organisation_0_0.csv", {"type", "name", "url"}},
      {"static/tag_0_0.csv", {"name", "url"}},
      {"static/tagclass_0_0.csv", {"name", "url"}},
      {"dynamic/person_0_0.csv", {"firstName", "lastName", "gender",
                                   "birthday", "creationDate", "locationIP",
                                   "browserUsed", "language", "email"}},
      {"dynamic/forum_0_0.csv", {"title", "creationDate"}},
      {"dynamic/post_0_0.csv", {"imageFile", "creationDate", "locationIP",
                                 "browserUsed", "language", "content",
                                 "length"}},
      {"dynamic/comment_0_0.csv", {"creationDate", "locationIP",
                                    "browserUsed", "content", "length"}},
  };
  return props;
}

void AddImplicit(std::unordered_map<std::string, uint64_t>* rows,
                 const std::string& type) {
  ++(*rows)[type];
}

void ScanSnbNodeFile(const fs::path& path,
                     const std::vector<std::string>& props,
                     const std::string& name,
                     SnbBase* out) {
  std::ifstream in(path, std::ios::binary);
  if (!in.is_open()) {
    std::cerr << "failed to open " << path << "\n";
    std::exit(1);
  }
  std::string line;
  if (!std::getline(in, line)) {
    return;
  }
  if (!line.empty() && line.back() == '\r') {
    line.pop_back();
  }
  const auto idx = HeaderIndex(line);
  const auto prop_idx = ColumnIndices(idx, props);
  auto ci = [&](const std::string& col) { return ColumnIndex(idx, col); };
  const int is_part_of = ci("isPartOf");
  const int place = ci("place");
  const int has_type = ci("hasType");
  const int is_sub = ci("isSubclassOf");
  const int moderator = ci("moderator");
  const int creator = ci("creator");
  const int forum = ci("Forum.id");
  const int reply_post = ci("replyOfPost");
  const int reply_comment = ci("replyOfComment");

  while (std::getline(in, line)) {
    if (!line.empty() && line.back() == '\r') {
      line.pop_back();
    }
    if (line.empty()) {
      continue;
    }
    const auto fields = SplitViews(line);
    ++out->stats.node_count;
    out->stats.node_type_bytes += SnbNodeTypeLength(name);
    out->stats.node_prop_bytes += PayloadBytesByIndex(fields, prop_idx);
    auto valid_idx = [&](int i) {
      return i >= 0 && static_cast<size_t>(i) < fields.size() &&
             ValidValue(fields[static_cast<size_t>(i)]);
    };
    if (name == "static/place_0_0.csv" && valid_idx(is_part_of)) {
      AddImplicit(&out->implicit_rows, "place_isPartOf_place");
    } else if (name == "static/organisation_0_0.csv" && valid_idx(place)) {
      AddImplicit(&out->implicit_rows, "organisation_isLocatedIn_place");
    } else if (name == "static/tag_0_0.csv" && valid_idx(has_type)) {
      AddImplicit(&out->implicit_rows, "tag_hasType_tagclass");
    } else if (name == "static/tagclass_0_0.csv" && valid_idx(is_sub)) {
      AddImplicit(&out->implicit_rows, "tagclass_isSubclassOf_tagclass");
    } else if (name == "dynamic/person_0_0.csv" && valid_idx(place)) {
      AddImplicit(&out->implicit_rows, "person_isLocatedIn_place");
    } else if (name == "dynamic/forum_0_0.csv" && valid_idx(moderator)) {
      AddImplicit(&out->implicit_rows, "forum_hasModerator_person");
    } else if (name == "dynamic/post_0_0.csv") {
      if (valid_idx(creator)) AddImplicit(&out->implicit_rows, "post_hasCreator_person");
      if (valid_idx(forum)) AddImplicit(&out->implicit_rows, "post_isPartOf_forum");
      if (valid_idx(place)) AddImplicit(&out->implicit_rows, "post_isLocatedIn_place");
    } else if (name == "dynamic/comment_0_0.csv") {
      if (valid_idx(creator)) AddImplicit(&out->implicit_rows, "comment_hasCreator_person");
      if (valid_idx(place)) AddImplicit(&out->implicit_rows, "comment_isLocatedIn_place");
      if (valid_idx(reply_post)) AddImplicit(&out->implicit_rows, "comment_replyOf_post");
      if (valid_idx(reply_comment)) AddImplicit(&out->implicit_rows, "comment_replyOf_comment");
    }
  }
}

const std::unordered_map<std::string, std::pair<bool, uint64_t>>& SnbExplicitProp() {
  static const std::unordered_map<std::string, std::pair<bool, uint64_t>> props = {
      {"person_knows_person_0_0.csv", {true, 13}},
      {"forum_hasMember_person_0_0.csv", {true, 13}},
      {"forum_hasTag_tag_0_0.csv", {false, 0}},
      {"person_hasInterest_tag_0_0.csv", {false, 0}},
      {"person_likes_post_0_0.csv", {true, 13}},
      {"person_likes_comment_0_0.csv", {true, 13}},
      {"person_studyAt_organisation_0_0.csv", {true, 4}},
      {"person_workAt_organisation_0_0.csv", {true, 4}},
      {"post_hasTag_tag_0_0.csv", {false, 0}},
      {"comment_hasTag_tag_0_0.csv", {false, 0}},
  };
  return props;
}

void ScanSnbExplicitEdges(const fs::path& root, SnbBase* out) {
  for (const auto& [file, prop] : SnbExplicitProp()) {
    const uint64_t rows = CountDataRows(root / "dynamic" / file);
    out->stats.edge_count += rows;
    out->stats.edge_type_bytes +=
        rows * SnbEdgeTypeLength(SnbExplicitTypeFromFile(file));
    auto& c = out->explicit_edges[file];
    c.rows += rows;
    if (prop.first) {
      c.nonzero += rows;
      out->stats.edge_prop_bytes += rows * prop.second;
    } else {
      c.zero += rows;
    }
  }
}

void ScanSnbUpdatePerson(const fs::path& path, SnbBase* out) {
  std::ifstream in(path, std::ios::binary);
  std::string line;
  while (std::getline(in, line)) {
    if (!line.empty() && line.back() == '\r') line.pop_back();
    if (line.empty()) continue;
    const auto f = SplitViews(line);
    if (f.size() <= 3 || f[2] != "1") continue;
    ++out->stats.node_count;
    out->stats.node_type_bytes += 6;  // Person
    out->stats.node_prop_bytes += PayloadBytes({
        f.size() > 4 ? f[4] : std::string_view{},
        f.size() > 5 ? f[5] : std::string_view{},
        f.size() > 6 ? f[6] : std::string_view{},
        f.size() > 7 ? f[7] : std::string_view{},
        f.size() > 8 ? f[8] : std::string_view{},
        f.size() > 9 ? f[9] : std::string_view{},
        f.size() > 10 ? f[10] : std::string_view{},
        f.size() > 12 ? f[12] : std::string_view{},
        f.size() > 13 ? f[13] : std::string_view{},
    });
    ++out->stats.edge_count;  // person_isLocatedIn
    out->stats.edge_type_bytes += SnbEdgeTypeLength("person_isLocatedIn_place");
    if (f.size() > 14) {
      const uint64_t n = SplitList(f[14], ';').size();
      out->stats.edge_count += n;
      out->stats.edge_type_bytes +=
          n * SnbEdgeTypeLength("person_hasInterest_tag");
    }
    if (f.size() > 15) {
      for (const auto item : SplitList(f[15], ';')) {
        const size_t comma = item.find(',');
        if (comma != std::string_view::npos) {
          ++out->stats.edge_count;
          out->stats.edge_type_bytes +=
              SnbEdgeTypeLength("person_studyAt_organisation");
          out->stats.edge_prop_bytes += item.size() - comma - 1;
        }
      }
    }
    if (f.size() > 16) {
      for (const auto item : SplitList(f[16], ';')) {
        const size_t comma = item.find(',');
        if (comma != std::string_view::npos) {
          ++out->stats.edge_count;
          out->stats.edge_type_bytes +=
              SnbEdgeTypeLength("person_workAt_organisation");
          out->stats.edge_prop_bytes += item.size() - comma - 1;
        }
      }
    }
  }
}

void ScanSnbUpdateForum(const fs::path& path, SnbBase* out) {
  std::ifstream in(path, std::ios::binary);
  std::string line;
  while (std::getline(in, line)) {
    if (!line.empty() && line.back() == '\r') line.pop_back();
    if (line.empty()) continue;
    const auto f = SplitViews(line);
    if (f.size() <= 2) continue;
    const auto op = f[2];
    if (op == "2" || op == "3") {
      ++out->stats.edge_count;
      out->stats.edge_type_bytes +=
          SnbEdgeTypeLength(op == "2" ? "person_likes_post"
                                      : "person_likes_comment");
      if (f.size() > 5 && ValidValue(f[5])) out->stats.edge_prop_bytes += f[5].size();
    } else if (op == "4") {
      ++out->stats.node_count;
      out->stats.node_type_bytes += 5;  // Forum
      out->stats.node_prop_bytes += PayloadBytes({
          f.size() > 4 ? f[4] : std::string_view{},
          f.size() > 5 ? f[5] : std::string_view{},
      });
      ++out->stats.edge_count;  // moderator
      out->stats.edge_type_bytes +=
          SnbEdgeTypeLength("forum_hasModerator_person");
      if (f.size() > 7) {
        const uint64_t n = SplitList(f[7], ';').size();
        out->stats.edge_count += n;
        out->stats.edge_type_bytes += n * SnbEdgeTypeLength("forum_hasTag_tag");
      }
    } else if (op == "5") {
      ++out->stats.edge_count;
      out->stats.edge_type_bytes +=
          SnbEdgeTypeLength("forum_hasMember_person");
      if (f.size() > 5 && ValidValue(f[5])) out->stats.edge_prop_bytes += f[5].size();
    } else if (op == "6") {
      ++out->stats.node_count;
      out->stats.node_type_bytes += 4;  // Post
      out->stats.node_prop_bytes += PayloadBytes({
          f.size() > 4 ? f[4] : std::string_view{},
          f.size() > 5 ? f[5] : std::string_view{},
          f.size() > 6 ? f[6] : std::string_view{},
          f.size() > 7 ? f[7] : std::string_view{},
          f.size() > 8 ? f[8] : std::string_view{},
          f.size() > 9 ? f[9] : std::string_view{},
          f.size() > 10 ? f[10] : std::string_view{},
      });
      out->stats.edge_count += 3;  // creator, forum, place
      out->stats.edge_type_bytes +=
          SnbEdgeTypeLength("post_hasCreator_person") +
          SnbEdgeTypeLength("post_isPartOf_forum") +
          SnbEdgeTypeLength("post_isLocatedIn_place");
      if (f.size() > 14) {
        const uint64_t n = SplitList(f[14], ';').size();
        out->stats.edge_count += n;
        out->stats.edge_type_bytes += n * SnbEdgeTypeLength("post_hasTag_tag");
      }
    } else if (op == "7") {
      ++out->stats.node_count;
      out->stats.node_type_bytes += 7;  // Comment
      out->stats.node_prop_bytes += PayloadBytes({
          f.size() > 4 ? f[4] : std::string_view{},
          f.size() > 5 ? f[5] : std::string_view{},
          f.size() > 6 ? f[6] : std::string_view{},
          f.size() > 7 ? f[7] : std::string_view{},
          f.size() > 8 ? f[8] : std::string_view{},
      });
      out->stats.edge_count += 2;  // creator, place
      out->stats.edge_type_bytes +=
          SnbEdgeTypeLength("comment_hasCreator_person") +
          SnbEdgeTypeLength("comment_isLocatedIn_place");
      if (f.size() > 11 && ValidValue(f[11])) {
        ++out->stats.edge_count;
        out->stats.edge_type_bytes += SnbEdgeTypeLength("comment_replyOf_post");
      }
      if (f.size() > 12 && ValidValue(f[12])) {
        ++out->stats.edge_count;
        out->stats.edge_type_bytes +=
            SnbEdgeTypeLength("comment_replyOf_comment");
      }
      if (f.size() > 13) {
        const uint64_t n = SplitList(f[13], ';').size();
        out->stats.edge_count += n;
        out->stats.edge_type_bytes += n * SnbEdgeTypeLength("comment_hasTag_tag");
      }
    } else if (op == "8") {
      ++out->stats.edge_count;
      out->stats.edge_type_bytes += SnbEdgeTypeLength("person_knows_person");
      if (f.size() > 5 && ValidValue(f[5])) out->stats.edge_prop_bytes += f[5].size();
    }
  }
}

SnbBase ScanSnbBase(const fs::path& root) {
  SnbBase out;
  for (const auto& [file, props] : SnbNodeProps()) {
    ScanSnbNodeFile(root / file, props, file, &out);
  }
  ScanSnbExplicitEdges(root, &out);
  for (const auto& [type, rows] : out.implicit_rows) {
    out.stats.edge_count += rows;
    out.stats.edge_type_bytes += rows * SnbEdgeTypeLength(type);
  }
  ScanSnbUpdatePerson(root / "updateStream_0_0_person.csv", &out);
  ScanSnbUpdateForum(root / "updateStream_0_0_forum.csv", &out);
  return out;
}

uint64_t SnbExplicitExtraCount(const fs::path& schema,
                               const std::string& file) {
  if (!fs::exists(schema)) return 0;
  const uint64_t occurrences = CountOccurrences(schema, file);
  return occurrences == 0 ? 0 : occurrences - 1;
}

uint64_t SnbImplicitExtraCount(const fs::path& schema,
                               const std::string& type) {
  if (!fs::exists(schema)) return 0;
  const uint64_t occurrences = CountOccurrences(schema, type);
  return occurrences == 0 ? 0 : occurrences - 1;
}

uint64_t SnbVariantBytes(const SnbBase& base,
                         const fs::path& dataset_base,
                         const std::string& scale,
                         int target) {
  uint64_t extra = 0;
  if (target > 0) {
    const fs::path schema = dataset_base / ("sf" + scale + "+" +
                                            std::to_string(target)) /
                            "plus_schema.json";
    for (const auto& [file, counters] : base.explicit_edges) {
      const uint64_t k = SnbExplicitExtraCount(schema, file);
      if (k == 0) continue;
      extra += counters.nonzero * (k * 13ULL + k);
      extra += counters.zero * (k * 13ULL + (k == 0 ? 0 : k - 1ULL));
    }
    for (const auto& [type, rows] : base.implicit_rows) {
      const uint64_t k = SnbImplicitExtraCount(schema, type);
      if (k == 0) continue;
      extra += rows * (k * 13ULL + (k == 0 ? 0 : k - 1ULL));
    }
  }
  return LogicalBytes(base.stats) + extra;
}

std::string SnbDatasetDir(const fs::path& base, const std::string& scale) {
  return (base / ("sf" + scale) /
          ("social_network-sf" + scale +
           "-CsvCompositeMergeForeign-LongDateFormatter")).string();
}

void PrintRow(const std::string& dataset,
              const std::string& variant,
              uint64_t nodes,
              uint64_t edges,
              uint64_t bytes) {
  const double gib = static_cast<double>(bytes) / 1073741824.0;
  const double gb = static_cast<double>(bytes) / 1000000000.0;
  std::cout << dataset << '\t' << variant << '\t' << nodes << '\t' << edges
            << '\t' << bytes << '\t' << std::fixed << std::setprecision(3)
            << gib << '\t' << gb << '\n';
}

int main(int argc, char** argv) {
  const fs::path fin_base = "datasets/FinBench";
  const fs::path snb_base = "datasets/Snb-v1";
  const std::vector<std::string> scales = {"0.1", "30", "100"};
  const std::vector<int> targets = {0, 16, 32, 64, 128};

  const auto selected = [&](const std::string& dataset,
                            const std::string& scale) {
    if (argc <= 1) {
      return true;
    }
    std::string ds = dataset;
    std::transform(ds.begin(), ds.end(), ds.begin(), [](unsigned char c) {
      return static_cast<char>(std::tolower(c));
    });
    for (int i = 1; i < argc; ++i) {
      std::string arg = argv[i];
      std::transform(arg.begin(), arg.end(), arg.begin(), [](unsigned char c) {
        return static_cast<char>(std::tolower(c));
      });
      const std::string fin_key = "finbench:" + scale;
      const std::string snb_key = "snb:" + scale;
      const std::string snb_v1_key = "snb-v1:" + scale;
      if ((ds == "finbench" && arg == fin_key) ||
          (ds == "snb-v1" && (arg == snb_key || arg == snb_v1_key))) {
        return true;
      }
    }
    return false;
  };

  std::cout << "DATASET\tVARIANT\tNODES\tEDGES_EXPANDED\tLOGICAL_BYTES\tLOGICAL_GiB\tLOGICAL_GB\n";
  for (const auto& scale : scales) {
    if (!selected("FinBench", scale)) {
      continue;
    }
    std::cerr << "[scan] FinBench sf" << scale << "\n";
    const auto base = ScanFinBenchBase(fin_base / ("sf" + scale));
    for (const int target : targets) {
      const std::string variant =
          target == 0 ? "sf" + scale : "sf" + scale + "+" + std::to_string(target);
      const uint64_t bytes =
          target == 0 ? LogicalBytes(base.stats)
                      : FinBenchVariantBytes(base, fin_base, scale, target);
      PrintRow("FinBench", variant, base.stats.node_count,
               base.stats.edge_count, bytes);
    }
  }
  for (const auto& scale : scales) {
    if (!selected("SNB-v1", scale)) {
      continue;
    }
    std::cerr << "[scan] SNB sf" << scale << "\n";
    const auto base = ScanSnbBase(SnbDatasetDir(snb_base, scale));
    for (const int target : targets) {
      const std::string variant =
          target == 0 ? "sf" + scale : "sf" + scale + "+" + std::to_string(target);
      const uint64_t bytes =
          target == 0 ? LogicalBytes(base.stats)
                      : SnbVariantBytes(base, snb_base, scale, target);
      PrintRow("SNB-v1", variant, base.stats.node_count,
               base.stats.edge_count, bytes);
    }
  }
  return 0;
}
