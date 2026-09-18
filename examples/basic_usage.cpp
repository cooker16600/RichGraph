#include <richgraph/graph_db.h>

#include <iostream>
#include <memory>
#include <string>
#include <unordered_map>

int main(int argc, char** argv) {
  if (argc != 3) {
    std::cerr << "usage: " << argv[0] << " <database-dir> <schema.yaml>\n";
    return 2;
  }

  lsmgraph::GraphDbOptions options;
  options.load_existing = false;
  options.default_memtable_capacity = 1024;
  options.property_updates.buffer_capacity_records = 1024;
  options.property_updates.buffer_capacity_bytes = 4U * 1024U * 1024U;

  std::unique_ptr<lsmgraph::GraphDb> db;
  std::string error;
  const auto status =
      lsmgraph::GraphDb::OpenFromYaml(argv[1], argv[2], options, &db, &error);
  if (status != lsmgraph::Status::kOk) {
    std::cerr << "failed to open RichGraph (" << lsmgraph::StatusName(status)
              << "): " << error << '\n';
    return 1;
  }
  db->InitVerticesUpTo(2);
  if (db->PutEdge(0, 1, {{"amount", "42"}}) != lsmgraph::Status::kOk) {
    std::cerr << "failed to insert edge\n";
    return 1;
  }

  std::unordered_map<std::string, std::string> properties;
  if (db->GetEdge(0, 1, {"amount"}, &properties) != lsmgraph::Status::kOk) {
    std::cerr << "failed to read edge\n";
    return 1;
  }

  std::cout << "amount=" << properties.at("amount") << '\n';
  return 0;
}
