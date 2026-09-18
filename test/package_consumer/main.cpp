#include <richgraph/graph_db.h>

#include <string_view>

int main() {
  const lsmgraph::GraphDbOptions options;
  if (!options.Validate().empty()) {
    return 1;
  }
  return lsmgraph::StatusName(lsmgraph::Status::kOk) == std::string_view("ok")
             ? 0
             : 2;
}
