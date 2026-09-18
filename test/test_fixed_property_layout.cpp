#include "core/fixed_property_layout.h"

#include <cassert>
#include <cstring>
#include <iostream>
#include <string>
#include <vector>

int main() {
  using namespace lsmgraph;

  // 约定：当前固定长度为 10。
  assert(GetSubPropertyFixedLength(0) == 10);
  assert(GetSubPropertyFixedLength(1) == 10);

  // 模拟 2 条边、3 个子属性的属性文件布局。
  const size_t edge_num = 2;
  const int sub_property_num = 3;
  std::vector<std::vector<char>> pfiles(
      sub_property_num,
      std::vector<char>(edge_num * GetSubPropertyFixedLength(0), '\0'));

  // edge0: ["a", "BBBBBBBBBB", "tail_0"]
  {
    WritePaddedSubPropertySlot(
        pfiles[0].data() + GetSubPropertyOffsetByEdgeOrdinal(0, 0),
        "a", 1, 0);
    WritePaddedSubPropertySlot(
        pfiles[1].data() + GetSubPropertyOffsetByEdgeOrdinal(0, 1),
        "BBBBBBBBBB", 10, 1);
    WritePaddedSubPropertySlot(
        pfiles[2].data() + GetSubPropertyOffsetByEdgeOrdinal(0, 2),
        "tail_0", 6, 2);
  }

  // edge1: ["abcdef", "b", "123456789ABCDE"]，第三个属性会被截断到 10 字节。
  {
    WritePaddedSubPropertySlot(
        pfiles[0].data() + GetSubPropertyOffsetByEdgeOrdinal(1, 0),
        "abcdef", 6, 0);
    WritePaddedSubPropertySlot(
        pfiles[1].data() + GetSubPropertyOffsetByEdgeOrdinal(1, 1),
        "b", 1, 1);
    WritePaddedSubPropertySlot(
        pfiles[2].data() + GetSubPropertyOffsetByEdgeOrdinal(1, 2),
        "123456789ABCDE", 14, 2);
  }

  // 校验偏移公式。
  assert(GetSubPropertyOffsetByEdgeOrdinal(0, 0) == 0);
  assert(GetSubPropertyOffsetByEdgeOrdinal(1, 0) == 10);
  assert(GetSubPropertyOffsetByEdgeOrdinal(1, 2) == 10);

  // Verify that the remainder of the fixed-width slot is zero-filled.
  assert(pfiles[0][0] == 'a');
  for (size_t i = 1; i < 10; ++i) {
    assert(pfiles[0][i] == '\0');
  }

  // 校验读取去补零与截断逻辑。
  {
    std::string out;
    ReadSubPropertyFromSlot(
        pfiles[0].data() + GetSubPropertyOffsetByEdgeOrdinal(0, 0), 0, &out);
    assert(out == "a");

    ReadSubPropertyFromSlot(
        pfiles[1].data() + GetSubPropertyOffsetByEdgeOrdinal(0, 1), 1, &out);
    assert(out == "BBBBBBBBBB");

    ReadSubPropertyFromSlot(
        pfiles[2].data() + GetSubPropertyOffsetByEdgeOrdinal(0, 2), 2, &out);
    assert(out == "tail_0");

    ReadSubPropertyFromSlot(
        pfiles[0].data() + GetSubPropertyOffsetByEdgeOrdinal(1, 0), 0, &out);
    assert(out == "abcdef");

    ReadSubPropertyFromSlot(
        pfiles[1].data() + GetSubPropertyOffsetByEdgeOrdinal(1, 1), 1, &out);
    assert(out == "b");

    ReadSubPropertyFromSlot(
        pfiles[2].data() + GetSubPropertyOffsetByEdgeOrdinal(1, 2), 2, &out);
    assert(out == "123456789A");  // 10 字节截断结果
  }

  std::cout << "test_fixed_property_layout passed." << std::endl;
  return 0;
}
