#pragma once

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

#include "core/config.h"
#include "core/flags.h"
#include "util/slice.h"

namespace lsmgraph {

// Centralizes fixed-width property layout rules. By default all property
// columns use the process-wide width. ScopedFixedPropertyLayout can override
// column count and widths for the current thread.
inline thread_local const std::vector<uint32_t>* tls_fixed_property_lengths =
    nullptr;

inline const std::vector<uint32_t>* GetCurrentFixedPropertyLayoutOverride() {
  return tls_fixed_property_lengths;
}

class ScopedFixedPropertyLayout {
 public:
  explicit ScopedFixedPropertyLayout(
      const std::vector<uint32_t>* property_lengths)
      : prev_(tls_fixed_property_lengths) {
    if (property_lengths != nullptr && !property_lengths->empty()) {
      tls_fixed_property_lengths = property_lengths;
    } else {
      tls_fixed_property_lengths = nullptr;
    }
  }

  ~ScopedFixedPropertyLayout() {
    tls_fixed_property_lengths = prev_;
  }

 private:
  const std::vector<uint32_t>* prev_;
};

inline int GetActiveSubPropertyNum() {
  if (tls_fixed_property_lengths != nullptr
      && !tls_fixed_property_lengths->empty()) {
    return static_cast<int>(tls_fixed_property_lengths->size());
  }
  return static_cast<int>(FLAGS_sub_property_num);
}

inline std::vector<uint32_t> BuildUniformSubPropertyLengths(
    int property_num,
    uint32_t fixed_length) {
  const int count = std::max(1, property_num);
  return std::vector<uint32_t>(static_cast<size_t>(count),
                               std::max<uint32_t>(1, fixed_length));
}

inline uint32_t GetSubPropertyFixedLength(int sub_property_id) {
  if (sub_property_id < 0) {
    return std::max<uint32_t>(1, FLAGS_max_property_length);
  }
  if (tls_fixed_property_lengths != nullptr
      && sub_property_id < static_cast<int>(tls_fixed_property_lengths->size())) {
    return (*tls_fixed_property_lengths)[static_cast<size_t>(sub_property_id)];
  }
  return std::max<uint32_t>(1, FLAGS_max_property_length);
}

// Converts a byte offset in the packed edge-body file to an edge ordinal.
inline size_t GetEdgeOrdinalFromBodyOffset(uint32_t body_offset_bytes) {
  assert(body_offset_bytes % EDGEBODY_SIZE == 0);
  return body_offset_bytes / EDGEBODY_SIZE;
}

// Returns a column-file offset for a fixed-width property slot.
inline size_t GetSubPropertyOffsetByEdgeOrdinal(size_t edge_ordinal,
                                                int sub_property_id) {
  return edge_ordinal * static_cast<size_t>(GetSubPropertyFixedLength(sub_property_id));
}

// Writes one fixed-width slot, truncating oversized values and zero-padding
// shorter values.
inline void WritePaddedSubPropertySlot(char* slot_begin,
                                       const char* src,
                                       size_t src_len,
                                       int sub_property_id) {
  const uint32_t slot_len = GetSubPropertyFixedLength(sub_property_id);
  std::memset(slot_begin, 0, slot_len);
  const size_t copy_len = std::min<size_t>(slot_len, src_len);
  if (copy_len > 0) {
    std::memcpy(slot_begin, src, copy_len);
  }
}

inline void WritePaddedSubPropertySlot(char* slot_begin,
                                       const Slice& src,
                                       int sub_property_id) {
  WritePaddedSubPropertySlot(slot_begin, src.data(), src.size(), sub_property_id);
}

// Removes storage padding from a logical property value.
inline void TrimTrailingZero(std::string* value) {
  while (!value->empty() && value->back() == '\0') {
    value->pop_back();
  }
}

// Reads a logical property value and removes trailing zero padding.
inline void ReadSubPropertyFromSlot(const char* slot_begin,
                                    int sub_property_id,
                                    std::string* out) {
  const uint32_t slot_len = GetSubPropertyFixedLength(sub_property_id);
  out->assign(slot_begin, slot_len);
  TrimTrailingZero(out);
}

}  // namespace lsmgraph
