#pragma once

#include <cstdint>
#include <limits>
#include <string_view>

namespace lsmgraph {

using VertexId_t = std::uint64_t;
using SequenceNumber_t = std::int64_t;
using FileId_t = std::uint32_t;
using PropertyCommitSequence = std::uint64_t;

inline constexpr VertexId_t kInvalidVertexId =
    (static_cast<VertexId_t>(UINT16_MAX) << 32U) + UINT32_MAX;
inline constexpr FileId_t kInvalidFileId =
    std::numeric_limits<FileId_t>::max() >> 1U;
inline constexpr SequenceNumber_t kMaxSequence = static_cast<SequenceNumber_t>(
    (static_cast<VertexId_t>(UINT16_MAX) << 32U) + UINT32_MAX);
inline constexpr PropertyCommitSequence kLatestPropertyCommit =
    std::numeric_limits<PropertyCommitSequence>::max();

// Compatibility spellings used by the original benchmark sources.
inline constexpr VertexId_t INVALID_VERTEX_ID = kInvalidVertexId;
inline constexpr FileId_t INVALID_File_ID = kInvalidFileId;
inline constexpr SequenceNumber_t MAX_SEQ_ID = kMaxSequence;

// Kept as an unscoped enum for source compatibility with the research
// prototype. New code should qualify values as Status::kOk, etc.
enum Status : unsigned char {
  kOk = 0,
  kDelete = 1,
  kNotFound = 2,
  kInvalidArgument = 3,
  kIOError = 4,
  kCorruption = 5,
  kBackgroundError = 6,
};

[[nodiscard]] constexpr std::string_view StatusName(Status status) noexcept {
  switch (status) {
    case Status::kOk:
      return "ok";
    case Status::kDelete:
      return "deleted";
    case Status::kNotFound:
      return "not_found";
    case Status::kInvalidArgument:
      return "invalid_argument";
    case Status::kIOError:
      return "io_error";
    case Status::kCorruption:
      return "corruption";
    case Status::kBackgroundError:
      return "background_error";
  }
  return "unknown";
}

enum class EdgeInsertMode : std::uint8_t {
  kSingle = 0,
  kBidirectional = 1,
};

struct PropertyUpdateManagerStats {
  std::uint64_t submitted_records{0};
  std::uint64_t submitted_bytes{0};
  std::uint64_t buffers_flushed{0};
  std::uint64_t delta_batches_published{0};
  std::uint64_t flush_failures{0};
  std::uint64_t blocked_submissions{0};
  std::uint64_t pending_keys{0};
};

}  // namespace lsmgraph
