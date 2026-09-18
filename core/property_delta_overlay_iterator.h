#pragma once

#include <memory>
#include <utility>

#include "core/edge_iterator_base.h"
#include "core/property_delta_store.h"

namespace lsmgraph {

// Adds query-visible property deltas to an existing topology iterator.  The
// this adapter only replaces the selected property's value when a visible
// update exists.  Holding the immutable read view also pins a replacement
// base mapping for the lifetime of the scan.
class PropertyDeltaOverlayIterator final : public EdgeIteratorBase {
 public:
  PropertyDeltaOverlayIterator(
      VertexId_t src,
      std::shared_ptr<EdgeIteratorBase> base,
      std::shared_ptr<const PropertyDeltaReadView> read_view,
      PropertyCommitSequence read_sequence = kLatestPropertyCommit)
      : src_(src),
        base_(std::move(base)),
        read_view_(std::move(read_view)),
        read_sequence_(read_sequence) {}

  bool valid() const override { return base_ != nullptr && base_->valid(); }
  void next() override { base_->next(); }
  VertexId_t dst_id() const override { return base_->dst_id(); }
  SequenceNumber_t sequence() const override { return base_->sequence(); }
  Marker_t marker() const override { return base_->marker(); }
  bool empty() const override { return base_->empty(); }
  size_t size() const override { return base_->size(); }
  FileId_t get_fid() const override { return base_->get_fid(); }
  bool is_out() const override { return base_->is_out(); }
  uint8_t edge_type() const override { return base_->edge_type(); }
  bool IsMemTable() const override { return base_->IsMemTable(); }

  EdgeProperty_t edge_data(int id) const override {
    if (!valid() || read_view_ == nullptr) return base_->edge_data(id);
    std::string value;
    if (read_view_->Lookup(src_,
                           base_->dst_id(),
                           base_->sequence(),
                           base_->is_out(),
                           base_->edge_type(),
                           read_sequence_,
                           &value)) {
      return value;
    }
    return base_->edge_data(id);
  }

 private:
  VertexId_t src_{0};
  std::shared_ptr<EdgeIteratorBase> base_;
  std::shared_ptr<const PropertyDeltaReadView> read_view_;
  PropertyCommitSequence read_sequence_{kLatestPropertyCommit};
};

}  // namespace lsmgraph
