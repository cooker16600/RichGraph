#pragma once
#include "core/types.h"
namespace lsmgraph {

  class EdgeIteratorBase {
   public:
      virtual ~EdgeIteratorBase() = default;
      virtual bool valid() const = 0;
      virtual void next() = 0;
      virtual VertexId_t dst_id() const = 0;
      virtual SequenceNumber_t sequence() const = 0;
      virtual Marker_t marker() const = 0;
      virtual bool empty() const = 0;
      virtual size_t size() const = 0;
      virtual EdgeProperty_t edge_data(int id) const = 0;
      virtual FileId_t get_fid() const = 0;
      // 默认按“出边”处理；支持方向过滤的迭代器会覆盖该接口。
      virtual bool is_out() const { return true; }
      // 默认类型为0；支持类型过滤的迭代器会覆盖该接口。
      virtual uint8_t edge_type() const { return 0; }

    SequenceNumber_t newest_edge = 0;
#ifndef NO_VIRTUAL
      virtual bool IsMemTable() const = 0;
#endif
  };

}
