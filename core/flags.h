#ifndef FLAGS_H
#define FLAGS_H

#include <gflags/gflags_declare.h>

DECLARE_bool(OPEN_SSTDATA_CACHE);
DECLARE_bool(LOAD_OLD_DATA);
DECLARE_bool(open_rw_concurrent);
DECLARE_bool(directed);
DECLARE_bool(shuffle_edge);
DECLARE_double(fresh_rate);
DECLARE_double(ppr_threshold);

DECLARE_string(get_edges_type);
DECLARE_string(dataset_name);
DECLARE_string(dataset_path);
DECLARE_string(db_path);
DECLARE_string(alg_app);
DECLARE_string(mmap_path);
DECLARE_string(SST_store_mode);

DECLARE_uint32(memtable_size);
DECLARE_uint32(memproperty_size);
DECLARE_uint32(sstable_size);
DECLARE_uint32(sub_property_num);
DECLARE_uint32(max_property_length);
DECLARE_uint32(max_subcompactions);
DECLARE_uint32(multi_level_merge_degree);
DECLARE_uint32(thread_num);
DECLARE_uint32(memtable_num);
DECLARE_uint32(memproperty_num);
DECLARE_bool(enable_memproperty);
DECLARE_uint32(khop);
DECLARE_uint32(khop_num);
DECLARE_uint32(test_times);
DECLARE_uint64(read_p);
DECLARE_uint32(del_rate);
DECLARE_uint32(reserve_node);
DECLARE_uint32(ppr_maxstep);
DECLARE_uint64(source);


DECLARE_uint32(large_vertex);

DECLARE_bool(support_mulversion);
DECLARE_bool(use_csr_disk);
DECLARE_bool(richgraph_verbose);
DECLARE_uint32(csr_topo_soft_limit_mb);
#endif  // FLAGS_H
