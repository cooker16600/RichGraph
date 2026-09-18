#include "core/flags.h"

#include <gflags/gflags.h>


DEFINE_bool(OPEN_SSTDATA_CACHE, true, "load sstable to mmap file");
DEFINE_bool(LOAD_OLD_DATA, false, "load old sstdata before running");
DEFINE_bool(open_rw_concurrent, false, "open read and write concurrent");
DEFINE_bool(directed, true, "if directed graph is ture, else false");
DEFINE_bool(shuffle_edge, true, "if shuffle_edge");
DEFINE_double(fresh_rate, 1, "the freshness of the queried dataset");

DEFINE_string(SST_store_mode, "var", "property storage mode");
DEFINE_string(get_edges_type, "iterator", "vector/iterator: return type of test_edges()");
DEFINE_string(dataset_name, "", "dataset name");
DEFINE_string(dataset_path, "", "input dataset path");
DEFINE_string(db_path, "./richgraph_db", "database directory");
DEFINE_string(alg_app, "all", "graph algorithm name");
DEFINE_string(mmap_path, "", "mmap file path");

DEFINE_uint32(memtable_size, 818401, "size of memtable");
DEFINE_uint32(memproperty_size, 5162220, "size of memproperty");
DEFINE_uint32(sstable_size, 1995001 * 2, "size of sstable");
DEFINE_uint32(sub_property_num, 64, "numbers of sub property");
DEFINE_uint32(max_property_length, 10, "max length of each property");
DEFINE_uint32(max_subcompactions, 8, "max_subcompactions thread num");
DEFINE_uint32(multi_level_merge_degree, 5, "multi level merege small degree vertex's degree");
DEFINE_uint32(thread_num, 16, "app thread num");
DEFINE_uint32(memtable_num, 2, "memtable num");
DEFINE_uint32(memproperty_num, 2, "memproperty num");
DEFINE_bool(enable_memproperty, true,
            "enable in-memory property update buffers");
DEFINE_uint32(khop, 1, "khop num");
DEFINE_uint32(khop_num, 10000, "khop query num");
DEFINE_uint32(test_times, 1, "khop num");
DEFINE_uint64(read_p, 10, "read edges of 1/p all edges");
DEFINE_uint32(del_rate, 0, "delete edges of 1/p all edges");
DEFINE_uint32(reserve_node, 2, "reserve node num of skiplist");
DEFINE_double(ppr_threshold, 0.001, "ppr");
DEFINE_uint32(ppr_maxstep, 50, "ppr max run step");
DEFINE_uint64(source, 0, "source node of the app");

DEFINE_uint32(large_vertex, 50, "large degree vertex threshold");

DEFINE_bool(support_mulversion,true, "support multiple versions");
DEFINE_bool(use_csr_disk, false,
            "enable single-level CSR disk mode (parallel to legacy LSM mode)");
DEFINE_bool(richgraph_verbose, false,
            "print legacy storage diagnostics and configuration dumps");
DEFINE_uint32(csr_topo_soft_limit_mb, 64,
              "soft limit (MB) of one CSR topology SST file");
