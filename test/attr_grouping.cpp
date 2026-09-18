/**
 * Attribute Grouping Algorithm for RichGraph Edge Properties
 *
 * Implements the workload-aware greedy merging algorithm described in the paper.
 * Analyzes FinBench sf30 read/write workloads to determine optimal edge property
 * groupings that minimize I/O cost.
 *
 * Build: g++ -std=c++17 -O2 -o attr_grouping attr_grouping.cpp
 */

#include <algorithm>
#include <cmath>
#include <iomanip>
#include <iostream>
#include <map>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

using namespace std;

// Configuration Parameters
constexpr double PAGE_SIZE_B      = 4096.0;   // B:  page size (bytes)
constexpr double TOPO_ENTRY_SIZE  = 40.0;     // Se: sizeof(NeighBors) = 40 bytes
constexpr double AVG_PROP_SLOT    = 15.0;     // s_bar: average fixed-length (bytes)

// Data Definitions from FinBench sf30

// 14 edge tail properties (edgeLabel already removed).
const vector<string> ALL_PROPS = {
    "createTime",      //  0
    "dependencyTime",  //  1
    "amount",          //  2
    "comment",         //  3
    "orderNum",        //  4
    "payType",         //  5
    "goodsType",       //  6
    "fromType",        //  7
    "toType",          //  8
    "loanAmount",      //  9
    "org",             // 10
    "relation",        // 11
    "ratio",           // 12
    "location",        // 13
};

// Per-property fixed lengths (from dataset analysis).
const unordered_map<string, uint32_t> PROP_LENGTHS = {
    {"createTime",      13},
    {"dependencyTime",  13},
    {"amount",          11},
    {"comment",         20},   // truncated
    {"orderNum",        15},
    {"payType",         22},
    {"goodsType",       22},
    {"fromType",        22},
    {"toType",          10},
    {"loanAmount",      10},
    {"org",             25},
    {"relation",        18},
    {"ratio",           22},
    {"location",        20},   // truncated
};

// --- Relation definitions with property sets and edge counts ---
struct Relation {
    string name;
    vector<string> props;      // properties written together
    uint64_t total_edges;      // snapshot + incremental
};

const vector<Relation> RELATIONS = {
    {"AccountTransferAccount",
     {"createTime","amount","orderNum","comment","payType","goodsType","dependencyTime"},
     7910958},
    {"AccountWithdrawAccount",
     {"createTime","amount","fromType","toType","comment","dependencyTime"},
     10005719},
    {"AccountRepayLoan",
     {"createTime","amount","comment","dependencyTime"},
     1785011},
    {"LoanDepositAccount",
     {"createTime","amount","comment","dependencyTime"},
     2551090},
    {"MediumSignInAccount",
     {"createTime","location","comment","dependencyTime"},
     4562637},
    {"CompanyOwnAccount",
     {"createTime","comment"},
     660329},
    {"PersonOwnAccount",
     {"createTime","comment"},
     658171},
    {"CompanyApplyLoan",
     {"createTime","loanAmount","org","comment"},
     396394},
    {"PersonApplyLoan",
     {"createTime","loanAmount","org","comment"},
     396244},
    {"CompanyInvestCompany",
     {"ratio","createTime","comment"},
     900262},
    {"PersonInvestCompany",
     {"ratio","createTime","comment"},
     900439},
    {"CompanyGuaranteeCompany",
     {"createTime","relation","comment"},
     239714},
    {"PersonGuaranteePerson",
     {"createTime","relation","comment"},
     239847},
};

// --- Query definitions with accessed properties and frequencies ---
struct QueryDef {
    int    id;
    vector<string> accessed_props;   // properties read by this query
    uint64_t freq;                    // param_rows as frequency proxy
};

const vector<QueryDef> QUERIES = {
    { 1, {"createTime"},                               9994},
    { 2, {"createTime"},                               2753},
    { 3, {"createTime"},                              11054},
    { 4, {"createTime"},                              11054},
    { 5, {"createTime"},                               2753},
    { 6, {"createTime","amount"},                       878},
    { 7, {"createTime","amount"},                     11054},
    { 8, {"createTime","amount"},                      5706},
    { 9, {"createTime","amount"},                     11054},
    {10, {},                                           2635},  // topology-only
    {11, {},                                           1501},  // topology-only
    {12, {"createTime","amount"},                      2753},
};

// Property index helpers
int propIndex(const string& name) {
    for (size_t i = 0; i < ALL_PROPS.size(); ++i)
        if (ALL_PROPS[i] == name) return static_cast<int>(i);
    return -1;
}

// Group representation
struct Group {
    int id;
    set<string> props;

    // Read set: query ids that read AT LEAST ONE property in this group
    set<int> read_set;

    // Write set: relation indices that write AT LEAST ONE property in this group
    set<int> write_set;

    // Number of properties
    size_t size() const { return props.size(); }
};

// Workload statistics collection
struct WorkloadStats {
    // prop_read_sets[p]: query ids that read property p
    unordered_map<string, set<int>> prop_read_sets;

    // prop_write_sets[p]: relation indices that include property p
    unordered_map<string, set<int>> prop_write_sets;

    // rel_edge_counts[r]: total edges for relation r
    unordered_map<int, uint64_t> rel_edge_counts;

    // query_freqs[q]: frequency of query q (0-based index into QUERIES)
    vector<uint64_t> query_freqs;

    void collect() {
        // --- Reads: build prop → queries ---
        for (size_t qi = 0; qi < QUERIES.size(); ++qi) {
            const auto& q = QUERIES[qi];
            for (const auto& p : q.accessed_props) {
                prop_read_sets[p].insert(static_cast<int>(qi));
            }
        }

        // --- Writes: build prop → relations ---
        for (size_t ri = 0; ri < RELATIONS.size(); ++ri) {
            const auto& rel = RELATIONS[ri];
            rel_edge_counts[static_cast<int>(ri)] = rel.total_edges;
            for (const auto& p : rel.props) {
                prop_write_sets[p].insert(static_cast<int>(ri));
            }
        }

        // --- Query frequencies ---
        for (const auto& q : QUERIES) {
            query_freqs.push_back(q.freq);
        }
    }
};

// Algorithm: Gain computation and greedy merging
class AttributeGrouper {
public:
    AttributeGrouper() {
        stats_.collect();
        initGroups();
    }

    void run() {
        printHeader();
        printInitialState();

        int round = 0;
        while (true) {
            ++round;
            double best_gain = 0.0;
            pair<int,int> best_pair = {-1, -1};

            // Enumerate all pairs of groups
            cout << "\n--- Round " << round << " ("
                 << groups_.size() << " groups, "
                 << (groups_.size()*(groups_.size()-1)/2) << " pairs) ---\n";
            cout << left
                 << setw(38) << "Gi"
                 << setw(38) << "Gj"
                 << right << setw(10) << "ReadSave"
                 << setw(10) << "WriteSave"
                 << setw(10) << "WriteLoss"
                 << setw(10) << "GAIN"
                 << "  Merge?\n";
            cout << string(116, '-') << "\n";

            for (size_t i = 0; i < groups_.size(); ++i) {
                for (size_t j = i + 1; j < groups_.size(); ++j) {
                    double gain = computeGain(groups_[i], groups_[j]);
                    double rs  = computeReadSaving(groups_[i], groups_[j]);
                    double ws  = computeWriteSaving(groups_[i], groups_[j]);
                    double wl  = computeWriteLoss(groups_[i], groups_[j]);
                    bool is_best = (gain > best_gain);

                    cout << left
                         << setw(38) << ("  "+groupName(groups_[i]))
                         << setw(38) << groupName(groups_[j])
                         << right << setw(10) << fixed << setprecision(1) << rs
                         << setw(10) << ws
                         << setw(10) << wl
                         << setw(10) << gain;
                    if (is_best) cout << "  ← BEST";
                    cout << "\n";

                    if (gain > best_gain) {
                        best_gain = gain;
                        best_pair = {static_cast<int>(i), static_cast<int>(j)};
                    }
                }
            }

            if (best_gain <= 0.0) {
                cout << "\n  best_gain = " << best_gain
                     << " <= 0  →  STOP\n";
                break;
            }

            // Merge the best pair
            int i = best_pair.first;
            int j = best_pair.second;
            cout << "\n--- Round " << round << " ---\n";
            cout << "  Merge: " << groupName(groups_[i])
                 << "  +  " << groupName(groups_[j])
                 << "  (gain = " << fixed << setprecision(2) << best_gain << ")\n";
            printGainDetails(groups_[i], groups_[j], best_gain);

            mergeGroups(i, j);
        }

        printFinalResult();
    }

private:
    WorkloadStats stats_;
    vector<Group> groups_;

    // --- Initialization ---
    void initGroups() {
        for (size_t p = 0; p < ALL_PROPS.size(); ++p) {
            const string& name = ALL_PROPS[p];
            Group g;
            g.id = static_cast<int>(p);
            g.props.insert(name);

            // Read set: queries that read this property
            auto rit = stats_.prop_read_sets.find(name);
            if (rit != stats_.prop_read_sets.end())
                g.read_set = rit->second;

            // Write set: relations that include this property
            auto wit = stats_.prop_write_sets.find(name);
            if (wit != stats_.prop_write_sets.end())
                g.write_set = wit->second;

            groups_.push_back(g);
        }
    }

    // --- Compute Gain(Gi, Gj) ---
    double computeGain(const Group& gi, const Group& gj) {
        double read_saving   = computeReadSaving(gi, gj);
        double write_saving  = computeWriteSaving(gi, gj);
        double write_loss    = computeWriteLoss(gi, gj);
        return read_saving + write_saving - write_loss;
    }

    // ReadSaving(Gi, Gj) = sum_{q in Qij} fq * CR(q)
    double computeReadSaving(const Group& gi, const Group& gj) {
        // Qij = queries that read from BOTH Gi and Gj
        set<int> qij;
        set_intersection(gi.read_set.begin(), gi.read_set.end(),
                         gj.read_set.begin(), gj.read_set.end(),
                         inserter(qij, qij.begin()));

        double saving = 0.0;
        for (int qi : qij) {
            uint64_t fq = stats_.query_freqs[static_cast<size_t>(qi)];
            // CR(q): cost of one CSR topological lookup (in pages).
            // For simplicity, each query does 1 CSR lookup per accessed relation.
            double cr = 1.0;
            saving += static_cast<double>(fq) * cr;
        }
        return saving;
    }

    // WriteSaving(Gi, Gj) = ceil(Wij * Se / B)
    double computeWriteSaving(const Group& gi, const Group& gj) {
        // Wij = relations that include BOTH Gi and Gj
        set<int> wij;
        set_intersection(gi.write_set.begin(), gi.write_set.end(),
                         gj.write_set.begin(), gj.write_set.end(),
                         inserter(wij, wij.begin()));

        // Count total edge insertions across these relations
        uint64_t total_writes = 0;
        for (int ri : wij) {
            total_writes += stats_.rel_edge_counts[ri];
        }

        double ws = ceil(static_cast<double>(total_writes) * TOPO_ENTRY_SIZE
                         / PAGE_SIZE_B);
        return ws;
    }

    // WriteLoss(Gi, Gj) = |Gj| * ceil(Wi_only * s_bar / B)
    //                    + |Gi| * ceil(Wj_only * s_bar / B)
    double computeWriteLoss(const Group& gi, const Group& gj) {
        // Wi_only = relations in Gi.write_set but NOT in Gj.write_set
        set<int> wi_only_set;
        set_difference(gi.write_set.begin(), gi.write_set.end(),
                       gj.write_set.begin(), gj.write_set.end(),
                       inserter(wi_only_set, wi_only_set.begin()));

        uint64_t wi_only = 0;
        for (int ri : wi_only_set) {
            wi_only += stats_.rel_edge_counts[ri];
        }

        // Wj_only = relations in Gj.write_set but NOT in Gi.write_set
        set<int> wj_only_set;
        set_difference(gj.write_set.begin(), gj.write_set.end(),
                       gi.write_set.begin(), gi.write_set.end(),
                       inserter(wj_only_set, wj_only_set.begin()));

        uint64_t wj_only = 0;
        for (int ri : wj_only_set) {
            wj_only += stats_.rel_edge_counts[ri];
        }

        double loss_i = static_cast<double>(gj.size())
                      * ceil(static_cast<double>(wi_only) * AVG_PROP_SLOT
                             / PAGE_SIZE_B);
        double loss_j = static_cast<double>(gi.size())
                      * ceil(static_cast<double>(wj_only) * AVG_PROP_SLOT
                             / PAGE_SIZE_B);

        return loss_i + loss_j;
    }

    // --- Merge two groups ---
    void mergeGroups(int i, int j) {
        Group merged;
        merged.id = groups_[i].id;
        merged.props = groups_[i].props;
        merged.props.insert(groups_[j].props.begin(), groups_[j].props.end());

        // Union read sets
        merged.read_set = groups_[i].read_set;
        merged.read_set.insert(groups_[j].read_set.begin(),
                               groups_[j].read_set.end());

        // Union write sets
        merged.write_set = groups_[i].write_set;
        merged.write_set.insert(groups_[j].write_set.begin(),
                                groups_[j].write_set.end());

        // Replace: remove j first (larger index), then i
        if (i < j) {
            groups_.erase(groups_.begin() + j);
            groups_.erase(groups_.begin() + i);
        } else {
            groups_.erase(groups_.begin() + i);
            groups_.erase(groups_.begin() + j);
        }
        groups_.push_back(merged);
    }

    // --- Output helpers ---
    string groupName(const Group& g) {
        string s = "{";
        bool first = true;
        for (const auto& p : g.props) {
            if (!first) s += ",";
            s += p;
            first = false;
        }
        s += "}";
        return s;
    }

    void printHeader() {
        cout << "╔══════════════════════════════════════════════════════════╗\n";
        cout << "║  Attribute Grouping Algorithm for FinBench Edge Props   ║\n";
        cout << "╠══════════════════════════════════════════════════════════╣\n";
        cout << "║  B  (page size)        = " << setw(6) << PAGE_SIZE_B << " bytes              ║\n";
        cout << "║  Se (topo entry)       = " << setw(6) << TOPO_ENTRY_SIZE << " bytes              ║\n";
        cout << "║  s_bar (avg slot)      = " << setw(6) << AVG_PROP_SLOT << " bytes              ║\n";
        cout << "╚══════════════════════════════════════════════════════════╝\n\n";
    }

    void printInitialState() {
        cout << "Initial state: " << groups_.size() << " singleton groups\n\n";

        cout << "=== Read Access Matrix ===\n";
        cout << setw(18) << "Property";
        for (size_t qi = 0; qi < QUERIES.size(); ++qi)
            cout << " Q" << setw(2) << QUERIES[qi].id;
        cout << "  fq\n";
        cout << string(18, '-');
        for (size_t qi = 0; qi < QUERIES.size(); ++qi)
            cout << "----";
        cout << "-----\n";

        for (const auto& p : ALL_PROPS) {
            cout << setw(18) << p;
            for (size_t qi = 0; qi < QUERIES.size(); ++qi) {
                bool hit = false;
                for (const auto& qp : QUERIES[qi].accessed_props)
                    if (qp == p) { hit = true; break; }
                cout << (hit ? "  X " : "  . ");
            }
            cout << "\n";
        }

        cout << setw(18) << "freq:";
        for (size_t qi = 0; qi < QUERIES.size(); ++qi)
            cout << setw(4) << QUERIES[qi].freq;
        cout << "\n\n";

        cout << "=== Write Co-occurrence Matrix ===\n";
        cout << setw(18) << "Property";
        for (const auto& p : ALL_PROPS)
            cout << " " << setw(3) << p.substr(0,3);
        cout << "\n" << string(18, '-');
        for (size_t i = 0; i < ALL_PROPS.size(); ++i)
            cout << "----";
        cout << "\n";

        for (const auto& p1 : ALL_PROPS) {
            cout << setw(18) << p1;
            const auto& ws1 = stats_.prop_write_sets[p1];
            for (const auto& p2 : ALL_PROPS) {
                const auto& ws2 = stats_.prop_write_sets[p2];
                set<int> common;
                set_intersection(ws1.begin(), ws1.end(),
                                 ws2.begin(), ws2.end(),
                                 inserter(common, common.begin()));
                uint64_t total = 0;
                for (int ri : common)
                    total += stats_.rel_edge_counts[ri];
                if (p1 == p2)
                    cout << "  --";
                else
                    cout << setw(4) << (total / 1000000);
            }
            cout << "\n";
        }
        cout << "  (values in millions of edges that write both properties)\n\n";
    }

    void printGainDetails(const Group& gi, const Group& gj, double gain) {
        double rs = computeReadSaving(gi, gj);
        double ws = computeWriteSaving(gi, gj);
        double wl = computeWriteLoss(gi, gj);

        // Qij
        set<int> qij;
        set_intersection(gi.read_set.begin(), gi.read_set.end(),
                         gj.read_set.begin(), gj.read_set.end(),
                         inserter(qij, qij.begin()));

        // Wij
        set<int> wij;
        set_intersection(gi.write_set.begin(), gi.write_set.end(),
                         gj.write_set.begin(), gj.write_set.end(),
                         inserter(wij, wij.begin()));
        uint64_t wij_edges = 0;
        for (int ri : wij) wij_edges += stats_.rel_edge_counts[ri];

        // Wi_only
        set<int> wi_only_set;
        set_difference(gi.write_set.begin(), gi.write_set.end(),
                       gj.write_set.begin(), gj.write_set.end(),
                       inserter(wi_only_set, wi_only_set.begin()));
        uint64_t wi_only = 0;
        for (int ri : wi_only_set) wi_only += stats_.rel_edge_counts[ri];

        // Wj_only
        set<int> wj_only_set;
        set_difference(gj.write_set.begin(), gj.write_set.end(),
                       gi.write_set.begin(), gi.write_set.end(),
                       inserter(wj_only_set, wj_only_set.begin()));
        uint64_t wj_only = 0;
        for (int ri : wj_only_set) wj_only += stats_.rel_edge_counts[ri];

        cout << "    ReadSaving  = " << rs
             << "  (|Qij|=" << qij.size() << ")\n";
        cout << "    WriteSaving = " << ws
             << "  (|Wij|=" << wij_edges << " edges)\n";
        cout << "    WriteLoss   = " << wl
             << "  (Wi_only=" << wi_only << ", Wj_only=" << wj_only << ")\n";
    }

    void printFinalResult() {
        cout << "\n";
        cout << "╔══════════════════════════════════════════════════════════╗\n";
        cout << "║                   FINAL GROUPING                        ║\n";
        cout << "╠══════════════════════════════════════════════════════════╣\n";

        // Sort groups by size for display
        vector<Group> sorted = groups_;
        sort(sorted.begin(), sorted.end(),
             [](const Group& a, const Group& b) { return a.size() > b.size(); });

        int gid = 0;
        size_t total_props = 0;
        for (const auto& g : sorted) {
            ++gid;
            total_props += g.size();

            // Compute total bytes for this group
            uint64_t group_bytes = 0;
            for (const auto& p : g.props) {
                auto it = PROP_LENGTHS.find(p);
                group_bytes += (it != PROP_LENGTHS.end()) ? it->second : 20;
            }

            cout << "  Group " << gid << ": " << groupName(g) << "\n";
            cout << "    " << g.size() << " properties, "
                 << group_bytes << " bytes/edge fixed-length\n";

            // Print which relations write to this group
            cout << "    Relations: ";
            bool first = true;
            for (int ri : g.write_set) {
                if (!first) cout << ", ";
                cout << RELATIONS[static_cast<size_t>(ri)].name;
                first = false;
            }
            cout << "\n";

            // Print which queries read from this group
            if (!g.read_set.empty()) {
                cout << "    Queries:   ";
                first = true;
                for (int qi : g.read_set) {
                    if (!first) cout << ", ";
                    cout << "Q" << QUERIES[static_cast<size_t>(qi)].id;
                    first = false;
                }
                cout << "\n";
            } else {
                cout << "    Queries:   (none — cold storage)\n";
            }
            cout << "\n";
        }

        cout << "  Total: " << total_props << " properties in "
             << sorted.size() << " groups\n";

        // Summary: compute total storage per edge
        uint64_t total_bytes = 0;
        for (const auto& g : groups_) {
            for (const auto& p : g.props) {
                auto it = PROP_LENGTHS.find(p);
                total_bytes += (it != PROP_LENGTHS.end()) ? it->second : 20;
            }
        }
        cout << "  Total storage per edge: " << total_bytes
             << " bytes (fixed-length)\n";

        // Compare with 2-shard baseline
        cout << "\n  === Comparison with current 2-shard scheme ===\n";
        cout << "  Current: Shard0(" << 9 << " props, "
             << 13+20+11+13+15+22+22+22+10 << "B) + Shard1("
             << 5 << " props, " << 10+25+18+22+20 << "B) = "
             << 13+20+11+13+15+22+22+22+10+10+25+18+22+20 << "B total\n";
        cout << "  Algorithm: " << sorted.size() << " groups, "
             << total_bytes << "B total\n";

        cout << "╚══════════════════════════════════════════════════════════╝\n";
    }
};

int main() {
    AttributeGrouper grouper;
    grouper.run();
    return 0;
}
