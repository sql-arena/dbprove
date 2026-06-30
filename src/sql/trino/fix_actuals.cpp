#include "fix_actuals.h"

#include "explain/node_type.h"
#include "explain/node.h"

#include <plog/Log.h>

#include <cmath>
#include <functional>
#include <optional>
#include <limits>
#include <map>
#include <sstream>
#include <string>
#include <vector>

namespace {

constexpr double kNaN = std::numeric_limits<double>::quiet_NaN();

// ── Text plan node ────────────────────────────────────────────────────────────

struct TextNode {
    std::string name;
    double rows_estimated = kNaN;
    double rows_actual    = kNaN;
    std::vector<int> source_fragment_ids;   // RemoteSource / RemoteMerge only
    std::vector<TextNode> children;
};

using FragmentMap = std::map<int, TextNode>;

// ── Line-level parsers ────────────────────────────────────────────────────────

// Try to parse a line as a plan-node declaration.
// After stripping leading spaces and UTF-8 box-drawing connectors (└─ / ├─),
// a node line starts with an upper-case ASCII letter followed by more word
// characters and then immediately by '['.
//
// Returns {depth, name} on success; {-1, ""} for metadata / stats lines.
//
// Depth formula (visual columns):
//   root  → 4 leading spaces  → depth = 0
//   child → 4 + 3*d leading spaces + box chars → name starts at visual col 4 + 3*d
//   depth = (visual_col_of_name - 4) / 3
static std::pair<int, std::string> parseNodeLine(const std::string& line) {
    size_t i = 0;
    int visual_col = 0;

    // Count leading spaces (1 visual col each).
    while (i < line.size() && line[i] == ' ') { ++i; ++visual_col; }

    if (i >= line.size()) return {-1, ""};

    auto c = static_cast<unsigned char>(line[i]);

    // Handle UTF-8 box-drawing characters (all start with 0xE2).
    if (c == 0xE2 && i + 2 < line.size()) {
        auto c1 = static_cast<unsigned char>(line[i + 1]);
        auto c2 = static_cast<unsigned char>(line[i + 2]);

        if (c1 != 0x94) return {-1, ""};

        if (c2 == 0x82) return {-1, ""};  // │ — metadata line

        // └ (0x94) or ├ (0x9C) — tree connector.
        if (c2 == 0x94 || c2 == 0x9C) {
            i += 3; ++visual_col;
            // Consume the horizontal bar ─ (E2 94 80) that follows.
            if (i + 2 < line.size() &&
                static_cast<unsigned char>(line[i]) == 0xE2 &&
                static_cast<unsigned char>(line[i + 1]) == 0x94 &&
                static_cast<unsigned char>(line[i + 2]) == 0x80) {
                i += 3; ++visual_col;
            }
            // Consume the space after the connector.
            if (i < line.size() && line[i] == ' ') { ++i; ++visual_col; }
        }
    }

    // We must now be at the start of the node name.
    if (i >= line.size() || !isupper(static_cast<unsigned char>(line[i]))) return {-1, ""};

    // Read name characters (alphanumeric, no spaces / brackets).
    size_t name_start = i;
    while (i < line.size() && line[i] != '[' && line[i] != ' ' &&
           line[i] != '\n' && line[i] != '\r') {
        ++i;
    }
    // Node lines must be immediately followed by '[' — this filters out fragment
    // metadata lines like "Output layout:", "CPU:", "Peak Memory:", etc.
    if (i >= line.size() || line[i] != '[') return {-1, ""};

    std::string name = line.substr(name_start, i - name_start);
    if (name.empty()) return {-1, ""};

    // depth = (visual_col_of_name - 4) / 3
    int depth = (visual_col - 4) / 3;
    if (depth < 0) depth = 0;
    return {depth, name};
}

// Parse all source fragment IDs from a RemoteSource / RemoteMerge line.
// Handles "sourceFragmentIds = [N, M]" and "sourceFragmentIds=[N]".
static std::vector<int> parseSourceFragmentIds(const std::string& line) {
    std::vector<int> ids;
    auto pos = line.find("sourceFragmentIds");
    if (pos == std::string::npos) return ids;
    pos = line.find('[', pos);
    if (pos == std::string::npos) return ids;
    ++pos;
    while (pos < line.size() && line[pos] != ']') {
        if (isdigit(static_cast<unsigned char>(line[pos]))) {
            int id = 0;
            while (pos < line.size() && isdigit(static_cast<unsigned char>(line[pos])))
                id = id * 10 + (line[pos++] - '0');
            ids.push_back(id);
        } else {
            ++pos;
        }
    }
    return ids;
}

// Parse the LAST rows value from an Estimates line.
// "Estimates: {rows: 12345 (?), ...}" or ".../{ rows: 99 (?), ...}"
// Returns NaN when the value is "?" or not found.
static double parseLastEstimate(const std::string& line) {
    double last = kNaN;
    size_t pos = 0;
    while ((pos = line.find("rows: ", pos)) != std::string::npos) {
        pos += 6;
        if (pos < line.size() && line[pos] != '?') {
            try {
                size_t n = 0;
                double v = std::stod(line.substr(pos), &n);
                if (n > 0) last = v;
            } catch (...) {}
        }
        ++pos;
    }
    return last;
}

// Parse actual output rows from "Output: N rows (...)".
// Returns NaN when the value is "?" or not found.
static double parseActualRows(const std::string& line) {
    auto pos = line.find("Output: ");
    if (pos == std::string::npos) return kNaN;
    pos += 8;
    if (pos >= line.size() || line[pos] == '?') return kNaN;
    try {
        size_t n = 0;
        double v = std::stod(line.substr(pos), &n);
        if (n > 0) return v;
    } catch (...) {}
    return kNaN;
}

// ── Parse EXPLAIN ANALYZE text → FragmentMap ─────────────────────────────────

static FragmentMap parseAnalyzeText(const std::string& text) {
    FragmentMap result;

    // Flat accumulator per fragment: (depth, TextNode_without_children)
    struct FlatEntry { int depth; TextNode data; };
    std::map<int, std::vector<FlatEntry>> flat;

    int cur_frag = -1;
    std::vector<FlatEntry>* cur_list = nullptr;
    FlatEntry* active_entry = nullptr;   // most-recently seen node (for metadata)

    std::istringstream iss(text);
    std::string line;
    while (std::getline(iss, line)) {
        // Fragment header: "Fragment N [type]"
        if (line.rfind("Fragment ", 0) == 0) {
            size_t p = 9;
            int fid = 0;
            while (p < line.size() && isdigit(static_cast<unsigned char>(line[p])))
                fid = fid * 10 + (line[p++] - '0');
            cur_frag = fid;
            flat[cur_frag] = {};
            cur_list = &flat[cur_frag];
            active_entry = nullptr;
            continue;
        }

        if (cur_frag < 0 || !cur_list) continue;

        // Node declaration line?
        auto [depth, name] = parseNodeLine(line);
        if (!name.empty()) {
            FlatEntry fe;
            fe.depth = depth;
            fe.data.name = name;
            if (name == "RemoteSource" || name == "RemoteMerge")
                fe.data.source_fragment_ids = parseSourceFragmentIds(line);
            cur_list->push_back(std::move(fe));
            active_entry = &cur_list->back();
            continue;
        }

        if (!active_entry) continue;

        // Estimates metadata line?
        if (line.find("Estimates:") != std::string::npos) {
            double est = parseLastEstimate(line);
            if (!std::isnan(est)) active_entry->data.rows_estimated = est;
            continue;
        }

        // CPU / Output metadata line?  ("Output: N rows" or "Output: 1 row")
        if (line.find("Output:") != std::string::npos &&
            line.find("row") != std::string::npos) {
            double act = parseActualRows(line);
            if (!std::isnan(act)) active_entry->data.rows_actual = act;
            continue;
        }
    }

    // Reconstruct per-fragment trees from flat depth-indexed lists.
    for (auto& [fid, entries] : flat) {
        if (entries.empty()) { result[fid] = TextNode{}; continue; }

        // depth_to_idx[d] = index of the most-recently added node at depth d.
        std::map<int, int> depth_to_idx;
        std::vector<int> parent_of(entries.size(), -1);   // parent index per node

        for (int k = 0; k < static_cast<int>(entries.size()); ++k) {
            int d = entries[k].depth;
            auto it = depth_to_idx.find(d - 1);
            if (it != depth_to_idx.end()) parent_of[k] = it->second;
            depth_to_idx[d] = k;
            // Invalidate all depths below d (children of earlier siblings at d-1 are gone).
            auto jt = depth_to_idx.upper_bound(d);
            depth_to_idx.erase(jt, depth_to_idx.end());
        }

        // Build tree bottom-up (reverse order so children exist before parents).
        std::vector<TextNode> nodes;
        nodes.reserve(entries.size());
        for (auto& e : entries) nodes.push_back(std::move(e.data));

        // Attach children (in DFS order they appear after the parent, so iterate in reverse).
        for (int k = static_cast<int>(nodes.size()) - 1; k >= 0; --k) {
            if (parent_of[k] >= 0) {
                nodes[parent_of[k]].children.push_back(std::move(nodes[k]));
            }
        }

        // Reverse children lists (they were appended in reverse, so flip them back).
        std::function<void(TextNode&)> fixOrder = [&](TextNode& n) {
            std::reverse(n.children.begin(), n.children.end());
            for (auto& c : n.children) fixOrder(c);
        };
        fixOrder(nodes[0]);

        result[fid] = std::move(nodes[0]);
    }

    return result;
}

// ── Effective-children helper ─────────────────────────────────────────────────

// Collect the "effective" non-transparent children of a TextNode:
//  • RemoteSource / RemoteMerge → substitute the referenced fragment root.
//  • Limit / LimitPartial / PartialSort → transparent in our plan tree; expand through them.
//  • Everything else → keep as-is.
static void appendEffective(const TextNode& node,
                             const FragmentMap& frags,
                             std::vector<const TextNode*>& out);

static void appendEffective(const TextNode& node,
                             const FragmentMap& frags,
                             std::vector<const TextNode*>& out) {
    if (node.name == "RemoteSource" || node.name == "RemoteMerge") {
        for (int fid : node.source_fragment_ids) {
            auto it = frags.find(fid);
            if (it != frags.end()) appendEffective(it->second, frags, out);
        }
        return;
    }
    if (node.name == "Limit" || node.name == "LimitPartial" || node.name == "PartialSort") {
        for (const auto& c : node.children) appendEffective(c, frags, out);
        return;
    }
    out.push_back(&node);
}

static std::vector<const TextNode*> effectiveChildren(const TextNode& node,
                                                        const FragmentMap& frags) {
    std::vector<const TextNode*> result;
    // When the node itself is a RemoteSource/RemoteMerge, its "children" are
    // the fragment roots pointed to by source_fragment_ids (the direct children
    // list is empty in this case).
    if (node.name == "RemoteSource" || node.name == "RemoteMerge") {
        for (int fid : node.source_fragment_ids) {
            auto it = frags.find(fid);
            if (it != frags.end()) appendEffective(it->second, frags, result);
        }
        return result;
    }
    for (const auto& c : node.children) appendEffective(c, frags, result);
    return result;
}

// ── Recursive plan-tree enrichment ───────────────────────────────────────────

static void matchAndEnrich(const TextNode& text,
                            sql::explain::Node& plan,
                            const FragmentMap& frags);

// Apply stats from text onto plan and all its descendants.
// Used for composite scan/filter/project nodes where one text node covers
// a linear chain (Scan → Selection → Projection) in the plan tree.
static void assignToSubtree(const TextNode& text, sql::explain::Node& plan) {
    for (auto& n : plan.depth_first()) {
        if (!std::isnan(text.rows_actual))    n.rows_actual    = text.rows_actual;
        if (!std::isnan(text.rows_estimated)) n.rows_estimated = text.rows_estimated;
    }
}

static void matchAndEnrich(const TextNode& text,
                            sql::explain::Node& plan,
                            const FragmentMap& frags) {
    using NodeType = sql::explain::NodeType;

    // ── Transparent text nodes ──────────────────────────────────────────────
    // These were collapsed / made transparent during JSON → plan-tree build.

    // Output: may produce a Projection or be fully transparent.
    if (text.name == "Output") {
        if (plan.type != NodeType::PROJECTION) {
            // Transparent: forward to child.
            if (!text.children.empty())
                matchAndEnrich(text.children.front(), plan, frags);
            return;
        }
        // Fall through: Output produced a Projection → treat normally.
    }

    // Always-transparent text nodes.
    if (text.name == "PartialSort" || text.name == "Limit" || text.name == "LimitPartial") {
        if (!text.children.empty())
            matchAndEnrich(text.children.front(), plan, frags);
        return;
    }

    // Project[]: transparent when the plan node is not a PROJECTION.
    // A Project with an empty descriptor in the plan JSON is transparent in
    // explain.cpp (no PROJECTION node is created).  When it appears in the EA
    // text but the plan tree has already "absorbed" it, skip this text node.
    if (text.name == "Project" && plan.type != NodeType::PROJECTION) {
        if (!text.children.empty())
            matchAndEnrich(text.children.front(), plan, frags);
        return;
    }

    // LocalMerge: transparent when the plan node is not a SORT.
    // When Output is fully transparent (no PROJECTION wrapper), the plan root is
    // a SORT (LocalMerge).  The EA root is RemoteMerge whose effective child is
    // the LocalMerge fragment — but LocalMerge corresponds to the SORT already
    // matched above.  Forwarding through its effective child re-aligns the trees.
    if (text.name == "LocalMerge" && plan.type != NodeType::SORT) {
        auto eff_ch = effectiveChildren(text, frags);
        if (!eff_ch.empty())
            matchAndEnrich(*eff_ch[0], plan, frags);
        return;
    }

    // FilterProject handling: the plan represents this as 0-2 wrapper nodes
    // (Projection, Selection, or both), or as an AntiJoin when the SemiJoin it
    // wraps was rewritten.
    if (text.name == "FilterProject") {
        if (plan.type == NodeType::JOIN) {
            // AntiJoin rewrite — the FilterProject disappeared from the plan.
            if (!text.children.empty())
                matchAndEnrich(text.children.front(), plan, frags);
            return;
        }
        if (plan.type == NodeType::PROJECTION || plan.type == NodeType::FILTER) {
            // Assign FilterProject stats to the Projection/Selection chain.
            if (!std::isnan(text.rows_actual))    plan.rows_actual    = text.rows_actual;
            if (!std::isnan(text.rows_estimated)) plan.rows_estimated = text.rows_estimated;
            sql::explain::Node* inner = plan.firstChild();
            while (inner != nullptr &&
                   (inner->type == NodeType::PROJECTION || inner->type == NodeType::FILTER)) {
                if (!std::isnan(text.rows_actual))    inner->rows_actual    = text.rows_actual;
                if (!std::isnan(text.rows_estimated)) inner->rows_estimated = text.rows_estimated;
                inner = inner->firstChild();
            }
            // Match FilterProject's child (e.g. CrossJoin) to the inner plan node.
            if (!text.children.empty() && inner != nullptr)
                matchAndEnrich(text.children.front(), *inner, frags);
            return;
        }
        // FilterProject produced no wrapper (transparent in plan) — forward to child.
        if (!text.children.empty())
            matchAndEnrich(text.children.front(), plan, frags);
        return;
    }

    // ── Assign stats to the current plan node ──────────────────────────────
    if (!std::isnan(text.rows_actual))    plan.rows_actual    = text.rows_actual;
    if (!std::isnan(text.rows_estimated)) plan.rows_estimated = text.rows_estimated;

    // Get the effective (inlined, transparent-stripped) text children.
    auto text_ch = effectiveChildren(text, frags);

    // Get plan children (copy from the thread-local span before recursing).
    auto span = plan.children();
    std::vector<sql::explain::Node*> plan_ch(span.begin(), span.end());

    // ── Composite leaf: ScanFilter / ScanFilterProject / ScanProject / TableScan
    // These produce a linear chain in the plan tree but appear as leaves in the
    // EXPLAIN ANALYZE text (no effective children).
    if (text_ch.empty() && !plan_ch.empty()) {
        assignToSubtree(text, plan);
        return;
    }

    // ── TopN → Limit + Sort in plan ─────────────────────────────────────────
    // TopN[orderBy+count] → plan Limit wrapping Sort; assign TopN stats to Sort.
    if (text.name == "TopN" && plan.type == NodeType::LIMIT &&
        !plan_ch.empty() && plan_ch[0]->type == NodeType::SORT) {
        auto* sort_node = plan_ch[0];
        if (!std::isnan(text.rows_actual))    sort_node->rows_actual    = text.rows_actual;
        if (!std::isnan(text.rows_estimated)) sort_node->rows_estimated = text.rows_estimated;
        auto sort_span = sort_node->children();
        std::vector<sql::explain::Node*> sort_ch(sort_span.begin(), sort_span.end());
        for (size_t i = 0; i < std::min(text_ch.size(), sort_ch.size()); ++i)
            matchAndEnrich(*text_ch[i], *sort_ch[i], frags);
        return;
    }

    // ── Join child reversal ─────────────────────────────────────────────────
    // EXPLAIN ANALYZE text lists probe side first, build side second.
    // Our plan tree stores build first, probe second.
    const bool is_hash_join = (text.name == "InnerJoin" || text.name == "LeftJoin" ||
                                text.name == "RightJoin" || text.name == "FullJoin");
    if (is_hash_join && text_ch.size() == 2 && plan_ch.size() == 2) {
        matchAndEnrich(*text_ch[0], *plan_ch[1], frags);  // probe → plan[1]
        matchAndEnrich(*text_ch[1], *plan_ch[0], frags);  // build → plan[0]
        return;
    }
    if (text.name == "SemiJoin" && text_ch.size() == 2 && plan_ch.size() == 2) {
        matchAndEnrich(*text_ch[0], *plan_ch[1], frags);  // source    → plan[1]
        matchAndEnrich(*text_ch[1], *plan_ch[0], frags);  // filtering → plan[0]
        return;
    }

    // ── GroupBy with a mask Selection child ─────────────────────────────────
    // When sharedAggregateMask inserts a Selection under a GroupBy, the text
    // has one fewer plan child.  Assign GroupBy's stats to the mask Selection
    // and match text children against the Selection's children.
    //
    // Skip this handler when the text child is "Filter", "FilterProject" (the
    // plan Selection already appears as a text Filter node), or "Aggregate"
    // (the text child is a fragment-root Aggregate reached via RemoteSource
    // expansion — matching it against the Selection's grandchild would be wrong).
    if (plan.type == NodeType::GROUP_BY && !plan_ch.empty() &&
        plan_ch[0]->type == NodeType::FILTER && !text_ch.empty()) {
        const std::string& first_text_ch_name = text_ch[0]->name;
        const bool text_child_is_filter = (first_text_ch_name == "Filter" ||
                                            first_text_ch_name == "FilterProject" ||
                                            first_text_ch_name == "Aggregate");
        if (!text_child_is_filter) {
            auto* mask_sel = plan_ch[0];
            if (!std::isnan(text.rows_actual))    mask_sel->rows_actual    = text.rows_actual;
            if (!std::isnan(text.rows_estimated)) mask_sel->rows_estimated = text.rows_estimated;
            auto mask_span = mask_sel->children();
            std::vector<sql::explain::Node*> mask_ch(mask_span.begin(), mask_span.end());
            for (size_t i = 0; i < std::min(text_ch.size(), mask_ch.size()); ++i)
                matchAndEnrich(*text_ch[i], *mask_ch[i], frags);
            return;
        }
    }

    // ── Normal case: zip text children with plan children ──────────────────
    if (text_ch.size() != plan_ch.size()) {
        PLOGW << "fixActuals (EXPLAIN ANALYZE): child count mismatch for text node \""
              << text.name << "\": text has " << text_ch.size()
              << " children, plan has " << plan_ch.size();
    }
    for (size_t i = 0; i < std::min(text_ch.size(), plan_ch.size()); ++i)
        matchAndEnrich(*text_ch[i], *plan_ch[i], frags);
}

// ── Post-pass: Distribute nodes inherit from their first child ────────────────

static void fixDistributeNodes(sql::explain::Node& root) {
    for (auto& node : root.bottom_up()) {
        if (node.type != sql::explain::NodeType::DISTRIBUTE) continue;
        auto* child = node.firstChild();
        if (!child) continue;
        if (!std::isnan(child->rows_actual))    node.rows_actual    = child->rows_actual;
        if (!std::isnan(child->rows_estimated)) node.rows_estimated = child->rows_estimated;
    }
}

// ── Sequence sync (mirrors Plan::syncSequenceRowCounts) ──────────────────────

static void syncSequenceNodes(sql::explain::Node& root) {
    for (auto& node : root.depth_first()) {
        if (node.type != sql::explain::NodeType::SEQUENCE) continue;
        auto* last = node.lastChild();
        if (!last) continue;
        if (!std::isnan(last->rows_actual))    node.rows_actual    = last->rows_actual;
        if (!std::isnan(last->rows_estimated)) node.rows_estimated = last->rows_estimated;
    }
}

}  // namespace

// ── Public entry point ────────────────────────────────────────────────────────

namespace sql::trino {

void fixActualsFromExplainAnalyze(explain::Plan& plan,
                                   std::string_view statement,
                                   ConnectionBase& connection) {
    const std::string artefact_name =
        std::to_string(std::hash<std::string_view>{}(statement)) + "_analyze";

    std::string analyze_text;
    std::optional<std::string> cached;
    try {
        cached = connection.getArtefact(artefact_name, "txt");
    } catch (const std::exception&) {
        // Artefact not available in replay mode — skip actuals gracefully.
        return;
    }
    if (cached) {
        PLOGI << "fixActuals (EXPLAIN ANALYZE): loaded from artefact " << artefact_name;
        analyze_text = std::move(*cached);
    } else {
        const std::string analyze_sql = "EXPLAIN ANALYZE\n" + std::string(statement);
        try {
            analyze_text = connection.fetchScalar(analyze_sql).asString();
        } catch (const std::exception& e) {
            PLOGE << "fixActuals (EXPLAIN ANALYZE): query failed: " << e.what();
            return;
        } catch (...) {
            PLOGE << "fixActuals (EXPLAIN ANALYZE): query failed with unknown error";
            return;
        }
        connection.storeArtefact(artefact_name, "txt", analyze_text);
    }

    FragmentMap frags = parseAnalyzeText(analyze_text);
    if (frags.empty()) {
        PLOGE << "fixActuals (EXPLAIN ANALYZE): no fragments parsed from output";
        return;
    }

    // The first fragment in the output (lowest ID) is the root.
    const TextNode& root_text = frags.begin()->second;
    sql::explain::Node& root_plan = plan.planTree();

    // When the plan root is a Projection inserted by Output column renaming, the
    // EXPLAIN ANALYZE fragment root is the first real data node (Aggregate, TopN,
    // Sort, ...) which sits BELOW the Projection in the plan tree.  Skip the plan
    // Projection so the text root maps to the correct plan node beneath it.
    //
    // Exception: do NOT skip for "RemoteMerge" roots — RemoteMerge absorbs stats
    // at the Projection level and its effective children (the merge sub-fragments)
    // already align with the Projection's child (the local Sort/merge node).
    sql::explain::Node* effective_root = &root_plan;
    if (root_plan.type == sql::explain::NodeType::PROJECTION &&
        root_text.name != "Output" &&
        root_text.name != "RemoteMerge" &&
        root_plan.firstChild() != nullptr) {
        effective_root = root_plan.firstChild();
    }

    try {
        matchAndEnrich(root_text, *effective_root, frags);
    } catch (const std::exception& e) {
        PLOGE << "fixActuals (EXPLAIN ANALYZE): tree matching failed: " << e.what();
    }

    fixDistributeNodes(root_plan);
    syncSequenceNodes(root_plan);
}

}  // namespace sql::trino
