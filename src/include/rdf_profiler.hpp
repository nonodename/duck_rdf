#pragma once

#include "I_triples_buffer.hpp"
#include "duckdb/common/file_system.hpp"
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

/// Whether an RDF object node is an IRI, blank node, or literal.
/// Determined directly from the parser node type, not by guessing from value strings.
enum class ObjectKind { IRI, BLANK, LITERAL };

/// Per-type statistics: count, lexicographic min and max.
struct TypeStats {
	uint64_t count = 0;
	std::string min_val;
	std::string max_val;
	bool has_value = false;

	void Update(const std::string &value) {
		count++;
		if (!has_value) {
			min_val = value;
			max_val = value;
			has_value = true;
		} else {
			if (value < min_val)
				min_val = value;
			if (value > max_val)
				max_val = value;
		}
	}

	void Merge(const TypeStats &other) {
		count += other.count;
		if (!other.has_value)
			return;
		if (!has_value) {
			min_val = other.min_val;
			max_val = other.max_val;
			has_value = true;
		} else {
			if (other.min_val < min_val)
				min_val = other.min_val;
			if (other.max_val > max_val)
				max_val = other.max_val;
		}
	}
};

/// Accumulated profiling data for one unique predicate URI.
struct PredicateProfile {
	/// Maps DuckDB type name (e.g. "BIGINT", "IRI", "BLANK", "VARCHAR") → per-type stats.
	std::unordered_map<std::string, TypeStats> type_stats;
	std::unordered_set<std::string> subjects;
	std::unordered_set<std::string> graphs;

	void AddTriple(const std::string &graph, const std::string &subject, const std::string &object,
	               const std::string &type_name) {
		type_stats[type_name].Update(object);
		subjects.insert(subject);
		graphs.insert(graph);
	}

	void Merge(const PredicateProfile &other) {
		for (const auto &kv : other.type_stats) {
			type_stats[kv.first].Merge(kv.second);
		}
		subjects.insert(other.subjects.begin(), other.subjects.end());
		graphs.insert(other.graphs.begin(), other.graphs.end());
	}
};

/// Maps an XSD datatype URI + ObjectKind to a DuckDB type name string.
/// For IRIs/blanks the datatype is ignored and the kind determines the label.
/// Unknown XSD types return the full datatype URI for transparency.
std::string XsdToDuckDBType(const std::string &datatype, const std::string &lang, ObjectKind kind);

/// Accumulates per-predicate profiling statistics from a stream of RDF triples.
/// Designed as a standalone intermediate class: parsers call AddTriple() directly;
/// results are available via GetProfiles() after parsing is complete.
/// A future two-pass pivoting implementation can reuse this class unchanged.
class RDFProfileAccumulator {
public:
	void AddTriple(const std::string &graph, const std::string &subject, const std::string &predicate,
	               const std::string &object, ObjectKind object_kind, const std::string &datatype,
	               const std::string &lang);

	void Merge(const RDFProfileAccumulator &other);

	const std::unordered_map<std::string, PredicateProfile> &GetProfiles() const {
		return _profiles;
	}

private:
	std::unordered_map<std::string, PredicateProfile> _profiles;
};

/// Parse an RDF file using the Serd library and feed all triples into accumulator.
/// file_type must be one of TURTLE, NTRIPLES, NQUADS, TRIG.
void ProfileFileSerd(const std::string &file_path, duckdb::FileSystem &fs, ITriplesBuffer::FileType file_type,
                     bool strict_parsing, RDFProfileAccumulator &accumulator);

/// Parse an RDF/XML file and feed all triples into accumulator.
void ProfileFileXML(const std::string &file_path, duckdb::FileSystem &fs, bool strict_parsing,
                    RDFProfileAccumulator &accumulator);
