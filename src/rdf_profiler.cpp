#include "include/rdf_profiler.hpp"
#include "include/rdf_xml_parser.hpp"
#include "duckdb/common/exception.hpp"

#include <serd/serd.h>
#include <stdexcept>
#include <unordered_map>

// ============================================================
// XSD → DuckDB type name mapping
// ============================================================

static const std::string XSD_PREFIX = "http://www.w3.org/2001/XMLSchema#";

std::string XsdToDuckDBType(const std::string &datatype, const std::string &lang, ObjectKind kind) {
	if (kind == ObjectKind::IRI)
		return "IRI";
	if (kind == ObjectKind::BLANK)
		return "BLANK";

	// LITERAL path
	if (!lang.empty())
		return "VARCHAR"; // language-tagged string

	if (datatype.empty())
		return "VARCHAR"; // plain literal, no datatype

	// Strip the XSD namespace prefix for lookup
	std::string local = datatype;
	if (datatype.compare(0, XSD_PREFIX.size(), XSD_PREFIX) == 0)
		local = datatype.substr(XSD_PREFIX.size());

	static const std::unordered_map<std::string, std::string> XSD_MAP = {
	    {"boolean", "BOOLEAN"},
	    {"byte", "TINYINT"},
	    {"unsignedByte", "UTINYINT"},
	    {"short", "SMALLINT"},
	    {"unsignedShort", "USMALLINT"},
	    {"int", "INTEGER"},
	    {"unsignedInt", "UINTEGER"},
	    {"long", "BIGINT"},
	    {"unsignedLong", "UBIGINT"},
	    {"integer", "HUGEINT"},
	    {"nonNegativeInteger", "HUGEINT"},
	    {"positiveInteger", "HUGEINT"},
	    {"negativeInteger", "HUGEINT"},
	    {"nonPositiveInteger", "HUGEINT"},
	    {"float", "FLOAT"},
	    {"double", "DOUBLE"},
	    {"decimal", "DECIMAL"},
	    {"string", "VARCHAR"},
	    {"normalizedString", "VARCHAR"},
	    {"token", "VARCHAR"},
	    {"anyURI", "VARCHAR"},
	    {"date", "DATE"},
	    {"time", "TIME"},
	    {"dateTime", "TIMESTAMP"},
	    {"dateTimeStamp", "TIMESTAMP WITH TIME ZONE"},
	    {"duration", "INTERVAL"},
	    {"hexBinary", "BLOB"},
	    {"base64Binary", "BLOB"},
	};

	auto it = XSD_MAP.find(local);
	if (it != XSD_MAP.end())
		return it->second;

	// Unknown datatype: return the full URI so callers can see what it is
	return datatype;
}

// ============================================================
// RDFProfileAccumulator
// ============================================================

void RDFProfileAccumulator::AddTriple(const std::string &graph, const std::string &subject,
                                      const std::string &predicate, const std::string &object, ObjectKind object_kind,
                                      const std::string &datatype, const std::string &lang) {
	std::string type_name = XsdToDuckDBType(datatype, lang, object_kind);
	_profiles[predicate].AddTriple(graph, subject, object, type_name);
}

void RDFProfileAccumulator::Merge(const RDFProfileAccumulator &other) {
	for (auto &[predicate, profile] : other._profiles) {
		_profiles[predicate].Merge(profile);
	}
}

// ============================================================
// Serd-based file profiler
// ============================================================

struct SerdProfileState {
	RDFProfileAccumulator *accumulator;
	SerdEnv *env;
	bool strict_parsing;
	std::string file_path;
	bool has_error = false;
	std::string error_message;
	uint64_t skip_count = 0;
};

static std::string SerdNodeToString(SerdEnv *env, const SerdNode *node) {
	if (!node || !node->buf)
		return {};
	if (node->type == SERD_CURIE) {
		SerdNode expanded = serd_env_expand_node(env, node);
		if (expanded.buf) {
			std::string result(reinterpret_cast<const char *>(expanded.buf), expanded.n_bytes);
			serd_node_free(&expanded);
			return result;
		}
	}
	return std::string(reinterpret_cast<const char *>(node->buf), node->n_bytes);
}

static ObjectKind SerdNodeKind(const SerdNode *node) {
	if (!node || !node->buf)
		return ObjectKind::LITERAL;
	switch (node->type) {
	case SERD_URI:
	case SERD_CURIE:
		return ObjectKind::IRI;
	case SERD_BLANK:
		return ObjectKind::BLANK;
	default:
		return ObjectKind::LITERAL;
	}
}

static SerdStatus SerdProfileStatement(void *user_data, SerdStatementFlags /*flags*/, const SerdNode *graph,
                                       const SerdNode *subject, const SerdNode *predicate, const SerdNode *object,
                                       const SerdNode *object_datatype, const SerdNode *object_lang) {
	auto *state = static_cast<SerdProfileState *>(user_data);

	std::string graph_str = SerdNodeToString(state->env, graph);
	std::string subject_str = SerdNodeToString(state->env, subject);
	std::string predicate_str = SerdNodeToString(state->env, predicate);
	std::string object_str = SerdNodeToString(state->env, object);
	std::string datatype_str = SerdNodeToString(state->env, object_datatype);
	std::string lang_str = SerdNodeToString(state->env, object_lang);
	ObjectKind kind = SerdNodeKind(object);

	state->accumulator->AddTriple(graph_str, subject_str, predicate_str, object_str, kind, datatype_str, lang_str);
	return SERD_SUCCESS;
}

static SerdStatus SerdProfileError(void *user_data, const SerdError *error) {
	auto *state = static_cast<SerdProfileState *>(user_data);
	if (state->strict_parsing) {
		state->has_error = true;
		state->error_message = std::string("SERD parsing error in '") + state->file_path + "', at line " +
		                       std::to_string(error->line) + ", column " + std::to_string(error->col);
		return SERD_FAILURE;
	} else {
		state->skip_count++;
		return SERD_SUCCESS;
	}
}

static SerdStatus SerdProfileBase(void *user_data, const SerdNode *uri) {
	auto *state = static_cast<SerdProfileState *>(user_data);
	serd_env_set_base_uri(state->env, uri);
	return SERD_SUCCESS;
}

static SerdStatus SerdProfilePrefix(void *user_data, const SerdNode *name, const SerdNode *uri) {
	auto *state = static_cast<SerdProfileState *>(user_data);
	serd_env_set_prefix(state->env, name, uri);
	return SERD_SUCCESS;
}

static SerdSyntax MapSerdSyntax(ITriplesBuffer::FileType file_type) {
	switch (file_type) {
	case ITriplesBuffer::TURTLE:
		return SERD_TURTLE;
	case ITriplesBuffer::NQUADS:
		return SERD_NQUADS;
	case ITriplesBuffer::NTRIPLES:
		return SERD_NTRIPLES;
	case ITriplesBuffer::TRIG:
		return SERD_TRIG;
	default:
		throw std::runtime_error("ProfileFileSerd: unsupported file type");
	}
}

void ProfileFileSerd(const std::string &file_path, duckdb::FileSystem &fs, ITriplesBuffer::FileType file_type,
                     bool strict_parsing, RDFProfileAccumulator &accumulator) {
	// Open file via DuckDB FileSystem (supports remote filesystems)
	std::unique_ptr<duckdb::FileHandle> fh;
	try {
		fh = fs.OpenFile(file_path, duckdb::FileFlags::FILE_FLAGS_READ);
	} catch (std::exception &ex) {
		throw std::runtime_error("Could not open RDF file: " + file_path + ": " + ex.what());
	}

	SerdNode base = SERD_NODE_NULL;
	std::unique_ptr<SerdEnv, decltype(&serd_env_free)> env(serd_env_new(&base), &serd_env_free);
	if (!env)
		throw std::runtime_error("Unable to create serd environment");

	SerdProfileState state;
	state.accumulator = &accumulator;
	state.env = env.get();
	state.strict_parsing = strict_parsing;
	state.file_path = file_path;

	SerdSyntax syntax = MapSerdSyntax(file_type);
	std::unique_ptr<SerdReader, decltype(&serd_reader_free)> reader(
	    serd_reader_new(syntax, &state, nullptr, &SerdProfileBase, &SerdProfilePrefix, &SerdProfileStatement, nullptr),
	    &serd_reader_free);
	if (!reader)
		throw std::runtime_error("Unable to create serd reader");

	serd_reader_set_strict(reader.get(), strict_parsing);
	serd_reader_set_error_sink(reader.get(), &SerdProfileError, &state);

	// Bridge DuckDB FileHandle → SerdSource
	auto duckdb_source = [](void *buf, size_t size, size_t nmemb, void *stream) -> size_t {
		(void)size;
		auto *handle = static_cast<duckdb::FileHandle *>(stream);
		int64_t n = handle->Read(buf, static_cast<duckdb::idx_t>(nmemb));
		return static_cast<size_t>(std::max<int64_t>(n, 0));
	};
	auto duckdb_error = [](void *) -> int {
		return 0;
	};

	serd_reader_start_source_stream(reader.get(), static_cast<SerdSource>(duckdb_source),
	                                static_cast<SerdStreamErrorFunc>(duckdb_error), fh.get(),
	                                reinterpret_cast<const uint8_t *>(file_path.c_str()), 4096U);

	// Read until EOF
	bool eof = false;
	while (!eof) {
		SerdStatus st = serd_reader_read_chunk(reader.get());
		if (st == SERD_SUCCESS) {
			continue;
		} else if (st == SERD_FAILURE) {
			serd_reader_end_stream(reader.get());
			try {
				duckdb::idx_t pos = fh->SeekPosition();
				int64_t sz = fs.GetFileSize(*fh);
				if (sz >= 0 && pos >= static_cast<duckdb::idx_t>(sz)) {
					eof = true;
				} else {
					if (state.has_error)
						throw duckdb::SyntaxException(state.error_message);
					throw std::runtime_error("SERD failure in " + file_path);
				}
			} catch (std::exception &ex) {
				if (state.has_error)
					throw duckdb::SyntaxException(state.error_message);
				throw std::runtime_error(std::string("SERD failure: ") + ex.what());
			}
		} else if (st == SERD_ERR_BAD_SYNTAX) {
			if (strict_parsing) {
				if (state.has_error)
					throw duckdb::SyntaxException(state.error_message);
				throw duckdb::SyntaxException("SERD bad RDF syntax in " + file_path);
			}
			serd_reader_skip_until_byte(reader.get(), '\n');
		} else {
			if (state.has_error)
				throw duckdb::SyntaxException(state.error_message);
			throw std::runtime_error("SERD error in " + file_path);
		}
	}
}

// ============================================================
// RDF/XML file profiler
// ============================================================

void ProfileFileXML(const std::string &file_path, duckdb::FileSystem &fs, bool strict_parsing,
                    RDFProfileAccumulator &accumulator) {
	std::unique_ptr<duckdb::FileHandle> fh;
	try {
		fh = fs.OpenFile(file_path, duckdb::FileFlags::FILE_FLAGS_READ);
	} catch (std::exception &ex) {
		throw std::runtime_error("Could not open RDF/XML file: " + file_path + ": " + ex.what());
	}

	RdfXmlParser parser(
	    // on_statement
	    [&](const RdfStatement &stmt) {
		    // Determine ObjectKind: blank nodes start with "_:", everything else is IRI unless it has
		    // a datatype or language tag (in which case it's a literal).
		    ObjectKind kind;
		    if (!stmt.datatype.empty() || !stmt.language.empty()) {
			    kind = ObjectKind::LITERAL;
		    } else if (stmt.object.size() >= 2 && stmt.object[0] == '_' && stmt.object[1] == ':') {
			    kind = ObjectKind::BLANK;
		    } else {
			    kind = ObjectKind::IRI;
		    }
		    accumulator.AddTriple("", stmt.subject, stmt.predicate, stmt.object, kind, stmt.datatype, stmt.language);
	    },
	    // on_namespace (not needed for profiling)
	    [](const std::string & /*prefix*/, const std::string & /*uri*/) {},
	    // on_error
	    [&](const std::string &msg) {
		    if (strict_parsing)
			    throw duckdb::SyntaxException("RDF/XML parse error in '" + file_path + "': " + msg);
	    });

	// Feed 4 KB chunks to the SAX parser
	const size_t BUF_SIZE = 4096;
	std::vector<char> buf(BUF_SIZE);
	bool eof = false;
	while (!eof) {
		int64_t n = fh->Read(buf.data(), BUF_SIZE);
		if (n < 0)
			n = 0;
		eof = static_cast<size_t>(n) < BUF_SIZE;
		if (n > 0)
			parser.parseChunk(buf.data(), static_cast<int>(n), eof);
	}
}
