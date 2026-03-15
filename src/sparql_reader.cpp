#include "include/sparql_reader.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/function/table_function.hpp"

#include <curl/curl.h>
#include <atomic>
#include <string>
#include <vector>

namespace duckdb {

// ============================================================
// CSV parsing (RFC 4180)
// ============================================================

// Parse a single CSV field starting at pos. Advances pos past the trailing
// comma (or to end of input). Returns the unescaped field value.
static std::string ParseCSVField(const std::string &s, size_t &pos) {
	std::string field;

	if (pos >= s.size()) {
		return field;
	}

	if (s[pos] == '"') {
		// Quoted field
		++pos; // skip opening quote
		while (pos < s.size()) {
			if (s[pos] == '"') {
				// Either end of field or escaped quote ""
				if (pos + 1 < s.size() && s[pos + 1] == '"') {
					field += '"';
					pos += 2;
				} else {
					++pos; // skip closing quote
					break;
				}
			} else {
				field += s[pos++];
			}
		}
		// Skip trailing comma if present
		if (pos < s.size() && s[pos] == ',') {
			++pos;
		}
	} else {
		// Unquoted field — read until comma or end of string
		while (pos < s.size() && s[pos] != ',' && s[pos] != '\r' && s[pos] != '\n') {
			field += s[pos++];
		}
		if (pos < s.size() && s[pos] == ',') {
			++pos;
		}
	}

	return field;
}

// Parse one CSV row from s starting at pos. Advances pos past the row
// terminator. Returns false when there is no row to parse (end of input).
static bool ParseCSVRow(const std::string &s, size_t &pos, std::vector<std::string> &row) {
	// Skip any leading CR+LF or LF-only line endings from previous rows
	while (pos < s.size() && (s[pos] == '\r' || s[pos] == '\n')) {
		++pos;
	}
	if (pos >= s.size()) {
		return false;
	}

	row.clear();

	// A quoted field may span multiple "lines", so we read fields until we hit
	// an unquoted newline (or end of input).
	while (pos < s.size()) {
		if (s[pos] == '\r' || s[pos] == '\n') {
			// End of row — consume the line terminator(s) and stop
			while (pos < s.size() && (s[pos] == '\r' || s[pos] == '\n')) {
				++pos;
			}
			break;
		}
		row.push_back(ParseCSVField(s, pos));
	}

	return !row.empty();
}

// Parse a complete SPARQL CSV result body.
// Returns {header_row, data_rows}.
static std::pair<std::vector<std::string>, std::vector<std::vector<std::string>>>
ParseSPARQLCSV(const std::string &body) {
	size_t pos = 0;
	std::vector<std::string> header;
	std::vector<std::vector<std::string>> rows;

	std::vector<std::string> row;
	bool first = true;
	while (ParseCSVRow(body, pos, row)) {
		if (first) {
			header = row;
			first = false;
		} else {
			rows.push_back(row);
		}
	}
	return {header, rows};
}

// ============================================================
// HTTP fetch via libcurl
// ============================================================

static size_t CurlWriteCallback(char *ptr, size_t size, size_t nmemb, void *userdata) {
	auto *body = static_cast<std::string *>(userdata);
	body->append(ptr, size * nmemb);
	return size * nmemb;
}

static std::string FetchSPARQLCSV(const std::string &endpoint, const std::string &query) {
	CURL *curl = curl_easy_init();
	if (!curl) {
		throw IOException("Failed to initialise libcurl handle");
	}

	// URL-encode the query string
	char *escaped_query = curl_easy_escape(curl, query.c_str(), (int)query.size());
	if (!escaped_query) {
		curl_easy_cleanup(curl);
		throw IOException("Failed to URL-encode SPARQL query");
	}

	std::string url = endpoint;
	// Append query parameter — detect whether endpoint already has a '?'
	url += (url.find('?') == std::string::npos) ? "?query=" : "&query=";
	url += escaped_query;
	curl_free(escaped_query);

	std::string response_body;

	struct curl_slist *headers = nullptr;
	headers = curl_slist_append(headers, "Accept: text/csv");

	curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
	curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, CurlWriteCallback);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response_body);
	curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
	// Identify ourselves politely — version injected by the build system
#ifdef EXT_VERSION_RDF
	curl_easy_setopt(curl, CURLOPT_USERAGENT, "duck_rdf/" EXT_VERSION_RDF);
#else
	curl_easy_setopt(curl, CURLOPT_USERAGENT, "duck_rdf/0.0.1-unknown");
#endif

	CURLcode res = curl_easy_perform(curl);

	if (res != CURLE_OK) {
		std::string err = curl_easy_strerror(res);
		curl_slist_free_all(headers);
		curl_easy_cleanup(curl);
		throw IOException("SPARQL HTTP request failed: " + err);
	}

	long http_code = 0;
	curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
	curl_slist_free_all(headers);
	curl_easy_cleanup(curl);

	if (http_code < 200 || http_code >= 300) {
		throw IOException("SPARQL endpoint returned HTTP " + std::to_string(http_code) + " for endpoint: " + endpoint);
	}

	return response_body;
}

// ============================================================
// Table function state
// ============================================================

struct SPARQLBindData : public TableFunctionData {
	std::vector<std::string> column_names;
	std::vector<std::vector<std::string>> rows;
};

struct SPARQLGlobalState : public GlobalTableFunctionState {
	std::atomic<idx_t> position {0};
};

// ============================================================
// Table function implementation
// ============================================================

static unique_ptr<FunctionData> SPARQLBind(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<string> &names) {
	std::string endpoint = input.inputs[0].GetValue<string>();
	std::string query = input.inputs[1].GetValue<string>();

	if (endpoint.empty()) {
		throw InvalidInputException("read_sparql: endpoint must not be empty");
	}
	if (query.empty()) {
		throw InvalidInputException("read_sparql: query must not be empty");
	}

	std::string body = FetchSPARQLCSV(endpoint, query);
	auto parsed = ParseSPARQLCSV(body);
	auto &header = parsed.first;
	auto &rows = parsed.second;

	if (header.empty()) {
		throw IOException("read_sparql: SPARQL endpoint returned an empty response (no CSV header)");
	}

	auto result = make_uniq<SPARQLBindData>();
	result->column_names = header;
	result->rows = std::move(rows);

	for (auto &col_name : header) {
		names.push_back(col_name);
	}
	return_types.assign(header.size(), LogicalType::VARCHAR);

	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> SPARQLGlobalInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<SPARQLGlobalState>();
}

static void SPARQLFunc(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &bind_data = input.bind_data->Cast<SPARQLBindData>();
	auto &global_state = input.global_state->Cast<SPARQLGlobalState>();

	idx_t num_rows = bind_data.rows.size();
	idx_t num_cols = bind_data.column_names.size();

	// Atomically claim a batch of rows
	idx_t start = global_state.position.fetch_add(STANDARD_VECTOR_SIZE);
	if (start >= num_rows) {
		return; // signals end of scan to DuckDB
	}
	idx_t end = (std::min)(start + (idx_t)STANDARD_VECTOR_SIZE, num_rows);
	idx_t count = end - start;

	// Populate output vectors
	for (idx_t col = 0; col < num_cols; col++) {
		auto &vec = output.data[col];
		for (idx_t row = 0; row < count; row++) {
			const auto &data_row = bind_data.rows[start + row];
			if (col < data_row.size()) {
				vec.SetValue(row, Value(data_row[col]));
			} else {
				vec.SetValue(row, Value(""));
			}
		}
	}

	output.SetCardinality(count);
}

// ============================================================
// Registration
// ============================================================

void RegisterSPARQLReader(ExtensionLoader &loader) {
	TableFunction tf("read_sparql", {LogicalType::VARCHAR, LogicalType::VARCHAR}, SPARQLFunc, SPARQLBind,
	                 SPARQLGlobalInit);
	loader.RegisterFunction(tf);
}

} // namespace duckdb
