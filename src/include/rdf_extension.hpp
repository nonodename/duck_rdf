#pragma once

#include "duckdb.hpp"

#define FUNCTION_NAME    "read_rdf"
#define FILE_TYPE        "file_type"
#define STRICT_PARSING   "strict_parsing"
#define PREFIX_EXPANSION "prefix_expansion"
#define FILENAME_PARAM   "filename"
#define PARALLEL_SCAN    "parallel_scan"
namespace duckdb {

class RdfExtension : public Extension {
public:
	void Load(ExtensionLoader &loader) override;
	std::string Name() override;
	std::string Version() const override;
};

} // namespace duckdb
