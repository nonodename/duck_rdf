#include "include/rdf_multi_file.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"

namespace duckdb {

vector<OpenFileInfo> ResolveRDFFiles(ClientContext &context, TableFunctionBindInput &input,
                                     const string &function_name) {
	auto multi_file_reader = MultiFileReader::CreateDefault(function_name);
	auto file_list = multi_file_reader->CreateFileList(context, input.inputs[0]);
	return file_list->GetAllFiles();
}

TableFunctionSet RegisterRDFFileListFunction(TableFunction tf) {
	return MultiFileReader::CreateFunctionSet(std::move(tf));
}

} // namespace duckdb
