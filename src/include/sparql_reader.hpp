#pragma once

namespace duckdb {

class ExtensionLoader;

void RegisterSPARQLReader(ExtensionLoader &loader);

} // namespace duckdb
