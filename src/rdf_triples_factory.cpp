#include "include/rdf_triples_factory.hpp"
#include "include/serd_buffer.hpp"
#include "include/serd_range_buffer.hpp"
#ifndef DUCK_RDF_NO_XML
#include "include/xml_buffer.hpp"
#endif

namespace duckdb {

unique_ptr<ITriplesBuffer> OpenTriplesFile(const string &file_path, ITriplesBuffer::FileType ft, FileSystem &fs,
                                           bool strict_parsing, bool expand_prefixes) {
	if (ft == ITriplesBuffer::UNKNOWN) {
		ft = ITriplesBuffer::DetectFileTypeFromPath(file_path);
	}
	switch (ft) {
	case ITriplesBuffer::TURTLE:
	case ITriplesBuffer::NQUADS:
	case ITriplesBuffer::NTRIPLES:
	case ITriplesBuffer::TRIG:
		return make_uniq<SerdBuffer>(file_path, "", &fs, strict_parsing, expand_prefixes, ft);
	case ITriplesBuffer::XML:
#ifdef DUCK_RDF_NO_XML
		throw IOException("RDF/XML parsing is not supported in this build");
#else
		return make_uniq<XMLBuffer>(file_path, "", &fs, strict_parsing, expand_prefixes, ft);
#endif
	default:
		throw IOException("Cannot determine file type for: " + file_path);
	}
}

unique_ptr<ITriplesBuffer> OpenTriplesFileRange(const string &file_path, ITriplesBuffer::FileType ft, FileSystem &fs,
                                                bool strict_parsing, bool expand_prefixes, idx_t range_start,
                                                idx_t range_end, idx_t file_size) {
	if (ft == ITriplesBuffer::UNKNOWN) {
		ft = ITriplesBuffer::DetectFileTypeFromPath(file_path);
	}
	switch (ft) {
	case ITriplesBuffer::NQUADS:
	case ITriplesBuffer::NTRIPLES:
		return make_uniq<SerdRangeBuffer>(file_path, "", &fs, strict_parsing, expand_prefixes, ft, range_start,
		                                  range_end, file_size);
	default:
		throw InternalException("OpenTriplesFileRange only supports NTriples/NQuads, got file: " + file_path);
	}
}

} // namespace duckdb
