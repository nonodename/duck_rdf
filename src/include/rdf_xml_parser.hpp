#ifndef RDF_XML_PARSER_H
#define RDF_XML_PARSER_H

#include <iostream>
#include <string>
#include <functional>
#include <vector>
#include <map>
#include <cstring>
#include <libxml/parser.h>
#include <libxml/SAX2.h>
#include <memory>

/// A single RDF triple with optional datatype and language tag on the object.
// TODO: can this be replaced with the existing RDFRow struct in I_triples_buffer.hpp?
struct RdfStatement {
	std::string subject;
	std::string predicate;
	std::string object;
	std::string datatype; // XSD datatype URI, or empty
	std::string language; // BCP 47 language tag, or empty
};

class RdfXmlParser;

/// Non-owning view into a libxml2 buffer, used to avoid copying attribute values during parsing.
/// The pointers remain valid only for the duration of the SAX callback in which they are obtained.
struct LibXMLView {
	const xmlChar *start;
	const xmlChar *end;

	LibXMLView() : start(nullptr), end(nullptr) {
	}
	LibXMLView(const xmlChar *s, const xmlChar *e) : start(s), end(e) {
	}
	bool equals(const xmlChar *str) const {
		if (empty()) {
			return (str == nullptr);
		}
		return xmlStrncmp(start, str, end - start) == 0 && str[xmlStrlen(str)] == '\0';
	}
	bool empty() const {
		return start == end || start == nullptr;
	}

	// Helper to convert to string only when we MUST (e.g., storing in the stack)
	std::string toString() const {
		return empty() ? "" : std::string(reinterpret_cast<const char *>(start), end - start);
	}
};
/// Parsed RDF/XML attributes for a single element, held as non-owning views into libxml2 buffers.
struct RdfAttributes {
	LibXMLView about;     // rdf:about
	LibXMLView nodeID;    // rdf:nodeID
	LibXMLView rdf_id;    // rdf:ID
	LibXMLView resource;  // rdf:resource
	LibXMLView datatype;  // rdf:datatype
	LibXMLView parseType; // rdf:parseType ("Literal", "Resource", "Collection")
	LibXMLView lang;      // xml:lang
	LibXMLView base;      // xml:base

	/// Returns the subject URI/bnode for this element, applying rdf:about > rdf:ID > rdf:nodeID > auto bnode priority.
	std::string getSubject(RdfXmlParser *parser) const;
};

/// Streaming SAX-based parser for RDF/XML documents (https://www.w3.org/TR/rdf-syntax-grammar/).
///
/// Feed data incrementally via parseChunk(). Each complete RDF statement is delivered
/// synchronously to the StatementCallback as it is parsed. Namespace declarations are
/// reported via NamespaceCallback. Errors are reported non-fatally via ErrorCallback.
class RdfXmlParser {
public:
	using StatementCallback = std::function<void(const RdfStatement &)>;
	using NamespaceCallback = std::function<void(const std::string &prefix, const std::string &uri)>;
	using ErrorCallback = std::function<void(const std::string &message)>;

	/// @param s_cb  Called for every emitted RDF statement.
	/// @param n_cb  Called for every namespace declaration encountered.
	/// @param e_cb  Called on non-fatal parse errors.
	/// @param base  Initial base URI used to resolve relative URIs.
	RdfXmlParser(StatementCallback s_cb = nullptr, NamespaceCallback n_cb = nullptr, ErrorCallback e_cb = nullptr,
	             std::string base = "");

	/// Feed a chunk of XML data to the parser.
	/// @param chunk     Pointer to the data buffer (may be nullptr on the final call).
	/// @param size      Number of bytes in @p chunk.
	/// @param is_final  Must be true on the last call; libxml2 will leak resources if omitted.
	void parseChunk(const char *chunk, int size, bool is_final);

	~RdfXmlParser();

	/// Register a namespace prefix → URI mapping (used for namespace-aware URI expansion).
	void addNameSpace(const std::string &prefix, const std::string &uri);

	/// Set the prefix string prepended to auto-generated blank node identifiers (default "_:b").
	void setBlankNodePrefix(const std::string &prefix) {
		_blank_node_prefix = prefix;
	}

private:
	friend struct RdfAttributes;
	constexpr static char const *RDF_NS_XS = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
	const std::string RDF_NS = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
	const std::string XML_NS = "http://www.w3.org/XML/1998/namespace";
	const std::string NIL_URI = "http://www.w3.org/1999/02/22-rdf-syntax-ns#nil";
	const std::string FIRST_URI = "http://www.w3.org/1999/02/22-rdf-syntax-ns#first";
	const std::string REST_URI = "http://www.w3.org/1999/02/22-rdf-syntax-ns#rest";
	constexpr static char const *LANG_TAG = "lang";
	constexpr static char const *BASE_TAG = "base";
	constexpr static char const *ABOUT_ATTR = "about";
	constexpr static char const *ID_ATTR = "ID";
	constexpr static char const *NODE_ID_ATTR = "nodeID";
	constexpr static char const *RESOURCE_ATTR = "resource";
	constexpr static char const *DATATYPE_ATTR = "datatype";
	constexpr static char const *PARSE_TYPE_ATTR = "parseType";
	StatementCallback on_statement;
	NamespaceCallback on_namespace;
	ErrorCallback on_error;
	std::string base_uri;
	bool _at_eof = false;
	unsigned long bnode_count;
	std::string _blank_node_prefix = "_:b";
	std::unique_ptr<xmlParserCtxt, decltype(&xmlFreeParserCtxt)> _ctxt;
	std::map<std::string, std::string> _nameSpaces;

	// Cached RDF URIs to avoid repeated string concatenations
	std::string RDF_LI_URI;
	std::string RDF_TYPE_URI;
	std::string RDF_DESCRIPTION_URI;
	std::string RDF_RDF_URI;
	std::string RDF_STATEMENT_URI;
	std::string RDF_SUBJECT_URI;
	std::string RDF_PREDICATE_URI;
	std::string RDF_OBJECT_URI;
	std::string RDF_XMLLITERAL_URI;
	/// Role of an element on the parse stack, following the RDF/XML grammar productions.
	enum class ElementType {
		NODE,                 // Subject node element
		PROPERTY,             // Property element (text or resource object)
		PROPERTY_XML_LITERAL, // Property element with rdf:parseType="Literal"
		PROPERTY_COLLECTION,  // Property element with rdf:parseType="Collection"
		ROOT                  // rdf:RDF wrapper element
	};

	/// Per-element state maintained on the parse stack while a SAX element is open.
	struct ElementFrame {
		ElementType type;
		std::string uri;
		std::string lang;
		std::string datatype;
		std::string reify_id;        // URI for reification statements (from rdf:ID on a property)
		std::string text_buf;        // Accumulated character data / XMLLiteral content
		std::string baseURI;         // Resolved xml:base for this element
		bool has_obj_nodes = false;  // True once a child node element has been seen (suppresses text literal)
		int li_counter = 0;          // Tracks rdf:_1, rdf:_2, … for NODE types with rdf:li children
		std::string collection_tail; // Last BNode in an rdf:parseType="Collection" linked list
		int literal_depth = 0;       // Nesting depth of child elements inside an XMLLiteral
		ElementFrame(ElementType t, std::string u, std::string l, LibXMLView d, std::string r, std::string tb,
		             std::string bu, bool obj)
		    : type(t), uri(u), lang(l), datatype(d.toString()), reify_id(r), text_buf(tb), baseURI(bu),
		      has_obj_nodes(obj) {
		}
	};

	std::vector<ElementFrame> _stack;

	xmlSAXHandler saxHandler;

	static bool isAbsolute(const std::string &uri);
	std::string generateBNode();
	static std::string xmlEscape(const std::string &data);
	static std::string xmlEscape(const char *data, int len);
	std::string literalXML(const xmlChar *localname, const xmlChar *prefix, const xmlChar **namespaces,
	                       int nb_namespaces, const xmlChar **attributes, int nb_attributes);
	std::string currentBaseURI();
	bool isReservedAttr(const std::string &uri);
	void setupSAX();
	void processAttributes(int nb_attributes, const xmlChar **attributes, const std::string &subject,
	                       const std::string &lang);
	RdfAttributes parseAttributes(int nb_attributes, const xmlChar **attributes, const ElementFrame *parentFrame);

	// Helper methods for onStartElement refactoring
	ElementType determineParentType(const ElementFrame *parent_frame) const;
	bool determineIsNode(ElementType parent_type) const;
	void resolveBaseAndLang(std::string &base, std::string &lang, const RdfAttributes &attrs,
	                        const ElementFrame *parent_frame);
	void processNodeInPropertyContext(ElementFrame *parent_frame, ElementType parent_type, const std::string &subject,
	                                  const RdfAttributes &attrs);
	void processNodeElement(ElementFrame *parent_frame, ElementType parent_type, const std::string &current_uri,
	                        const std::string &subject, const RdfAttributes &attrs, int nb_attributes,
	                        const xmlChar **attributes, const std::string &lang, const std::string &base);
	void processPropertyElement(ElementFrame *parent_frame, const std::string &current_uri, const RdfAttributes &attrs,
	                            int nb_attributes, const xmlChar **attributes, const std::string &lang);
	void handlePropertyLiteral(const std::string &current_uri, const RdfAttributes &attrs, const std::string &lang);
	void handlePropertyCollection(const std::string &current_uri, const RdfAttributes &attrs, const std::string &lang);
	void handlePropertyResource(ElementFrame *parent_frame, const std::string &current_uri, const RdfAttributes &attrs,
	                            const std::string &lang);
	void handlePropertyWithObject(ElementFrame *parent_frame, const std::string &current_uri,
	                              const RdfAttributes &attrs, int nb_attributes, const xmlChar **attributes,
	                              const std::string &lang);
	void handleEmptyProperty(const std::string &current_uri, const RdfAttributes &attrs, const std::string &lang);
	static void onStartElement(void *ctx, const xmlChar *localname, const xmlChar *prefix, const xmlChar *URI,
	                           int nb_namespaces, const xmlChar **namespaces, int nb_attributes, int nb_defaulted,
	                           const xmlChar **attributes);

	static void onEndElement(void *ctx, const xmlChar *localname, const xmlChar *prefix, const xmlChar *URI);
	static void onCharacters(void *ctx, const xmlChar *ch, int len);

	void emitWithReification(const std::string &s, const std::string &p, const std::string &o, const std::string &dt,
	                         const std::string &lang, const std::string &r_id);
	void emit(const std::string &s, const std::string &p, const std::string &o, const std::string &dt,
	          const std::string &lang);
	std::string expandUri(const xmlChar *URI, const xmlChar *localname);

	std::string trim(const std::string &s);
};

#endif // RDF_XML_PARSER_H
