use rio_api::model::NamedNode;

pub const PREFIXES: [(&str, &str); 8] = [
    ("bioschema", "http://bioschemas.org/"),
    ("owl", "http://www.w3.org/2002/07/owl#"),
    ("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#"),
    ("rdfs", "http://www.w3.org/2000/01/rdf-schema#"),
    ("schema", "http://schema.org/"),
    ("xsd", "http://www.w3.org/2001/XMLSchema#"),
    ("yago", "http://yago-knowledge.org/resource/"),
    ("yagov", "http://yago-knowledge.org/value/"),
];

pub const WIKIBASE_ITEM: NamedNode = NamedNode {
    iri: "http://wikiba.se/ontology#Item",
};
pub const WIKIBASE_BEST_RANK: NamedNode = NamedNode {
    iri: "http://wikiba.se/ontology#BestRank",
};
pub const WIKIBASE_TIME_VALUE: NamedNode = NamedNode {
    iri: "http://wikiba.se/ontology#timeValue",
};
pub const WIKIBASE_TIME_PRECISION: NamedNode = NamedNode {
    iri: "http://wikiba.se/ontology#timePrecision",
};
pub const WIKIBASE_TIME_CALENDAR_MODEL: NamedNode = NamedNode {
    iri: "http://wikiba.se/ontology#timeCalendarModel",
};
pub const WIKIBASE_GEO_LATITUDE: NamedNode = NamedNode {
    iri: "http://wikiba.se/ontology#geoLatitude",
};
pub const WIKIBASE_GEO_LONGITUDE: NamedNode = NamedNode {
    iri: "http://wikiba.se/ontology#geoLongitude",
};
pub const WIKIBASE_GEO_PRECISION: NamedNode = NamedNode {
    iri: "http://wikiba.se/ontology#geoPrecision",
};
pub const WIKIBASE_GEO_GLOBE: NamedNode = NamedNode {
    iri: "http://wikiba.se/ontology#geoGlobe",
};
pub const WIKIBASE_QUANTITY_AMOUNT: NamedNode = NamedNode {
    iri: "http://wikiba.se/ontology#quantityAmount",
};
pub const WIKIBASE_QUANTITY_UPPER_BOUND: NamedNode = NamedNode {
    iri: "http://wikiba.se/ontology#quantityUpperBound",
};
pub const WIKIBASE_QUANTITY_LOWER_BOUND: NamedNode = NamedNode {
    iri: "http://wikiba.se/ontology#quantityLowerBound",
};
pub const WIKIBASE_QUANTITY_UNIT: NamedNode = NamedNode {
    iri: "http://wikiba.se/ontology#quantityUnit",
};
pub const WDT_P31: NamedNode = NamedNode {
    iri: "http://www.wikidata.org/prop/direct/P31",
};
pub const WDT_P279: NamedNode = NamedNode {
    iri: "http://www.wikidata.org/prop/direct/P279",
};
pub const WDT_P646: NamedNode = NamedNode {
    iri: "http://www.wikidata.org/prop/direct/P646",
};
pub const WD_Q7727: NamedNode = NamedNode {
    iri: "http://www.wikidata.org/entity/Q7727",
};
pub const WD_Q11574: NamedNode = NamedNode {
    iri: "http://www.wikidata.org/entity/Q11574",
};
pub const WD_Q25235: NamedNode = NamedNode {
    iri: "http://www.wikidata.org/entity/Q25235",
};
pub const WD_Q573: NamedNode = NamedNode {
    iri: "http://www.wikidata.org/entity/Q573",
};
pub const WD_Q199: NamedNode = NamedNode {
    iri: "http://www.wikidata.org/entity/Q199",
};
pub const WD_Q2: NamedNode = NamedNode {
    iri: "http://www.wikidata.org/entity/Q2",
};
pub const WD_Q1985727: NamedNode = NamedNode {
    iri: "http://www.wikidata.org/entity/Q1985727",
};
pub const SCHEMA_THING: NamedNode = NamedNode {
    iri: "http://schema.org/Thing",
};
pub const SCHEMA_ENUMERATION: NamedNode = NamedNode {
    iri: "http://schema.org/Enumeration",
};
pub const SCHEMA_MEDICAL_ENUMERATION: NamedNode = NamedNode {
    iri: "http://schema.org/MedicalEnumeration",
};
pub const SCHEMA_INTANGIBLE: NamedNode = NamedNode {
    iri: "http://schema.org/Intangible",
};
pub const SCHEMA_MEDICAL_INTANGIBLE: NamedNode = NamedNode {
    iri: "http://schema.org/MedicalIntangible",
};
pub const SCHEMA_MEDICAL_ENTITY: NamedNode = NamedNode {
    iri: "http://schema.org/MedicalEntity",
};
pub const SCHEMA_SERIES: NamedNode = NamedNode {
    iri: "http://schema.org/Series",
};
pub const SCHEMA_STRUCTURED_VALUE: NamedNode = NamedNode {
    iri: "http://schema.org/StructuredValue",
};
pub const SCHEMA_GEO_COORDINATES: NamedNode = NamedNode {
    iri: "http://schema.org/GeoCoordinates",
};
pub const SCHEMA_QUANTITATIVE_VALUE: NamedNode = NamedNode {
    iri: "http://schema.org/QuantitativeValue",
};
pub const SCHEMA_IMAGE_OBJECT: NamedNode = NamedNode {
    iri: "http://schema.org/ImageObject",
};
pub const SCHEMA_ABOUT: NamedNode = NamedNode {
    iri: "http://schema.org/about",
};
pub const SCHEMA_ALTERNATE_NAME: NamedNode = NamedNode {
    iri: "http://schema.org/alternateName",
};
pub const SCHEMA_DESCRIPTION: NamedNode = NamedNode {
    iri: "http://schema.org/description",
};
pub const SCHEMA_INVERSE_OF: NamedNode = NamedNode {
    iri: "http://schema.org/inverseOf",
};
pub const SCHEMA_SAME_AS: NamedNode = NamedNode {
    iri: "http://schema.org/sameAs",
};
pub const SCHEMA_MAX_VALUE: NamedNode = NamedNode {
    iri: "http://schema.org/maxValue",
};
pub const SCHEMA_MIN_VALUE: NamedNode = NamedNode {
    iri: "http://schema.org/minValue",
};
pub const SCHEMA_UNIT_CODE: NamedNode = NamedNode {
    iri: "http://schema.org/unitCode",
};
pub const SCHEMA_LATITUDE: NamedNode = NamedNode {
    iri: "http://schema.org/latitude",
};
pub const SCHEMA_LONGITUDE: NamedNode = NamedNode {
    iri: "http://schema.org/longitude",
};
pub const SCHEMA_VALUE: NamedNode = NamedNode {
    iri: "http://schema.org/value",
};
pub const SKOS_PREF_LABEL: NamedNode = NamedNode {
    iri: "http://www.w3.org/2004/02/skos/core#prefLabel",
};
pub const XSD_ANY_URI: NamedNode = NamedNode {
    iri: "http://www.w3.org/2001/XMLSchema#anyURI",
};
pub const XSD_BOOLEAN: NamedNode = NamedNode {
    iri: "http://www.w3.org/2001/XMLSchema#boolean",
};
pub const XSD_DATE: NamedNode = NamedNode {
    iri: "http://www.w3.org/2001/XMLSchema#date",
};
pub const XSD_DATE_TIME: NamedNode = NamedNode {
    iri: "http://www.w3.org/2001/XMLSchema#dateTime",
};
pub const XSD_DECIMAL: NamedNode = NamedNode {
    iri: "http://www.w3.org/2001/XMLSchema#decimal",
};
pub const XSD_DOUBLE: NamedNode = NamedNode {
    iri: "http://www.w3.org/2001/XMLSchema#double",
};
pub const XSD_DURATION: NamedNode = NamedNode {
    iri: "http://www.w3.org/2001/XMLSchema#duration",
};
pub const XSD_INTEGER: NamedNode = NamedNode {
    iri: "http://www.w3.org/2001/XMLSchema#integer",
};
pub const XSD_G_YEAR: NamedNode = NamedNode {
    iri: "http://www.w3.org/2001/XMLSchema#gYear",
};
pub const XSD_G_YEAR_MONTH: NamedNode = NamedNode {
    iri: "http://www.w3.org/2001/XMLSchema#gYearMonth",
};
pub const XSD_STRING: NamedNode = NamedNode {
    iri: "http://www.w3.org/2001/XMLSchema#string",
};
pub const RDF_FIRST: NamedNode = NamedNode {
    iri: "http://www.w3.org/1999/02/22-rdf-syntax-ns#first",
};
pub const RDF_LANG_STRING: NamedNode = NamedNode {
    iri: "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString",
};
pub const RDF_NIL: NamedNode = NamedNode {
    iri: "http://www.w3.org/1999/02/22-rdf-syntax-ns#nil",
};
pub const RDF_PLAIN_LITERAL: NamedNode = NamedNode {
    iri: "http://www.w3.org/1999/02/22-rdf-syntax-ns#PlainLiteral",
};
pub const RDF_PROPERTY: NamedNode = NamedNode {
    iri: "http://www.w3.org/1999/02/22-rdf-syntax-ns#Property",
};
pub const RDF_REST: NamedNode = NamedNode {
    iri: "http://www.w3.org/1999/02/22-rdf-syntax-ns#rest",
};
pub const RDF_TYPE: NamedNode = NamedNode {
    iri: "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
};
pub const RDFS_CLASS: NamedNode = NamedNode {
    iri: "http://www.w3.org/2000/01/rdf-schema#Class",
};
pub const RDFS_COMMENT: NamedNode = NamedNode {
    iri: "http://www.w3.org/2000/01/rdf-schema#comment",
};
pub const RDFS_DATATYPE: NamedNode = NamedNode {
    iri: "http://www.w3.org/2000/01/rdf-schema#Datatype",
};
pub const RDFS_DOMAIN: NamedNode = NamedNode {
    iri: "http://www.w3.org/2000/01/rdf-schema#domain",
};
pub const RDFS_LABEL: NamedNode = NamedNode {
    iri: "http://www.w3.org/2000/01/rdf-schema#label",
};
pub const RDFS_RANGE: NamedNode = NamedNode {
    iri: "http://www.w3.org/2000/01/rdf-schema#range",
};
pub const RDFS_SUB_CLASS_OF: NamedNode = NamedNode {
    iri: "http://www.w3.org/2000/01/rdf-schema#subClassOf",
};
pub const RDFS_SUB_PROPERTY_OF: NamedNode = NamedNode {
    iri: "http://www.w3.org/2000/01/rdf-schema#subPropertyOf",
};
pub const OWL_CLASS: NamedNode = NamedNode {
    iri: "http://www.w3.org/2002/07/owl#Class",
};
pub const OWL_DATATYPE_PROPERTY: NamedNode = NamedNode {
    iri: "http://www.w3.org/2002/07/owl#DatatypeProperty",
};
pub const OWL_DISJOINT_WITH: NamedNode = NamedNode {
    iri: "http://www.w3.org/2002/07/owl#disjointWith",
};
pub const OWL_FUNCTIONAL_PROPERTY: NamedNode = NamedNode {
    iri: "http://www.w3.org/2002/07/owl#FunctionalProperty",
};
pub const OWL_INVERSE_OF: NamedNode = NamedNode {
    iri: "http://www.w3.org/2002/07/owl#inverseOf",
};
pub const OWL_OBJECT_PROPERTY: NamedNode = NamedNode {
    iri: "http://www.w3.org/2002/07/owl#ObjectProperty",
};
pub const OWL_SAME_AS: NamedNode = NamedNode {
    iri: "http://www.w3.org/2002/07/owl#sameAs",
};
pub const OWL_UNION_OF: NamedNode = NamedNode {
    iri: "http://www.w3.org/2002/07/owl#unionOf",
};
pub const SH_DATATYPE: NamedNode = NamedNode {
    iri: "http://www.w3.org/ns/shacl#datatype",
};
pub const SH_MAX_COUNT: NamedNode = NamedNode {
    iri: "http://www.w3.org/ns/shacl#maxCount",
};
pub const SH_NODE: NamedNode = NamedNode {
    iri: "http://www.w3.org/ns/shacl#node",
};
pub const SH_NODE_SHAPE: NamedNode = NamedNode {
    iri: "http://www.w3.org/ns/shacl#NodeShape",
};
pub const SH_OR: NamedNode = NamedNode {
    iri: "http://www.w3.org/ns/shacl#or",
};
pub const SH_PATTERN: NamedNode = NamedNode {
    iri: "http://www.w3.org/ns/shacl#pattern",
};
pub const SH_PATH: NamedNode = NamedNode {
    iri: "http://www.w3.org/ns/shacl#path",
};
pub const SH_PROPERTY: NamedNode = NamedNode {
    iri: "http://www.w3.org/ns/shacl#property",
};
pub const SH_PROPERTY_SHAPE: NamedNode = NamedNode {
    iri: "http://www.w3.org/ns/shacl#PropertyShape",
};
pub const SH_TARGET_CLASS: NamedNode = NamedNode {
    iri: "http://www.w3.org/ns/shacl#targetClass",
};
pub const SH_UNIQUE_LANG: NamedNode = NamedNode {
    iri: "http://www.w3.org/ns/shacl#uniqueLang",
};

pub const YS_FROM_CLASS: NamedNode = NamedNode {
    iri: "http://yago-knowledge.org/schema#fromClass",
};
pub const YS_FROM_PROPERTY: NamedNode = NamedNode {
    iri: "http://yago-knowledge.org/schema#fromProperty",
};
pub const YS_ANNOTATION_PROPERTY_SHAPE: NamedNode = NamedNode {
    iri: "http://yago-knowledge.org/schema#AnnotationPropertyShape",
};
