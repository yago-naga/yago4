//! Useful constants

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

pub const WIKIBASE_ITEM: NamedNode<'_> = NamedNode {
    iri: "http://wikiba.se/ontology#Item",
};
pub const WIKIBASE_BEST_RANK: NamedNode<'_> = NamedNode {
    iri: "http://wikiba.se/ontology#BestRank",
};
pub const WIKIBASE_TIME_VALUE: NamedNode<'_> = NamedNode {
    iri: "http://wikiba.se/ontology#timeValue",
};
pub const WIKIBASE_TIME_PRECISION: NamedNode<'_> = NamedNode {
    iri: "http://wikiba.se/ontology#timePrecision",
};
pub const WIKIBASE_TIME_CALENDAR_MODEL: NamedNode<'_> = NamedNode {
    iri: "http://wikiba.se/ontology#timeCalendarModel",
};
pub const WIKIBASE_GEO_LATITUDE: NamedNode<'_> = NamedNode {
    iri: "http://wikiba.se/ontology#geoLatitude",
};
pub const WIKIBASE_GEO_LONGITUDE: NamedNode<'_> = NamedNode {
    iri: "http://wikiba.se/ontology#geoLongitude",
};
pub const WIKIBASE_GEO_PRECISION: NamedNode<'_> = NamedNode {
    iri: "http://wikiba.se/ontology#geoPrecision",
};
pub const WIKIBASE_GEO_GLOBE: NamedNode<'_> = NamedNode {
    iri: "http://wikiba.se/ontology#geoGlobe",
};
pub const WIKIBASE_QUANTITY_AMOUNT: NamedNode<'_> = NamedNode {
    iri: "http://wikiba.se/ontology#quantityAmount",
};
pub const WIKIBASE_QUANTITY_UPPER_BOUND: NamedNode<'_> = NamedNode {
    iri: "http://wikiba.se/ontology#quantityUpperBound",
};
pub const WIKIBASE_QUANTITY_LOWER_BOUND: NamedNode<'_> = NamedNode {
    iri: "http://wikiba.se/ontology#quantityLowerBound",
};
pub const WIKIBASE_QUANTITY_UNIT: NamedNode<'_> = NamedNode {
    iri: "http://wikiba.se/ontology#quantityUnit",
};
pub const WDT_P31: NamedNode<'_> = NamedNode {
    iri: "http://www.wikidata.org/prop/direct/P31",
};
pub const WDT_P279: NamedNode<'_> = NamedNode {
    iri: "http://www.wikidata.org/prop/direct/P279",
};
pub const WDT_P646: NamedNode<'_> = NamedNode {
    iri: "http://www.wikidata.org/prop/direct/P646",
};
pub const WD_Q7727: NamedNode<'_> = NamedNode {
    iri: "http://www.wikidata.org/entity/Q7727",
};
pub const WD_Q11574: NamedNode<'_> = NamedNode {
    iri: "http://www.wikidata.org/entity/Q11574",
};
pub const WD_Q25235: NamedNode<'_> = NamedNode {
    iri: "http://www.wikidata.org/entity/Q25235",
};
pub const WD_Q573: NamedNode<'_> = NamedNode {
    iri: "http://www.wikidata.org/entity/Q573",
};
pub const WD_Q199: NamedNode<'_> = NamedNode {
    iri: "http://www.wikidata.org/entity/Q199",
};
pub const WD_Q2: NamedNode<'_> = NamedNode {
    iri: "http://www.wikidata.org/entity/Q2",
};
pub const WD_Q1985727: NamedNode<'_> = NamedNode {
    iri: "http://www.wikidata.org/entity/Q1985727",
};
pub const SCHEMA_THING: NamedNode<'_> = NamedNode {
    iri: "http://schema.org/Thing",
};
pub const SCHEMA_ENUMERATION: NamedNode<'_> = NamedNode {
    iri: "http://schema.org/Enumeration",
};
pub const SCHEMA_MEDICAL_ENUMERATION: NamedNode<'_> = NamedNode {
    iri: "http://schema.org/MedicalEnumeration",
};
pub const SCHEMA_INTANGIBLE: NamedNode<'_> = NamedNode {
    iri: "http://schema.org/Intangible",
};
pub const SCHEMA_MEDICAL_INTANGIBLE: NamedNode<'_> = NamedNode {
    iri: "http://schema.org/MedicalIntangible",
};
pub const SCHEMA_MEDICAL_ENTITY: NamedNode<'_> = NamedNode {
    iri: "http://schema.org/MedicalEntity",
};
pub const SCHEMA_SERIES: NamedNode<'_> = NamedNode {
    iri: "http://schema.org/Series",
};
pub const SCHEMA_STRUCTURED_VALUE: NamedNode<'_> = NamedNode {
    iri: "http://schema.org/StructuredValue",
};
pub const SCHEMA_GEO_COORDINATES: NamedNode<'_> = NamedNode {
    iri: "http://schema.org/GeoCoordinates",
};
pub const SCHEMA_QUANTITATIVE_VALUE: NamedNode<'_> = NamedNode {
    iri: "http://schema.org/QuantitativeValue",
};
pub const SCHEMA_IMAGE_OBJECT: NamedNode<'_> = NamedNode {
    iri: "http://schema.org/ImageObject",
};
pub const SCHEMA_ABOUT: NamedNode<'_> = NamedNode {
    iri: "http://schema.org/about",
};
pub const SCHEMA_ALTERNATE_NAME: NamedNode<'_> = NamedNode {
    iri: "http://schema.org/alternateName",
};
pub const SCHEMA_DESCRIPTION: NamedNode<'_> = NamedNode {
    iri: "http://schema.org/description",
};
pub const SCHEMA_INVERSE_OF: NamedNode<'_> = NamedNode {
    iri: "http://schema.org/inverseOf",
};
pub const SCHEMA_SAME_AS: NamedNode<'_> = NamedNode {
    iri: "http://schema.org/sameAs",
};
pub const SCHEMA_MAX_VALUE: NamedNode<'_> = NamedNode {
    iri: "http://schema.org/maxValue",
};
pub const SCHEMA_MIN_VALUE: NamedNode<'_> = NamedNode {
    iri: "http://schema.org/minValue",
};
pub const SCHEMA_UNIT_CODE: NamedNode<'_> = NamedNode {
    iri: "http://schema.org/unitCode",
};
pub const SCHEMA_LATITUDE: NamedNode<'_> = NamedNode {
    iri: "http://schema.org/latitude",
};
pub const SCHEMA_LONGITUDE: NamedNode<'_> = NamedNode {
    iri: "http://schema.org/longitude",
};
pub const SCHEMA_VALUE: NamedNode<'_> = NamedNode {
    iri: "http://schema.org/value",
};
pub const SKOS_PREF_LABEL: NamedNode<'_> = NamedNode {
    iri: "http://www.w3.org/2004/02/skos/core#prefLabel",
};
pub const XSD_ANY_URI: NamedNode<'_> = NamedNode {
    iri: "http://www.w3.org/2001/XMLSchema#anyURI",
};
pub const XSD_BOOLEAN: NamedNode<'_> = NamedNode {
    iri: "http://www.w3.org/2001/XMLSchema#boolean",
};
pub const XSD_DATE: NamedNode<'_> = NamedNode {
    iri: "http://www.w3.org/2001/XMLSchema#date",
};
pub const XSD_DATE_TIME: NamedNode<'_> = NamedNode {
    iri: "http://www.w3.org/2001/XMLSchema#dateTime",
};
pub const XSD_DECIMAL: NamedNode<'_> = NamedNode {
    iri: "http://www.w3.org/2001/XMLSchema#decimal",
};
pub const XSD_DOUBLE: NamedNode<'_> = NamedNode {
    iri: "http://www.w3.org/2001/XMLSchema#double",
};
pub const XSD_DURATION: NamedNode<'_> = NamedNode {
    iri: "http://www.w3.org/2001/XMLSchema#duration",
};
pub const XSD_INTEGER: NamedNode<'_> = NamedNode {
    iri: "http://www.w3.org/2001/XMLSchema#integer",
};
pub const XSD_G_YEAR: NamedNode<'_> = NamedNode {
    iri: "http://www.w3.org/2001/XMLSchema#gYear",
};
pub const XSD_G_YEAR_MONTH: NamedNode<'_> = NamedNode {
    iri: "http://www.w3.org/2001/XMLSchema#gYearMonth",
};
pub const XSD_STRING: NamedNode<'_> = NamedNode {
    iri: "http://www.w3.org/2001/XMLSchema#string",
};
pub const RDF_FIRST: NamedNode<'_> = NamedNode {
    iri: "http://www.w3.org/1999/02/22-rdf-syntax-ns#first",
};
pub const RDF_LANG_STRING: NamedNode<'_> = NamedNode {
    iri: "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString",
};
pub const RDF_NIL: NamedNode<'_> = NamedNode {
    iri: "http://www.w3.org/1999/02/22-rdf-syntax-ns#nil",
};
pub const RDF_PLAIN_LITERAL: NamedNode<'_> = NamedNode {
    iri: "http://www.w3.org/1999/02/22-rdf-syntax-ns#PlainLiteral",
};
pub const RDF_PROPERTY: NamedNode<'_> = NamedNode {
    iri: "http://www.w3.org/1999/02/22-rdf-syntax-ns#Property",
};
pub const RDF_REST: NamedNode<'_> = NamedNode {
    iri: "http://www.w3.org/1999/02/22-rdf-syntax-ns#rest",
};
pub const RDF_TYPE: NamedNode<'_> = NamedNode {
    iri: "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
};
pub const RDFS_CLASS: NamedNode<'_> = NamedNode {
    iri: "http://www.w3.org/2000/01/rdf-schema#Class",
};
pub const RDFS_COMMENT: NamedNode<'_> = NamedNode {
    iri: "http://www.w3.org/2000/01/rdf-schema#comment",
};
pub const RDFS_DATATYPE: NamedNode<'_> = NamedNode {
    iri: "http://www.w3.org/2000/01/rdf-schema#Datatype",
};
pub const RDFS_DOMAIN: NamedNode<'_> = NamedNode {
    iri: "http://www.w3.org/2000/01/rdf-schema#domain",
};
pub const RDFS_LABEL: NamedNode<'_> = NamedNode {
    iri: "http://www.w3.org/2000/01/rdf-schema#label",
};
pub const RDFS_RANGE: NamedNode<'_> = NamedNode {
    iri: "http://www.w3.org/2000/01/rdf-schema#range",
};
pub const RDFS_SUB_CLASS_OF: NamedNode<'_> = NamedNode {
    iri: "http://www.w3.org/2000/01/rdf-schema#subClassOf",
};
pub const RDFS_SUB_PROPERTY_OF: NamedNode<'_> = NamedNode {
    iri: "http://www.w3.org/2000/01/rdf-schema#subPropertyOf",
};
pub const OWL_CLASS: NamedNode<'_> = NamedNode {
    iri: "http://www.w3.org/2002/07/owl#Class",
};
pub const OWL_DATATYPE_PROPERTY: NamedNode<'_> = NamedNode {
    iri: "http://www.w3.org/2002/07/owl#DatatypeProperty",
};
pub const OWL_DISJOINT_WITH: NamedNode<'_> = NamedNode {
    iri: "http://www.w3.org/2002/07/owl#disjointWith",
};
pub const OWL_FUNCTIONAL_PROPERTY: NamedNode<'_> = NamedNode {
    iri: "http://www.w3.org/2002/07/owl#FunctionalProperty",
};
pub const OWL_INVERSE_OF: NamedNode<'_> = NamedNode {
    iri: "http://www.w3.org/2002/07/owl#inverseOf",
};
pub const OWL_OBJECT_PROPERTY: NamedNode<'_> = NamedNode {
    iri: "http://www.w3.org/2002/07/owl#ObjectProperty",
};
pub const OWL_SAME_AS: NamedNode<'_> = NamedNode {
    iri: "http://www.w3.org/2002/07/owl#sameAs",
};
pub const OWL_UNION_OF: NamedNode<'_> = NamedNode {
    iri: "http://www.w3.org/2002/07/owl#unionOf",
};
pub const SH_DATATYPE: NamedNode<'_> = NamedNode {
    iri: "http://www.w3.org/ns/shacl#datatype",
};
pub const SH_MAX_COUNT: NamedNode<'_> = NamedNode {
    iri: "http://www.w3.org/ns/shacl#maxCount",
};
pub const SH_NODE: NamedNode<'_> = NamedNode {
    iri: "http://www.w3.org/ns/shacl#node",
};
pub const SH_NODE_SHAPE: NamedNode<'_> = NamedNode {
    iri: "http://www.w3.org/ns/shacl#NodeShape",
};
pub const SH_OR: NamedNode<'_> = NamedNode {
    iri: "http://www.w3.org/ns/shacl#or",
};
pub const SH_PATTERN: NamedNode<'_> = NamedNode {
    iri: "http://www.w3.org/ns/shacl#pattern",
};
pub const SH_PATH: NamedNode<'_> = NamedNode {
    iri: "http://www.w3.org/ns/shacl#path",
};
pub const SH_PROPERTY: NamedNode<'_> = NamedNode {
    iri: "http://www.w3.org/ns/shacl#property",
};
pub const SH_PROPERTY_SHAPE: NamedNode<'_> = NamedNode {
    iri: "http://www.w3.org/ns/shacl#PropertyShape",
};
pub const SH_TARGET_CLASS: NamedNode<'_> = NamedNode {
    iri: "http://www.w3.org/ns/shacl#targetClass",
};
pub const SH_UNIQUE_LANG: NamedNode<'_> = NamedNode {
    iri: "http://www.w3.org/ns/shacl#uniqueLang",
};

pub const YS_FROM_CLASS: NamedNode<'_> = NamedNode {
    iri: "http://yago-knowledge.org/schema#fromClass",
};
pub const YS_FROM_PROPERTY: NamedNode<'_> = NamedNode {
    iri: "http://yago-knowledge.org/schema#fromProperty",
};
pub const YS_ANNOTATION_PROPERTY_SHAPE: NamedNode<'_> = NamedNode {
    iri: "http://yago-knowledge.org/schema#AnnotationPropertyShape",
};
