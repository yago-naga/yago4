use crate::model::{YagoTerm, YagoTriple};
use crate::vocab::*;
use rio_api::model::NamedNode;
use rio_api::parser::{ParseError, TriplesParser};
use rio_turtle::{TurtleError, TurtleParser};
use std::collections::hash_map::DefaultHasher;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::io::Cursor;
use std::iter::once;

#[derive(Eq, PartialEq, Debug, Clone, Hash)]
pub struct Class {
    pub id: YagoTerm,
    pub label: Option<YagoTerm>,
    pub comment: Option<YagoTerm>,
    pub super_classes: Vec<YagoTerm>,
    pub disjoint_classes: Vec<YagoTerm>,
}

#[derive(Eq, PartialEq, Debug, Clone, Hash)]
pub struct Property {
    pub id: YagoTerm,
    pub label: Option<YagoTerm>,
    pub comment: Option<YagoTerm>,
    pub super_properties: Vec<YagoTerm>,
    pub inverse: Vec<YagoTerm>,
}

#[derive(Eq, PartialEq, Debug, Clone, Hash)]
pub struct NodeShape {
    id: YagoTerm,
    pub target_class: YagoTerm,
    pub properties: Vec<PropertyShape>,
    pub from_classes: Vec<YagoTerm>,
}

#[derive(Eq, PartialEq, Debug, Clone, Hash)]
pub struct PropertyShape {
    id: YagoTerm,
    pub path: YagoTerm,
    pub parent_shape: Option<YagoTerm>,
    pub datatypes: Vec<YagoTerm>,
    pub nodes: Vec<YagoTerm>,
    pub max_count: Option<usize>,
    pub is_unique_lang: bool,
    pub pattern: Option<String>,
    pub from_properties: Vec<YagoTerm>,
}

pub struct Schema {
    graph: SimpleGraph,
}

const PROPERTY_TYPES: [NamedNode<'_>; 3] =
    [RDF_PROPERTY, OWL_DATATYPE_PROPERTY, OWL_OBJECT_PROPERTY];

impl Schema {
    pub fn open() -> Self {
        let mut graph = SimpleGraph::default();
        for d in &SCHEMA_DATA {
            graph.load_turtle(d);
        }
        Self { graph }
    }

    pub fn class(&self, id: &YagoTerm) -> Option<Class> {
        if self.graph.contains(&YagoTriple {
            subject: id.clone(),
            predicate: RDF_TYPE.into(),
            object: RDFS_CLASS.into(),
        }) {
            Some(Class {
                id: id.clone(),
                label: self
                    .graph
                    .object_for_subject_predicate(id, &RDFS_LABEL.into())
                    .cloned(),
                comment: self
                    .graph
                    .object_for_subject_predicate(id, &RDFS_COMMENT.into())
                    .cloned(),
                super_classes: self
                    .graph
                    .objects_for_subject_predicate(id, &RDFS_SUB_CLASS_OF.into())
                    .cloned()
                    .collect(),
                disjoint_classes: self
                    .graph
                    .objects_for_subject_predicate(id, &OWL_DISJOINT_WITH.into())
                    .cloned()
                    .collect(),
            })
        } else {
            None
        }
    }

    pub fn classes(&self) -> Vec<Class> {
        self.graph
            .subjects_for_predicate_object(&RDF_TYPE.into(), &RDFS_CLASS.into())
            .filter_map(|id| self.class(id))
            .collect()
    }

    pub fn node_shape(&self, id: &YagoTerm) -> NodeShape {
        NodeShape {
            id: id.clone(),
            target_class: self
                .graph
                .object_for_subject_predicate(id, &SH_TARGET_CLASS.into())
                .cloned()
                .unwrap_or_else(|| id.clone()),
            properties: self
                .graph
                .objects_for_subject_predicate(id, &SH_PROPERTY.into())
                .map(|id| self.property_shape(id))
                .collect(),
            from_classes: self
                .graph
                .objects_for_subject_predicate(id, &YS_FROM_CLASS.into())
                .cloned()
                .collect(),
        }
    }

    pub fn node_shapes(&self) -> Vec<NodeShape> {
        self.graph
            .subjects_for_predicate_object(&RDF_TYPE.into(), &SH_NODE_SHAPE.into())
            .map(|id| self.node_shape(id))
            .collect()
    }

    pub fn property(&self, id: &YagoTerm) -> Option<Property> {
        let is_property = PROPERTY_TYPES.iter().any(|t| {
            self.graph.contains(&YagoTriple {
                subject: id.clone(),
                predicate: RDF_TYPE.into(),
                object: t.clone().into(),
            })
        });
        if is_property {
            Some(Property {
                id: id.clone(),
                label: self
                    .graph
                    .object_for_subject_predicate(id, &RDFS_LABEL.into())
                    .cloned(),
                comment: self
                    .graph
                    .object_for_subject_predicate(id, &RDFS_COMMENT.into())
                    .cloned(),
                super_properties: self
                    .graph
                    .objects_for_subject_predicate(id, &RDFS_SUB_PROPERTY_OF.into())
                    .cloned()
                    .collect(),
                inverse: self
                    .graph
                    .objects_for_subject_predicate(id, &OWL_INVERSE_OF.into())
                    .chain(
                        self.graph
                            .objects_for_subject_predicate(id, &SCHEMA_INVERSE_OF.into()),
                    )
                    .cloned()
                    .collect(),
            })
        } else {
            None
        }
    }

    pub fn property_shape(&self, id: &YagoTerm) -> PropertyShape {
        PropertyShape {
            id: id.clone(),
            path: self
                .graph
                .object_for_subject_predicate(id, &SH_PATH.into())
                .cloned()
                .unwrap(),
            parent_shape: self
                .graph
                .subject_for_predicate_object(&SH_PROPERTY.into(), id)
                .cloned(),
            datatypes: self
                .property_shape_roots(id)
                .iter()
                .flat_map(|cid| {
                    self.graph
                        .objects_for_subject_predicate(cid, &SH_DATATYPE.into())
                        .cloned()
                        .collect::<Vec<_>>()
                })
                .collect(),
            nodes: self
                .property_shape_roots(id)
                .iter()
                .flat_map(|cid| {
                    self.graph
                        .objects_for_subject_predicate(cid, &SH_NODE.into())
                        .cloned()
                        .collect::<Vec<_>>()
                })
                .collect(),
            max_count: self
                .graph
                .object_for_subject_predicate(id, &SH_MAX_COUNT.into())
                .and_then(|pattern| {
                    if let YagoTerm::IntegerLiteral(c) = pattern {
                        Some(*c as usize)
                    } else {
                        None
                    }
                }),
            is_unique_lang: self
                .graph
                .object_for_subject_predicate(id, &SH_UNIQUE_LANG.into())
                .map_or(false, |pattern| {
                    if let YagoTerm::TypedLiteral(v, _) = pattern {
                        v == "true" || v == "1"
                    } else {
                        false
                    }
                }),
            pattern: self
                .graph
                .object_for_subject_predicate(id, &SH_PATTERN.into())
                .and_then(|pattern| {
                    if let YagoTerm::StringLiteral(l) = pattern {
                        Some(l)
                    } else {
                        None
                    }
                })
                .cloned(),
            from_properties: self
                .graph
                .objects_for_subject_predicate(id, &YS_FROM_PROPERTY.into())
                .cloned()
                .collect(),
        }
    }

    pub fn property_shapes(&self) -> Vec<PropertyShape> {
        self.graph
            .triples_for_predicate(&SH_PROPERTY.into())
            .map(|t| self.property_shape(&t.object))
            .collect()
    }

    pub fn annotation_property_shapes(&self) -> Vec<PropertyShape> {
        self.graph
            .subjects_for_predicate_object(&RDF_TYPE.into(), &YS_ANNOTATION_PROPERTY_SHAPE.into())
            .map(|t| self.property_shape(&t))
            .collect()
    }

    fn property_shape_roots(&self, main_root: &YagoTerm) -> Vec<YagoTerm> {
        once(main_root.clone())
            .chain(
                self.graph
                    .objects_for_subject_predicate(main_root, &SH_OR.into())
                    .flat_map(|v| self.list_values(v)),
            )
            .collect()
    }

    fn list_values(&self, root: &YagoTerm) -> Vec<YagoTerm> {
        let mut elements = Vec::default();
        let mut root = root.clone();
        while let Some(next) = self
            .graph
            .object_for_subject_predicate(&root, &RDF_REST.into())
        {
            if let Some(e) = self
                .graph
                .object_for_subject_predicate(&root, &RDF_FIRST.into())
            {
                elements.push(e.clone());
            }
            root = next.clone();
        }
        elements
    }
}

const SCHEMA_DATA: [&str; 4] = [
    include_str!("data/schema.ttl"),
    include_str!("data/shapes.ttl"),
    include_str!("data/bioschemas.ttl"),
    include_str!("data/shapes-bio.ttl"),
];

/// A simple implementation of [RDF graphs](https://www.w3.org/TR/rdf11-concepts/#dfn-graph).
#[derive(Debug, Clone, Default)]
struct SimpleGraph {
    triples: HashSet<YagoTriple>,
}

impl SimpleGraph {
    /// Load a file
    fn load_turtle(&mut self, data: &str) {
        let mut hasher = DefaultHasher::new();
        data.hash(&mut hasher);
        let seed = hasher.finish().to_string();

        TurtleParser::new(Cursor::new(data), "")
            .unwrap()
            .parse_all(&mut move |t| {
                self.triples.insert(YagoTriple {
                    subject: YagoTerm::from_parser(t.subject.into(), &seed),
                    predicate: YagoTerm::from_parser(t.predicate.into(), &seed),
                    object: YagoTerm::from_parser(t.object, &seed),
                });
                Ok(()) as std::result::Result<_, TurtleError>
            })
            .map_err(|e| {
                if let Some(position) = e.textual_position() {
                    eprintln!(
                        "Error while parsing line '{}': {}",
                        data.lines().nth(position.line_number()).unwrap(),
                        &e
                    )
                }
                e
            })
            .unwrap();
    }

    /// Returns all triples contained by the graph
    fn iter(&self) -> impl Iterator<Item = &YagoTriple> {
        self.triples.iter()
    }

    fn objects_for_subject_predicate<'a>(
        &'a self,
        subject: &'a YagoTerm,
        predicate: &'a YagoTerm,
    ) -> impl Iterator<Item = &YagoTerm> + 'a {
        self.iter()
            .filter(move |t| t.subject == *subject && t.predicate == *predicate)
            .map(|t| &t.object)
    }

    fn object_for_subject_predicate<'a>(
        &'a self,
        subject: &'a YagoTerm,
        predicate: &'a YagoTerm,
    ) -> Option<&'a YagoTerm> {
        self.objects_for_subject_predicate(subject, predicate)
            .next()
    }

    fn triples_for_predicate<'a>(
        &'a self,
        predicate: &'a YagoTerm,
    ) -> impl Iterator<Item = &YagoTriple> + 'a {
        self.iter().filter(move |t| t.predicate == *predicate)
    }

    fn subjects_for_predicate_object<'a>(
        &'a self,
        predicate: &'a YagoTerm,
        object: &'a YagoTerm,
    ) -> impl Iterator<Item = &YagoTerm> + 'a {
        self.iter()
            .filter(move |t| t.predicate == *predicate && t.object == *object)
            .map(|t| &t.subject)
    }

    fn subject_for_predicate_object<'a>(
        &'a self,
        predicate: &'a YagoTerm,
        object: &'a YagoTerm,
    ) -> Option<&'a YagoTerm> {
        self.subjects_for_predicate_object(predicate, object).next()
    }

    /// Checks if the graph contains the given triple
    fn contains(&self, triple: &YagoTriple) -> bool {
        self.triples.contains(triple)
    }
}

#[test]
fn load_schema_test() {
    Schema::open();
}
