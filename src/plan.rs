use crate::model::{YagoTerm, YagoTriple};
use crate::multimap::Multimap;
use crate::partitioned_statements::PartitionedStatements;
use crate::schema::{PropertyShape, Schema};
use crate::vocab::*;
use crossbeam::thread;
use percent_encoding::percent_decode_str;
use regex::Regex;
use rio_api::model::NamedNode;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::fmt::Write;
use std::fs::{create_dir_all, File};
use std::hash::Hash;
use std::io::BufWriter;
use std::iter::{empty, once};
use std::path::Path;
use std::str::FromStr;
use url::Url;

const P_PREFIX: &str = "http://www.wikidata.org/prop/";
const PS_PREFIX: &str = "http://www.wikidata.org/prop/statement/";
const PSV_PREFIX: &str = "http://www.wikidata.org/prop/statement/value/";
const PQ_PREFIX: &str = "http://www.wikidata.org/prop/qualifier/";
const PQV_PREFIX: &str = "http://www.wikidata.org/prop/qualifier/value/";
const YAGO_RESOURCE_PREFIX: &str = "http://yago-knowledge.org/resource/";
const YAGO_VALUE_PREFIX: &str = "http://yago-knowledge.org/value/";

#[allow(clippy::unreadable_literal)]
const WD_BAD_CLASSES: [YagoTerm; 6] = [
    YagoTerm::WikidataItem(17379835), //Wikimedia page outside the main knowledge tree
    YagoTerm::WikidataItem(17442446), //Wikimedia internal stuff
    YagoTerm::WikidataItem(4167410),  //disambiguation page
    YagoTerm::WikidataItem(13406463), //list article
    YagoTerm::WikidataItem(17524420), //aspect of history
    YagoTerm::WikidataItem(18340514), //article about events in a specific year or time period
];

const MIN_NUMBER_OF_INSTANCES: usize = 10;

pub fn generate_yago(index_dir: impl AsRef<Path>, to_dir: &str) {
    let schema = Schema::open();
    let partitioned_statements = PartitionedStatements::open(index_dir);

    let wikidata_to_enwikipedia_mapping = wikidata_to_enwikipedia_mapping(&partitioned_statements);
    let wikidata_to_yago_uris_mapping = wikidata_to_yago_uris_mapping(
        &schema,
        &partitioned_statements,
        &wikidata_to_enwikipedia_mapping,
    );

    let (yago_classes, wikidata_to_yago_class_mapping, yago_super_class_of) =
        build_yago_classes_and_super_class_of(
            &schema,
            &partitioned_statements,
            &wikidata_to_yago_uris_mapping,
            &wikidata_to_enwikipedia_mapping,
        );

    let yago_shape_instances = yago_shape_instances(
        &schema,
        &partitioned_statements,
        &wikidata_to_yago_class_mapping,
        &yago_super_class_of,
        &yago_classes,
        &wikidata_to_yago_uris_mapping,
    );

    thread::scope(|s| {
        s.spawn(|_| {
            write_ntriples(
                build_classes_description(
                    &yago_classes,
                    &yago_super_class_of,
                    &partitioned_statements,
                    &wikidata_to_yago_uris_mapping,
                ),
                &to_dir,
                "yago-wd-class.nt",
            );
        });

        s.spawn(|_| {
            write_ntriples(
                build_simple_instance_of(&yago_shape_instances),
                &to_dir,
                "yago-wd-simple-types.nt",
            );
        });

        s.spawn(|_| {
            write_ntriples(
                build_full_instance_of(
                    &yago_shape_instances.get(&SCHEMA_THING.into()).unwrap(),
                    &wikidata_to_yago_class_mapping,
                    &partitioned_statements,
                    &wikidata_to_yago_uris_mapping,
                ),
                &to_dir,
                "yago-wd-full-types.nt",
            );
        });

        s.spawn(|_| {
            write_ntriples(
                build_simple_properties_from_schema(
                    &schema,
                    &partitioned_statements,
                    &yago_shape_instances,
                    &wikidata_to_yago_uris_mapping,
                    vec![
                        RDFS_LABEL.into(),
                        RDFS_COMMENT.into(),
                        SCHEMA_ALTERNATE_NAME.into(),
                    ],
                ),
                &to_dir,
                "yago-wd-labels.nt",
            );
        });

        s.spawn(|_| {
            build_properties_from_wikidata_and_schema(
                &schema,
                &partitioned_statements,
                &yago_shape_instances,
                &wikidata_to_yago_uris_mapping,
                vec![
                    RDFS_LABEL.into(),
                    RDFS_COMMENT.into(),
                    SCHEMA_ALTERNATE_NAME.into(),
                ],
                &to_dir,
                "yago-wd-facts.nt",
            );
        });

        s.spawn(|_| {
            write_ntriples(
                build_same_as(
                    &partitioned_statements,
                    &yago_shape_instances.get(&SCHEMA_THING.into()).unwrap(),
                    &wikidata_to_yago_uris_mapping,
                    &wikidata_to_enwikipedia_mapping,
                ),
                &to_dir,
                "yago-wd-sameAs.nt",
            );
        });

        s.spawn(|_| {
            write_ntriples(build_yago_schema(&schema), &to_dir, "yago-wd-schema.nt");
        });
    })
    .unwrap();
}

fn write_ntriples(
    triples: impl IntoIterator<Item = YagoTriple>,
    dir: impl AsRef<Path>,
    file_name: &str,
) {
    use std::io::Write;

    println!("Writing file {}", file_name);

    create_dir_all(dir.as_ref()).unwrap();
    let mut path = dir.as_ref().to_owned();
    path.push(file_name);
    let mut file = BufWriter::new(File::create(path).unwrap());
    for t in triples {
        writeln!(file, "{}", t).unwrap();
    }
}

fn wikidata_to_enwikipedia_mapping(
    partitioned_statements: &PartitionedStatements,
) -> HashMap<YagoTerm, String> {
    partitioned_statements
        .subjects_objects_for_predicate(SCHEMA_ABOUT)
        .filter_map(|(s, o)| {
            if let (YagoTerm::Iri(i), object) = (s, o) {
                if i.starts_with("https://en.wikipedia.org/wiki/") {
                    Some((object, i))
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect()
}

/// Converts Wikidata URI to Yago URIs based on en.wikipedia article titles
fn wikidata_to_yago_uris_mapping(
    schema: &Schema,
    partitioned_statements: &PartitionedStatements,
    wikidata_to_enwikipedia_mapping: &HashMap<YagoTerm, String>,
) -> HashMap<YagoTerm, YagoTerm> {
    println!("Generating Wikidata to Yago URI mapping");

    let wikidata_items_to_keep: HashSet<YagoTerm> = partitioned_statements
        .subjects_objects_for_predicate(SCHEMA_ABOUT)
        .filter_map(|(wp, wd)| {
            if let YagoTerm::Iri(wp) = wp {
                if wp.contains(".wikipedia.org/wiki/") {
                    Some(wd)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    let from_schema_mapping: HashMap<YagoTerm, YagoTerm> = schema
        .node_shapes()
        .into_iter()
        .flat_map(|shape| {
            let target_class = shape.target_class;
            shape
                .from_classes
                .into_iter()
                .map(move |from_cls| (from_cls, target_class.clone()))
        })
        .collect();

    let from_wikipedia_mapping: HashMap<YagoTerm, YagoTerm> = wikidata_to_enwikipedia_mapping
        .iter()
        .filter(|(k, _)| {
            wikidata_items_to_keep.contains(*k) && !from_schema_mapping.contains_key(*k)
        })
        .flat_map(|(wd, wp)| {
            let mut uri = YAGO_RESOURCE_PREFIX.to_owned();
            encode_iri_path(
                &percent_decode_str(&wp["https://en.wikipedia.org/wiki/".len()..])
                    .decode_utf8()
                    .unwrap(),
                &mut uri,
            );
            Some((wd.to_owned(), YagoTerm::Iri(uri)))
        })
        .collect();

    let from_label_mapping: HashMap<YagoTerm, YagoTerm> = partitioned_statements
        .subjects_objects_for_predicate(SKOS_PREF_LABEL)
        .filter_map(|(s, o)| {
            if let (subject, YagoTerm::LanguageTaggedString(v, l)) = (s, o) {
                if l == "en" {
                    Some((subject, v))
                } else {
                    None
                }
            } else {
                None
            }
        })
        .filter(|(k, _)| {
            wikidata_items_to_keep.contains(k)
                && !from_schema_mapping.contains_key(k)
                && !from_wikipedia_mapping.contains_key(k)
        })
        .filter_map(|(k, v)| {
            if let YagoTerm::WikidataItem(i) = k {
                let mut uri = YAGO_RESOURCE_PREFIX.to_owned();
                encode_iri_path(&v, &mut uri);
                write!(&mut uri, "_Q{}", i).unwrap();
                Some((YagoTerm::WikidataItem(i), YagoTerm::Iri(uri)))
            } else {
                None
            }
        })
        .collect();

    let wikibase_item = YagoTerm::from(WIKIBASE_ITEM);
    let wikidata_items = partitioned_statements
        .subjects_objects_for_predicate(RDF_TYPE)
        .filter(|(_, o)| o == &wikibase_item)
        .map(|(s, _)| s);

    /*TODO: bad hack for tests
    wikidata_items.extend(
        partitioned_statements
            .triples_for_predicate(WDT_P31)
            .map(|t| t.object),
    );
    wikidata_items.extend(
        partitioned_statements
            .triples_for_predicate(WDT_P279)
            .flat_map(|t| once(t.subject).chain(once(t.object))),
    );*/

    let fallback_mapping: HashMap<YagoTerm, YagoTerm> = wikidata_items
        .filter(|i| {
            wikidata_items_to_keep.contains(i)
                && !from_schema_mapping.contains_key(i)
                && !from_wikipedia_mapping.contains_key(i)
                && !from_label_mapping.contains_key(i)
        })
        .filter_map(|i| {
            if let YagoTerm::WikidataItem(i) = i {
                Some((
                    YagoTerm::WikidataItem(i),
                    YagoTerm::Iri(format!("{}_Q{}", YAGO_RESOURCE_PREFIX, i)),
                ))
            } else {
                None
            }
        })
        .collect();

    from_schema_mapping
        .into_iter()
        .chain(from_wikipedia_mapping)
        .chain(from_label_mapping)
        .chain(fallback_mapping)
        .collect()
}

/// Builds the class set and class hierarchy from Wikidata, schema.org ontology and shapes
/// <p>
/// Algorithm:
/// 1. Take all subClassOf (P279) from Wikidata
/// 2. Only keep the classes that are subclass of a Yago defined class
/// 3. Only keep the classes that have at least 10 instances or have a subclass with at least 10 instances
/// 4. Remove the bad classes
/// 5. Remove the classes that are sub class of two disjoint classes
/// It gives the set of Wikidata classes to consider.
/// <p>
/// 6. Build the set of Yago classes by keeping only the classes with at least 10 direct instances and an English Wikipedia article.
/// 7. Compute the mapping from Wikidata classes and Yago classes and the Yago type hierarchy from it.
fn build_yago_classes_and_super_class_of(
    schema: &Schema,
    partitioned_statements: &PartitionedStatements,
    wikidata_to_yago_uris_mapping: &HashMap<YagoTerm, YagoTerm>,
    wikidata_to_en_wikipedia_mapping: &HashMap<YagoTerm, String>,
) -> (
    HashSet<YagoTerm>,
    Multimap<YagoTerm, YagoTerm>,
    Multimap<YagoTerm, YagoTerm>,
) {
    println!("Generating Yago class set");

    let yago_schema_from_classes: Vec<YagoTerm> = schema
        .node_shapes()
        .into_iter()
        .flat_map(|shape| shape.from_classes)
        .collect();

    let wikidata_sub_class_of: Multimap<YagoTerm, YagoTerm> = partitioned_statements
        .subjects_objects_for_predicate(WDT_P279)
        .filter(|(s, _)| !yago_schema_from_classes.contains(s))
        .collect(); // Yago shape classes only have super classes which are shapes

    let wikidata_super_class_of: Multimap<YagoTerm, YagoTerm> = wikidata_sub_class_of
        .iter()
        .map(|(s, o)| (o.clone(), s.clone()))
        .collect();

    let wikidata_bad_classes =
        transitive_closure(WD_BAD_CLASSES.iter().cloned(), &wikidata_super_class_of);

    let wikidata_classes_with_at_least_min_count_instances: HashSet<YagoTerm> =
        partitioned_statements
            .subjects_objects_for_predicate(WDT_P31)
            .map(|(s, o)| (o, s))
            .collect::<Multimap<YagoTerm, YagoTerm>>()
            .into_iter_grouped()
            .filter(|(_, v)| v.len() >= MIN_NUMBER_OF_INSTANCES)
            .map(|(k, _)| k)
            .collect();

    let wikidata_classes_with_at_least_min_count_instances_recursively = transitive_closure(
        wikidata_classes_with_at_least_min_count_instances
            .iter()
            .cloned(),
        &wikidata_sub_class_of,
    );

    let yago_classes_sub_classes = transitive_closure(
        yago_schema_from_classes.iter().cloned(),
        &wikidata_super_class_of,
    );

    // sub classes of disjoint classes computation
    let mut subclasses_of_disjoint: HashSet<YagoTerm> = HashSet::default();
    for class1 in schema.classes() {
        let shape1 = schema.node_shape(&class1.id);
        for class2 in &class1.disjoint_classes {
            let shape2 = schema.node_shape(class2);
            for wd_class1 in &shape1.from_classes {
                for wd_class2 in &shape2.from_classes {
                    subclasses_of_disjoint.extend(
                        transitive_closure(once(wd_class1.clone()), &wikidata_super_class_of)
                            .intersection(&transitive_closure(
                                once(wd_class2.clone()),
                                &wikidata_super_class_of,
                            ))
                            .cloned(),
                    )
                }
            }
        }
    }

    let wikidata_classes_to_keep: HashSet<YagoTerm> = yago_classes_sub_classes
        .intersection(&wikidata_classes_with_at_least_min_count_instances_recursively)
        .filter(|c| !wikidata_bad_classes.contains(c) && !subclasses_of_disjoint.contains(c))
        .chain(&yago_schema_from_classes)
        .cloned()
        .collect();

    let wikidata_classes_to_keep_for_yago: HashSet<YagoTerm> = wikidata_classes_to_keep
        .intersection(&wikidata_classes_with_at_least_min_count_instances)
        .filter(|c| wikidata_to_en_wikipedia_mapping.contains_key(c))
        .chain(&yago_schema_from_classes)
        .cloned()
        .collect();

    let yago_classes: HashSet<YagoTerm> = map_to_yago(
        wikidata_classes_to_keep_for_yago.iter().cloned(),
        wikidata_to_yago_uris_mapping,
    )
    .collect();

    let wikidata_sub_class_of_without_classes_to_keep_for_yago: Multimap<YagoTerm, YagoTerm> =
        wikidata_sub_class_of
            .iter()
            .filter(|(k, _)| !wikidata_classes_to_keep_for_yago.contains(k))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

    println!("Generating Wikidata to Yago class mapping");

    let wikidata_to_yago_class_mapping: Multimap<YagoTerm, YagoTerm> = map_value_to_yago(
        transitive_closure_pair(
            wikidata_classes_to_keep.into_iter().map(|c| (c.clone(), c)),
            &wikidata_sub_class_of_without_classes_to_keep_for_yago,
        )
        .into_iter()
        .filter(|(_, v)| wikidata_classes_to_keep_for_yago.contains(v)),
        wikidata_to_yago_uris_mapping,
    )
    .collect();

    println!("Generating Yago subClassOf relations");

    let yago_super_class_of: Multimap<YagoTerm, YagoTerm> = map_value_to_yago(
        map_key_to_yago(
            transitive_closure_pair(
                wikidata_sub_class_of
                    .into_iter()
                    .filter(|(k, _)| wikidata_classes_to_keep_for_yago.contains(k)),
                &wikidata_sub_class_of_without_classes_to_keep_for_yago,
            )
            .into_iter()
            .filter(|(_, v)| wikidata_classes_to_keep_for_yago.contains(v)),
            wikidata_to_yago_uris_mapping,
        ),
        wikidata_to_yago_uris_mapping,
    )
    .chain(subclass_of_from_yago_schema(schema))
    .map(|(k, v)| (v, k))
    .collect();

    (
        yago_classes,
        wikidata_to_yago_class_mapping,
        yago_super_class_of,
    )
}

fn yago_shape_instances(
    schema: &Schema,
    partitioned_statements: &PartitionedStatements,
    wikidata_to_yago_class_mapping: &Multimap<YagoTerm, YagoTerm>,
    yago_super_class_of: &Multimap<YagoTerm, YagoTerm>,
    yago_classes: &HashSet<YagoTerm>,
    wikidata_to_yago_uris_mapping: &HashMap<YagoTerm, YagoTerm>,
) -> HashMap<YagoTerm, HashSet<YagoTerm>> {
    println!("Generating the list of instances for each shape");
    let wikidata_instances_for_yago_class: Multimap<YagoTerm, YagoTerm> = join_pairs(
        partitioned_statements
            .subjects_objects_for_predicate(WDT_P31)
            .map(|(s, o)| (o, s)),
        wikidata_to_yago_class_mapping,
    )
    .map(|(_, wd_instance, yago_class)| (yago_class, wd_instance))
    .collect();

    let instances_without_intersection_removal: HashMap<YagoTerm, HashSet<YagoTerm>> = schema
        .node_shapes()
        .into_iter()
        .map(|node_shape| {
            let from_yago_classes =
                transitive_closure(once(node_shape.target_class.clone()), yago_super_class_of);

            let wd_instances = from_yago_classes
                .iter()
                .flat_map(|class| wikidata_instances_for_yago_class.get(&class))
                .flat_map(|v| v.iter().cloned());
            let instances = map_to_yago(wd_instances, wikidata_to_yago_uris_mapping)
                .filter(|i| !yago_classes.contains(i))
                .collect(); // We do not want classes

            (node_shape.target_class, instances)
        })
        .collect();

    let instances_in_disjoint_intersections: HashSet<YagoTerm> = schema
        .classes()
        .into_iter()
        .flat_map(|class1| {
            class1
                .disjoint_classes
                .clone()
                .into_iter()
                .map(move |class2| (class1.id.clone(), class2))
        })
        .flat_map(|(class1, class2)| {
            instances_without_intersection_removal
                .get(&class1)
                .unwrap()
                .intersection(instances_without_intersection_removal.get(&class2).unwrap())
        })
        .cloned()
        .collect();

    instances_without_intersection_removal
        .into_iter()
        .map(|(class, mut instances)| {
            let to_remove: Vec<_> = instances
                .intersection(&instances_in_disjoint_intersections)
                .cloned()
                .collect();
            to_remove.into_iter().for_each(|c| {
                instances.remove(&c);
            });
            (class, instances)
        })
        .collect()
}

fn build_simple_instance_of<'a>(
    yago_shape_instances: &'a HashMap<YagoTerm, HashSet<YagoTerm>>,
) -> impl Iterator<Item = YagoTriple> + 'a {
    yago_shape_instances.iter().flat_map(|(class, instances)| {
        instances.iter().map(move |instance| YagoTriple {
            subject: instance.clone(),
            predicate: RDF_TYPE.into(),
            object: class.clone(),
        })
    })
}

fn build_full_instance_of<'a>(
    yago_things: &'a HashSet<YagoTerm>,
    wikidata_to_yago_class_mapping: &'a Multimap<YagoTerm, YagoTerm>,
    partitioned_statements: &'a PartitionedStatements,
    wikidata_to_yago_uris_mapping: &'a HashMap<YagoTerm, YagoTerm>,
) -> impl Iterator<Item = YagoTriple> + 'a {
    let wikidata_instances = partitioned_statements
        .subjects_objects_for_predicate(WDT_P31)
        .map(|(s, o)| (o, s));

    let instances_filtered_subject =
        map_value_to_yago(wikidata_instances, wikidata_to_yago_uris_mapping)
            .filter(move |(_, instance)| yago_things.contains(instance));

    join_pairs(instances_filtered_subject, wikidata_to_yago_class_mapping).map(
        |(_, yago_instance, yago_class)| YagoTriple {
            subject: yago_instance,
            predicate: RDF_TYPE.into(),
            object: yago_class.clone(),
        },
    )
}

fn build_classes_description<'a>(
    yago_classes: &'a HashSet<YagoTerm>,
    yago_super_class_of: &'a Multimap<YagoTerm, YagoTerm>,
    partitioned_statements: &'a PartitionedStatements,
    wikidata_to_yago_uris_mapping: &'a HashMap<YagoTerm, YagoTerm>,
) -> impl Iterator<Item = YagoTriple> + 'a {
    let yago_owl_class_triple = yago_classes.iter().map(|c| YagoTriple {
        subject: c.clone(),
        predicate: RDF_TYPE.into(),
        object: OWL_CLASS.into(),
    });

    let yago_sub_class_of = yago_super_class_of.iter().map(|(o, s)| YagoTriple {
        subject: s.clone(),
        predicate: RDFS_SUB_CLASS_OF.into(),
        object: o.clone(),
    });

    let wikidata_label = partitioned_statements.subjects_objects_for_predicate(SKOS_PREF_LABEL);

    let rdfs_label = map_key_to_yago(wikidata_label, wikidata_to_yago_uris_mapping)
        .filter(move |(c, _)| yago_classes.contains(c))
        .map(|(s, o)| YagoTriple {
            subject: s,
            predicate: RDFS_LABEL.into(),
            object: o,
        });

    let wikidata_description =
        partitioned_statements.subjects_objects_for_predicate(SCHEMA_DESCRIPTION);

    let rdfs_comment = map_key_to_yago(wikidata_description, wikidata_to_yago_uris_mapping)
        .filter(move |(c, _)| yago_classes.contains(c))
        .map(|(s, o)| YagoTriple {
            subject: s,
            predicate: RDFS_COMMENT.into(),
            object: o,
        });

    yago_sub_class_of
        .chain(yago_owl_class_triple)
        .chain(rdfs_label)
        .chain(rdfs_comment)
}

fn build_simple_properties_from_schema<'a>(
    schema: &'a Schema,
    partitioned_statements: &'a PartitionedStatements,
    yago_shape_instances: &'a HashMap<YagoTerm, HashSet<YagoTerm>>,
    wikidata_to_yago_uris_mapping: &'a HashMap<YagoTerm, YagoTerm>,
    properties: Vec<YagoTerm>,
) -> impl Iterator<Item = YagoTriple> + 'a {
    schema
        .property_shapes()
        .into_iter()
        .filter(move |property_shape| properties.contains(&property_shape.path))
        .flat_map(move |property_shape| {
            let subject_object: Box<dyn Iterator<Item = (YagoTerm, YagoTerm)>> =
                if !property_shape.datatypes.is_empty() {
                    if !property_shape.nodes.is_empty() {
                        Box::new(empty())
                    } else {
                        let dts: Vec<_> = property_shape
                            .datatypes
                            .iter()
                            .filter_map(|dt| {
                                if let YagoTerm::Iri(dt) = dt {
                                    Some(NamedNode { iri: dt.as_str() })
                                } else {
                                    eprintln!("Invalid datatype: {}", dt);
                                    None
                                }
                            })
                            .collect();

                        Box::new(
                            property_shape
                                .from_properties
                                .iter()
                                .flat_map(|p| {
                                    partitioned_statements.subjects_objects_for_predicate(p.clone())
                                })
                                .filter_map(move |(subject, object)| {
                                    if object.datatype().map_or(false, |dt| dts.contains(&dt)) {
                                        Some((subject, object))
                                    } else {
                                        None
                                    }
                                }),
                        )
                    }
                } else {
                    unimplemented!();
                };

            let subject_object = filter_domain(
                map_key_to_yago(subject_object, wikidata_to_yago_uris_mapping),
                yago_shape_instances,
                &property_shape,
            );

            // Max count
            if property_shape.max_count.is_some() {
                unimplemented!();
            }

            // Regex
            if property_shape.pattern.is_some() {
                unimplemented!();
            }

            subject_object
                .map(|(subject, object)| YagoTriple {
                    subject,
                    predicate: property_shape.path.clone(),
                    object,
                })
                .collect::<Vec<_>>()
                .into_iter()
        })
}

fn build_properties_from_wikidata_and_schema(
    schema: &Schema,
    partitioned_statements: &PartitionedStatements,
    yago_shape_instances: &HashMap<YagoTerm, HashSet<YagoTerm>>,
    wikidata_to_yago_uris_mapping: &HashMap<YagoTerm, YagoTerm>,
    exclude_properties: Vec<YagoTerm>,
    dir: impl AsRef<Path>,
    file_name: &str,
) {
    // Some utility plans
    let clean_times: HashMap<YagoTerm, YagoTerm> = partitioned_statements
        .subjects_objects_for_predicate(WIKIBASE_TIME_VALUE)
        .filter_map(|(s, value)| {
            partitioned_statements
                .object_for_subject_predicate(&s, &WIKIBASE_TIME_PRECISION.into())
                .map(|precision| (s, value, precision))
        })
        .filter_map(|(s, value, precision)| {
            partitioned_statements
                .object_for_subject_predicate(&s, &WIKIBASE_TIME_CALENDAR_MODEL.into())
                .map(|calendar| (s, value, precision, calendar))
        })
        .filter_map(|(k, value, precision, calendar)| {
            convert_time(value, precision, calendar).map(|t| (k, t))
        })
        .collect();

    let clean_coordinates: HashMap<YagoTerm, (YagoTerm, Vec<YagoTriple>)> = partitioned_statements
        .subjects_objects_for_predicate(WIKIBASE_GEO_LATITUDE)
        .filter_map(|(s, latitude)| {
            partitioned_statements
                .object_for_subject_predicate(&s, &WIKIBASE_GEO_LONGITUDE.into())
                .map(|longitude| (s, latitude, longitude))
        })
        .filter_map(|(s, latitude, longitude)| {
            partitioned_statements
                .object_for_subject_predicate(&s, &WIKIBASE_GEO_PRECISION.into())
                .map(|precision| (s, latitude, longitude, precision))
        })
        .filter_map(|(s, latitude, longitude, precision)| {
            partitioned_statements
                .object_for_subject_predicate(&s, &WIKIBASE_GEO_GLOBE.into())
                .map(|globe| (s, latitude, longitude, precision, globe))
        })
        .filter_map(|(k, lat, long, precision, globe)| {
            convert_globe_coordinates(lat, long, precision, globe).map(|t| (k, t))
        })
        .collect();

    let clean_durations: HashMap<YagoTerm, YagoTerm> = partitioned_statements
        .subjects_objects_for_predicate(WIKIBASE_QUANTITY_AMOUNT)
        .filter_map(|(s, amount)| {
            partitioned_statements
                .object_for_subject_predicate(&s, &WIKIBASE_QUANTITY_UNIT.into())
                .map(|unit| (s, amount, unit))
        })
        .filter_map(|(k, amount, unit)| convert_duration_quantity(amount, unit).map(|t| (k, t)))
        .collect();

    let clean_integers: HashMap<YagoTerm, YagoTerm> = partitioned_statements
        .subjects_objects_for_predicate(WIKIBASE_QUANTITY_AMOUNT)
        .filter_map(|(s, amount)| {
            partitioned_statements
                .object_for_subject_predicate(&s, &WIKIBASE_QUANTITY_UNIT.into())
                .map(|unit| (s, amount, unit))
        })
        .filter_map(|(k, amount, unit)| convert_integer_quantity(amount, unit).map(|t| (k, t)))
        .collect();

    let clean_quantities: HashMap<YagoTerm, (YagoTerm, Vec<YagoTriple>)> = map_value_to_yago(
        partitioned_statements.subjects_objects_for_predicate(WIKIBASE_QUANTITY_UNIT),
        wikidata_to_yago_uris_mapping,
    )
    .filter_map(|(s, unit)| {
        partitioned_statements
            .object_for_subject_predicate(&s, &WIKIBASE_QUANTITY_AMOUNT.into())
            .map(|amount| (s, unit, amount))
    })
    .filter_map(|(s, unit, amount)| {
        partitioned_statements
            .object_for_subject_predicate(&s, &WIKIBASE_QUANTITY_LOWER_BOUND.into())
            .map(|lower| (s, unit, amount, lower))
    })
    .filter_map(|(s, unit, amount, lower)| {
        partitioned_statements
            .object_for_subject_predicate(&s, &WIKIBASE_QUANTITY_UPPER_BOUND.into())
            .map(|upper| (s, unit, amount, lower, upper))
    })
    .filter_map(|(k, unit, amount, lower, upper)| {
        convert_quantity(&k, unit, amount, lower, upper).map(|v| (k, v))
    })
    .collect();

    /* let statements_with_annotations = vec![];
    TODO schemaShaclSchema.getScfhema().getAnnotationPropertyShapes().map(annotationShape ->
    map_wikidata_property_value(annotationShape,
                             partitioned_statements, yagoShapeInstances, wikidataToYagoUrisMapping,
                             clean_times, clean_durations, clean_integers, clean_quantities, clean_coordinates,
                             PQ_PREFIX, PQV_PREFIX
    ).mapValue(v -> (annotationShape.getProperty(), v))
    ).reduce(PairPlanNode::union).orElseGet(PairPlanNode::empty);*/

    let triples = schema.property_shapes().into_iter().filter(|property_shape| !exclude_properties.contains(&property_shape.path)).flat_map(|property_shape| {

        // We map the statement -> object relation
        let statement_object = map_wikidata_property_value(
            schema, &property_shape,
            partitioned_statements, yago_shape_instances, wikidata_to_yago_uris_mapping,
            &clean_times, &clean_durations, &clean_integers, &clean_quantities, &clean_coordinates,
            PS_PREFIX, PSV_PREFIX,
        );

        // We map the subject -> statement relation with domain filter
        let statement_subject: Multimap<YagoTerm, YagoTerm> = filter_domain(
            map_key_to_yago(get_subject_statement(partitioned_statements, &property_shape), wikidata_to_yago_uris_mapping),
            yago_shape_instances,
            &property_shape,
        ).map(|(subject, statement)| (statement, subject)).collect();

        let property_name = property_shape.path.clone();
        let statement_triple = join_pairs(statement_object.map(|(s, o, a)| (s, (o, a))), &statement_subject)
            .map(move |(statement_id, (object, mut additional), subject)| {
                additional.push(YagoTriple { subject, predicate: property_name.clone(), object });
                (statement_id, additional)
            });

        let rdf_type: YagoTerm = RDF_TYPE.into();
        let wikibase_best_rank: YagoTerm = WIKIBASE_BEST_RANK.into();
        let mut best_main_facts: Box<dyn Iterator<Item=(YagoTerm, Vec<YagoTriple>)>> = Box::new(statement_triple
            .filter(|(statement, _)| partitioned_statements.contains(statement, &rdf_type, &wikibase_best_rank)));  // We keep only best ranks

        // Max count
        if let Some(max_count) = property_shape.max_count {
            best_main_facts = Box::new(best_main_facts
                .map(|(statement, triples)| (triples[triples.len() - 1].subject.clone(), (statement, triples)))
                .collect::<Multimap<_, _>>()
                .into_iter_grouped()
                .filter(move |(_, t)| t.len() <= max_count)
                .flat_map(|(_, v)| v));
        }

        /* TODO Annotations
        //TODO: emit object annotations
        //TODO max_count on annotations
        let annotations = statement_triple
        .join(statements_with_annotations)
        .map((s, e) -> new AnnotatedStatement(e.getKey().getKey(), e.getValue().getKey(), e.getValue().getValue().getKey()));*/

        //TODO: avoid collect
        best_main_facts.flat_map(|(_, triples)| triples).collect::<Vec<_>>().into_iter()
    });

    write_ntriples(triples, dir, file_name)
}

type WikidataPropertyValueIterator<'a> =
    Box<dyn Iterator<Item = (YagoTerm, YagoTerm, Vec<YagoTriple>)> + 'a>;

fn map_wikidata_property_value<'a>(
    schema: &Schema,
    property_shape: &'a PropertyShape,
    partitioned_statements: &'a PartitionedStatements,
    yago_shape_instances: &'a HashMap<YagoTerm, HashSet<YagoTerm>>,
    wikidata_to_yago_uris_mapping: &'a HashMap<YagoTerm, YagoTerm>,
    clean_times: &'a HashMap<YagoTerm, YagoTerm>,
    clean_durations: &'a HashMap<YagoTerm, YagoTerm>,
    clean_integers: &'a HashMap<YagoTerm, YagoTerm>,
    clean_quantities: &'a HashMap<YagoTerm, (YagoTerm, Vec<YagoTriple>)>,
    clean_coordinates: &'a HashMap<YagoTerm, (YagoTerm, Vec<YagoTriple>)>,
    simple_value_prefix: &'a str,
    complex_value_prefix: &'a str,
) -> WikidataPropertyValueIterator<'a> {
    //TODO: smallvec for triples
    let mut statement_object: WikidataPropertyValueIterator<'a> = if !property_shape
        .datatypes
        .is_empty()
    {
        if !property_shape.nodes.is_empty() {
            eprintln!("The property {} could not have both a datatype domain and a node domain. Ignoring it.", property_shape.path);
            return Box::new(empty());
        }

        let mut dts: Vec<_> = property_shape
            .datatypes
            .iter()
            .filter_map(|dt| {
                if let YagoTerm::Iri(dt) = dt {
                    Some(NamedNode { iri: dt.as_str() })
                } else {
                    eprintln!("Invalid datatype: {}", dt);
                    None
                }
            })
            .collect();
        dts.sort();

        // Datatype filter
        if dts == [XSD_ANY_URI] {
            //We map IRIs to xsd:anyURI
            Box::new(
                get_triples_from_wikidata_property_relation(
                    partitioned_statements,
                    property_shape,
                    simple_value_prefix,
                )
                .filter_map(|(statement, object)| {
                    if let YagoTerm::Iri(object) = object {
                        if let Ok(url) = Url::parse(&object) {
                            Some((
                                statement,
                                YagoTerm::TypedLiteral(url.to_string(), XSD_ANY_URI.iri.to_owned()),
                                Vec::new(),
                            ))
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                }),
            )
        } else if dts == [XSD_DATE, XSD_DATE_TIME, XSD_G_YEAR, XSD_G_YEAR_MONTH] {
            //We clean up times by retrieving their full representation
            Box::new(get_and_convert_statements_complex_value(
                partitioned_statements,
                property_shape,
                clean_times,
                complex_value_prefix,
            ))
        } else if dts == [XSD_DURATION] {
            //We clean up durations from Wikibase quantities by retrieving their full representation
            Box::new(get_and_convert_statements_complex_value(
                partitioned_statements,
                property_shape,
                clean_durations,
                complex_value_prefix,
            ))
        } else if dts == [XSD_INTEGER] {
            //We clean up integers from Wikibase quantities by retrieving their full representation
            Box::new(get_and_convert_statements_complex_value(
                partitioned_statements,
                property_shape,
                clean_integers,
                complex_value_prefix,
            ))
        } else {
            Box::new(
                get_triples_from_wikidata_property_relation(
                    partitioned_statements,
                    property_shape,
                    simple_value_prefix,
                )
                .filter_map(move |(statement, object)| {
                    if object.datatype().map_or(false, |dt| dts.contains(&dt)) {
                        Some((statement, object, Vec::new()))
                    } else {
                        None
                    }
                }),
            )
        }
    } else if !property_shape.nodes.is_empty() {
        // Range type filter
        let expected_classes: Vec<YagoTerm> = property_shape
            .nodes
            .iter()
            .map(|id| schema.node_shape(id))
            .map(|shape| shape.target_class)
            .collect();

        if expected_classes.contains(&SCHEMA_GEO_COORDINATES.into()) && expected_classes.len() == 1
        {
            //We clean up globe coordinates by retrieving their full representation
            Box::new(get_and_convert_statements_annotated_complex_value(
                partitioned_statements,
                property_shape,
                clean_coordinates,
                complex_value_prefix,
            ))
        } else if expected_classes.contains(&SCHEMA_QUANTITATIVE_VALUE.into())
            && expected_classes.len() == 1
        {
            Box::new(get_and_convert_statements_annotated_complex_value(
                partitioned_statements,
                property_shape,
                clean_quantities,
                complex_value_prefix,
            ))
        } else if expected_classes.contains(&SCHEMA_IMAGE_OBJECT.into())
            && expected_classes.len() == 1
        {
            //We clean up image by retrieving their full representation
            Box::new(
                get_triples_from_wikidata_property_relation(
                    partitioned_statements,
                    property_shape,
                    simple_value_prefix,
                )
                .filter_map(|(statement, value)| {
                    if let YagoTerm::Iri(v) = value {
                        if v.starts_with("http://commons.wikimedia.org/wiki/Special:FilePath/") {
                            Some((statement, YagoTerm::Iri(v), Vec::new()))
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                }),
            )
        //TODO: image descriptions
        } else {
            Box::new(
                filter_object_range(
                    map_value_to_yago(
                        get_triples_from_wikidata_property_relation(
                            partitioned_statements,
                            property_shape,
                            simple_value_prefix,
                        ),
                        wikidata_to_yago_uris_mapping,
                    ),
                    yago_shape_instances,
                    expected_classes,
                )
                .map(|(statement, object)| (statement, object, Vec::new())),
            )
        }
    } else {
        eprintln!(
            "No range constraint found for property shape {:?}. Ignoring it.",
            property_shape,
        );
        return Box::new(empty());
    };

    //Regex
    if let Some(pattern) = &property_shape.pattern {
        let regex = Regex::new(pattern).unwrap();
        statement_object = Box::new(statement_object.filter(move |(_, object, _)| {
            if let YagoTerm::StringLiteral(v) = object {
                regex.is_match(&v)
            } else {
                false //TODO: uspport something else than xsd:string?
            }
        }));
    }

    statement_object
}

fn get_and_convert_statements_complex_value<'a>(
    partitioned_statements: &'a PartitionedStatements,
    property_shape: &'a PropertyShape,
    clean: &'a HashMap<YagoTerm, YagoTerm>,
    complex_value_prefix: &'a str,
) -> impl Iterator<Item = (YagoTerm, YagoTerm, Vec<YagoTriple>)> + 'a {
    get_triples_from_wikidata_property_relation(
        partitioned_statements,
        property_shape,
        complex_value_prefix,
    )
    .filter_map(move |(statement, object)| {
        clean
            .get(&object)
            .map(|object| (statement, object.clone(), Vec::new()))
    })
}

fn get_and_convert_statements_annotated_complex_value<'a>(
    partitioned_statements: &'a PartitionedStatements,
    property_shape: &'a PropertyShape,
    clean: &'a HashMap<YagoTerm, (YagoTerm, Vec<YagoTriple>)>,
    complex_value_prefix: &'a str,
) -> impl Iterator<Item = (YagoTerm, YagoTerm, Vec<YagoTriple>)> + 'a {
    get_triples_from_wikidata_property_relation(
        partitioned_statements,
        property_shape,
        complex_value_prefix,
    )
    .filter_map(move |(statement, object)| {
        clean
            .get(&object)
            .map(|(object, annotations)| (statement, object.clone(), annotations.clone()))
    })
}

fn get_subject_statement<'a>(
    partitioned_statements: &'a PartitionedStatements,
    property_shape: &'a PropertyShape,
) -> impl Iterator<Item = (YagoTerm, YagoTerm)> + 'a {
    get_triples_from_wikidata_property_relation(partitioned_statements, property_shape, P_PREFIX)
}

fn get_triples_from_wikidata_property_relation<'a>(
    partitioned_statements: &'a PartitionedStatements,
    property_shape: &'a PropertyShape,
    prefix: &'a str,
) -> impl Iterator<Item = (YagoTerm, YagoTerm)> + 'a {
    property_shape
        .from_properties
        .iter()
        .flat_map(move |property| {
            if let YagoTerm::WikidataProperty(id, _) = property {
                partitioned_statements
                    .subjects_objects_for_predicate(YagoTerm::iri(&format!("{}P{}", prefix, id)))
            } else {
                panic!("Invalid Wikidata property IRI: {:?}", property)
            }
        })
}

fn filter_domain<'a>(
    subject_statements: impl Iterator<Item = (YagoTerm, YagoTerm)> + 'a,
    yago_shape_instances: &'a HashMap<YagoTerm, HashSet<YagoTerm>>,
    property_shape: &PropertyShape,
) -> impl Iterator<Item = (YagoTerm, YagoTerm)> + 'a {
    let allowed = yago_shape_instances
        .get(&property_shape.parent_shape)
        .unwrap();
    subject_statements.filter(move |(s, _)| allowed.contains(s))
}

fn filter_object_range<'a>(
    subjects_objects: impl Iterator<Item = (YagoTerm, YagoTerm)> + 'a,
    yago_shape_instances: &'a HashMap<YagoTerm, HashSet<YagoTerm>>,
    expected_classes: Vec<YagoTerm>,
) -> impl Iterator<Item = (YagoTerm, YagoTerm)> + 'a {
    subjects_objects.filter(move |(_, o)| {
        expected_classes.iter().any(move |class| {
            yago_shape_instances
                .get(class)
                .map_or(false, |set| set.contains(o))
        })
    })
}

fn convert_time(
    value: YagoTerm,
    precision: YagoTerm,
    calendar_model: YagoTerm,
) -> Option<YagoTerm> {
    if calendar_model != WD_Q1985727.into() {
        return None; //TODO: add julian calendar support
    }
    if let YagoTerm::DateTimeLiteral(time) = value {
        Some(match precision {
            YagoTerm::IntegerLiteral(9) => {
                YagoTerm::TypedLiteral(time.format("%Y").to_string(), XSD_G_YEAR.iri.to_owned())
            }
            YagoTerm::IntegerLiteral(10) => YagoTerm::TypedLiteral(
                time.format("%Y-%m").to_string(),
                XSD_G_YEAR_MONTH.iri.to_owned(),
            ),
            YagoTerm::IntegerLiteral(11) => YagoTerm::TypedLiteral(
                time.format("%Y-%m-%d").to_string().to_string(),
                XSD_DATE.iri.to_owned(),
            ),
            YagoTerm::IntegerLiteral(14) => YagoTerm::DateTimeLiteral(time),
            _ => return None,
        })
    } else {
        None
    }
}

fn convert_globe_coordinates(
    latitude: YagoTerm,
    longitude: YagoTerm,
    precision: YagoTerm,
    globe: YagoTerm,
) -> Option<(YagoTerm, Vec<YagoTriple>)> {
    if globe != WD_Q2.into() {
        None //Not earth
    } else if let (
        YagoTerm::DoubleDecimal(latitude),
        YagoTerm::DoubleDecimal(longitude),
        YagoTerm::DoubleDecimal(precision),
    ) = (latitude, longitude, precision)
    {
        let rounded_latitude = round_degrees(*latitude, *precision);
        let rounded_longitude = round_degrees(*longitude, *precision);
        let iri = YagoTerm::Iri(format!("geo:{},{}", rounded_latitude, rounded_longitude));
        Some((
            iri.clone(),
            vec![
                YagoTriple {
                    subject: iri.clone(),
                    predicate: RDF_TYPE.into(),
                    object: SCHEMA_GEO_COORDINATES.into(),
                },
                YagoTriple {
                    subject: iri.clone(),
                    predicate: SCHEMA_LATITUDE.into(),
                    object: YagoTerm::DoubleDecimal(latitude),
                },
                YagoTriple {
                    subject: iri,
                    predicate: SCHEMA_LONGITUDE.into(),
                    object: YagoTerm::DoubleDecimal(longitude),
                },
            ],
        ))
    } else {
        None
    }
}

/// From https://github.com/DataValues/Geo/blob/master/src/Formatters/LatLongFormatter.php
fn round_degrees(degrees: f64, precision: f64) -> f64 {
    let reduced = (degrees.abs() / precision).round();
    let expended = reduced * precision;
    degrees.signum() * expended
}

fn convert_duration_quantity(amount: YagoTerm, unit: YagoTerm) -> Option<YagoTerm> {
    if let YagoTerm::DecimalLiteral(amount) = amount {
        if let Ok(amount) = i128::from_str(&amount) {
            if unit == WD_Q11574.into() {
                // seconds
                Some(YagoTerm::TypedLiteral(
                    if amount >= 0 {
                        format!("PT{}S", amount)
                    } else {
                        format!("-PT{}S", amount)
                    },
                    XSD_DURATION.iri.to_owned(),
                ))
            } else if unit == WD_Q7727.into() {
                // minutes
                Some(YagoTerm::TypedLiteral(
                    if amount >= 0 {
                        format!("PT{}M", amount)
                    } else {
                        format!("-PT{}M", amount)
                    },
                    XSD_DURATION.iri.to_owned(),
                ))
            } else if unit == WD_Q25235.into() {
                // hours
                Some(YagoTerm::TypedLiteral(
                    if amount >= 0 {
                        format!("PT{}H", amount)
                    } else {
                        format!("-PT{}H", amount)
                    },
                    XSD_DURATION.iri.to_owned(),
                ))
            } else if unit == WD_Q573.into() {
                // days
                Some(YagoTerm::TypedLiteral(
                    if amount >= 0 {
                        format!("P{}D", amount)
                    } else {
                        format!("-P{}D", amount)
                    },
                    XSD_DURATION.iri.to_owned(),
                ))
            } else {
                None
            }
        } else {
            None
        }
    } else {
        None
    }
}

fn convert_integer_quantity(amount: YagoTerm, unit: YagoTerm) -> Option<YagoTerm> {
    if unit != WD_Q199.into() {
        None
    } else if let YagoTerm::DecimalLiteral(amount) = amount {
        Some(YagoTerm::IntegerLiteral(i64::from_str(&amount).ok()?))
    } else {
        None
    }
}

fn convert_quantity(
    subject: &YagoTerm,
    unit: YagoTerm,
    amount: YagoTerm,
    lower_bound: YagoTerm,
    upper_bound: YagoTerm,
) -> Option<(YagoTerm, Vec<YagoTriple>)> {
    if let YagoTerm::Iri(subject) = subject {
        let quantity = YagoTerm::Iri(subject.replace(P_PREFIX, YAGO_VALUE_PREFIX));
        Some((
            quantity.clone(),
            vec![
                YagoTriple {
                    subject: quantity.clone(),
                    predicate: RDF_TYPE.into(),
                    object: SCHEMA_QUANTITATIVE_VALUE.into(),
                },
                YagoTriple {
                    subject: quantity.clone(),
                    predicate: SCHEMA_VALUE.into(),
                    object: amount,
                },
                YagoTriple {
                    subject: quantity.clone(),
                    predicate: SCHEMA_MIN_VALUE.into(),
                    object: lower_bound,
                },
                YagoTriple {
                    subject: quantity.clone(),
                    predicate: SCHEMA_MAX_VALUE.into(),
                    object: upper_bound,
                },
                YagoTriple {
                    subject: quantity,
                    predicate: SCHEMA_UNIT_CODE.into(),
                    object: unit,
                }, //TODO: best value
            ],
        ))
    } else {
        None
    }
}

fn build_same_as<'a>(
    partitioned_statements: &'a PartitionedStatements,
    yago_things: &'a HashSet<YagoTerm>,
    wikidata_to_yago_uris_mapping: &'a HashMap<YagoTerm, YagoTerm>,
    wikidata_to_en_wikipedia_mapping: &'a HashMap<YagoTerm, String>,
) -> impl Iterator<Item = YagoTriple> + 'a {
    // Wikidata
    let wikidata = wikidata_to_yago_uris_mapping
        .iter()
        .filter(move |(_, yago)| yago_things.contains(yago))
        .map(|(wd, yago)| YagoTriple {
            subject: yago.clone(),
            predicate: OWL_SAME_AS.into(),
            object: wd.clone(),
        });

    //dbPedia
    let db_pedia = map_key_to_yago(
        wikidata_to_en_wikipedia_mapping
            .iter()
            .map(|(a, b)| (a.clone(), b.clone())),
        wikidata_to_yago_uris_mapping,
    )
    .filter(move |(yago, _)| yago_things.contains(yago))
    .map(|(yago, wp)| YagoTriple {
        subject: yago.clone(),
        predicate: OWL_SAME_AS.into(),
        object: YagoTerm::Iri(wp.replace(
            "https://en.wikipedia.org/wiki/",
            "http://dbpedia.org/resource/",
        )),
    });

    //Freebase
    let freebase_id_pattern = Regex::new("/m/0([0-9a-z_]{2,6}|1[0123][0-9a-z_]{5})$").unwrap();
    let freebase = map_key_to_yago(
        partitioned_statements.subjects_objects_for_predicate(WDT_P646),
        wikidata_to_yago_uris_mapping,
    )
    .filter_map(|(yago, freebase)| {
        if let YagoTerm::StringLiteral(id) = freebase {
            Some((yago, id))
        } else {
            None
        }
    })
    .filter(move |(yago, freebase)| {
        yago_things.contains(yago) && freebase_id_pattern.is_match(freebase)
    })
    .map(|(yago, freebase)| YagoTriple {
        subject: yago.clone(),
        predicate: OWL_SAME_AS.into(),
        object: YagoTerm::Iri(
            "http://rdf.freebase.com/ns/".to_owned() + &freebase[1..].replace('/', "."),
        ),
    });

    //Wikipedia
    let wikipedia = map_value_to_yago(
        partitioned_statements.subjects_objects_for_predicate(SCHEMA_ABOUT),
        wikidata_to_yago_uris_mapping,
    )
    .filter_map(|(wp, yago)| {
        if let YagoTerm::Iri(iri) = wp {
            Some((yago, iri))
        } else {
            None
        }
    })
    .filter(move |(yago, wp)| yago_things.contains(yago) && wp.contains(".wikipedia.org/wiki/"))
    .map(|(yago, wp)| YagoTriple {
        subject: yago.clone(),
        predicate: SCHEMA_SAME_AS.into(),
        object: YagoTerm::TypedLiteral(wp.to_owned(), XSD_ANY_URI.iri.to_owned()),
    });

    wikidata.chain(db_pedia).chain(freebase).chain(wikipedia)
}

fn build_yago_schema(schema: &Schema) -> impl Iterator<Item = YagoTriple> {
    let mut yago_triples = HashSet::new();
    let mut domains = HashMap::new();
    let mut object_ranges = HashMap::new();
    let mut datatype_ranges = HashMap::new();

    // Classes
    for shape in schema.node_shapes() {
        if let Some(class) = schema.class(&shape.target_class) {
            yago_triples.insert(YagoTriple {
                subject: class.id.clone(),
                predicate: RDF_TYPE.into(),
                object: OWL_CLASS.into(),
            });
            if let Some(label) = &class.label {
                yago_triples.insert(YagoTriple {
                    subject: class.id.clone(),
                    predicate: RDFS_LABEL.into(),
                    object: term_caml_case_to_regular(label),
                });
            }
            if let Some(comment) = &class.comment {
                yago_triples.insert(YagoTriple {
                    subject: class.id.clone(),
                    predicate: RDFS_COMMENT.into(),
                    object: comment.clone(),
                });
            }
            for super_class in &class.super_classes {
                if super_class == &YagoTerm::Iri(SCHEMA_INTANGIBLE.iri.to_owned()) {
                    yago_triples.insert(YagoTriple {
                        subject: class.id.clone(),
                        predicate: RDFS_SUB_CLASS_OF.into(),
                        object: SCHEMA_THING.into(),
                    });
                } else if super_class == &YagoTerm::Iri(SCHEMA_STRUCTURED_VALUE.iri.to_owned())
                    || super_class == &YagoTerm::Iri(SCHEMA_SERIES.iri.to_owned())
                {
                    //Nothing
                } else {
                    yago_triples.insert(YagoTriple {
                        subject: class.id.clone(),
                        predicate: RDFS_SUB_CLASS_OF.into(),
                        object: super_class.clone(),
                    });
                }
            }
            for disjoint in &class.disjoint_classes {
                yago_triples.insert(YagoTriple {
                    subject: class.id.clone(),
                    predicate: OWL_DISJOINT_WITH.into(),
                    object: disjoint.clone(),
                });
            }
        }
    }

    // Properties
    for shape in schema.property_shapes() {
        if let Some(property) = schema.property(&shape.path) {
            yago_triples.insert(YagoTriple {
                subject: property.id.clone(),
                predicate: RDF_TYPE.into(),
                object: if !shape.nodes.is_empty() && shape.datatypes.is_empty() {
                    OWL_OBJECT_PROPERTY
                } else if shape.nodes.is_empty() && !shape.datatypes.is_empty() {
                    OWL_DATATYPE_PROPERTY
                } else {
                    eprintln!(
                        "Property {} could not be both an object and a datatype property",
                        property.id
                    );
                    RDF_PROPERTY
                }
                .into(),
            });

            if let Some(label) = &property.label {
                yago_triples.insert(YagoTriple {
                    subject: property.id.clone(),
                    predicate: RDFS_LABEL.into(),
                    object: term_caml_case_to_regular(label),
                });
            }
            if let Some(comment) = &property.comment {
                yago_triples.insert(YagoTriple {
                    subject: property.id.clone(),
                    predicate: RDFS_COMMENT.into(),
                    object: comment.clone(),
                });
            }
            for super_property in &property.super_properties {
                yago_triples.insert(YagoTriple {
                    subject: property.id.clone(),
                    predicate: RDFS_SUB_PROPERTY_OF.into(),
                    object: super_property.clone(),
                });
            }
            for inverse in &property.inverse {
                yago_triples.insert(YagoTriple {
                    subject: property.id.clone(),
                    predicate: OWL_INVERSE_OF.into(),
                    object: inverse.clone(),
                });
            }
            if shape.max_count == Some(1) {
                yago_triples.insert(YagoTriple {
                    subject: property.id.clone(),
                    predicate: RDF_TYPE.into(),
                    object: OWL_FUNCTIONAL_PROPERTY.into(),
                });
                //TODO: owl:maxCardinality?
            }
            domains
                .entry(shape.path.clone())
                .or_insert_with(BTreeSet::new)
                .insert(schema.node_shape(&shape.parent_shape).target_class);
            for object_range in &shape.nodes {
                object_ranges
                    .entry(shape.path.clone())
                    .or_insert_with(BTreeSet::new)
                    .insert(schema.node_shape(&object_range).target_class);
            }
            for datatype_range in &shape.datatypes {
                datatype_ranges
                    .entry(shape.path.clone())
                    .or_insert_with(BTreeSet::new)
                    .insert(if datatype_range == &RDF_LANG_STRING.into() {
                        RDF_PLAIN_LITERAL.into() // rdf:langString is not available in OWL 2 that depends on RDF 1.0
                    } else {
                        datatype_range.clone()
                    });
            }
        }
    }

    // Domains
    for (property, domain) in domains {
        add_union_of_object(
            &mut yago_triples,
            property,
            RDFS_DOMAIN.into(),
            domain,
            OWL_CLASS.into(),
        );
    }
    // Ranges
    for (property, range) in object_ranges {
        add_union_of_object(
            &mut yago_triples,
            property,
            RDFS_RANGE.into(),
            range,
            OWL_CLASS.into(),
        );
    }
    for (property, range) in datatype_ranges {
        if range.len() == 1 {
            add_union_of_object(
                &mut yago_triples,
                property,
                RDFS_RANGE.into(),
                range,
                RDFS_DATATYPE.into(),
            );
        }
    }

    yago_triples.into_iter()
}

fn add_union_of_object(
    model: &mut HashSet<YagoTriple>,
    subject: YagoTerm,
    predicate: YagoTerm,
    objects: BTreeSet<YagoTerm>,
    class: YagoTerm,
) {
    if objects.len() == 1 {
        model.insert(YagoTriple {
            subject,
            predicate,
            object: objects.into_iter().next().unwrap(),
        });
    } else {
        let union = YagoTerm::Iri(format!(
            "{}owl:unionOf-{}",
            YAGO_VALUE_PREFIX,
            string_name(&objects)
        ));
        model.insert(YagoTriple {
            subject,
            predicate,
            object: union.clone(),
        });
        model.insert(YagoTriple {
            subject: union.clone(),
            predicate: RDF_TYPE.into(),
            object: class,
        });
        add_list_object(model, union, OWL_UNION_OF.into(), objects);
    }
}

fn add_list_object(
    model: &mut HashSet<YagoTriple>,
    subject: YagoTerm,
    predicate: YagoTerm,
    objects: impl IntoIterator<Item = YagoTerm>,
) {
    let mut list: Vec<_> = objects.into_iter().collect();
    let name = format!("{}list-{}-", YAGO_VALUE_PREFIX, string_name(&list));

    let mut current: YagoTerm = RDF_NIL.into();
    while let Some(next) = list.pop() {
        let new_current = YagoTerm::Iri(format!("{}{}", name, list.len() + 1));
        model.insert(YagoTriple {
            subject: new_current.clone(),
            predicate: RDF_REST.into(),
            object: current.clone(),
        });
        model.insert(YagoTriple {
            subject: new_current.clone(),
            predicate: RDF_FIRST.into(),
            object: next,
        });
        current = new_current;
    }
    model.insert(YagoTriple {
        subject,
        predicate,
        object: current,
    });
}

fn string_name<'a>(list: impl IntoIterator<Item = &'a YagoTerm>) -> String {
    list.into_iter()
        .map(|t| match t {
            YagoTerm::Iri(v) => {
                for (p, start) in PREFIXES.iter() {
                    if v.starts_with(start) {
                        return v.replacen(start, p, 1);
                    }
                }
                v.replace('/', "").replace('?', "").replace('#', "")
            }
            _ => panic!("Not able to create a nice string name for: {}", t),
        })
        .collect::<Vec<_>>()
        .join("-")
}

fn term_caml_case_to_regular(term: &YagoTerm) -> YagoTerm {
    match term {
        YagoTerm::StringLiteral(s) => YagoTerm::StringLiteral(caml_case_to_regular(s)),
        YagoTerm::LanguageTaggedString(s, l) => {
            YagoTerm::LanguageTaggedString(caml_case_to_regular(s), l.to_owned())
        }
        _ => term.clone(),
    }
}

fn caml_case_to_regular(txt: &str) -> String {
    let mut out = String::with_capacity(txt.len());
    for c in txt.chars() {
        if c.is_uppercase() {
            if let Some(last) = out.chars().last() {
                if !last.is_ascii_whitespace() {
                    out.push(' ');
                }
            }
            out.extend(c.to_lowercase());
        } else {
            out.push(c);
        }
    }
    out
}

fn map_to_yago<'a>(
    input: impl Iterator<Item = YagoTerm> + 'a,
    with: &'a HashMap<YagoTerm, YagoTerm>,
) -> impl Iterator<Item = YagoTerm> + 'a {
    input.filter_map(move |i| with.get(&i).cloned())
}

fn map_key_to_yago<'a, V>(
    input: impl Iterator<Item = (YagoTerm, V)> + 'a,
    with: &'a HashMap<YagoTerm, YagoTerm>,
) -> impl Iterator<Item = (YagoTerm, V)> + 'a {
    input.filter_map(move |(k, v)| with.get(&k).map(|k| (k.clone(), v)))
}

fn map_value_to_yago<'a, K>(
    input: impl Iterator<Item = (K, YagoTerm)> + 'a,
    with: &'a HashMap<YagoTerm, YagoTerm>,
) -> impl Iterator<Item = (K, YagoTerm)> + 'a {
    input.filter_map(move |(k, v)| with.get(&v).map(|v| (k, v.clone())))
}

fn subclass_of_from_yago_schema(schema: &Schema) -> Multimap<YagoTerm, YagoTerm> {
    schema
        .node_shapes()
        .into_iter()
        .map(|s| s.target_class)
        .filter_map(|c| schema.class(&c))
        .flat_map(|c| {
            let sub_class = c.id.clone();
            c.super_classes.into_iter().filter_map(move |super_class| {
                if super_class == YagoTerm::from(SCHEMA_INTANGIBLE) {
                    Some((sub_class.clone(), SCHEMA_THING.into())) //We ignore schema:Intangible
                } else if super_class == YagoTerm::from(SCHEMA_STRUCTURED_VALUE)
                    || super_class == YagoTerm::from(SCHEMA_SERIES)
                {
                    None //schema:StructuredValue are not schema:Thing
                } else {
                    Some((sub_class.clone(), super_class))
                }
            })
        })
        .collect()
}

fn transitive_closure<T: Eq + Hash + Clone>(
    input: impl IntoIterator<Item = T>,
    with: &Multimap<T, T>,
) -> HashSet<T> {
    let mut closure: HashSet<T> = input.into_iter().collect();
    let mut to_do: Vec<T> = closure.iter().cloned().collect();
    while let Some(old_element) = to_do.pop() {
        if let Some(new_elements) = with.get(&old_element) {
            for new_element in new_elements {
                if closure.insert(new_element.clone()) {
                    to_do.push(new_element.clone());
                }
            }
        }
    }
    closure
}

fn transitive_closure_pair<K: Eq + Hash + Clone, V: Eq + Hash + Clone>(
    input: impl IntoIterator<Item = (K, V)>,
    with: &Multimap<V, V>,
) -> Multimap<K, V> {
    let mut closure: Multimap<K, V> = input.into_iter().collect();
    let mut to_do: Vec<(K, V)> = closure
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    while let Some((old_key, old_value)) = to_do.pop() {
        if let Some(new_values) = with.get(&old_value) {
            if let Some(existing_values) = closure.get_mut(&old_key) {
                for new_value in new_values {
                    if !existing_values.contains(new_value) {
                        to_do.push((old_key.clone(), new_value.clone()));
                        existing_values.push(new_value.clone());
                    }
                }
            } else {
                for new_value in new_values {
                    to_do.push((old_key.clone(), new_value.clone()));
                    closure.insert(old_key.clone(), new_value.clone());
                }
            }
        }
    }
    closure
}

fn join_pairs<'a, K: Eq + Hash + Clone, V1: Clone + 'a, V2: Clone + Eq>(
    left: impl Iterator<Item = (K, V1)> + 'a,
    right: &'a Multimap<K, V2>,
) -> impl Iterator<Item = (K, V1, V2)> + 'a {
    left.flat_map(move |(k, v1)| right.get(&k).map(move |v2s| (k, v1, v2s)))
        .flat_map(|(k, v1, v2s)| {
            v2s.iter()
                .map(move |v2| (k.clone(), v1.clone(), v2.clone()))
        })
}

fn encode_iri_path(path: &str, output: &mut String) {
    // See https://tools.ietf.org/html/rfc3987#section-2.2 rule ipchar
    path.chars().for_each(|c| match c {
        ' ' => output.push('_'),
        'a'..='z'
        | 'A'..='Z'
        | '0'..='9'
        | '-'
        | '.'
        | '_'
        | '~'
        | ':'
        | '@'
        | '!'
        | '$'
        | '&'
        | '\''
        | '('
        | ')'
        | '*'
        | '+'
        | ','
        | ';'
        | '='
        | '\u{A0}'..='\u{D7FF}'
        | '\u{F900}'..='\u{FDCF}'
        | '\u{FDF0}'..='\u{FFEF}'
        | '\u{10000}'..='\u{EFFFD}' => output.push(c),
        c => {
            let mut buf = [0 as u8; 4];
            c.encode_utf8(&mut buf).bytes().for_each(|b| {
                write!(output, "%{:X}", b).unwrap();
            });
        }
    });
}

#[test]
fn test_encode_iri_path() {
    let encode = |input| {
        let mut output = String::new();
        encode_iri_path(input, &mut output);
        output
    };
    assert_eq!(encode("Dürst"), "Dürst");
    assert_eq!(encode("Paris Hilton/Bio"), "Paris_Hilton%2FBio");
}
