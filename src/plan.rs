//! The big piece of code. Generates YAGO 4 from the partioned statements

use crate::model::{AnnotatedYagoTriple, YagoTerm, YagoTriple};
use crate::multimap::Multimap;
use crate::partitioned_statements::PartitionedStatements;
use crate::schema::{PropertyShape, Schema};
use crate::vocab::*;
use crossbeam::thread;
use flate2::write::GzEncoder;
use flate2::Compression;
use percent_encoding::percent_decode_str;
use regex::Regex;
use rio_api::model::NamedNode;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt::Display;
use std::fmt::Write;
use std::fs::{create_dir_all, File};
use std::hash::Hash;
use std::io::BufWriter;
use std::iter::{empty, once};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Mutex;
use url::Url;

/// The different sizes/flavors of YAGO
#[derive(Copy, Clone, Debug)]
pub enum YagoSize {
    Full,
    AllWikipedias,
    EnglishWikipedia,
}

const P_PREFIX: &str = "http://www.wikidata.org/prop/";
const PS_PREFIX: &str = "http://www.wikidata.org/prop/statement/";
const PSV_PREFIX: &str = "http://www.wikidata.org/prop/statement/value/";
const PQ_PREFIX: &str = "http://www.wikidata.org/prop/qualifier/";
const PQV_PREFIX: &str = "http://www.wikidata.org/prop/qualifier/value/";
const YAGO_RESOURCE_PREFIX: &str = "http://yago-knowledge.org/resource/";
const YAGO_VALUE_PREFIX: &str = "http://yago-knowledge.org/value/";

/// Wikidata classes to filter out from YAGO
#[allow(clippy::unreadable_literal)]
const WD_BAD_CLASSES: [YagoTerm; 6] = [
    YagoTerm::WikidataItem(17379835), //Wikimedia page outside the main knowledge tree
    YagoTerm::WikidataItem(17442446), //Wikimedia internal stuff
    YagoTerm::WikidataItem(4167410),  //disambiguation page
    YagoTerm::WikidataItem(13406463), //list article
    YagoTerm::WikidataItem(17524420), //aspect of history
    YagoTerm::WikidataItem(18340514), //article about events in a specific year or time period
];

/// Wikidata items to always include in YAGO
#[allow(clippy::unreadable_literal)]
const MANDATORY_WD_ITEMS: [YagoTerm; 2] = [
    YagoTerm::WikidataItem(6581097), //Male
    YagoTerm::WikidataItem(6581072), //Female
];

/// The minimal number of instance threshold to consider classes for inclusion in YAGO
const MIN_NUMBER_OF_INSTANCES: usize = 10;

/// Main function to generate YAGO from an index
pub fn generate_yago(index_dir: impl AsRef<Path>, to_dir: &str, size: YagoSize) {
    let stats = Stats::default();
    let schema = Schema::open();
    let partitioned_statements = PartitionedStatements::open(index_dir);

    // Some useful tables
    let wikidata_to_enwikipedia_mapping = wikidata_to_enwikipedia_mapping(&partitioned_statements);
    stats.set_global(
        "Wikidata items mapped to English Wikipedia articles",
        wikidata_to_enwikipedia_mapping.len(),
    );

    let wikidata_to_yago_uris_mapping = wikidata_to_yago_uris_mapping(
        &stats,
        &schema,
        &partitioned_statements,
        &wikidata_to_enwikipedia_mapping,
        size,
    );

    let (yago_classes, wikidata_to_yago_class_mapping, yago_super_class_of) =
        build_yago_classes_and_super_class_of(
            &stats,
            &schema,
            &partitioned_statements,
            &wikidata_to_yago_uris_mapping,
            &wikidata_to_enwikipedia_mapping,
        );

    let yago_shape_instances = yago_shape_instances(
        &stats,
        &schema,
        &partitioned_statements,
        &wikidata_to_yago_class_mapping,
        &yago_super_class_of,
        &yago_classes,
        &wikidata_to_yago_uris_mapping,
    );

    // Incantations to build each YAGO 4 file in parallel
    // each spawn create a new thread and scope exits only when all the thread have finished
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
                "yago-wd-class.nt.gz",
            );
        });

        s.spawn(|_| {
            write_ntriples(
                build_simple_instance_of(&yago_shape_instances),
                &to_dir,
                "yago-wd-simple-types.nt.gz",
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
                "yago-wd-full-types.nt.gz",
            );
        });

        s.spawn(|_| {
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
                &to_dir,
                "yago-wd-labels.nt.gz",
            );
        });

        s.spawn(|_| {
            build_properties_from_wikidata_and_schema(
                &stats,
                &schema,
                &partitioned_statements,
                &yago_shape_instances,
                &wikidata_to_yago_uris_mapping,
                vec![
                    RDFS_LABEL.into(),
                    RDFS_COMMENT.into(),
                    SCHEMA_ALTERNATE_NAME.into(),
                ],
                to_dir.as_ref(),
                "yago-wd-facts.nt.gz",
                "yago-wd-annotated-facts.ntx.gz",
            );
        });

        s.spawn(|_| {
            write_ntriples(
                build_same_as(
                    &stats,
                    &partitioned_statements,
                    &yago_shape_instances.get(&SCHEMA_THING.into()).unwrap(),
                    &wikidata_to_yago_uris_mapping,
                    &wikidata_to_enwikipedia_mapping,
                ),
                &to_dir,
                "yago-wd-sameAs.nt.gz",
            );
        });

        s.spawn(|_| {
            write_ntriples(build_yago_schema(&schema), &to_dir, "yago-wd-schema.nt.gz");
        });

        s.spawn(|_| {
            write_ntriples(build_yago_shapes(&schema), &to_dir, "yago-wd-shapes.nt.gz");
        });
    })
    .unwrap();

    {
        let mut path = PathBuf::from(to_dir);
        path.push("stats.tsv");
        stats.write(path);
    }
}

/// The mapping between Wikidata (key) and the English Wikipedia (value)
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

/// Converts Wikidata URI (key) to Yago URIs (values)
/// It builds multiple URI candidates (from en.wikipedia titles, Wikidata labels) and merges them with a priority order
fn wikidata_to_yago_uris_mapping(
    stats: &Stats,
    schema: &Schema,
    partitioned_statements: &PartitionedStatements,
    wikidata_to_enwikipedia_mapping: &HashMap<YagoTerm, String>,
    size: YagoSize,
) -> HashMap<YagoTerm, YagoTerm> {
    println!("Generating Wikidata to Yago URI mapping");

    let wikibase_item = YagoTerm::from(WIKIBASE_ITEM);
    let wikidata_items: HashSet<YagoTerm> = partitioned_statements
        .subjects_objects_for_predicate(RDF_TYPE)
        .filter(|(_, o)| o == &wikibase_item)
        .map(|(s, _)| s)
        .collect();
    stats.set_global("Wikidata items", wikidata_items.len());

    let wikidata_items_with_wikipedia_article: HashSet<YagoTerm> = partitioned_statements
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
    stats.set_global(
        "Wikidata items with Wikipedia articles",
        wikidata_items_with_wikipedia_article.len(),
    );

    let mut wikidata_items_to_keep: HashSet<YagoTerm> = match size {
        YagoSize::Full => {
            println!("Considering all Wikidata items");
            wikidata_items
        }
        YagoSize::AllWikipedias => {
            println!("Considering only Wikidata items with a Wikipedia article in any language");
            wikidata_items_with_wikipedia_article
        }
        YagoSize::EnglishWikipedia => {
            println!("Considering only Wikidata items with an English Wikipedia");
            wikidata_to_enwikipedia_mapping
                .iter()
                .map(|(k, _)| k.clone())
                .collect()
        }
    };
    for item in MANDATORY_WD_ITEMS.iter() {
        wikidata_items_to_keep.insert(item.clone());
    }

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
    stats.set_global(
        "Possible Yago resource with English Wikipedia URI",
        from_wikipedia_mapping.len(),
    );

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
    stats.set_global(
        "Possible Yago resource with English label URI",
        from_label_mapping.len(),
    );

    let fallback_mapping: HashMap<YagoTerm, YagoTerm> = wikidata_items_to_keep
        .into_iter()
        .filter(|i| {
            !from_schema_mapping.contains_key(i)
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
    stats.set_global(
        "Possible Yago resource with Qid only URI",
        fallback_mapping.len(),
    );

    let result: HashMap<YagoTerm, YagoTerm> = from_schema_mapping
        .into_iter()
        .chain(from_wikipedia_mapping)
        .chain(from_label_mapping)
        .chain(fallback_mapping)
        .collect();
    stats.set_global("Total URI mapping size", result.len());
    result
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
/// <p>
/// It returns multiple elements:
/// 1. the set of all YAGO classes
/// the mapping between Wikidata (key) and YAGO (value) classes
/// The relations between super classes (key) and sub classes (value) of YAGO
fn build_yago_classes_and_super_class_of(
    stats: &Stats,
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
    stats.set_global(
        "Wikidata classes mapped to schema.org",
        yago_schema_from_classes.len(),
    );

    let all_wikidata_sub_class_of: Multimap<YagoTerm, YagoTerm> = partitioned_statements
        .subjects_objects_for_predicate(WDT_P279)
        .collect();
    stats.set_global("Wikidata sub class of", all_wikidata_sub_class_of.len());
    stats.set_global(
        "Wikidata items with sub class of",
        all_wikidata_sub_class_of.iter_grouped().count(),
    );

    let wikidata_sub_class_of: Multimap<YagoTerm, YagoTerm> = all_wikidata_sub_class_of
        .into_iter()
        .filter(|(s, _)| !yago_schema_from_classes.contains(s))
        .collect(); // Yago shape classes only have super classes which are shapes
    stats.set_global(
        "Wikidata sub class of from mapped classes",
        wikidata_sub_class_of.len(),
    );

    let wikidata_super_class_of: Multimap<YagoTerm, YagoTerm> = wikidata_sub_class_of
        .iter()
        .map(|(s, o)| (o.clone(), s.clone()))
        .collect();

    let wikidata_instances_by_class: Multimap<YagoTerm, YagoTerm> = partitioned_statements
        .subjects_objects_for_predicate(WDT_P31)
        .map(|(s, o)| (o, s))
        .collect::<Multimap<YagoTerm, YagoTerm>>();

    let wikidata_bad_classes =
        transitive_closure(WD_BAD_CLASSES.iter().cloned(), &wikidata_super_class_of);
    stats.set_global("bad classes", wikidata_bad_classes.len());
    stats.set_global(
        "bad classes instances",
        map_to_yago(
            wikidata_bad_classes
                .iter()
                .filter_map(|c| wikidata_instances_by_class.get(c))
                .flatten()
                .cloned(),
            wikidata_to_yago_uris_mapping,
        )
        .collect::<HashSet<_>>()
        .len(),
    );

    let yago_classes_sub_classes = transitive_closure(
        yago_schema_from_classes.iter().cloned(),
        &wikidata_super_class_of,
    );
    stats.set_global(
        "Wikidata classes subclass of a mapped class",
        yago_classes_sub_classes.len(),
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
    stats.set_global("Not disjoint classes", subclasses_of_disjoint.len());
    stats.set_global(
        "Not disjoint classes instances",
        map_to_yago(
            subclasses_of_disjoint
                .iter()
                .filter_map(|c| wikidata_instances_by_class.get(c))
                .flatten()
                .cloned(),
            wikidata_to_yago_uris_mapping,
        )
        .collect::<HashSet<_>>()
        .len(),
    );

    let wikidata_classes_with_at_least_min_count_instances: HashSet<YagoTerm> =
        wikidata_instances_by_class
            .into_iter_grouped()
            .filter(|(_, v)| v.len() >= MIN_NUMBER_OF_INSTANCES)
            .map(|(k, _)| k)
            .collect();
    stats.set_global(
        "classes with at least 10 instances",
        wikidata_classes_with_at_least_min_count_instances.len(),
    );

    let wikidata_classes_to_keep: HashSet<YagoTerm> = yago_classes_sub_classes
        .intersection(&wikidata_classes_with_at_least_min_count_instances)
        .filter(|c| !wikidata_bad_classes.contains(c) && !subclasses_of_disjoint.contains(c))
        .chain(&yago_schema_from_classes)
        .cloned()
        .collect();
    stats.set_global(
        "Wikidata classes used for the instance of extraction",
        wikidata_classes_to_keep.len(),
    );

    let wikidata_classes_to_keep_for_yago: HashSet<YagoTerm> = wikidata_classes_to_keep
        .iter()
        .filter(|c| wikidata_to_en_wikipedia_mapping.contains_key(c))
        .chain(&yago_schema_from_classes)
        .cloned()
        .collect();
    stats.set_global(
        "Wikidata classes kept for Yago",
        wikidata_classes_to_keep_for_yago.len(),
    );

    println!("Generating Yago subClassOf relations");

    let yago_sub_class_of_not_simplified: Multimap<YagoTerm, YagoTerm> = map_value_to_yago(
        map_key_to_yago(
            transitive_closure_pair(
                wikidata_sub_class_of
                    .iter()
                    .filter(|(k, _)| wikidata_classes_to_keep_for_yago.contains(k))
                    .map(|(k, v)| (k.clone(), v.clone())),
                &wikidata_sub_class_of,
            )
            .into_iter()
            .filter(|(_, v)| yago_schema_from_classes.contains(v)),
            wikidata_to_yago_uris_mapping,
        ),
        wikidata_to_yago_uris_mapping,
    )
    .chain(subclass_of_from_yago_schema(schema))
    .collect();

    let yago_super_class_of_not_simplified: Multimap<YagoTerm, YagoTerm> =
        yago_sub_class_of_not_simplified
            .iter()
            .map(|(k, v)| (v.clone(), k.clone()))
            .collect();

    let yago_super_class_of: Multimap<YagoTerm, YagoTerm> = filter_redundant_sub_class_of(
        yago_super_class_of_not_simplified,
        &yago_sub_class_of_not_simplified,
    );
    stats.set_global("sub class of relations in Yago", yago_super_class_of.len());

    println!("Generating Wikidata to Yago class mapping");

    let wikidata_to_yago_class_mapping: Multimap<YagoTerm, YagoTerm> =
        filter_redundant_sub_class_of(
            map_value_to_yago(
                wikidata_classes_to_keep_for_yago
                    .iter()
                    .map(|c| (c.clone(), c.clone()))
                    .chain(
                        transitive_closure_pair(
                            wikidata_classes_to_keep
                                .into_iter()
                                .filter(|c| !wikidata_classes_to_keep_for_yago.contains(c))
                                .map(|c| (c.clone(), c)),
                            &wikidata_sub_class_of,
                        )
                        .into_iter()
                        .filter(|(_, v)| yago_schema_from_classes.contains(v)),
                    ),
                wikidata_to_yago_uris_mapping,
            )
            .collect(),
            &yago_super_class_of,
        );

    let yago_classes: HashSet<YagoTerm> = map_to_yago(
        wikidata_classes_to_keep_for_yago.iter().cloned(),
        wikidata_to_yago_uris_mapping,
    )
    .collect();

    (
        yago_classes,
        wikidata_to_yago_class_mapping.into_iter().collect(), //TODO: avoid
        yago_super_class_of,
    )
}

/// Returns all (a,b) when there exists no c such that (a,c) in sub_class_of and (b,c) in super_class_of
fn filter_redundant_sub_class_of(
    sub_class_of: Multimap<YagoTerm, YagoTerm>,
    super_class_of: &Multimap<YagoTerm, YagoTerm>,
) -> Multimap<YagoTerm, YagoTerm> {
    sub_class_of
        .into_iter_grouped()
        .flat_map(|(child, parents)| {
            // Hacky filter to remove redundant sub class of
            parents
                .clone()
                .into_iter()
                .filter(move |parent| {
                    super_class_of.get(parent).map_or(true, |parent_children| {
                        !parent_children
                            .iter()
                            .any(|parent_child| parents.contains(parent_child))
                    })
                })
                .map(move |parent| (child.clone(), parent))
        })
        .collect()
}

/// Returns for each YAGO shape (key) the set of all its instances (value)
fn yago_shape_instances(
    stats: &Stats,
    schema: &Schema,
    partitioned_statements: &PartitionedStatements,
    wikidata_to_yago_class_mapping: &Multimap<YagoTerm, YagoTerm>,
    yago_super_class_of: &Multimap<YagoTerm, YagoTerm>,
    yago_classes: &HashSet<YagoTerm>,
    wikidata_to_yago_uris_mapping: &HashMap<YagoTerm, YagoTerm>,
) -> HashMap<YagoTerm, HashSet<YagoTerm>> {
    println!("Generating the list of instances for each shape");

    stats.set_global(
        "Wikidata instance of",
        partitioned_statements
            .subjects_objects_for_predicate(WDT_P31)
            .count(),
    );

    let wikidata_instances_for_yago_class: Multimap<YagoTerm, YagoTerm> = join_pairs(
        partitioned_statements
            .subjects_objects_for_predicate(WDT_P31)
            .map(|(s, o)| (o, s)),
        wikidata_to_yago_class_mapping,
    )
    .map(|(_, wd_instance, yago_class)| (yago_class, wd_instance))
    .collect();
    stats.set_global(
        "Wikidata instance of to a Yago class",
        wikidata_instances_for_yago_class.len(),
    );

    let instances_without_intersection_removal: HashMap<YagoTerm, HashSet<YagoTerm>> = schema
        .node_shapes()
        .into_iter()
        .map(|node_shape| {
            let from_yago_classes =
                transitive_closure(once(node_shape.target_class.clone()), yago_super_class_of);

            let wd_instances = from_yago_classes
                .iter()
                .flat_map(|class| wikidata_instances_for_yago_class.get(&class))
                .flatten()
                .cloned();
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
    stats.set_global(
        "Yago instances in a disjoint intersection",
        instances_in_disjoint_intersections.len(),
    );

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

            stats.set_local("Instances of a shape", class.to_string(), instances.len());

            (class, instances)
        })
        .collect()
}

/// Returns all the rdf:type triples between YAGO instances and schema.org classes
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

/// Returns the rdf:type triples between YAGO instances and YAGO classes extracted from Wikidata
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
            object: yago_class,
        },
    )
}

/// Returns the RDF triples describing YAGO classes (rdf:type, rdfs:subclassof, rdfs:label...)
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

/// Build facts without annotations from Wikidata mapping.
/// The `properties` parameter restricts the set of created facts to the one with the given properties.
fn build_simple_properties_from_schema<'a>(
    schema: &'a Schema,
    partitioned_statements: &'a PartitionedStatements,
    yago_shape_instances: &'a HashMap<YagoTerm, HashSet<YagoTerm>>,
    wikidata_to_yago_uris_mapping: &'a HashMap<YagoTerm, YagoTerm>,
    properties: Vec<YagoTerm>,
    dir: impl AsRef<Path>,
    file_name: &str,
) {
    let mut writer = NTriplesWriter::open(dir, file_name);

    for property_shape in schema.property_shapes() {
        if !properties.contains(&property_shape.path) {
            continue;
        }
        let subject_object = if !property_shape.datatypes.is_empty() {
            if !property_shape.nodes.is_empty() {
                continue;
            } else {
                let dts: BTreeSet<String> = property_shape
                    .datatypes
                    .iter()
                    .filter_map(|dt| {
                        if let YagoTerm::Iri(dt) = dt {
                            Some(dt.to_owned())
                        } else {
                            eprintln!("Invalid datatype: {}", dt);
                            None
                        }
                    })
                    .collect();

                property_shape
                    .from_properties
                    .clone()
                    .into_iter()
                    .flat_map(|p| partitioned_statements.subjects_objects_for_predicate(p))
                    .filter_map(move |(subject, object)| {
                        if object.datatype().map_or(false, |dt| dts.contains(dt.iri)) {
                            Some((subject, object))
                        } else {
                            None
                        }
                    })
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

        let predicate = property_shape.path.clone();
        writer.write_all(subject_object.map(move |(subject, object)| YagoTriple {
            subject,
            predicate: predicate.clone(),
            object,
        }))
    }

    writer.finish()
}

/// Build facts with annotations from Wikidata mapping.
/// The `exclude_properties` parameter restricts the set of created facts to the one without the given properties.
fn build_properties_from_wikidata_and_schema(
    stats: &Stats,
    schema: &Schema,
    partitioned_statements: &PartitionedStatements,
    yago_shape_instances: &HashMap<YagoTerm, HashSet<YagoTerm>>,
    wikidata_to_yago_uris_mapping: &HashMap<YagoTerm, YagoTerm>,
    exclude_properties: Vec<YagoTerm>,
    dir: &Path,
    file_name: &str,
    annotated_file_name: &str,
) {
    // Some utility plans executed in parallel
    let (clean_times, clean_coordinates, clean_durations, clean_integers, clean_quantities) =
        thread::scope(|s| {
            let clean_times = s.spawn(|_| {
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
                stats.set_local("Cleaned complex type", "time", clean_times.len());
                clean_times
            });

            let clean_coordinates = s.spawn(|_| {
                let clean_coordinates: HashMap<YagoTerm, (YagoTerm, Vec<YagoTriple>)> =
                    partitioned_statements
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
                stats.set_local(
                    "Cleaned complex type",
                    "coordinates",
                    clean_coordinates.len(),
                );
                clean_coordinates
            });

            let clean_durations = s.spawn(|_| {
                let clean_durations: HashMap<YagoTerm, YagoTerm> = partitioned_statements
                    .subjects_objects_for_predicate(WIKIBASE_QUANTITY_AMOUNT)
                    .filter_map(|(s, amount)| {
                        partitioned_statements
                            .object_for_subject_predicate(&s, &WIKIBASE_QUANTITY_UNIT.into())
                            .map(|unit| (s, amount, unit))
                    })
                    .filter_map(|(k, amount, unit)| {
                        convert_duration_quantity(amount, unit).map(|t| (k, t))
                    })
                    .collect();
                stats.set_local("Cleaned complex type", "duration", clean_durations.len());
                clean_durations
            });

            let clean_integers = s.spawn(|_| {
                let clean_integers: HashMap<YagoTerm, YagoTerm> = partitioned_statements
                    .subjects_objects_for_predicate(WIKIBASE_QUANTITY_AMOUNT)
                    .filter_map(|(s, amount)| {
                        partitioned_statements
                            .object_for_subject_predicate(&s, &WIKIBASE_QUANTITY_UNIT.into())
                            .map(|unit| (s, amount, unit))
                    })
                    .filter_map(|(k, amount, unit)| {
                        convert_integer_quantity(amount, unit).map(|t| (k, t))
                    })
                    .collect();
                stats.set_local("Cleaned complex type", "integer", clean_integers.len());
                clean_integers
            });

            let clean_quantities = s.spawn(|_| {
                let clean_quantities: HashMap<YagoTerm, (YagoTerm, Vec<YagoTriple>)> =
                    map_value_to_yago(
                        partitioned_statements
                            .subjects_objects_for_predicate(WIKIBASE_QUANTITY_UNIT),
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
                stats.set_local("Cleaned complex type", "quantity", clean_quantities.len());
                clean_quantities
            });

            (
                clean_times.join().unwrap(),
                clean_coordinates.join().unwrap(),
                clean_durations.join().unwrap(),
                clean_integers.join().unwrap(),
                clean_quantities.join().unwrap(),
            )
        })
        .unwrap();

    let statements_with_annotations: Multimap<YagoTerm, (YagoTerm, YagoTerm, Vec<YagoTriple>)> =
        schema
            .annotation_property_shapes()
            .iter()
            .flat_map(|property_shape| {
                let annotations: Vec<_> = map_wikidata_property_value(
                    schema,
                    property_shape,
                    partitioned_statements,
                    yago_shape_instances,
                    wikidata_to_yago_uris_mapping,
                    &clean_times,
                    &clean_durations,
                    &clean_integers,
                    &clean_quantities,
                    &clean_coordinates,
                    PQ_PREFIX,
                    PQV_PREFIX,
                )
                .map(move |(statement, object, facts)| {
                    (statement, (property_shape.path.clone(), object, facts))
                })
                .collect();
                stats.set_local(
                    "Possible annotations",
                    property_shape.path.to_string(),
                    annotations.len(),
                );
                annotations.into_iter()
            })
            .collect();

    let mut writer = NTriplesWriter::open(dir, file_name);
    let mut annotated_writer = NTriplesWriter::open(dir, annotated_file_name);

    for property_shape in schema.property_shapes() {
        if exclude_properties.contains(&property_shape.path) {
            continue;
        }
        // We map the subject -> statement relation and we apply best rank filter
        let rdf_type: YagoTerm = RDF_TYPE.into();
        let wikibase_best_rank: YagoTerm = WIKIBASE_BEST_RANK.into();
        let subject_statement: Vec<(YagoTerm, YagoTerm)> = map_key_to_yago(
            get_subject_statement(partitioned_statements, &property_shape),
            wikidata_to_yago_uris_mapping,
        )
        .filter(|(_, statement)| {
            partitioned_statements.contains(statement, &rdf_type, &wikibase_best_rank)
        }) // We keep only best ranks
        .collect();
        stats.set_local(
            "Yago facts before any filter",
            property_shape.path.to_string(),
            subject_statement.len(),
        );

        // We apply domain filter
        let statement_subject: Multimap<YagoTerm, YagoTerm> = filter_domain(
            subject_statement.into_iter(),
            yago_shape_instances,
            &property_shape,
        )
        .map(|(subject, statement)| (statement, subject))
        .collect();
        stats.add_local(
            "Yago facts after domain filter",
            property_shape.path.to_string(),
            statement_subject.len(),
        );

        // We map the statement -> object relation
        let statement_object = map_wikidata_property_value(
            schema,
            &property_shape,
            partitioned_statements,
            yago_shape_instances,
            wikidata_to_yago_uris_mapping,
            &clean_times,
            &clean_durations,
            &clean_integers,
            &clean_quantities,
            &clean_coordinates,
            PS_PREFIX,
            PSV_PREFIX,
        );

        let property_name = property_shape.path.clone();
        let statement_triple: Vec<(YagoTerm, Vec<YagoTriple>)> = join_pairs(
            statement_object.map(|(s, o, a)| (s, (o, a))),
            &statement_subject,
        )
        .map(move |(statement_id, (object, mut additional), subject)| {
            additional.push(YagoTriple {
                subject,
                predicate: property_name.clone(),
                object,
            });
            (statement_id, additional)
        })
        .collect();
        stats.add_local(
            "Yago facts after range filter",
            property_shape.path.to_string(),
            statement_triple.len(),
        );

        // Max count
        let statement_triple = if let Some(max_count) = property_shape.max_count {
            statement_triple
                .into_iter()
                .filter_map(|(statement, triples)| {
                    if let Some(main) = triples.last() {
                        Some((main.subject.clone(), (statement, triples)))
                    } else {
                        None
                    }
                })
                .collect::<Multimap<YagoTerm, _>>()
                .into_iter_grouped()
                .filter(move |(_, t)| t.len() <= max_count)
                .flat_map(|(_, v)| v)
                .collect()
        } else {
            statement_triple
        };
        stats.add_local(
            "Yago facts",
            property_shape.path.to_string(),
            statement_triple.len(),
        );

        // Annotations
        statement_triple
            .iter()
            .for_each(|(statement, main_triples)| {
                if let Some(annotations) = statements_with_annotations.get(statement) {
                    annotations.iter().for_each(
                        |(annotation_predicate, annotation_object, object_facts)| {
                            if let Some(main) = main_triples.last() {
                                annotated_writer.write(AnnotatedYagoTriple {
                                    subject: main.subject.clone(),
                                    predicate: main.predicate.clone(),
                                    object: main.object.clone(),
                                    annotation_predicate: annotation_predicate.clone(),
                                    annotation_object: annotation_object.clone(),
                                });
                                for fact in object_facts {
                                    annotated_writer.write(fact);
                                }
                                stats.add_local(
                                    "Yago annotations",
                                    format!("{} {}", main.predicate, annotation_predicate),
                                    1,
                                );
                            }
                        },
                    );
                }
            });

        writer.write_all(
            statement_triple
                .into_iter()
                .flat_map(|(_, triples)| triples),
        )
    }

    writer.finish()
}

/// Type of iterator of (statement id, object, extra triples about the objects)
type WikidataPropertyValueIterator<'a> =
    Box<dyn Iterator<Item = (YagoTerm, YagoTerm, Vec<YagoTriple>)> + 'a>;

/// Maps Wikidata statement values with optional annotations
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
                false //TODO: support something else than xsd:string?
            }
        }));
    }

    statement_object
}

/// Fetches the objects for the Wikidata statements and map them to the YAGO values using the `clean` map
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

/// Fetches the objects for the Wikidata statements and map them to the YAGO values using the `clean` map
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

/// Fetches the subject of the Wikidata statements which main property is a value of the yago:fromProperty relation of the given shape
fn get_subject_statement<'a>(
    partitioned_statements: &'a PartitionedStatements,
    property_shape: &'a PropertyShape,
) -> impl Iterator<Item = (YagoTerm, YagoTerm)> + 'a {
    get_triples_from_wikidata_property_relation(partitioned_statements, property_shape, P_PREFIX)
}

/// Fetches the triples which properties is a value of the yago:fromProperty relation of the given shape
/// The http://www.wikidata.org/prop/direct/ prefix is replaced by the given prefix
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

/// Filters subject_objects iterator to keep only the subjects that are instance of an element of expected_classes according to yago_shape_instances
fn filter_domain<'a>(
    subject_statements: impl Iterator<Item = (YagoTerm, YagoTerm)> + 'a,
    yago_shape_instances: &'a HashMap<YagoTerm, HashSet<YagoTerm>>,
    property_shape: &PropertyShape,
) -> impl Iterator<Item = (YagoTerm, YagoTerm)> + 'a {
    if let Some(parent_shape) = &property_shape.parent_shape {
        let allowed = yago_shape_instances.get(parent_shape).unwrap();
        subject_statements.filter(move |(s, _)| allowed.contains(s))
    } else {
        panic!("No parent shape for {:?}", property_shape)
    }
}

/// Filters subject_objects iterator to keep only the objects that are instance of an element of expected_classes according to yago_shape_instances
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

/// Builds an RDF term encoding a Wikibase time
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
            YagoTerm::IntegerLiteral(11) => {
                YagoTerm::TypedLiteral(time.format("%Y-%m-%d").to_string(), XSD_DATE.iri.to_owned())
            }
            YagoTerm::IntegerLiteral(14) => YagoTerm::DateTimeLiteral(time),
            _ => return None,
        })
    } else {
        None
    }
}

/// Builds an RDF representation encoding for a Wikibase geo coordinates
/// Returns the pair (term, describing triple)
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

/// Builds an RDF term encoding for a Wikibase quantity encoding a duration
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

/// Builds an RDF term encoding for a Wikibase integer quantity
fn convert_integer_quantity(amount: YagoTerm, unit: YagoTerm) -> Option<YagoTerm> {
    if unit != WD_Q199.into() {
        None
    } else if let YagoTerm::DecimalLiteral(amount) = amount {
        Some(YagoTerm::IntegerLiteral(i64::from_str(&amount).ok()?))
    } else {
        None
    }
}

/// Builds an RDF representation encoding for a Wikibase quantity
/// Returns the pair (term, describing triples)
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

/// Builds the RDF containing same as relations between Yago and Wikidata, Wikipedia, DBpedia and Freebase
fn build_same_as<'a>(
    stats: &Stats,
    partitioned_statements: &'a PartitionedStatements,
    yago_things: &'a HashSet<YagoTerm>,
    wikidata_to_yago_uris_mapping: &'a HashMap<YagoTerm, YagoTerm>,
    wikidata_to_en_wikipedia_mapping: &'a HashMap<YagoTerm, String>,
) -> impl Iterator<Item = YagoTriple> + 'a {
    // Wikidata
    let wikidata: Vec<YagoTriple> = wikidata_to_yago_uris_mapping
        .iter()
        .filter(move |(_, yago)| yago_things.contains(yago))
        .map(|(wd, yago)| YagoTriple {
            subject: yago.clone(),
            predicate: OWL_SAME_AS.into(),
            object: wd.clone(),
        })
        .collect();
    stats.set_local("sameAs", "Wikidata", wikidata.len());

    //dbPedia
    let db_pedia: Vec<YagoTriple> = map_key_to_yago(
        wikidata_to_en_wikipedia_mapping
            .iter()
            .map(|(a, b)| (a.clone(), b.clone())),
        wikidata_to_yago_uris_mapping,
    )
    .filter(move |(yago, _)| yago_things.contains(yago))
    .map(|(yago, wp)| YagoTriple {
        subject: yago,
        predicate: OWL_SAME_AS.into(),
        object: YagoTerm::Iri(wp.replace(
            "https://en.wikipedia.org/wiki/",
            "http://dbpedia.org/resource/",
        )),
    })
    .collect();
    stats.set_local("sameAs", "dbPedia", db_pedia.len());

    //Freebase
    let freebase_id_pattern = Regex::new("/m/0([0-9a-z_]{2,6}|1[0123][0-9a-z_]{5})$").unwrap();
    let freebase: Vec<YagoTriple> = map_key_to_yago(
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
        subject: yago,
        predicate: OWL_SAME_AS.into(),
        object: YagoTerm::Iri(
            "http://rdf.freebase.com/ns/".to_owned() + &freebase[1..].replace('/', "."),
        ),
    })
    .collect();
    stats.set_local("sameAs", "Freebase", freebase.len());

    //Wikipedia
    let wikipedia: Vec<YagoTriple> = map_value_to_yago(
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
        subject: yago,
        predicate: SCHEMA_SAME_AS.into(),
        object: YagoTerm::TypedLiteral(wp, XSD_ANY_URI.iri.to_owned()),
    })
    .collect();
    stats.set_local("sameAs", "Wikipedia", wikipedia.len());

    wikidata
        .into_iter()
        .chain(db_pedia)
        .chain(freebase)
        .chain(wikipedia)
}

/// builds the RDF for Yago schema
fn build_yago_schema(schema: &Schema) -> impl Iterator<Item = YagoTriple> {
    let mut yago_triples = Vec::new();
    let mut domains = HashMap::new();
    let mut object_ranges = HashMap::new();
    let mut datatype_ranges = HashMap::new();

    // Classes
    for shape in schema.node_shapes() {
        if let Some(class) = schema.class(&shape.target_class) {
            yago_triples.push(YagoTriple {
                subject: class.id.clone(),
                predicate: RDF_TYPE.into(),
                object: OWL_CLASS.into(),
            });
            if let Some(label) = &class.label {
                yago_triples.push(YagoTriple {
                    subject: class.id.clone(),
                    predicate: RDFS_LABEL.into(),
                    object: term_caml_case_to_regular(label),
                });
            }
            if let Some(comment) = &class.comment {
                yago_triples.push(YagoTriple {
                    subject: class.id.clone(),
                    predicate: RDFS_COMMENT.into(),
                    object: comment.clone(),
                });
            }
            for super_class in &class.super_classes {
                if super_class == &YagoTerm::Iri(SCHEMA_INTANGIBLE.iri.to_owned())
                    || super_class == &YagoTerm::Iri(SCHEMA_ENUMERATION.iri.to_owned())
                {
                    yago_triples.push(YagoTriple {
                        subject: class.id.clone(),
                        predicate: RDFS_SUB_CLASS_OF.into(),
                        object: SCHEMA_THING.into(),
                    });
                } else if super_class == &YagoTerm::Iri(SCHEMA_MEDICAL_INTANGIBLE.iri.to_owned())
                    || super_class == &YagoTerm::Iri(SCHEMA_MEDICAL_ENUMERATION.iri.to_owned())
                {
                    yago_triples.push(YagoTriple {
                        subject: class.id.clone(),
                        predicate: RDFS_SUB_CLASS_OF.into(),
                        object: SCHEMA_MEDICAL_ENTITY.into(),
                    });
                } else if super_class == &YagoTerm::Iri(SCHEMA_STRUCTURED_VALUE.iri.to_owned())
                    || super_class == &YagoTerm::Iri(SCHEMA_SERIES.iri.to_owned())
                {
                    //Nothing
                } else {
                    yago_triples.push(YagoTriple {
                        subject: class.id.clone(),
                        predicate: RDFS_SUB_CLASS_OF.into(),
                        object: super_class.clone(),
                    });
                }
            }
            for disjoint in &class.disjoint_classes {
                yago_triples.push(YagoTriple {
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
            yago_triples.push(YagoTriple {
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
                yago_triples.push(YagoTriple {
                    subject: property.id.clone(),
                    predicate: RDFS_LABEL.into(),
                    object: term_caml_case_to_regular(label),
                });
            }
            if let Some(comment) = &property.comment {
                yago_triples.push(YagoTriple {
                    subject: property.id.clone(),
                    predicate: RDFS_COMMENT.into(),
                    object: comment.clone(),
                });
            }
            for super_property in &property.super_properties {
                yago_triples.push(YagoTriple {
                    subject: property.id.clone(),
                    predicate: RDFS_SUB_PROPERTY_OF.into(),
                    object: super_property.clone(),
                });
            }
            for inverse in &property.inverse {
                yago_triples.push(YagoTriple {
                    subject: property.id.clone(),
                    predicate: OWL_INVERSE_OF.into(),
                    object: inverse.clone(),
                });
            }
            if shape.max_count == Some(1) {
                yago_triples.push(YagoTriple {
                    subject: property.id.clone(),
                    predicate: RDF_TYPE.into(),
                    object: OWL_FUNCTIONAL_PROPERTY.into(),
                });
                //TODO: owl:maxCardinality?
            }
            if let Some(parent_shape) = &shape.parent_shape {
                domains
                    .entry(shape.path.clone())
                    .or_insert_with(BTreeSet::new)
                    .insert(schema.node_shape(parent_shape).target_class);
            }
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
        add_union_of_object(
            &mut yago_triples,
            property,
            RDFS_RANGE.into(),
            range,
            RDFS_DATATYPE.into(),
        );
    }

    yago_triples.into_iter()
}

/// builds the RDF "subject predicate [owl:unionOf (object[0] ... object[n])]"
fn add_union_of_object(
    model: &mut Vec<YagoTriple>,
    subject: YagoTerm,
    predicate: YagoTerm,
    objects: BTreeSet<YagoTerm>,
    class: YagoTerm,
) {
    if objects.len() == 1 {
        model.push(YagoTriple {
            subject,
            predicate,
            object: objects.into_iter().next().unwrap(),
        });
    } else {
        let union = YagoTerm::BlankNode(format!(
            "{}-{}-owl-unionOf-{}",
            string_name(once(&subject)),
            string_name(once(&predicate)),
            string_name(&objects)
        ));
        model.push(YagoTriple {
            subject,
            predicate,
            object: union.clone(),
        });
        model.push(YagoTriple {
            subject: union.clone(),
            predicate: RDF_TYPE.into(),
            object: class,
        });
        add_list_object(model, union, OWL_UNION_OF.into(), objects);
    }
}

/// builds the RDF "subject predicate (object[0] ... object[n])"
fn add_list_object(
    model: &mut Vec<YagoTriple>,
    subject: YagoTerm,
    predicate: YagoTerm,
    objects: impl IntoIterator<Item = YagoTerm>,
) {
    let mut list: Vec<_> = objects.into_iter().collect();
    let name = format!("list-{}-", string_name(&list));

    let mut current: YagoTerm = RDF_NIL.into();
    while let Some(next) = list.pop() {
        let new_current = YagoTerm::BlankNode(format!("{}{}", name, list.len() + 1));
        model.push(YagoTriple {
            subject: new_current.clone(),
            predicate: RDF_REST.into(),
            object: current.clone(),
        });
        model.push(YagoTriple {
            subject: new_current.clone(),
            predicate: RDF_FIRST.into(),
            object: next,
        });
        current = new_current;
    }
    model.push(YagoTriple {
        subject,
        predicate,
        object: current,
    });
}

/// builds a string identifier from a set of RDF terms
fn string_name<'a>(list: impl IntoIterator<Item = &'a YagoTerm>) -> String {
    list.into_iter()
        .map(|t| match t {
            YagoTerm::Iri(v) => {
                for (p, start) in PREFIXES.iter() {
                    if v.starts_with(start) {
                        return v.replacen(start, &((*p).to_owned() + "-"), 1);
                    }
                }
                v.replace('/', "").replace('?', "").replace('#', "")
            }
            _ => panic!("Not able to create a nice string name for: {}", t),
        })
        .collect::<Vec<_>>()
        .join("-")
}

/// Converts "fooBar" to "foo bar"
fn term_caml_case_to_regular(term: &YagoTerm) -> YagoTerm {
    match term {
        YagoTerm::StringLiteral(s) => YagoTerm::StringLiteral(caml_case_to_regular(s)),
        YagoTerm::LanguageTaggedString(s, l) => {
            YagoTerm::LanguageTaggedString(caml_case_to_regular(s), l.to_owned())
        }
        _ => term.clone(),
    }
}

/// Converts "fooBar" to "foo bar"
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

/// Builds the RDF triples representing YAGO shapes
fn build_yago_shapes(schema: &Schema) -> impl Iterator<Item = YagoTriple> {
    let mut yago_triples = Vec::new();

    for node_shape in schema.node_shapes() {
        if node_shape.properties.is_empty() {
            continue; // Not useful
        }
        yago_triples.push(YagoTriple {
            subject: node_shape.target_class.clone(),
            predicate: RDF_TYPE.into(),
            object: SH_NODE_SHAPE.into(),
        });
        yago_triples.push(YagoTriple {
            subject: node_shape.target_class.clone(),
            predicate: SH_TARGET_CLASS.into(),
            object: node_shape.target_class.clone(),
        });
        for property_shape in node_shape.properties {
            let id = YagoTerm::Iri(format!(
                "{}shape-prop-{}",
                YAGO_VALUE_PREFIX,
                string_name(once(&node_shape.target_class).chain(once(&property_shape.path)))
            ));
            yago_triples.push(YagoTriple {
                subject: node_shape.target_class.clone(),
                predicate: SH_PROPERTY.into(),
                object: id.clone(),
            });
            yago_triples.push(YagoTriple {
                subject: id.clone(),
                predicate: RDF_TYPE.into(),
                object: SH_PROPERTY_SHAPE.into(),
            });
            yago_triples.push(YagoTriple {
                subject: id.clone(),
                predicate: SH_PATH.into(),
                object: property_shape.path.clone(),
            });
            match property_shape.datatypes.len() {
                0 => (),
                1 => {
                    yago_triples.push(YagoTriple {
                        subject: id.clone(),
                        predicate: SH_DATATYPE.into(),
                        object: property_shape.datatypes[0].clone(),
                    });
                }
                _ => {
                    let objects: Vec<_> = property_shape
                        .datatypes
                        .iter()
                        .map(|datatype| {
                            let subject = YagoTerm::Iri(format!(
                                "{}sh-datatype-{}",
                                YAGO_VALUE_PREFIX,
                                string_name(once(datatype))
                            ));
                            yago_triples.push(YagoTriple {
                                subject: subject.clone(),
                                predicate: SH_DATATYPE.into(),
                                object: datatype.clone(),
                            });
                            subject
                        })
                        .collect();
                    add_list_object(&mut yago_triples, id.clone(), SH_OR.into(), objects);
                }
            }
            match property_shape.nodes.len() {
                0 => (),
                1 => {
                    yago_triples.push(YagoTriple {
                        subject: id.clone(),
                        predicate: SH_NODE.into(),
                        object: property_shape.nodes[0].clone(),
                    });
                }
                _ => {
                    let objects: Vec<_> = property_shape
                        .nodes
                        .iter()
                        .map(|node| {
                            let subject = YagoTerm::Iri(format!(
                                "{}sh-node-{}",
                                YAGO_VALUE_PREFIX,
                                string_name(once(node))
                            ));
                            yago_triples.push(YagoTriple {
                                subject: subject.clone(),
                                predicate: SH_NODE.into(),
                                object: node.clone(),
                            });
                            subject
                        })
                        .collect();
                    add_list_object(&mut yago_triples, id.clone(), SH_OR.into(), objects);
                }
            }
            if property_shape.is_unique_lang {
                yago_triples.push(YagoTriple {
                    subject: id.clone(),
                    predicate: SH_UNIQUE_LANG.into(),
                    object: YagoTerm::TypedLiteral("true".to_owned(), XSD_BOOLEAN.iri.to_owned()),
                });
            }
            if let Some(max_count) = property_shape.max_count {
                yago_triples.push(YagoTriple {
                    subject: id.clone(),
                    predicate: SH_MAX_COUNT.into(),
                    object: YagoTerm::IntegerLiteral(max_count as i64),
                });
            }
            if let Some(pattern) = property_shape.pattern {
                yago_triples.push(YagoTriple {
                    subject: id.clone(),
                    predicate: SH_PATTERN.into(),
                    object: YagoTerm::StringLiteral(pattern),
                });
            }
        }
    }

    yago_triples.into_iter()
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
                if super_class == YagoTerm::from(SCHEMA_INTANGIBLE)
                    || super_class == YagoTerm::from(SCHEMA_MEDICAL_INTANGIBLE)
                {
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

/// Evaluates the transitive closure of with starting from input
/// i.e. the fixed point of "i -> i ⨝_{i = with[0]} with" containing input
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

/// Evaluates the transitive closure of with starting from input
/// i.e. the fixed point of "i -> i ⨝_{i[1] = with[0]} with" containing input
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

/// Evaluates σ_{left[0], left[1], right[1]}(left ⨝_{left[0] = right[0]} right)
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

/// Writes a .nt file
struct NTriplesWriter {
    inner: BufWriter<GzEncoder<File>>,
}

impl NTriplesWriter {
    fn open(dir: impl AsRef<Path>, file_name: &str) -> Self {
        println!("Writing file {}", file_name);

        create_dir_all(dir.as_ref()).unwrap();
        let mut path = dir.as_ref().to_owned();
        path.push(file_name);
        if !file_name.ends_with(".gz") {
            panic!("It only supportz .gz files");
        }
        Self {
            inner: BufWriter::new(GzEncoder::new(
                File::create(path).unwrap(),
                Compression::fast(),
            )),
        }
    }

    fn write(&mut self, triple: impl Display) {
        use std::io::Write;
        writeln!(self.inner, "{}", triple).unwrap();
    }

    fn write_all(&mut self, triples: impl IntoIterator<Item = YagoTriple>) {
        triples.into_iter().for_each(|t| self.write(t));
    }

    fn finish(self) {
        self.inner.into_inner().unwrap().finish().unwrap();
    }
}

fn write_ntriples(
    triples: impl IntoIterator<Item = YagoTriple>,
    dir: impl AsRef<Path>,
    file_name: &str,
) {
    let mut writer = NTriplesWriter::open(dir, file_name);
    writer.write_all(triples);
    writer.finish()
}

/// Utility to store and print statistics about the build
#[derive(Default)]
struct Stats {
    inner: Mutex<BTreeMap<&'static str, BTreeMap<String, usize>>>,
}

impl Stats {
    fn set_global(&self, key: &'static str, value: usize) {
        self.set_local(key, "*", value);
    }

    /// add a new value for a given (key, entry). If a value already exits the existing value is overrided
    fn set_local(&self, key: &'static str, entry: impl ToString, value: usize) {
        self.inner
            .lock()
            .unwrap()
            .entry(key)
            .or_insert_with(BTreeMap::new)
            .entry(entry.to_string())
            .or_insert(value);
    }

    /// add a new value for a given (key, entry). If a value already exits the sum of the two values is saved
    fn add_local(&self, key: &'static str, entry: impl ToString, value: usize) {
        self.inner
            .lock()
            .unwrap()
            .entry(key)
            .or_insert_with(BTreeMap::new)
            .entry(entry.to_string())
            .and_modify(|v| *v += value)
            .or_insert(value);
    }

    /// Write the stats as a TSV file
    fn write(&self, path: impl AsRef<Path>) {
        use std::io::Write;

        let mut file = BufWriter::new(File::create(path).unwrap());
        for (key, values) in &*self.inner.lock().unwrap() {
            if values.len() > 1 && !values.contains_key("*") {
                let sum: usize = values.values().sum();
                writeln!(file, "{}\t*\t{}", key, sum).unwrap();
            }
            for (entry, value) in values {
                writeln!(file, "{}\t{}\t{}", key, entry, value).unwrap();
            }
        }
    }
}

/// escapes the path of an IRI (the /foo/bar part of http://example.com/foo/bar?query)
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
            let mut buf = [0; 4];
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
