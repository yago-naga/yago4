use crate::partitioned_statements::PartitionedStatements;
use crate::plan::generate_yago;
use clap::{App, Arg, SubCommand};

mod model;
mod multimap;
mod partitioned_statements;
mod plan;
mod schema;
mod vocab;

fn main() {
    let matches = App::new("Yago 4 builder")
        .arg(
            Arg::with_name("cache")
                .short("c")
                .help("Path to the Yago builder cache database")
                .takes_value(true)
                .default_value("temp.db"),
        )
        .subcommand(
            SubCommand::with_name("partition")
                .about("Partition Wikidata N-Triples dump into multiple files")
                .arg(
                    Arg::with_name("file")
                        .short("f")
                        .help("Path to the N-Triples file")
                        .takes_value(true)
                        .required(true),
                ),
        )
        .subcommand(
            SubCommand::with_name("build").about("Build Yago 4").arg(
                Arg::with_name("output")
                    .short("o")
                    .help("Directory to output Yago 4 to")
                    .takes_value(true)
                    .required(true),
            ),
        )
        .get_matches();

    let cache_name = matches.value_of("cache").unwrap();

    if let Some(matches) = matches.subcommand_matches("partition") {
        PartitionedStatements::open(cache_name).load_ntriples(matches.value_of("file").unwrap());
    }
    if let Some(matches) = matches.subcommand_matches("build") {
        generate_yago(cache_name, matches.value_of("output").unwrap())
    }
}
