YAGO 4 pipeline
===============

[![actions status](https://github.com/yago-naga/yago4/workflows/build/badge.svg)](https://github.com/yago-naga/yago4/actions)

This is the pipeline to run YAGO 4.

It allows to build YAGO 4 from a Wikidata dump.

This pipeline is described in details in [the "YAGO 4: A Reason-able Knowledge Base" paper](https://suchanek.name/work/publications/eswc-2020-yago.pdf).



## How to run.
To install and compile it you need to have installed Clang,  [Rust and Cargo](https://www.rust-lang.org/tools/install).
* Ubuntu/Debian: `apt-get install cargo clang`.
* Arch: `pacman -S rust clang`.

Then you need to download a full Wikidata dump in the N-Triples format compressed using GZip. The latest is available at
https://dumps.wikimedia.org/other/wikibase/wikidatawiki/latest-all.nt.gz
(115GB as of December 2019).

Then you need to make the pipeline preprocess the file in order to feed the pipeline.
It could be done by running in the root directory of the code:

```cargo run --release -- -c wd-preprocessed.db partition -f latest-all.nt.gz```

where `preprocessed.db` is the directory where the preprocessed data are going to be stored
(beware, it takes 300GB as of December 2019) and `latest-all.nt.gz` the downloaded Wikidata dump.
This process should take around a night if you use an SSD.

When it's done you could build YAGO 4 itself with:

``` cargo run --release -- -c wd-preprocessed.db build -o yago4 --full```

where `yago4` is the output directory when YAGO 4 is going to be written
and `--full` the option to build the full YAGO 4.
If you want to only build YAGO 4 with entities with a Wikipedia article use `--all-wikis` instead
and `--en-wiki` to include only the entities with an English Wikipedia article.
The process should take a few hours.


## How to contribute

The source code of YAGO 4 pipeline is written in Rust.

The source code is split in multiple files:
* `main.rs`: The program entry point, parses the command line arguments
* `model.rs`: Data structures to represent efficiently RDF data in memory
* `multimap.rs`: A Multimap implementation.
* `partitioned_statements.rs`: Indexes the Wikidata dump into RocksDB and allow to query it efficiently.
* `plan.rs`: The actual building plan for YAGO 4.
* `schema.rs`: Reads schema.org data and SHACL shapes. Used by the pipeline.
* `vocab.rs`: Useful URIs for the program.

Multiple data files are used:
* `data/schema.ttl`: Official Turtle definition of schema.org from https://schema.org/version/latest/all-layers.ttl
* `data/bioschemas.ttl` Turtle definitions of [biochemas](https://bioschemas.org/).
* `data/shapes.ttl`: Shapes definitions and mapping from Wikidata.
* `data/shapes-bio.ttl`: Shapes definitions and mapping from Wikidata for bioschemas.


Tips:
* To format your source code please, use `cargo fmt`.
* `cargo clippy` is a very powerful linter.


## License

Copyright (C) 2019-2020 YAGO 4 contributors.

This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with this program.  If not, see <https://www.gnu.org/licenses/>.

## Citation

If you use this software for an academic publication, please cite:
```
@inproceedings{DBLP:conf/esws/TanonWS20,
  author    = {Pellissier Tanon, Thomas and Weikum, Gerhard and Suchanek, Fabian M.},
  title     = {{YAGO} 4: {A} Reason-able Knowledge Base},
  booktitle = {The Semantic Web - 17th International Conference, {ESWC} 2020, Heraklion, Crete, Greece, May 31-June 4, 2020, Proceedings},
  series    = {Lecture Notes in Computer Science},
  volume    = {12123},
  pages     = {583--596},
  publisher = {Springer},
  year      = {2020},
  url       = {https://doi.org/10.1007/978-3-030-49461-2_34},
  doi       = {10.1007/978-3-030-49461-2_34}
}
```