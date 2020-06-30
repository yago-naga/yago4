use crate::model::{Double, YagoTerm};
use byteorder::{NativeEndian, ReadBytesExt, WriteBytesExt};
use chrono::{DateTime, FixedOffset, NaiveDateTime};
use flate2::read::GzDecoder;
use rio_api::model::Triple;
use rio_api::parser::TriplesParser;
use rio_turtle::{NTriplesParser, TurtleError};
use rocksdb::{DBCompactionStyle, DBCompressionType, DBRawIterator, Options, WriteBatch, DB};
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Write};
use std::io::{Cursor, Result};
use std::mem::{size_of, take};
use std::path::Path;
use std::time::Instant;

/// A storage based on RocksDB that allows to retrieve triples based on a pattern.
/// It is based on a (predicate, subject, object) tree index and so allows to retrieve efficiently
/// all triples for a given predicate or a given (subject, predicate) tuple.
pub struct PartitionedStatements {
    db: DB,
}

impl PartitionedStatements {
    pub fn open(path: impl AsRef<Path>) -> Self {
        let mut options = Options::default();
        options.set_max_open_files(512);
        options.create_if_missing(true);
        options.set_allow_mmap_reads(true);
        options.set_allow_mmap_writes(true);
        options.set_compaction_style(DBCompactionStyle::Universal);
        options.set_compression_type(DBCompressionType::Lz4);
        options.set_max_background_flushes(8);
        options.set_max_background_compactions(16);
        Self {
            db: DB::open(&options, path).unwrap(),
        }
    }

    pub fn subjects_objects_for_predicate<'a>(
        &'a self,
        predicate: impl Into<YagoTerm>,
    ) -> impl Iterator<Item = (YagoTerm, YagoTerm)> + 'a {
        let mut prefix = Vec::new();
        self.write_term(&predicate.into(), &mut prefix).unwrap();
        let mut iter = self.db.raw_iterator();
        iter.seek(&prefix);
        SubjectObjectIterator { iter, prefix }
    }

    pub fn objects_for_subject_predicate<'a>(
        &'a self,
        subject: &YagoTerm,
        predicate: &YagoTerm,
    ) -> impl Iterator<Item = YagoTerm> + 'a {
        let mut prefix = Vec::new();
        self.write_term(predicate, &mut prefix).unwrap();
        self.write_term(subject, &mut prefix).unwrap();
        let mut iter = self.db.raw_iterator();
        iter.seek(&prefix);
        SubjectObjectIterator { iter, prefix }.map(|(_, o)| o)
    }

    pub fn object_for_subject_predicate<'a>(
        &'a self,
        subject: &YagoTerm,
        predicate: &YagoTerm,
    ) -> Option<YagoTerm> {
        self.objects_for_subject_predicate(subject, predicate)
            .next()
    }

    pub fn contains(&self, subject: &YagoTerm, predicate: &YagoTerm, object: &YagoTerm) -> bool {
        let mut key = Vec::new();
        self.write_term(predicate, &mut key).unwrap();
        self.write_term(subject, &mut key).unwrap();
        self.write_term(object, &mut key).unwrap();
        self.db.get(&key).unwrap().is_some()
    }

    pub fn load_ntriples(&self, file: &str) {
        if file.ends_with(".gz") {
            self.do_load_ntriples(GzDecoder::new(File::open(file).unwrap()))
        } else {
            self.do_load_ntriples(File::open(file).unwrap())
        }
    }

    fn do_load_ntriples(&self, read: impl Read) {
        let mut buffer = Vec::new();
        let mut i = 0;
        let start = Instant::now();
        let mut batch = WriteBatch::default();
        let mut parser = NTriplesParser::new(BufReader::new(read)).unwrap();
        let mut on_triple = |t: Triple<'_>| {
            self.write_term(&t.predicate.into(), &mut buffer).unwrap();
            self.write_term(&t.subject.into(), &mut buffer).unwrap();
            self.write_term(&t.object.into(), &mut buffer).unwrap();
            batch.put(buffer.as_slice(), &[]).unwrap();
            buffer.clear();

            i += 1;
            if i % 10_000 == 0 {
                self.db.write_without_wal(take(&mut batch)).unwrap();
                if i % 1_000_000 == 0 {
                    println!(
                        "{}M loaded at {} triples / s",
                        i / 1_000_000,
                        i / (Instant::now() - start).as_secs()
                    );
                }
            }
            Ok(()) as std::result::Result<(), TurtleError>
        };
        while !parser.is_end() {
            if let Err(e) = parser.parse_step(&mut on_triple) {
                eprintln!("Error while parsing N-Triples: {}", e);
            }
        }

        self.db.write_without_wal(batch).unwrap();
        println!("{} triples loaded, starting compaction", i);
        self.db
            .compact_range(None as Option<&[u8]>, None as Option<&[u8]>)
    }

    fn write_term(&self, t: &YagoTerm, buffer: &mut Vec<u8>) -> Result<()> {
        match t {
            YagoTerm::WikidataItem(i) => {
                buffer.write_u8(WIKIDATA_ITEM)?;
                buffer.write_u32::<NativeEndian>(*i)?;
            }
            YagoTerm::WikidataProperty(i, t) => {
                buffer.write_u8(WIKIDATA_PROPERTY)?;
                buffer.write_u32::<NativeEndian>(*i)?;
                buffer.write_u8(*t)?;
            }
            YagoTerm::Iri(i) => {
                buffer.write_u8(IRI)?;
                self.write_string(i, buffer)?;
            }
            YagoTerm::BlankNode(i) => {
                buffer.write_u8(BLANK_NODE)?;
                self.write_string(i, buffer)?;
            }
            YagoTerm::StringLiteral(i) => {
                buffer.write_u8(STRING_LITERAL)?;
                self.write_string(i, buffer)?;
            }
            YagoTerm::IntegerLiteral(i) => {
                buffer.write_u8(INTEGER_LITERAL)?;
                buffer.write_i64::<NativeEndian>(*i)?;
            }
            YagoTerm::DecimalLiteral(i) => {
                buffer.write_u8(DECIMAL_LITERAL)?;
                self.write_string(i, buffer)?;
            }
            YagoTerm::DoubleDecimal(i) => {
                buffer.write_u8(DOUBLE_LITERAL)?;
                buffer.write_f64::<NativeEndian>(**i)?;
            }
            YagoTerm::DateTimeLiteral(i) => {
                buffer.write_u8(DATETIME_LITERAL)?;
                buffer.write_i64::<NativeEndian>(i.timestamp())?;
                buffer.write_u32::<NativeEndian>(i.timestamp_subsec_nanos())?;
                buffer.write_i32::<NativeEndian>(i.timezone().local_minus_utc())?;
            }
            YagoTerm::LanguageTaggedString(v, l) => {
                buffer.write_u8(LANGUAGE_TAGGED_STRING)?;
                self.write_string(v, buffer)?;
                self.write_string(l, buffer)?;
            }
            YagoTerm::TypedLiteral(v, t) => {
                buffer.write_u8(TYPED_LITERAL)?;
                self.write_string(v, buffer)?;
                self.write_string(t, buffer)?;
            }
        }
        Ok(())
    }

    fn write_string(&self, string: &str, buffer: &mut Vec<u8>) -> Result<()> {
        buffer.write_all(string.as_bytes()).unwrap();
        buffer.write_u8(END_OF_STRING)
    }
}

const WIKIDATA_ITEM: u8 = 1;
const WIKIDATA_PROPERTY: u8 = 2;
const IRI: u8 = 3;
const BLANK_NODE: u8 = 4;
const STRING_LITERAL: u8 = 5;
const INTEGER_LITERAL: u8 = 6;
const DECIMAL_LITERAL: u8 = 7;
const DOUBLE_LITERAL: u8 = 8;
const DATETIME_LITERAL: u8 = 9;
const LANGUAGE_TAGGED_STRING: u8 = 10;
const TYPED_LITERAL: u8 = 11;
const END_OF_STRING: u8 = 0xFF;

struct SubjectObjectIterator<'a> {
    iter: DBRawIterator<'a>,
    prefix: Vec<u8>,
}

impl<'a> Iterator for SubjectObjectIterator<'a> {
    type Item = (YagoTerm, YagoTerm);

    fn next(&mut self) -> Option<(YagoTerm, YagoTerm)> {
        if let Some(key) = self.iter.key() {
            if key.starts_with(&self.prefix) {
                let result = self.read_next_triple(&mut Cursor::new(key)).unwrap();
                self.iter.next();
                Some(result)
            } else {
                None
            }
        } else {
            None
        }
    }
}

impl<'a> SubjectObjectIterator<'a> {
    fn read_next_triple(&self, buffer: &mut Cursor<&[u8]>) -> Result<(YagoTerm, YagoTerm)> {
        self.consume_next_term(buffer);
        let subject = self.read_next_term(buffer)?;
        let object = self.read_next_term(buffer)?;
        Ok((subject, object))
    }

    fn read_next_term(&self, buffer: &mut Cursor<&[u8]>) -> Result<YagoTerm> {
        Ok(match buffer.read_u8()? {
            WIKIDATA_ITEM => YagoTerm::WikidataItem(buffer.read_u32::<NativeEndian>()?),
            WIKIDATA_PROPERTY => {
                YagoTerm::WikidataProperty(buffer.read_u32::<NativeEndian>()?, buffer.read_u8()?)
            }
            IRI => YagoTerm::Iri(self.read_next_string(buffer)?),
            BLANK_NODE => YagoTerm::BlankNode(self.read_next_string(buffer)?),
            STRING_LITERAL => YagoTerm::StringLiteral(self.read_next_string(buffer)?),
            INTEGER_LITERAL => YagoTerm::IntegerLiteral(buffer.read_i64::<NativeEndian>()?),
            DECIMAL_LITERAL => YagoTerm::DecimalLiteral(self.read_next_string(buffer)?),
            DOUBLE_LITERAL => YagoTerm::DoubleDecimal(Double(buffer.read_f64::<NativeEndian>()?)),
            DATETIME_LITERAL => YagoTerm::DateTimeLiteral(DateTime::from_utc(
                NaiveDateTime::from_timestamp_opt(
                    buffer.read_i64::<NativeEndian>()?,
                    buffer.read_u32::<NativeEndian>()?,
                )
                .unwrap(),
                FixedOffset::east_opt(buffer.read_i32::<NativeEndian>()?).unwrap(),
            )),
            LANGUAGE_TAGGED_STRING => YagoTerm::LanguageTaggedString(
                self.read_next_string(buffer)?,
                self.read_next_string(buffer)?,
            ),
            TYPED_LITERAL => YagoTerm::TypedLiteral(
                self.read_next_string(buffer)?,
                self.read_next_string(buffer)?,
            ),
            b => panic!("Unexpected type byte {}", b),
        })
    }

    fn read_next_string(&self, buffer: &mut Cursor<&[u8]>) -> Result<String> {
        let mut buf = Vec::new();
        buffer.read_until(END_OF_STRING, &mut buf)?;
        buf.pop().unwrap();
        Ok(String::from_utf8(buf).unwrap())
    }

    fn consume_next_term(&self, buffer: &mut Cursor<&[u8]>) {
        match buffer.read_u8().unwrap() {
            WIKIDATA_ITEM => buffer.consume(size_of::<u32>()),
            WIKIDATA_PROPERTY => {
                buffer.consume(size_of::<u32>());
                buffer.consume(size_of::<u8>());
            }
            IRI | BLANK_NODE | STRING_LITERAL => self.consume_next_string(buffer),
            INTEGER_LITERAL => buffer.consume(size_of::<i64>()),
            DECIMAL_LITERAL => self.consume_next_string(buffer),
            DOUBLE_LITERAL => buffer.consume(size_of::<f64>()),
            DATETIME_LITERAL => {
                buffer.consume(size_of::<i64>());
                buffer.consume(size_of::<u32>());
                buffer.consume(size_of::<i32>());
            }
            LANGUAGE_TAGGED_STRING | TYPED_LITERAL => {
                self.consume_next_string(buffer);
                self.consume_next_string(buffer);
            }
            b => panic!("Unexpected type byte {}", b),
        }
    }

    fn consume_next_string(&self, buffer: &mut Cursor<&[u8]>) {
        let mut buf = Vec::new();
        buffer.read_until(END_OF_STRING, &mut buf).unwrap();
    }
}

#[test]
fn roundtrip() {
    use rio_api::model::NamedNode;
    use std::fs::{remove_dir_all, remove_file};

    {
        let mut file = File::create("unittest.nt").unwrap();
        file.write_all("<http://foo> <http://bar> <http://baz> .\n".as_bytes())
            .unwrap();
    }

    let part = PartitionedStatements::open("unittest.db");
    part.load_ntriples("unittest.nt");

    assert_eq!(
        part.subjects_objects_for_predicate(NamedNode { iri: "http://bar" })
            .count(),
        1
    );

    remove_file("unittest.nt").unwrap();
    remove_dir_all("unittest.db").unwrap();
}
