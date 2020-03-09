use crate::vocab::*;
use chrono::{DateTime, FixedOffset};
use rio_api::model::NamedNode;
use rio_api::model::{BlankNode, Literal, NamedOrBlankNode, Term, Triple};
use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::ops::Deref;
use std::str::FromStr;
use u32;

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Clone)]
pub struct YagoTriple {
    pub subject: YagoTerm,
    pub predicate: YagoTerm,
    pub object: YagoTerm,
}

impl fmt::Display for YagoTriple {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}\t{}\t{}\t.",
            self.subject, self.predicate, self.object
        )
    }
}

impl<'a> From<Triple<'a>> for YagoTriple {
    fn from(t: Triple<'a>) -> Self {
        YagoTriple {
            subject: t.subject.into(),
            predicate: t.predicate.into(),
            object: t.object.into(),
        }
    }
}

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Clone)]
pub struct AnnotatedYagoTriple {
    pub subject: YagoTerm,
    pub predicate: YagoTerm,
    pub object: YagoTerm,
    pub annotation_predicate: YagoTerm,
    pub annotation_object: YagoTerm,
}

impl fmt::Display for AnnotatedYagoTriple {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "<<\t{}\t{}\t{}\t>>\t{}\t{}\t.",
            self.subject,
            self.predicate,
            self.object,
            self.annotation_predicate,
            self.annotation_object
        )
    }
}

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Clone)]
pub enum YagoTerm {
    WikidataItem(u32),
    WikidataProperty(u32, u8),
    Iri(String),
    BlankNode(String),
    StringLiteral(String),
    IntegerLiteral(i64),
    DecimalLiteral(String),
    DoubleDecimal(Double),
    DateTimeLiteral(DateTime<FixedOffset>),
    LanguageTaggedString(String, String),
    TypedLiteral(String, String),
}

impl YagoTerm {
    pub fn iri(iri: &str) -> Self {
        if iri.starts_with("http://www.wikidata.org/") {
            if iri.starts_with("http://www.wikidata.org/entity/Q") {
                iri.get(32..)
                    .and_then(|v| u32::from_str(v).ok())
                    .map(YagoTerm::WikidataItem)
                    .unwrap_or_else(|| YagoTerm::Iri(iri.to_string()))
            } else {
                for (i, prefix) in PROPERTY_PREFIXES.iter().enumerate() {
                    if iri.starts_with(prefix) {
                        return iri
                            .get(prefix.len()..)
                            .and_then(|v| u32::from_str(v).ok())
                            .map(|v| YagoTerm::WikidataProperty(v, i as u8))
                            .unwrap_or_else(|| YagoTerm::Iri(iri.to_string()));
                    }
                }
                YagoTerm::Iri(iri.to_string())
            }
        } else {
            YagoTerm::Iri(iri.to_string())
        }
    }

    pub fn from_parser(t: Term<'_>, seed: &str) -> Self {
        match t {
            Term::NamedNode(n) => YagoTerm::iri(n.iri),
            Term::BlankNode(b) => YagoTerm::BlankNode(b.id.to_owned() + seed),
            Term::Literal(Literal::Simple { value }) => YagoTerm::StringLiteral(value.to_owned()),
            Term::Literal(Literal::LanguageTaggedString { value, language }) => {
                YagoTerm::LanguageTaggedString(value.to_owned(), language.to_owned())
            }
            Term::Literal(Literal::Typed { value, datatype }) => {
                if datatype.iri == "http://www.w3.org/2001/XMLSchema#string" {
                    return YagoTerm::StringLiteral(value.to_string());
                } else if datatype.iri == "http://www.w3.org/2001/XMLSchema#integer" {
                    if let Ok(i) = i64::from_str(value) {
                        return YagoTerm::IntegerLiteral(i);
                    }
                } else if datatype.iri == "http://www.w3.org/2001/XMLSchema#decimal" {
                    return YagoTerm::DecimalLiteral(value.to_owned()); //TODO: encode
                } else if datatype.iri == "http://www.w3.org/2001/XMLSchema#double" {
                    if let Ok(i) = f64::from_str(value) {
                        return YagoTerm::DoubleDecimal(Double(i));
                    }
                } else if datatype.iri == "http://www.w3.org/2001/XMLSchema#dateTime" {
                    if let Ok(d) = DateTime::parse_from_rfc3339(value) {
                        return YagoTerm::DateTimeLiteral(d);
                    }
                }
                YagoTerm::TypedLiteral(value.to_owned(), datatype.iri.to_owned())
            }
        }
    }

    pub fn datatype(&self) -> Option<NamedNode<'_>> {
        match self {
            YagoTerm::WikidataItem(_)
            | YagoTerm::WikidataProperty(_, _)
            | YagoTerm::Iri(_)
            | YagoTerm::BlankNode(_) => None,
            YagoTerm::StringLiteral(_) => Some(XSD_STRING),
            YagoTerm::IntegerLiteral(_) => Some(XSD_INTEGER),
            YagoTerm::DecimalLiteral(_) => Some(XSD_DECIMAL),
            YagoTerm::DoubleDecimal(_) => Some(XSD_DOUBLE),
            YagoTerm::DateTimeLiteral(_) => Some(XSD_DATE_TIME),
            YagoTerm::LanguageTaggedString(_, _) => Some(RDF_LANG_STRING),
            YagoTerm::TypedLiteral(_, dt) => Some(NamedNode { iri: &dt }),
        }
    }
}

#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct Double(pub f64);

impl Deref for Double {
    type Target = f64;

    fn deref(&self) -> &f64 {
        &self.0
    }
}

impl PartialEq for Double {
    fn eq(&self, other: &Self) -> bool {
        if self.is_nan() && other.is_nan() {
            true
        } else {
            *self == *other
        }
    }
}

impl Eq for Double {}

impl PartialOrd for Double {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Double {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(&*other).unwrap_or_else(|| {
            if self.is_nan() && !other.is_nan() {
                Ordering::Less
            } else if !self.is_nan() && other.is_nan() {
                Ordering::Greater
            } else {
                Ordering::Equal
            }
        })
    }
}

impl Hash for Double {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.to_bits().hash(state);
    }
}

pub const PROPERTY_PREFIXES: [&str; 14] = [
    "http://www.wikidata.org/entity/P",
    "http://www.wikidata.org/prop/direct-normalized/P",
    "http://www.wikidata.org/prop/direct/P",
    "http://www.wikidata.org/prop/statement/value-normalized/P",
    "http://www.wikidata.org/prop/statement/value/P",
    "http://www.wikidata.org/prop/statement/P",
    "http://www.wikidata.org/prop/qualifier/value-normalized/P",
    "http://www.wikidata.org/prop/qualifier/value/P",
    "http://www.wikidata.org/prop/qualifier/P",
    "http://www.wikidata.org/prop/reference/value-normalized/P",
    "http://www.wikidata.org/prop/reference/value/P",
    "http://www.wikidata.org/prop/reference/P",
    "http://www.wikidata.org/prop/novalue/P",
    "http://www.wikidata.org/prop/P",
];

impl fmt::Display for YagoTerm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            YagoTerm::WikidataItem(i) => write!(f, "<http://www.wikidata.org/entity/Q{}>", i),
            YagoTerm::WikidataProperty(i, t) => {
                write!(f, "<{}{}>", PROPERTY_PREFIXES[*t as usize], i)
            }
            YagoTerm::Iri(i) => write!(f, "<{}>", i),
            YagoTerm::BlankNode(b) => write!(f, "_:{}", b),
            YagoTerm::StringLiteral(l) => Literal::Simple { value: l }.fmt(f),
            YagoTerm::IntegerLiteral(l) => {
                write!(f, "\"{}\"^^<http://www.w3.org/2001/XMLSchema#integer>", l)
            }
            YagoTerm::DecimalLiteral(l) => Literal::Typed {
                value: l,
                datatype: XSD_DECIMAL,
            }
            .fmt(f),
            YagoTerm::DoubleDecimal(l) => write!(
                f,
                "\"{}\"^^<http://www.w3.org/2001/XMLSchema#double>",
                l.deref()
            ),
            YagoTerm::DateTimeLiteral(l) => write!(
                f,
                "\"{}\"^^<http://www.w3.org/2001/XMLSchema#dateTime>",
                l.to_rfc3339()
            ),
            YagoTerm::LanguageTaggedString(v, l) => Literal::LanguageTaggedString {
                value: v,
                language: l,
            }
            .fmt(f),
            YagoTerm::TypedLiteral(v, t) => Literal::Typed {
                value: v,
                datatype: NamedNode { iri: t },
            }
            .fmt(f),
        }
    }
}

impl<'a> From<NamedNode<'a>> for YagoTerm {
    fn from(t: NamedNode<'a>) -> Self {
        Term::from(t).into()
    }
}

impl<'a> From<BlankNode<'a>> for YagoTerm {
    fn from(t: BlankNode<'a>) -> Self {
        Term::from(t).into()
    }
}

impl<'a> From<Literal<'a>> for YagoTerm {
    fn from(t: Literal<'a>) -> Self {
        Term::from(t).into()
    }
}

impl<'a> From<NamedOrBlankNode<'a>> for YagoTerm {
    fn from(t: NamedOrBlankNode<'a>) -> Self {
        Term::from(t).into()
    }
}

impl<'a> From<Term<'a>> for YagoTerm {
    fn from(t: Term<'a>) -> Self {
        Self::from_parser(t, "")
    }
}
