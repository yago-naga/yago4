package org.yago.yago4.converter.utils;

import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.datatypes.XMLDatatypeUtil;
import org.eclipse.rdf4j.model.impl.BooleanLiteral;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.util.URIUtil;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.model.vocabulary.SKOS;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.Year;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.SignStyle;
import java.time.temporal.ChronoField;
import java.util.*;

public class YagoValueFactory implements ValueFactory {
  private static final SimpleValueFactory VALUE_FACTORY = SimpleValueFactory.getInstance();
  private static final DatatypeFactory DATATYPE_FACTORY;
  static {
    try {
      DATATYPE_FACTORY = DatatypeFactory.newInstance();
    } catch (DatatypeConfigurationException e) {
      throw new RuntimeException(e);
    }
  }

  private static final DateTimeFormatter GYEAR_FORMATTER = (new DateTimeFormatterBuilder())
          .appendValue(ChronoField.YEAR, 4, 19, SignStyle.NORMAL)
          .toFormatter();
  private static final DateTimeFormatter GYEARMONTH_FORMATTER = (new DateTimeFormatterBuilder())
          .appendValue(ChronoField.YEAR, 4, 19, SignStyle.NORMAL)
          .appendLiteral('-')
          .appendValue(ChronoField.MONTH_OF_YEAR, 2, 2, SignStyle.NORMAL)
          .toFormatter();
  private static final DateTimeFormatter DATE_FORMATTER = (new DateTimeFormatterBuilder())
          .appendValue(ChronoField.YEAR, 4, 19, SignStyle.NORMAL)
          .appendLiteral('-')
          .appendValue(ChronoField.MONTH_OF_YEAR, 2, 2, SignStyle.NORMAL)
          .appendLiteral('-')
          .appendValue(ChronoField.DAY_OF_MONTH, 2, 2, SignStyle.NORMAL)
          .toFormatter();
  private static final DateTimeFormatter DATE_TIME_FORMATTER = (new DateTimeFormatterBuilder())
          .appendValue(ChronoField.YEAR, 4, 19, SignStyle.NORMAL)
          .appendLiteral('-')
          .appendValue(ChronoField.MONTH_OF_YEAR, 2, 2, SignStyle.NORMAL)
          .appendLiteral('-')
          .appendValue(ChronoField.DAY_OF_MONTH, 2, 2, SignStyle.NORMAL)
          .appendLiteral('T')
          .appendValue(ChronoField.HOUR_OF_DAY, 2, 2, SignStyle.NORMAL)
          .appendLiteral(':')
          .appendValue(ChronoField.MINUTE_OF_HOUR, 2, 2, SignStyle.NORMAL)
          .appendLiteral(':')
          .appendValue(ChronoField.SECOND_OF_MINUTE, 2, 2, SignStyle.NORMAL)
          .appendOffsetId()
          .toFormatter();

  private static final String[] NUMERIC_NAMESPACES = new String[]{
          "http://www.wikidata.org/Special:EntityData/",
          "http://www.wikidata.org/entity/",
          "http://www.wikidata.org/prop/direct-normalized/",
          "http://www.wikidata.org/prop/direct/",
          "http://www.wikidata.org/prop/novalue/",
          "http://www.wikidata.org/prop/statement/value-normalized/",
          "http://www.wikidata.org/prop/statement/value/",
          "http://www.wikidata.org/prop/statement/",
          "http://www.wikidata.org/prop/qualifier/value-normalized/",
          "http://www.wikidata.org/prop/qualifier/value/",
          "http://www.wikidata.org/prop/qualifier/",
          "http://www.wikidata.org/prop/reference/value-normalized/",
          "http://www.wikidata.org/prop/reference/value/",
          "http://www.wikidata.org/prop/reference/",
          "http://www.wikidata.org/prop/"
  };
  static {
    assert NUMERIC_NAMESPACES.length < Byte.MAX_VALUE;
  }

  private static final ValueFactory SVF = SimpleValueFactory.getInstance();
  private static final IRI[] CONSTANTS = new IRI[]{
          XMLSchema.STRING,
          XMLSchema.BOOLEAN,
          XMLSchema.DECIMAL,
          XMLSchema.INTEGER,
          XMLSchema.FLOAT,
          XMLSchema.DOUBLE,
          XMLSchema.DATE,
          XMLSchema.DATETIME,
          XMLSchema.TIME,
          XMLSchema.GYEAR,
          XMLSchema.GYEARMONTH,
          XMLSchema.DURATION,
          RDF.TYPE,
          RDF.LANGSTRING,
          RDF.PROPERTY,
          RDFS.LABEL,
          RDFS.COMMENT,
          RDFS.CLASS,
          RDFS.SUBCLASSOF,
          RDFS.SUBPROPERTYOF,
          SKOS.PREF_LABEL,
          SKOS.ALT_LABEL,
          SKOS.DEFINITION,
          SKOS.NOTE,
          SKOS.BROADER,
          SKOS.NARROWER,
          SKOS.BROAD_MATCH,
          SKOS.NARROW_MATCH,
          SKOS.EXACT_MATCH,
          SVF.createIRI("http://www.w3.org/ns/prov#wasDerivedFrom"),
          SVF.createIRI("http://schema.org/Dataset"),
          SVF.createIRI("http://schema.org/name"),
          SVF.createIRI("http://schema.org/description"),
          SVF.createIRI("http://schema.org/Article"),
          SVF.createIRI("http://schema.org/dateModified"),
          SVF.createIRI("http://schema.org/about"),
          SVF.createIRI("http://schema.org/isPartOf"),
          SVF.createIRI("http://schema.org/inLanguage"),
          SVF.createIRI("http://wikiba.se/ontology#Item"),
          SVF.createIRI("http://wikiba.se/ontology#Property"),
          SVF.createIRI("http://wikiba.se/ontology#Lexeme"),
          SVF.createIRI("http://wikiba.se/ontology#Form"),
          SVF.createIRI("http://wikiba.se/ontology#Sense"),
          SVF.createIRI("http://wikiba.se/ontology#Statement"),
          SVF.createIRI("http://wikiba.se/ontology#Reference"),
          SVF.createIRI("http://wikiba.se/ontology#TimeValue"),
          SVF.createIRI("http://wikiba.se/ontology#QuantityValue"),
          SVF.createIRI("http://wikiba.se/ontology#GlobecoordinateValue"),
          SVF.createIRI("http://wikiba.se/ontology#PreferredRank"),
          SVF.createIRI("http://wikiba.se/ontology#NormalRank"),
          SVF.createIRI("http://wikiba.se/ontology#DeprecatedRank"),
          SVF.createIRI("http://wikiba.se/ontology#BestRank"),
          SVF.createIRI("http://wikiba.se/ontology#1"),
          SVF.createIRI("http://wikiba.se/ontology#badge"),
          SVF.createIRI("http://wikiba.se/ontology#propertyType"),
          SVF.createIRI("http://wikiba.se/ontology#directClaim"),
          SVF.createIRI("http://wikiba.se/ontology#directClaimNormalized"),
          SVF.createIRI("http://wikiba.se/ontology#claim"),
          SVF.createIRI("http://wikiba.se/ontology#statementProperty"),
          SVF.createIRI("http://wikiba.se/ontology#statementValue"),
          SVF.createIRI("http://wikiba.se/ontology#statementValueNormalized"),
          SVF.createIRI("http://wikiba.se/ontology#qualifier"),
          SVF.createIRI("http://wikiba.se/ontology#qualifierValue"),
          SVF.createIRI("http://wikiba.se/ontology#qualifierValueNormalized"),
          SVF.createIRI("http://wikiba.se/ontology#reference"),
          SVF.createIRI("http://wikiba.se/ontology#referenceValue"),
          SVF.createIRI("http://wikiba.se/ontology#referenceValueNormalized"),
          SVF.createIRI("http://wikiba.se/ontology#hasViolationForConstraint"),
          SVF.createIRI("http://wikiba.se/ontology#lemma"),
          SVF.createIRI("http://wikiba.se/ontology#lexicalCategory"),
          SVF.createIRI("http://wikiba.se/ontology#grammaticalFeature"),
          SVF.createIRI("http://wikiba.se/ontology#geoLatitude"),
          SVF.createIRI("http://wikiba.se/ontology#geoLongitude"),
          SVF.createIRI("http://wikiba.se/ontology#geoPrecision"),
          SVF.createIRI("http://wikiba.se/ontology#geoGlobe"),
          SVF.createIRI("http://wikiba.se/ontology#quantityAmount"),
          SVF.createIRI("http://wikiba.se/ontology#quantityUpperBound"),
          SVF.createIRI("http://wikiba.se/ontology#quantityLowerBound"),
          SVF.createIRI("http://wikiba.se/ontology#quantityUnit"),
          SVF.createIRI("http://wikiba.se/ontology#quantityNormalized"),
          SVF.createIRI("http://wikiba.se/ontology#timeValue"),
          SVF.createIRI("http://wikiba.se/ontology#timePrecision"),
          SVF.createIRI("http://wikiba.se/ontology#timeTimezone"),
          SVF.createIRI("http://wikiba.se/ontology#timeCalendarModel"),
          SVF.createIRI("http://wikiba.se/ontology#statements"),
          SVF.createIRI("http://wikiba.se/ontology#sitelinks")
  };
  private static final Map<String, IRI> CONSTANTS_IRIS_FOR_STRING = new HashMap<>();
  private static final Map<IRI, Integer> CONSTANTS_IDS_FOR_IRI = new HashMap<>();
  static {
    assert CONSTANTS.length < Byte.MAX_VALUE;

    for (int i = 0; i < CONSTANTS.length; i++) {
      IRI iri = CONSTANTS[i];
      CONSTANTS_IRIS_FOR_STRING.put(iri.stringValue(), iri);
      CONSTANTS_IDS_FOR_IRI.put(iri, i);
    }
  }

  private static final YagoValueFactory INSTANCE = new YagoValueFactory();

  public static YagoValueFactory getInstance() {
    return INSTANCE;
  }

  private YagoValueFactory() {
  }

  @Override
  public IRI createIRI(String iri) {
    return buildIRI(iri);
  }

  private static IRI buildIRI(String iri) {
    IRI constant = CONSTANTS_IRIS_FOR_STRING.get(iri);
    if (constant != null) {
      return constant;
    }

    for (byte i = 0; i < NUMERIC_NAMESPACES.length; i++) {
      if (iri.startsWith(NUMERIC_NAMESPACES[i])) {
        int prefixLength = NUMERIC_NAMESPACES[i].length();
        if (iri.length() > prefixLength && iri.lastIndexOf('/') < prefixLength) {
          try {
            return new NumericIri(i, iri.charAt(prefixLength), Integer.parseInt(iri.substring(prefixLength + 1)));
          } catch (NumberFormatException e) {
            break;
          }
        }
      }
    }

    return new BasicIRI(iri);
  }

  @Override
  public IRI createIRI(String namespace, String localName) {
    return buildIRI(namespace + localName);
  }

  @Override
  public BNode createBNode() {
    return VALUE_FACTORY.createBNode();
  }

  @Override
  public BNode createBNode(String nodeID) {
    return VALUE_FACTORY.createBNode(nodeID);
  }

  @Override
  public Literal createLiteral(String label) {
    return new StringLiteral(label);
  }

  @Override
  public Literal createLiteral(String label, String language) {
    return new LangStringLiteral(label, language);
  }

  @Override
  public Literal createLiteral(String label, IRI datatype) {
    if (XMLSchema.STRING.equals(datatype)) {
      return new StringLiteral(label);
    }
    if (XMLSchema.DOUBLE.equals(datatype)) {
      try {
        return new DoubleLiteral(XMLDatatypeUtil.parseDouble(label));
      } catch (NumberFormatException e) {
        // fall thought
      }
    }
    if (XMLSchema.INTEGER.equals(datatype)) {
      try {
        return new IntegerLiteral(XMLDatatypeUtil.parseLong(label));
      } catch (NumberFormatException e) {
        // fall thought
      }
    }
    if (XMLSchema.DECIMAL.equals(datatype)) {
      try {
        return new DecimalLiteral(XMLDatatypeUtil.parseDecimal(label));
      } catch (NumberFormatException e) {
        // fall thought
      }
    }

    return new TypedLiteral(label, datatype);
  }

  @Override
  public Literal createLiteral(boolean value) {
    return value ? BooleanLiteral.TRUE : BooleanLiteral.FALSE;
  }

  @Override
  public Literal createLiteral(byte value) {
    return new IntegerLiteral(value);
  }

  @Override
  public Literal createLiteral(short value) {
    return new IntegerLiteral(value);
  }

  @Override
  public Literal createLiteral(int value) {
    return new IntegerLiteral(value);
  }

  @Override
  public Literal createLiteral(long value) {
    return new IntegerLiteral(value);
  }

  @Override
  public Literal createLiteral(float value) {
    return new DoubleLiteral(value);
  }

  @Override
  public Literal createLiteral(double value) {
    return new DoubleLiteral(value);
  }

  @Override
  public Literal createLiteral(BigDecimal value) {
    return new DecimalLiteral(value);
  }

  @Override
  public Literal createLiteral(BigInteger value) {
    try {
      return new IntegerLiteral(value.longValueExact());
    } catch (ArithmeticException e) {
      return new TypedLiteral(value.toString(), XMLSchema.INTEGER);
    }
  }

  @Override
  public Literal createLiteral(XMLGregorianCalendar calendar) {
    return createLiteral(calendar.toXMLFormat(), XMLDatatypeUtil.qnameToURI(calendar.getXMLSchemaType()));
  }

  @Override
  public Literal createLiteral(Date date) {
    GregorianCalendar c = new GregorianCalendar();
    c.setTime(date);
    return createLiteral(DATATYPE_FACTORY.newXMLGregorianCalendar(c));
  }

  public Literal createLiteral(OffsetDateTime instant) {
    return new TypedLiteral(instant.format(DATE_TIME_FORMATTER), XMLSchema.DATETIME);
  }

  public Literal createLiteral(LocalDate date) {
    return new TypedLiteral(date.format(DATE_FORMATTER), XMLSchema.DATE);
  }

  public Literal createLiteral(Year year) {
    return new TypedLiteral(year.format(GYEAR_FORMATTER), XMLSchema.GYEAR);
  }

  public Literal createLiteral(YearMonth yearMonth) {
    return new TypedLiteral(yearMonth.format(GYEARMONTH_FORMATTER), XMLSchema.GYEARMONTH);
  }

  @Override
  public Statement createStatement(Resource subject, IRI predicate, Value object) {
    return VALUE_FACTORY.createStatement(subject, predicate, object);
  }

  @Override
  public Statement createStatement(Resource subject, IRI predicate, Value object, Resource context) {
    return VALUE_FACTORY.createStatement(subject, predicate, object, context);
  }

  private static final int IRI_KEY = 1;
  private static final int CONSTANT_IRI_KEY = 6;
  private static final int NUMERIC_IRI_KEY = 7;
  private static final int BNODE_KEY = 2;
  private static final int STRING_LITERAL_KEY = 3;
  private static final int LANG_STRING_LITERAL_KEY = 4;
  private static final int DOUBLE_LITERAL_KEY = 8;
  private static final int INTEGER_LITERAL_KEY = 9;
  private static final int TYPED_LITERAL_KEY = 5;

  static Value readBinaryTerm(DataInputStream inputStream) throws IOException {
    int b = inputStream.readByte();
    switch (b) {
      case IRI_KEY:
        return new BasicIRI(inputStream.readUTF());
      case CONSTANT_IRI_KEY:
        return CONSTANTS[inputStream.readByte()];
      case NUMERIC_IRI_KEY:
        return new NumericIri(inputStream.readByte(), inputStream.readChar(), inputStream.readInt());
      case BNODE_KEY:
        return VALUE_FACTORY.createBNode(inputStream.readUTF());
      case STRING_LITERAL_KEY:
        return new StringLiteral(inputStream.readUTF());
      case LANG_STRING_LITERAL_KEY:
        return new LangStringLiteral(inputStream.readUTF(), inputStream.readUTF());
      case DOUBLE_LITERAL_KEY:
        return new DoubleLiteral(inputStream.readDouble());
      case INTEGER_LITERAL_KEY:
        return new IntegerLiteral(inputStream.readLong());
      case TYPED_LITERAL_KEY:
        return new TypedLiteral(inputStream.readUTF(), (IRI) readBinaryTerm(inputStream));
      default:
        throw new RuntimeException("Not expected type byte: " + b);
    }
  }

  static void writeBinaryTerm(Value term, DataOutputStream outputStream) throws IOException {
    if (term instanceof NumericIri) {
      NumericIri iri = (NumericIri) term;
      outputStream.writeByte(NUMERIC_IRI_KEY);
      outputStream.writeByte(iri.prefixId);
      outputStream.writeChar(iri.prefixChar);
      outputStream.writeInt(iri.id);
    } else if (term instanceof IRI) {
      Integer encoding = CONSTANTS_IDS_FOR_IRI.get(term);
      if (encoding == null) {
        outputStream.writeByte(IRI_KEY);
        outputStream.writeUTF(term.stringValue());
      } else {
        outputStream.writeByte(CONSTANT_IRI_KEY);
        outputStream.writeByte(encoding);
      }
    } else if (term instanceof BNode) {
      outputStream.writeByte(BNODE_KEY);
      outputStream.writeUTF(term.stringValue());
    } else if (term instanceof Literal) {
      Literal literal = (Literal) term;

      if (literal instanceof StringLiteral) {
        outputStream.writeByte(STRING_LITERAL_KEY);
        outputStream.writeUTF(literal.stringValue());
      } else if (literal instanceof LangStringLiteral) {
        outputStream.writeByte(LANG_STRING_LITERAL_KEY);
        outputStream.writeUTF(literal.stringValue());
        outputStream.writeUTF(literal.getLanguage().get());
      } else if (literal instanceof DoubleLiteral) {
        outputStream.writeByte(DOUBLE_LITERAL_KEY);
        outputStream.writeDouble(literal.doubleValue());
      } else if (literal instanceof IntegerLiteral) {
        outputStream.writeByte(INTEGER_LITERAL_KEY);
        outputStream.writeLong(literal.longValue());
      } else {
        outputStream.writeByte(TYPED_LITERAL_KEY);
        outputStream.writeUTF(literal.stringValue());
        writeBinaryTerm(literal.getDatatype(), outputStream);
      }
    } else {
      throw new RuntimeException("Unexpected term: " + term);
    }
  }

  private static final class BasicIRI implements IRI {
    private final String iri;

    private BasicIRI(String iri) {
      this.iri = iri;
    }

    @Override
    public String stringValue() {
      return iri;
    }

    @Override
    public String getNamespace() {
      return iri.substring(0, URIUtil.getLocalNameIndex(iri));
    }

    @Override
    public String getLocalName() {
      return iri.substring(URIUtil.getLocalNameIndex(iri));
    }

    @Override
    public String toString() {
      return iri;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      } else if (o instanceof IRI) {
        return iri.equals(o.toString());
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return iri.hashCode();
    }
  }

  private static final class NumericIri implements IRI {
    private final byte prefixId;
    private final char prefixChar;
    private final int id;

    NumericIri(byte prefixId, char prefixChar, int id) {
      this.prefixId = prefixId;
      this.prefixChar = prefixChar;
      this.id = id;
    }

    @Override
    public String stringValue() {
      return NUMERIC_NAMESPACES[prefixId] + prefixChar + id;
    }

    @Override
    public String getNamespace() {
      return NUMERIC_NAMESPACES[prefixId];
    }

    @Override
    public String getLocalName() {
      return prefixChar + Integer.toString(id);
    }

    @Override
    public String toString() {
      return stringValue();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o instanceof NumericIri) {
        NumericIri other = (NumericIri) o;
        return prefixId == other.prefixId && prefixChar == other.prefixChar && id == other.id;
      }
      if (o instanceof IRI) {
        return stringValue().equals(((IRI) o).stringValue());
      }
      return false;
    }

    @Override
    public int hashCode() {
      return Integer.hashCode(id);
    }
  }


  private static abstract class BasicLiteral implements Literal {
    @Override
    public String toString() {
      return getLabel();
    }

    @Override
    public String stringValue() {
      return getLabel();
    }

    @Override
    public boolean booleanValue() {
      return XMLDatatypeUtil.parseBoolean(getLabel());
    }

    @Override
    public byte byteValue() {
      return XMLDatatypeUtil.parseByte(getLabel());
    }

    @Override
    public short shortValue() {
      return XMLDatatypeUtil.parseShort(getLabel());
    }

    @Override
    public int intValue() {
      return XMLDatatypeUtil.parseInt(getLabel());
    }

    @Override
    public long longValue() {
      return XMLDatatypeUtil.parseLong(getLabel());
    }

    @Override
    public float floatValue() {
      return XMLDatatypeUtil.parseFloat(getLabel());
    }

    @Override
    public double doubleValue() {
      return XMLDatatypeUtil.parseDouble(getLabel());
    }

    @Override
    public BigInteger integerValue() {
      return XMLDatatypeUtil.parseInteger(getLabel());
    }

    @Override
    public BigDecimal decimalValue() {
      return XMLDatatypeUtil.parseDecimal(getLabel());
    }

    @Override
    public XMLGregorianCalendar calendarValue() {
      return XMLDatatypeUtil.parseCalendar(getLabel());
    }
  }

  private static final class DoubleLiteral extends BasicLiteral {
    private final double value;

    private DoubleLiteral(double value) {
      this.value = value;
    }

    @Override
    public String getLabel() {
      return Double.toString(value);
    }

    @Override
    public Optional<String> getLanguage() {
      return Optional.empty();
    }

    @Override
    public IRI getDatatype() {
      return XMLSchema.DOUBLE;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      } else if (o instanceof DoubleLiteral) {
        return value == ((DoubleLiteral) o).value;
      } else if (o instanceof Literal) {
        Literal l = (Literal) o;
        return l.getDatatype().equals(XMLSchema.DOUBLE) && l.getLabel().equals(getLabel());
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return Double.hashCode(value);
    }

    @Override
    public byte byteValue() {
      return (byte) value;
    }

    @Override
    public short shortValue() {
      return (short) value;
    }

    @Override
    public int intValue() {
      return (int) value;
    }

    @Override
    public long longValue() {
      return (long) value;
    }

    @Override
    public BigInteger integerValue() {
      return BigInteger.valueOf((long) value);
    }

    @Override
    public BigDecimal decimalValue() {
      return BigDecimal.valueOf(value);
    }

    @Override
    public float floatValue() {
      return (float) value;
    }

    @Override
    public double doubleValue() {
      return value;
    }
  }

  private static final class IntegerLiteral extends BasicLiteral {
    private final long value;

    private IntegerLiteral(long value) {
      this.value = value;
    }

    @Override
    public String getLabel() {
      return Long.toString(value);
    }

    @Override
    public Optional<String> getLanguage() {
      return Optional.empty();
    }

    @Override
    public IRI getDatatype() {
      return XMLSchema.INTEGER;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      } else if (o instanceof IntegerLiteral) {
        return value == ((IntegerLiteral) o).value;
      } else if (o instanceof Literal) {
        Literal l = (Literal) o;
        return l.getDatatype().equals(XMLSchema.INTEGER) && l.getLabel().equals(getLabel());
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return Long.hashCode(value);
    }

    @Override
    public byte byteValue() {
      return (byte) value;
    }

    @Override
    public short shortValue() {
      return (short) value;
    }

    @Override
    public int intValue() {
      return (int) value;
    }

    @Override
    public long longValue() {
      return value;
    }

    @Override
    public BigInteger integerValue() {
      return BigInteger.valueOf(value);
    }

    @Override
    public BigDecimal decimalValue() {
      return BigDecimal.valueOf(value);
    }

    @Override
    public float floatValue() {
      return value;
    }

    @Override
    public double doubleValue() {
      return value;
    }
  }

  private static final class DecimalLiteral extends BasicLiteral {
    private final BigDecimal value;

    private DecimalLiteral(BigDecimal value) {
      this.value = value;
    }

    @Override
    public String getLabel() {
      return value.toPlainString();
    }

    @Override
    public Optional<String> getLanguage() {
      return Optional.empty();
    }

    @Override
    public IRI getDatatype() {
      return XMLSchema.DECIMAL;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      } else if (o instanceof DecimalLiteral) {
        return value.equals(((DecimalLiteral) o).value);
      } else if (o instanceof Literal) {
        Literal l = (Literal) o;
        return l.getDatatype().equals(XMLSchema.DECIMAL) && l.getLabel().equals(getLabel());
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return value.hashCode();
    }

    @Override
    public byte byteValue() {
      return value.byteValue();
    }

    @Override
    public short shortValue() {
      return value.shortValue();
    }

    @Override
    public int intValue() {
      return value.intValue();
    }

    @Override
    public long longValue() {
      return value.longValue();
    }

    @Override
    public BigInteger integerValue() {
      return value.toBigInteger();
    }

    @Override
    public BigDecimal decimalValue() {
      return value;
    }

    @Override
    public float floatValue() {
      return value.floatValue();
    }

    @Override
    public double doubleValue() {
      return value.longValue();
    }
  }

  private static final class StringLiteral extends BasicLiteral {
    private final String label;

    private StringLiteral(String label) {
      this.label = label;
    }

    @Override
    public String getLabel() {
      return label;
    }

    @Override
    public Optional<String> getLanguage() {
      return Optional.empty();
    }

    @Override
    public IRI getDatatype() {
      return XMLSchema.STRING;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      } else if (o instanceof StringLiteral) {
        StringLiteral l = (StringLiteral) o;
        return l.label.equals(label);
      } else if (o instanceof Literal) {
        Literal l = (Literal) o;
        return l.getDatatype().equals(XMLSchema.STRING) && l.getLabel().equals(label);
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return label.hashCode();
    }
  }

  private static final class LangStringLiteral extends BasicLiteral {
    private final String label;
    private final String language;

    private LangStringLiteral(String label, String language) {
      this.label = label;
      this.language = language;
    }

    @Override
    public String getLabel() {
      return label;
    }

    @Override
    public Optional<String> getLanguage() {
      return Optional.of(language);
    }

    @Override
    public IRI getDatatype() {
      return RDF.LANGSTRING;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      } else if (o instanceof LangStringLiteral) {
        LangStringLiteral l = (LangStringLiteral) o;
        return l.label.equals(label) && l.language.equals(language);
      } else if (o instanceof Literal) {
        Literal l = (Literal) o;
        Optional<String> lang = l.getLanguage();
        return l.getLabel().equals(label) && lang.isPresent() && lang.get().equals(label);
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return label.hashCode();
    }
  }

  private static final class TypedLiteral extends BasicLiteral {
    private final String label;
    private final IRI datatype;

    private TypedLiteral(String label, IRI datatype) {
      this.label = label;
      this.datatype = datatype;
    }

    @Override
    public String getLabel() {
      return label;
    }

    @Override
    public Optional<String> getLanguage() {
      return Optional.empty();
    }

    @Override
    public IRI getDatatype() {
      return datatype;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      } else if (o instanceof Literal) {
        Literal l = (Literal) o;
        return l.getDatatype().equals(datatype) && l.getLabel().equals(label);
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return label.hashCode();
    }
  }
}
