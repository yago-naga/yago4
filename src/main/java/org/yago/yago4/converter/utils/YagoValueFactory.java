package org.yago.yago4.converter.utils;

import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.impl.AbstractValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.model.vocabulary.SKOS;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.yago.yago4.converter.EvaluationException;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class YagoValueFactory extends AbstractValueFactory {
  private static final String[] NUMERIC_PREFIXES = new String[]{
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

    assert NUMERIC_PREFIXES.length < Byte.MAX_VALUE;
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
    IRI constant = CONSTANTS_IRIS_FOR_STRING.get(iri);
    if (constant != null) {
      return constant;
    }

    for (int i = 0; i < NUMERIC_PREFIXES.length; i++) {
      if (iri.startsWith(NUMERIC_PREFIXES[i])) {
        int prefixLength = NUMERIC_PREFIXES[i].length();
        if (iri.length() > prefixLength) {
          try {
            return new NumericIri(i, iri.charAt(prefixLength), Integer.parseInt(iri.substring(prefixLength + 1)));
          } catch (NumberFormatException e) {
            return super.createIRI(iri);
          }
        }
      }
    }

    return super.createIRI(iri);
  }

  private static final int IRI_KEY = 1;
  private static final int BNODE_KEY = 2;
  private static final int STRING_LITERAL_KEY = 3;
  private static final int LANG_STRING_LITERAL_KEY = 4;
  private static final int TYPED_LITERAL_KEY = 5;
  private static final int CONSTANT_IRI_KEY = 6;
  private static final int NUMERIC_IRI_KEY = 7;

  Value readBinaryTerm(DataInputStream inputStream) throws IOException {
    int b = inputStream.readByte();
    switch (b) {
      case IRI_KEY:
        return super.createIRI(inputStream.readUTF());
      case BNODE_KEY:
        return super.createBNode(inputStream.readUTF());
      case STRING_LITERAL_KEY:
        return super.createLiteral(inputStream.readUTF());
      case LANG_STRING_LITERAL_KEY:
        return super.createLiteral(inputStream.readUTF(), inputStream.readUTF());
      case TYPED_LITERAL_KEY:
        return super.createLiteral(inputStream.readUTF(), (IRI) readBinaryTerm(inputStream));
      case CONSTANT_IRI_KEY:
        return CONSTANTS[inputStream.readByte()];
      case NUMERIC_IRI_KEY:
        return new NumericIri(inputStream.readByte(), inputStream.readChar(), inputStream.readInt());
      default:
        throw new EvaluationException("Not expected type byte: " + b);
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
      switch (literal.getDatatype().stringValue()) {
        case "http://www.w3.org/2001/XMLSchema#string":
          outputStream.writeByte(STRING_LITERAL_KEY);
          outputStream.writeUTF(literal.stringValue());
          break;
        case "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString":
          outputStream.writeByte(LANG_STRING_LITERAL_KEY);
          outputStream.writeUTF(literal.stringValue());
          outputStream.writeUTF(literal.getLanguage().get());
          break;
        default:
          outputStream.writeByte(TYPED_LITERAL_KEY);
          outputStream.writeUTF(literal.stringValue());
          writeBinaryTerm(literal.getDatatype(), outputStream);
          break;
      }
    } else {
      throw new EvaluationException("Unexpected term: " + term);
    }
  }

  private static final class NumericIri implements IRI {
    private final int prefixId;
    private final char prefixChar;
    private final int id;

    NumericIri(int prefixId, char prefixChar, int id) {
      this.prefixId = prefixId;
      this.prefixChar = prefixChar;
      this.id = id;
    }

    @Override
    public String toString() {
      return stringValue();
    }

    @Override
    public String stringValue() {
      return NUMERIC_PREFIXES[prefixId] + prefixChar + id;
    }

    @Override
    public String getNamespace() {
      return NUMERIC_PREFIXES[prefixId];
    }

    @Override
    public String getLocalName() {
      return prefixChar + Integer.toString(id);
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
}
