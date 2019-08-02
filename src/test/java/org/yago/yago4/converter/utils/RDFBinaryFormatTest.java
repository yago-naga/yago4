package org.yago.yago4.converter.utils;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.Year;
import java.time.YearMonth;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RDFBinaryFormatTest {

  @Test
  void readAndWrite() throws IOException {
    YagoValueFactory valueFactory = YagoValueFactory.getInstance();
    List<Statement> Statements = List.of(
            valueFactory.createStatement(valueFactory.createIRI("http://foo"), valueFactory.createIRI("http://schema.org/url"), valueFactory.createIRI("http://bar")),
            valueFactory.createStatement(valueFactory.createIRI("http://foo"), valueFactory.createIRI("http://schema.org/url"), valueFactory.createIRI("http://www.wikidata.org/entity/Q12")),
            valueFactory.createStatement(valueFactory.createIRI("http://foo"), valueFactory.createIRI("http://schema.org/url"), valueFactory.createIRI("http://www.wikidata.org/value/ffffff")),
            valueFactory.createStatement(valueFactory.createIRI("http://foo"), valueFactory.createIRI("http://schema.org/parent"), valueFactory.createBNode()),
            valueFactory.createStatement(valueFactory.createIRI("http://foo"), valueFactory.createIRI("http://schema.org/name"), valueFactory.createLiteral("foo")),
            valueFactory.createStatement(valueFactory.createIRI("http://foo"), valueFactory.createIRI("http://schema.org/name"), valueFactory.createLiteral("foo", "en")),
            valueFactory.createStatement(valueFactory.createIRI("http://foo"), valueFactory.createIRI("http://schema.org/height"), valueFactory.createLiteral("1", valueFactory.createIRI("xsd:integer"))),
            valueFactory.createStatement(valueFactory.createIRI("http://foo"), valueFactory.createIRI("http://schema.org/height"), valueFactory.createLiteral(BigInteger.valueOf(1))),
            valueFactory.createStatement(valueFactory.createIRI("http://foo"), valueFactory.createIRI("http://schema.org/height"), valueFactory.createLiteral(BigDecimal.valueOf(1.33))),
            valueFactory.createStatement(valueFactory.createIRI("http://foo"), valueFactory.createIRI("http://schema.org/latitude"), valueFactory.createLiteral(12.0d))
    );

    Path file = Files.createTempFile("test", "binary");
    RDFBinaryFormat.write(Statements.stream(), file);
    assertEquals(Statements, RDFBinaryFormat.read(file).collect(Collectors.toList()));
  }

  @Test
  void numericEncodedIriNamespace() {
    YagoValueFactory valueFactory = YagoValueFactory.getInstance();
    IRI iri = valueFactory.createIRI("http://www.wikidata.org/entity/Q42");
    assertEquals("http://www.wikidata.org/entity/", iri.getNamespace());
    assertEquals("Q42", iri.getLocalName());
  }

  @Test
  void someEqualities() {
    YagoValueFactory valueFactory = YagoValueFactory.getInstance();
    assertEquals(valueFactory.createLiteral("foo"), valueFactory.createLiteral("foo", XMLSchema.STRING));
    assertEquals(valueFactory.createLiteral(1.0), valueFactory.createLiteral("1.0", XMLSchema.DOUBLE));
    assertEquals(valueFactory.createLiteral(1.0f), valueFactory.createLiteral("1.0", XMLSchema.DOUBLE));
    assertEquals(valueFactory.createLiteral(BigInteger.valueOf(1)), valueFactory.createLiteral("1", XMLSchema.INTEGER));
    assertEquals(valueFactory.createLiteral(BigDecimal.valueOf(100)), valueFactory.createLiteral("100", XMLSchema.DECIMAL));
    assertEquals(valueFactory.createLiteral(1), valueFactory.createLiteral("1", XMLSchema.INTEGER));
  }

  @Test
  void testJavaTimeConversion() {
    YagoValueFactory valueFactory = YagoValueFactory.getInstance();
    assertEquals(valueFactory.createLiteral("999999999", XMLSchema.GYEAR), valueFactory.createLiteral(Year.of(Year.MAX_VALUE)));
    assertEquals(valueFactory.createLiteral("-999999999", XMLSchema.GYEAR), valueFactory.createLiteral(Year.of(Year.MIN_VALUE)));
    assertEquals(valueFactory.createLiteral("2019", XMLSchema.GYEAR), valueFactory.createLiteral(Year.of(2019)));
    assertEquals(valueFactory.createLiteral("0019", XMLSchema.GYEAR), valueFactory.createLiteral(Year.of(19)));
    assertEquals(valueFactory.createLiteral("999999999-02", XMLSchema.GYEARMONTH), valueFactory.createLiteral(YearMonth.of(Year.MAX_VALUE, 2)));
    assertEquals(valueFactory.createLiteral("-999999999-02", XMLSchema.GYEARMONTH), valueFactory.createLiteral(YearMonth.of(Year.MIN_VALUE, 2)));
    assertEquals(valueFactory.createLiteral("2019-08", XMLSchema.GYEARMONTH), valueFactory.createLiteral(YearMonth.of(2019, 8)));
    assertEquals(valueFactory.createLiteral("0019-08", XMLSchema.GYEARMONTH), valueFactory.createLiteral(YearMonth.of(19, 8)));
    assertEquals(valueFactory.createLiteral("20000-02-28", XMLSchema.DATE), valueFactory.createLiteral(LocalDate.of(20000, 2, 28)));
    assertEquals(valueFactory.createLiteral("0020-02-28", XMLSchema.DATE), valueFactory.createLiteral(LocalDate.of(20, 2, 28)));
    assertEquals(valueFactory.createLiteral("999999999-12-31", XMLSchema.DATE), valueFactory.createLiteral(LocalDate.MAX));
    assertEquals(valueFactory.createLiteral("-999999999-01-01", XMLSchema.DATE), valueFactory.createLiteral(LocalDate.MIN));
    assertEquals(valueFactory.createLiteral("-20000-02-28", XMLSchema.DATE), valueFactory.createLiteral(LocalDate.of(-20000, 2, 28)));
    assertEquals(valueFactory.createLiteral("999999999-12-31T23:59:59-18:00", XMLSchema.DATETIME), valueFactory.createLiteral(OffsetDateTime.MAX));
    assertEquals(valueFactory.createLiteral("-999999999-01-01T00:00:00+18:00", XMLSchema.DATETIME), valueFactory.createLiteral(OffsetDateTime.MIN));
  }
}
