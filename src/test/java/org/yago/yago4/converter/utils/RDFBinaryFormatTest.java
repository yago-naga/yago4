package org.yago.yago4.converter.utils;

import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RDFBinaryFormatTest {

  @Test
  void readAndWrite() throws IOException {
    ValueFactory valueFactory = SimpleValueFactory.getInstance();
    List<Statement> Statements = Arrays.asList(
            valueFactory.createStatement(valueFactory.createIRI("http://foo"), valueFactory.createIRI("http://schema.org/url"), valueFactory.createIRI("http://bar")),
            valueFactory.createStatement(valueFactory.createIRI("http://foo"), valueFactory.createIRI("http://schema.org/parent"), valueFactory.createBNode()),
            valueFactory.createStatement(valueFactory.createIRI("http://foo"), valueFactory.createIRI("http://schema.org/name"), valueFactory.createLiteral("foo")),
            valueFactory.createStatement(valueFactory.createIRI("http://foo"), valueFactory.createIRI("http://schema.org/name"), valueFactory.createLiteral("foo", "en")),
            valueFactory.createStatement(valueFactory.createIRI("http://foo"), valueFactory.createIRI("http://schema.org/height"), valueFactory.createLiteral("1", valueFactory.createIRI("xsd:integer")))
    );

    Path file = Files.createTempFile("test", "binary");
    RDFBinaryFormat.write(Statements.stream(), file);
    assertEquals(Statements, RDFBinaryFormat.read(valueFactory, file).collect(Collectors.toList()));
  }
}
