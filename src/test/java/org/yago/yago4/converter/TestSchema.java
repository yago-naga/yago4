package org.yago.yago4.converter;

import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.vocabulary.RDF4J;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.sail.inferencer.fc.SchemaCachingRDFSInferencer;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.eclipse.rdf4j.sail.shacl.ShaclSail;
import org.eclipse.rdf4j.sail.shacl.ShaclSailValidationException;
import org.eclipse.rdf4j.sail.shacl.results.ValidationReport;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.StringWriter;
import java.net.URL;

import static org.junit.jupiter.api.Assertions.assertTrue;

class TestSchema {

  @Test
  void testShapeValidation() throws IOException {
    Class context = getClass();
    doValidation(
            context.getResource("/shapes_shape.ttl"),
            new URL("https://www.w3.org/ns/shacl.ttl"),
            new URL("https://schema.org/version/latest/schema.nt"),
            context.getResource("/shapes.ttl")
    );
  }

  void doValidation(URL shapeGraph, URL... dataGraphs) throws IOException {
    ShaclSail shaclSail = new ShaclSail(new SchemaCachingRDFSInferencer(new MemoryStore()));
    SailRepository sailRepository = new SailRepository(shaclSail);
    sailRepository.init();
    try (SailRepositoryConnection connection = sailRepository.getConnection()) {
      // We load the shape
      connection.begin();
      connection.add(shapeGraph, "", RDFFormat.TURTLE, RDF4J.SHACL_SHAPE_GRAPH);
      connection.commit();
    }

    try (SailRepositoryConnection connection = sailRepository.getConnection()) {
      // We load the data
      for (URL dataGraph : dataGraphs) {
        connection.begin();
        connection.add(dataGraph, "", RDFFormat.TURTLE, connection.getValueFactory().createIRI("http://example.com"));
        connection.commit();
      }
    } catch (RepositoryException exception) {
      Throwable cause = exception.getCause();
      if (cause instanceof ShaclSailValidationException) {
        ValidationReport validationReport = ((ShaclSailValidationException) cause).getValidationReport();
        assertTrue(validationReport.conforms(), () -> {
          Model validationReportModel = ((ShaclSailValidationException) cause).validationReportAsModel();
          StringWriter writer = new StringWriter();
          Rio.write(validationReportModel, writer, RDFFormat.TURTLE);
          return writer.toString();
        });
      } else {
        throw exception;
      }
    }
  }
}
