package org.yago.yago4.converter.utils;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.rio.ntriples.NTriplesUtil;
import org.yago.yago4.converter.EvaluationException;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

public class NTriplesWriter {

  public void write(Stream<Statement> stream, Path filePath) {
    try (BufferedWriter writer = Files.newBufferedWriter(filePath)) {
      stream.sequential().forEach(tuple -> {
        try {
          writer.append(NTriplesUtil.toNTriplesString(tuple.getSubject())).append(' ')
                  .append(NTriplesUtil.toNTriplesString(tuple.getPredicate())).append(' ')
                  .append(NTriplesUtil.toNTriplesString(tuple.getObject()));
          Resource context = tuple.getContext();
          if (context != null) {
            writer.append(' ').append(NTriplesUtil.toNTriplesString(context));
          }
          writer.append(" .\n");
        } catch (IOException e) {
          throw new EvaluationException(e);
        }
      });
    } catch (IOException e) {
      throw new EvaluationException(e);
    }
  }

  public String toString(Statement statement) {
    Resource s = statement.getSubject();
    IRI p = statement.getPredicate();
    Value o = statement.getObject();
    Resource c = statement.getContext();
    if (c == null) {
      return NTriplesUtil.toNTriplesString(s) + ' ' + NTriplesUtil.toNTriplesString(p) + ' ' + NTriplesUtil.toNTriplesString(o) + " .";
    } else {
      return NTriplesUtil.toNTriplesString(s) + ' ' + NTriplesUtil.toNTriplesString(p) + ' ' + NTriplesUtil.toNTriplesString(o) + ' ' + NTriplesUtil.toNTriplesString(c) + " .";
    }
  }
}
