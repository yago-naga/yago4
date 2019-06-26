package org.yago.yago4.converter.utils;

import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
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
      stream.forEachOrdered(tuple -> {
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
}
