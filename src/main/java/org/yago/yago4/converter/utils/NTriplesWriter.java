package org.yago.yago4.converter.utils;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.commons.io.FilenameUtils;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.rio.ntriples.NTriplesUtil;
import org.yago.yago4.converter.EvaluationException;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import java.util.zip.GZIPOutputStream;

public class NTriplesWriter {

  public void write(Stream<Statement> stream, Path filePath) {
    try (BufferedWriter writer = openWriter(filePath)) {
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

  private BufferedWriter openWriter(Path filePath) throws IOException {
    String extension = FilenameUtils.getExtension(filePath.toString());
    OutputStream OutputStream = new BufferedOutputStream(Files.newOutputStream(filePath), 16777216);
    if (extension.equals("gz")) {
      OutputStream = new GZIPOutputStream(OutputStream);
    } else if (extension.equals("bz2")) {
      OutputStream = new BZip2CompressorOutputStream(OutputStream);
    }
    return new BufferedWriter(new OutputStreamWriter(OutputStream));
  }
}
