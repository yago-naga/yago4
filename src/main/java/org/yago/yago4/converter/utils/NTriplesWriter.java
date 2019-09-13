package org.yago.yago4.converter.utils;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.commons.io.FilenameUtils;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.yago.yago4.AnnotatedStatement;
import org.yago.yago4.converter.EvaluationException;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.stream.Stream;
import java.util.zip.GZIPOutputStream;

public class NTriplesWriter {

  public void write(Stream<Statement> stream, Path filePath) {
    try (BufferedWriter writer = openWriter(filePath)) {
      stream.sequential().forEach(tuple -> {
        try {
          write(tuple.getSubject(), writer);
          writer.append(' ');
          write(tuple.getPredicate(), writer);
          writer.append(' ');
          write(tuple.getObject(), writer);
          writer.append(' ');
          Resource context = tuple.getContext();
          if (context != null) {
            write(context, writer);
            writer.append(' ');
          }
          writer.append(".\n");
        } catch (IOException e) {
          throw new EvaluationException(e);
        }
      });
    } catch (IOException e) {
      throw new EvaluationException(e);
    }
  }

  public void writeRdfStar(Stream<AnnotatedStatement> stream, Path filePath) {
    try (BufferedWriter writer = openWriter(filePath)) {
      stream.sequential().forEach(tuple -> {
        try {
          writer.append("<<");
          write(tuple.getSubject().getSubject(), writer);
          writer.append(' ');
          write(tuple.getSubject().getPredicate(), writer);
          writer.append(' ');
          write(tuple.getSubject().getObject(), writer);
          writer.append(">> ");
          write(tuple.getPredicate(), writer);
          writer.append(' ');
          write(tuple.getObject(), writer);
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

  private void write(IRI value, Appendable writer) throws IOException {
    writer.append('<').append(value.stringValue()).append('>');
  }

  private void write(BNode value, Appendable writer) throws IOException {
    String id = value.getID();
    if (id.isEmpty()) {
      writer.append("_:genid").append(Integer.toHexString(value.hashCode()));
    } else {
      writer.append("_:").append(id);
    }
  }

  private void write(Literal value, Appendable writer) throws IOException {
    String val = value.stringValue();
    Optional<String> language = value.getLanguage();
    IRI datatype = value.getDatatype();

    writer.append('"');
    for (int i = 0; i < val.length(); i++) {
      char c = val.charAt(i);
      if (c == '\\') {
        writer.append("\\\\");
      } else if (c == '"') {
        writer.append("\\\"");
      } else if (c == '\n') {
        writer.append("\\n");
      } else if (c == '\r') {
        writer.append("\\r");
      } else {
        writer.append(c);
      }
    }
    writer.append('"');

    if (language.isPresent()) {
      writer.append('@').append(language.get());
    } else if (!XMLSchema.STRING.equals(datatype)) {
      writer.append("^^");
      write(datatype, writer);
    }
  }

  private void write(Value value, Appendable writer) throws IOException {
    if (value instanceof IRI) {
      write((IRI) value, writer);
    } else if (value instanceof BNode) {
      write((BNode) value, writer);
    } else if (value instanceof Literal) {
      write((Literal) value, writer);
    } else {
      throw new IllegalArgumentException(value.toString());
    }
  }
}
