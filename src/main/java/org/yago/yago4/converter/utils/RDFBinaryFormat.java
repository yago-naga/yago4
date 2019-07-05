package org.yago.yago4.converter.utils;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.yago.yago4.converter.EvaluationException;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class RDFBinaryFormat implements Serializable {

  public static Stream<Statement> read(YagoValueFactory valueFactory, Path filePath) {
    return StreamSupport.stream(() -> new BinaryReaderIterator(valueFactory, filePath), 0, true);
  }

  public static void write(Stream<Statement> stream, Path filePath) {
    try (Writer writer = new Writer(filePath, false)) {
      stream.sequential().forEach(Statement -> {
        try {
          writer.write(Statement);
        } catch (IOException e) {
          throw new EvaluationException(e);
        }
      });
    } catch (IOException e) {
      throw new EvaluationException(e);
    }
  }

  private static class BinaryReaderIterator implements Spliterator<Statement>, AutoCloseable {
    private static final int BATCH_SIZE = 4096;

    private final YagoValueFactory valueFactory;
    private final DataInputStream inputStream;

    BinaryReaderIterator(YagoValueFactory valueFactory, Path filePath) {
      try {
        this.valueFactory = valueFactory;
        inputStream = new DataInputStream(new BufferedInputStream(Files.newInputStream(filePath)));
      } catch (IOException e) {
        throw new EvaluationException(e);
      }
    }

    @Override
    public boolean tryAdvance(Consumer<? super Statement> consumer) {
      try {
        consumer.accept(valueFactory.createStatement(
                (Resource) valueFactory.readBinaryTerm(inputStream),
                (IRI) valueFactory.readBinaryTerm(inputStream),
                valueFactory.readBinaryTerm(inputStream)
        ));
        return true;
      } catch (EOFException e) {
        close();
        return false;
      } catch (IOException e) {
        throw new EvaluationException(e);
      }
    }

    @Override
    public void forEachRemaining(Consumer<? super Statement> action) {
      try {
        while (true) {
          action.accept(valueFactory.createStatement(
                  (Resource) valueFactory.readBinaryTerm(inputStream),
                  (IRI) valueFactory.readBinaryTerm(inputStream),
                  valueFactory.readBinaryTerm(inputStream)
          ));
        }
      } catch (EOFException e) {
        // End of file
      } catch (IOException e) {
        throw new EvaluationException(e);
      } finally {
        close();
      }
    }

    @Override
    public Spliterator<Statement> trySplit() {
      Statement[] batch = new Statement[BATCH_SIZE];
      int i = 0;
      for (; i < batch.length; i++) {
        try {
          batch[i] = valueFactory.createStatement(
                  (Resource) valueFactory.readBinaryTerm(inputStream),
                  (IRI) valueFactory.readBinaryTerm(inputStream),
                  valueFactory.readBinaryTerm(inputStream)
          );
        } catch (EOFException e) {
          break;
        } catch (IOException e) {
          throw new EvaluationException(e);
        }
      }

      if (i > 0) {
        return Spliterators.spliterator(batch, 0, i, characteristics());
      } else {
        return null;
      }
    }

    @Override
    public long estimateSize() {
      return Long.MAX_VALUE;
    }

    @Override
    public int characteristics() {
      return 0;
    }

    @Override
    public void close() {
      try {
        inputStream.close();
      } catch (IOException e) {
        throw new EvaluationException(e);
      }
    }
  }

  public static final class Writer implements AutoCloseable {
    private final DataOutputStream outputStream;

    public Writer(Path filePath, boolean append) throws IOException {
      outputStream = new DataOutputStream(new BufferedOutputStream(
              append ? Files.newOutputStream(filePath, StandardOpenOption.APPEND, StandardOpenOption.CREATE) : Files.newOutputStream(filePath)
      ));
    }

    /**
     * Warning : not thread safe !!!
     */
    public void write(Statement statement) throws IOException {
      YagoValueFactory.writeBinaryTerm(statement.getSubject(), outputStream);
      YagoValueFactory.writeBinaryTerm(statement.getPredicate(), outputStream);
      YagoValueFactory.writeBinaryTerm(statement.getObject(), outputStream);
    }

    @Override
    public void close() {
      try {
        outputStream.close();
      } catch (IOException e) {
        throw new EvaluationException(e);
      }
    }
  }
}
