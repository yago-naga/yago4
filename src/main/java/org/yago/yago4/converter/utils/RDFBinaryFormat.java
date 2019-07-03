package org.yago.yago4.converter.utils;

import org.eclipse.rdf4j.model.*;
import org.yago.yago4.converter.EvaluationException;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class RDFBinaryFormat implements Serializable {

  private static final int IRI_KEY = 1;
  private static final int BNODE_KEY = 2;
  private static final int STRING_LITERAL_KEY = 3;
  private static final int LANG_STRING_LITERAL_KEY = 4;
  private static final int TYPED_LITERAL_KEY = 5;

  public static Stream<Statement> read(ValueFactory valueFactory, Path filePath) {
    return StreamSupport.stream(() -> new BinaryReaderIterator(valueFactory, filePath), 0, true);
  }

  public static void write(Stream<Statement> stream, Path filePath) {
    try (Writer writer = new Writer(filePath, false)) {
      stream.forEach(Statement -> {
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
    private final ValueFactory valueFactory;
    private final DataInputStream inputStream;

    BinaryReaderIterator(ValueFactory valueFactory, Path filePath) {
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
                (Resource) readTerm(),
                (IRI) readTerm(),
                readTerm()
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
                  (Resource) readTerm(),
                  (IRI) readTerm(),
                  readTerm()
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

    private Value readTerm() throws IOException {
      int b = inputStream.readByte();
      switch (b) {
        case IRI_KEY:
          return valueFactory.createIRI(inputStream.readUTF());
        case BNODE_KEY:
          return valueFactory.createBNode(inputStream.readUTF());
        case STRING_LITERAL_KEY:
          return valueFactory.createLiteral(inputStream.readUTF());
        case LANG_STRING_LITERAL_KEY:
          return valueFactory.createLiteral(inputStream.readUTF(), inputStream.readUTF());
        case TYPED_LITERAL_KEY:
          return valueFactory.createLiteral(inputStream.readUTF(), valueFactory.createIRI(inputStream.readUTF()));
        default:
          throw new EvaluationException("Not expected type byte: " + b);
      }
    }

    @Override
    public Spliterator<Statement> trySplit() {
      return null;
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

    public synchronized void write(Statement Statement) throws IOException {
      writeTerm(Statement.getSubject());
      writeTerm(Statement.getPredicate());
      writeTerm(Statement.getObject());
    }

    private void writeTerm(Value term) throws IOException {
      if (term instanceof IRI) {
        outputStream.writeByte(IRI_KEY);
        outputStream.writeUTF(term.stringValue());
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
            outputStream.writeUTF(literal.getDatatype().stringValue());
            break;
        }
      } else {
        throw new EvaluationException("Unexpected term: " + term);
      }
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
