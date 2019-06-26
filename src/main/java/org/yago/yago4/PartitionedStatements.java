package org.yago.yago4;

import org.eclipse.rdf4j.model.Statement;
import org.yago.yago4.converter.EvaluationException;
import org.yago.yago4.converter.plan.PlanNode;
import org.yago.yago4.converter.utils.RDFBinaryFormat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

final class PartitionedStatements {
  private final Path dir;

  PartitionedStatements(Path dir) {
    this.dir = dir;
  }

  public Writer getWriter(Function<Statement, String> computeKey) {
    return new Writer(dir, computeKey);
  }

  public PlanNode<Statement> getForKey(String key) {
    Path file = dir.resolve(key);
    if (Files.exists(file)) {
      return PlanNode.readBinaryRDF(dir.resolve(key));
    } else {
      System.out.println("No file found for key " + key);
      return PlanNode.empty();
    }
  }

  public static final class Writer implements AutoCloseable {
    private final Path dir;
    private final Function<Statement, String> computeKey;
    private final Map<String, RDFBinaryFormat.Writer> writers = new ConcurrentHashMap<>();

    Writer(Path dir, Function<Statement, String> computeKey) {
      this.dir = dir;
      this.computeKey = computeKey;
    }

    public void write(Statement statement) {
      try {
        String key = computeKey.apply(statement);
        writers.computeIfAbsent(key, p -> {
          try {
            return new RDFBinaryFormat.Writer(dir.resolve(key));
          } catch (IOException e) {
            throw new EvaluationException(e);
          }
        }).write(statement);
      } catch (IOException e) {
        throw new EvaluationException(e);
      }
    }

    @Override
    public void close() {
      writers.values().forEach(RDFBinaryFormat.Writer::close);
      writers.clear();
    }
  }
}
