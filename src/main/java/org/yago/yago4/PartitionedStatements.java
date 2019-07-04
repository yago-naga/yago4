package org.yago.yago4;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import org.eclipse.rdf4j.model.Statement;
import org.yago.yago4.converter.EvaluationException;
import org.yago.yago4.converter.plan.PlanNode;
import org.yago.yago4.converter.utils.RDFBinaryFormat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
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
    private final LoadingCache<String, RDFBinaryFormat.Writer> writers = CacheBuilder.newBuilder()
            .maximumSize(1000)
            .refreshAfterWrite(15, TimeUnit.MINUTES)
            .removalListener((RemovalListener<String, RDFBinaryFormat.Writer>) removal -> removal.getValue().close())
            .build(new CacheLoader<>() {
              @Override
              public RDFBinaryFormat.Writer load(String key) {
                try {
                  Path path = dir.resolve(key);
                  Files.createDirectories(path.getParent());
                  return new RDFBinaryFormat.Writer(path, true);
                } catch (IOException e) {
                  throw new EvaluationException(e);
                }
              }
            });

    private Writer(Path dir, Function<Statement, String> computeKey) {
      this.dir = dir;
      this.computeKey = computeKey;
    }

    public void write(Statement statement) {
      try {
        String key = computeKey.apply(statement);
        writers.get(key).write(statement);
      } catch (IOException | ExecutionException e) {
        System.err.println(e.getLocalizedMessage());
      }
    }

    @Override
    public void close() {
      writers.invalidateAll();
    }
  }
}
