package org.yago.yago4.converter.plan;

import org.eclipse.rdf4j.model.Statement;

import java.nio.file.Path;
import java.util.Collection;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

public abstract class PlanNode<T> {

  public <T2, TO> PlanNode<T> antiJoin(PlanNode<T2> right, Function<T, T2> leftKey) {
    return new AntiJoinNode<>(this, right, leftKey);
  }

  public PlanNode<T> filter(Predicate<T> predicate) {
    return new FilterNode<>(this, predicate);
  }

  public <TO> PlanNode<TO> flatMap(Function<T, Stream<TO>> function) {
    return new FlatMapNode<>(this, function);
  }

  public static <T> PlanNode<T> fromCollection(Collection<T> elements) {
    return new CollectionNode<>(elements);
  }

  public <T2, TO, K> PlanNode<TO> join(PlanNode<T2> right, Function<T, K> leftKey, Function<T2, K> rightKey, BiFunction<T, T2, TO> mergeFunction) {
    return new JoinNode<>(this, right, leftKey, rightKey, mergeFunction);
  }

  public <TO> PlanNode<TO> map(Function<T, TO> function) {
    return new MapNode<>(this, function);
  }

  public static PlanNode<Statement> readBinaryRDF(Path filePath) {
    return new RDFBinaryReaderNode(filePath);
  }

  public static PlanNode<Statement> readNTriples(Path filePath) {
    return new NTriplesReaderNode(filePath);
  }

  public <T2, K> PlanNode<T> transitiveClosure(PlanNode<T2> right, Function<T, K> leftKey, Function<T2, K> rightKey, BiFunction<T, T2, T> mergeFunction) {
    return new TransitiveClosureNode<>(this, right, leftKey, rightKey, mergeFunction);
  }

  public PlanNode<T> union(PlanNode<T> right) {
    return new UnionNode<>(this, right);
  }

  @Override
  public boolean equals(Object obj) {
    throw new UnsupportedOperationException("PlanNode.equals should be implemented");
  }

  @Override
  public int hashCode() {
    throw new UnsupportedOperationException("PlanNode.hashCode should be implemented");
  }
}
