package org.yago.yago4.converter.plan;

import org.eclipse.rdf4j.model.Statement;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class PlanNode<T> {

  public <T2> PlanNode<T> antiJoin(PlanNode<T2> right, Function<T, T2> leftKey) {
    return new AntiJoinNode<>(this, right, leftKey);
  }

  public PlanNode<T> cache() {
    return new CacheNode<>(this);
  }

  public static <T> PlanNode<T> empty() {
    return new CollectionNode<>(Collections.emptySet());
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

  /**
   * It is strongly recommended to use Function.identity() instead of t -> t because it unlocks some optimizations.
   */
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
    // We balance unions to avoid very deeply nested plans
    List<PlanNode<T>> children = Stream.concat(unionChildren(this), unionChildren(right)).collect(Collectors.toList());
    return union(children, 0, children.size());
  }

  private static <T> Stream<PlanNode<T>> unionChildren(PlanNode<T> node) {
    if (node instanceof UnionNode) {
      return Stream.concat(unionChildren(((UnionNode<T>) node).getLeftParent()), unionChildren(((UnionNode<T>) node).getRightParent()));
    } else {
      return Stream.of(node);
    }
  }

  private static <T> PlanNode<T> union(List<PlanNode<T>> nodes, int start, int end) {
    switch (end - start) {
      case 0:
        return PlanNode.empty();
      case 1:
        return nodes.get(start);
      default:
        int middle = (end + start) / 2;
        return new UnionNode<>(union(nodes, start, middle), union(nodes, middle, end));
    }
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
