package org.yago.yago4.converter.plan;

import org.eclipse.rdf4j.model.Statement;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class PlanNode<T> {

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

  public static <T> PlanNode<T> fromStream(Stream<T> elements) {
    return fromCollection(elements.collect(Collectors.toList()));
  }

  public PlanNode<T> intersection(PlanNode<T> right) {
    return new IntersectionNode<>(this, right);
  }

  public <V> PairPlanNode<T, V> join(PairPlanNode<T, V> right) {
    return new JoinNode<>(this, right);
  }

  public <TO> PlanNode<TO> map(Function<T, TO> function) {
    return new MapNode<>(this, function);
  }

  public <KO, VO> PairPlanNode<KO, VO> mapToPair(Function<T, Map.Entry<KO, VO>> function) {
    return new MapToPairNode<>(this, function);
  }

  public static PlanNode<Statement> readBinaryRDF(Path filePath) {
    return new RDFBinaryReaderNode(filePath);
  }

  public static PlanNode<Statement> readNTriples(Path filePath) {
    return new NTriplesReaderNode(filePath);
  }

  public PlanNode<T> subtract(PlanNode<T> right) {
    return new SubtractNode<>(this, right);
  }

  public PlanNode<T> transitiveClosure(PairPlanNode<T, T> right) {
    return new TransitiveClosureNode<>(this, right);
  }

  public PlanNode<T> union(PlanNode<T> right) {
    return new UnionNode<>(Stream.concat(unionChildren(this), unionChildren(right)).collect(Collectors.toList()));
  }

  private static <T> Stream<PlanNode<T>> unionChildren(PlanNode<T> node) {
    if (node instanceof UnionNode) {
      return ((UnionNode<T>) node).getParents().stream().flatMap(PlanNode::unionChildren);
    } else {
      return Stream.of(node);
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
