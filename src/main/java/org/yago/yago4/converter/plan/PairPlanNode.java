package org.yago.yago4.converter.plan;

import org.yago.yago4.converter.utils.Pair;

import java.util.Collections;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class PairPlanNode<K, V> {

  public PairPlanNode<K, V> cache() {
    return new CachePairNode<>(this);
  }

  public static <K, V> PairPlanNode<K, V> empty() {
    return new UnionPairNode<>(Collections.emptyList());
  }

  public PairPlanNode<K, V> filter(BiPredicate<K, V> predicate) {
    return new FilterPairNode<>(this, predicate);
  }

  public PairPlanNode<K, V> filterKey(Predicate<K> predicate) {
    return new FilterPairNode<>(this, (k, v) -> predicate.test(k));
  }

  public PairPlanNode<K, V> filterValue(Predicate<V> predicate) {
    return new FilterPairNode<>(this, (k, v) -> predicate.test(v));
  }

  public <KO, VO> PairPlanNode<KO, VO> flatMapPair(BiFunction<K, V, Stream<Map.Entry<KO, VO>>> function) {
    return new FlatMapPairNode<>(this, function);
  }

  public <KO> PairPlanNode<KO, V> flatMapKey(Function<K, Stream<KO>> function) {
    return flatMapPair((k, v) -> function.apply(k).map(k2 -> new Pair<>(k2, v)));
  }

  public <VO> PairPlanNode<K, VO> flatMapValue(Function<V, Stream<VO>> function) {
    return flatMapPair((k, v) -> function.apply(v).map(v2 -> new Pair<>(k, v2)));
  }

  public <V2> PairPlanNode<K, Map.Entry<V, V2>> join(PairPlanNode<K, V2> right) {
    return new JoinPairNode<>(this, right);
  }

  public PlanNode<K> keys() {
    return new KeysNode<>(this);
  }

  public PairPlanNode<K, V> intersection(PlanNode<K> right) {
    return new IntersectionPairNode<>(this, right);
  }

  public <TO> PlanNode<TO> map(BiFunction<K, V, TO> function) {
    return new MapFromPairNode<>(this, function);
  }

  public <KO, VO> PairPlanNode<KO, VO> mapPair(BiFunction<K, V, Map.Entry<KO, VO>> function) {
    return new MapPairNode<>(this, function);
  }

  public <KO> PairPlanNode<KO, V> mapKey(Function<K, KO> function) {
    return mapPair((k, v) -> new Pair<>(function.apply(k), v));
  }

  public <VO> PairPlanNode<K, VO> mapValue(Function<V, VO> function) {
    return mapPair((k, v) -> new Pair<>(k, function.apply(v)));
  }

  public PairPlanNode<K, V> subtract(PlanNode<K> right) {
    return new SubtractPairNode<>(this, right);
  }


  public PairPlanNode<V, K> swap() {
    return mapPair((k, v) -> new Pair<>(v, k));
  }

  public PairPlanNode<K, V> transitiveClosure(PairPlanNode<V, V> right) {
    return new TransitiveClosurePairNode<>(this, right);
  }

  public PairPlanNode<K, V> union(PairPlanNode<K, V> right) {
    return new UnionPairNode<>(Stream.concat(unionChildren(this), unionChildren(right)).collect(Collectors.toList()));
  }

  private static <K, V> Stream<PairPlanNode<K, V>> unionChildren(PairPlanNode<K, V> node) {
    if (node instanceof UnionPairNode) {
      return ((UnionPairNode<K, V>) node).getParents().stream().flatMap(PairPlanNode::unionChildren);
    } else {
      return Stream.of(node);
    }
  }

  public PlanNode<V> values() {
    return new ValuesNode<>(this);
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
