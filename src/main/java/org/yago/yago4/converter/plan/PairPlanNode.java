package org.yago.yago4.converter.plan;

import com.google.common.collect.Maps;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class PairPlanNode<K, V> {

  public PairPlanNode<K, V> cache() {
    return new CachePairNode<>(this);
  }

  public static <K, V> PairPlanNode<K, V> empty() {
    return new UnionPairNode<>(Collections.emptyList());
  }

  public PairPlanNode<K, V> filter(SerializableBiPredicate<K, V> predicate) {
    return new FilterPairNode<>(this, predicate);
  }

  public PairPlanNode<K, V> filterKey(SerializablePredicate<K> predicate) {
    return filter((k, v) -> predicate.test(k));
  }

  public PairPlanNode<K, V> filterValue(SerializablePredicate<V> predicate) {
    return filter((k, v) -> predicate.test(v));
  }

  public <KO, VO> PairPlanNode<KO, VO> flatMapPair(SerializableBiFunction<K, V, Stream<Map.Entry<KO, VO>>> function) {
    return new FlatMapPairNode<>(this, function);
  }

  public <KO> PairPlanNode<KO, V> flatMapKey(SerializableFunction<K, Stream<KO>> function) {
    return flatMapPair((k, v) -> function.apply(k).map(k2 -> Maps.immutableEntry(k2, v)));
  }

  public <VO> PairPlanNode<K, VO> flatMapValue(SerializableFunction<V, Stream<VO>> function) {
    return flatMapPair((k, v) -> function.apply(v).map(v2 -> Maps.immutableEntry(k, v2)));
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

  public <TO> PlanNode<TO> map(SerializableBiFunction<K, V, TO> function) {
    return new MapFromPairNode<>(this, function);
  }

  public <KO, VO> PairPlanNode<KO, VO> mapPair(SerializableBiFunction<K, V, Map.Entry<KO, VO>> function) {
    return new MapPairNode<>(this, function);
  }

  public <KO> PairPlanNode<KO, V> mapKey(SerializableFunction<K, KO> function) {
    return mapPair((k, v) -> Maps.immutableEntry(function.apply(k), v));
  }

  public <VO> PairPlanNode<K, VO> mapValue(SerializableFunction<V, VO> function) {
    return mapPair((k, v) -> Maps.immutableEntry(k, function.apply(v)));
  }

  public PairPlanNode<K, V> subtract(PlanNode<K> right) {
    return new SubtractPairNode<>(this, right);
  }


  public PairPlanNode<V, K> swap() {
    return mapPair((k, v) -> Maps.immutableEntry(v, k));
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
