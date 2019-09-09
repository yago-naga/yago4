package org.yago.yago4.converter.plan;

import java.util.stream.Stream;

public class AggregateByKeyNode<K, V> extends PairPlanNode<K, Stream<V>> {
  private final PairPlanNode<K, V> parent;

  AggregateByKeyNode(PairPlanNode<K, V> parent) {
    this.parent = parent;
  }

  public PairPlanNode<K, V> getParent() {
    return parent;
  }

  @Override
  public boolean equals(Object obj) {
    return (obj instanceof AggregateByKeyNode) && parent.equals(((AggregateByKeyNode) obj).parent);

  }

  @Override
  public int hashCode() {
    return parent.hashCode();
  }
}
