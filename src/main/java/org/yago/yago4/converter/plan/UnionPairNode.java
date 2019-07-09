package org.yago.yago4.converter.plan;

import java.util.List;

public class UnionPairNode<K, V> extends PairPlanNode<K, V> {
  private final List<PairPlanNode<K, V>> parents;

  UnionPairNode(List<PairPlanNode<K, V>> parents) {
    this.parents = parents;
  }

  public List<PairPlanNode<K, V>> getParents() {
    return parents;
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof UnionPairNode && parents.equals(((UnionPairNode) obj).parents);
  }

  @Override
  public int hashCode() {
    return parents.hashCode();
  }
}
