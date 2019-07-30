package org.yago.yago4.converter.plan;

public class DistinctPairNode<K, V> extends PairPlanNode<K, V> {
  private final PairPlanNode<K, V> parent;

  DistinctPairNode(PairPlanNode<K, V> parent) {
    this.parent = parent;
  }

  public PairPlanNode<K, V> getParent() {
    return parent;
  }

  @Override
  public boolean equals(Object obj) {
    return (obj instanceof DistinctPairNode) && parent.equals(((DistinctPairNode) obj).parent);

  }

  @Override
  public int hashCode() {
    return parent.hashCode();
  }
}
