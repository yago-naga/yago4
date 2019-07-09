package org.yago.yago4.converter.plan;

public class CachePairNode<K, V> extends PairPlanNode<K, V> {
  private final PairPlanNode<K, V> parent;

  CachePairNode(PairPlanNode<K, V> parent) {
    this.parent = parent;
  }

  public PairPlanNode<K, V> getParent() {
    return parent;
  }

  @Override
  public boolean equals(Object obj) {
    return (obj instanceof CachePairNode) && parent.equals(((CachePairNode) obj).parent);

  }

  @Override
  public int hashCode() {
    return parent.hashCode();
  }
}
