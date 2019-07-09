package org.yago.yago4.converter.plan;

public class KeysNode<K, V> extends PlanNode<K> {
  private final PairPlanNode<K, V> parent;

  KeysNode(PairPlanNode<K, V> parent) {
    this.parent = parent;
  }

  public PairPlanNode<K, V> getParent() {
    return parent;
  }

  @Override
  public boolean equals(Object obj) {
    return (obj instanceof KeysNode) && parent.equals(((KeysNode) obj).parent);

  }

  @Override
  public int hashCode() {
    return parent.hashCode();
  }
}
