package org.yago.yago4.converter.plan;

public class ValuesNode<K, V> extends PlanNode<V> {
  private final PairPlanNode<K, V> parent;

  ValuesNode(PairPlanNode<K, V> parent) {
    this.parent = parent;
  }

  public PairPlanNode<K, V> getParent() {
    return parent;
  }

  @Override
  public boolean equals(Object obj) {
    return (obj instanceof ValuesNode) && parent.equals(((ValuesNode) obj).parent);

  }

  @Override
  public int hashCode() {
    return parent.hashCode();
  }
}
