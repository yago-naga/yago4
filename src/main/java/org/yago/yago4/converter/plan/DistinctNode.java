package org.yago.yago4.converter.plan;

public class DistinctNode<T> extends PlanNode<T> {
  private final PlanNode<T> parent;

  DistinctNode(PlanNode<T> parent) {
    this.parent = parent;
  }

  public PlanNode<T> getParent() {
    return parent;
  }

  @Override
  public boolean equals(Object obj) {
    return (obj instanceof DistinctNode) && parent.equals(((DistinctNode) obj).parent);

  }

  @Override
  public int hashCode() {
    return parent.hashCode();
  }
}
