package org.yago.yago4.converter.plan;

public class CacheNode<T> extends PlanNode<T> {
  private final PlanNode<T> parent;

  CacheNode(PlanNode<T> parent) {
    this.parent = parent;
  }

  public PlanNode<T> getParent() {
    return parent;
  }

  @Override
  public boolean equals(Object obj) {
    return (obj instanceof CacheNode) && parent.equals(((CacheNode) obj).parent);

  }

  @Override
  public int hashCode() {
    return parent.hashCode();
  }
}
