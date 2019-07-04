package org.yago.yago4.converter.plan;

import java.util.List;

public class UnionNode<T> extends PlanNode<T> {
  private final List<PlanNode<T>> parents;

  UnionNode(List<PlanNode<T>> parents) {
    this.parents = parents;
  }

  public List<PlanNode<T>> getParents() {
    return parents;
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof UnionNode && parents.equals(((UnionNode) obj).parents);
  }

  @Override
  public int hashCode() {
    return parents.hashCode();
  }
}
