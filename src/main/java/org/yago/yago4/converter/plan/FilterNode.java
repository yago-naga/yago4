package org.yago.yago4.converter.plan;

import java.util.function.Predicate;

public class FilterNode<T> extends PlanNode<T> {
  private final PlanNode<T> parent;
  private final Predicate<T> predicate;

  FilterNode(PlanNode<T> parent, Predicate<T> predicate) {
    this.parent = parent;
    this.predicate = predicate;
  }

  public PlanNode<T> getParent() {
    return parent;
  }

  public Predicate<T> getPredicate() {
    return predicate;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof FilterNode) {
      FilterNode o = (FilterNode) obj;
      return parent.equals(o.parent) && predicate.equals(o.predicate);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return parent.hashCode() ^ predicate.hashCode();
  }
}
