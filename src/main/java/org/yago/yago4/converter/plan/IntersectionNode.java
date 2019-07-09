package org.yago.yago4.converter.plan;

public class IntersectionNode<T> extends PlanNode<T> {
  private final PlanNode<T> leftParent;
  private final PlanNode<T> rightParent;

  IntersectionNode(PlanNode<T> leftParent, PlanNode<T> rightParent) {
    this.leftParent = leftParent;
    this.rightParent = rightParent;
  }

  public PlanNode<T> getLeftParent() {
    return leftParent;
  }

  public PlanNode<T> getRightParent() {
    return rightParent;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof IntersectionNode) {
      IntersectionNode o = (IntersectionNode) obj;
      return leftParent.equals(o.leftParent) && rightParent.equals(o.rightParent);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return leftParent.hashCode() ^ rightParent.hashCode();
  }
}
