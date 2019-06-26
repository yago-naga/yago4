package org.yago.yago4.converter.plan;

public class UnionNode<T> extends PlanNode<T> {
  private final PlanNode<T> leftParent;
  private final PlanNode<T> rightParent;

  UnionNode(PlanNode<T> leftParent, PlanNode<T> rightParent) {
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
    if (obj instanceof UnionNode) {
      UnionNode o = (UnionNode) obj;
      return leftParent.equals(o.leftParent) && rightParent.equals(o.rightParent) || leftParent.equals(o.rightParent) && rightParent.equals(o.leftParent);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return leftParent.hashCode() ^ rightParent.hashCode();
  }
}
