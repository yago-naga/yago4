package org.yago.yago4.converter.plan;

public class TransitiveClosureNode<T> extends PlanNode<T> {
  private final PlanNode<T> leftParent;
  private final PairPlanNode<T, T> rightParent;

  TransitiveClosureNode(PlanNode<T> leftParent, PairPlanNode<T, T> rightParent) {
    this.leftParent = leftParent;
    this.rightParent = rightParent;
  }

  public PlanNode<T> getLeftParent() {
    return leftParent;
  }

  public PairPlanNode<T, T> getRightParent() {
    return rightParent;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof TransitiveClosureNode) {
      TransitiveClosureNode o = (TransitiveClosureNode) obj;
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
