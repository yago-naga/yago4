package org.yago.yago4.converter.plan;

public class JoinNode<K, V> extends PairPlanNode<K, V> {
  private final PlanNode<K> leftParent;
  private final PairPlanNode<K, V> rightParent;

  JoinNode(PlanNode<K> leftParent, PairPlanNode<K, V> rightParent) {
    this.leftParent = leftParent;
    this.rightParent = rightParent;
  }

  public PlanNode<K> getLeftParent() {
    return leftParent;
  }

  public PairPlanNode<K, V> getRightParent() {
    return rightParent;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof JoinNode) {
      JoinNode o = (JoinNode) obj;
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
