package org.yago.yago4.converter.plan;

public class IntersectionPairNode<K, V> extends PairPlanNode<K, V> {
  private final PairPlanNode<K, V> leftParent;
  private final PlanNode<K> rightParent;

  IntersectionPairNode(PairPlanNode<K, V> leftParent, PlanNode<K> rightParent) {
    this.leftParent = leftParent;
    this.rightParent = rightParent;
  }

  public PairPlanNode<K, V> getLeftParent() {
    return leftParent;
  }

  public PlanNode<K> getRightParent() {
    return rightParent;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof IntersectionPairNode) {
      IntersectionPairNode o = (IntersectionPairNode) obj;
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
