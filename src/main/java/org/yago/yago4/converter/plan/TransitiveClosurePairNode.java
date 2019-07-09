package org.yago.yago4.converter.plan;

public class TransitiveClosurePairNode<K, V> extends PairPlanNode<K, V> {
  private final PairPlanNode<K, V> leftParent;
  private final PairPlanNode<V, V> rightParent;

  TransitiveClosurePairNode(PairPlanNode<K, V> leftParent, PairPlanNode<V, V> rightParent) {
    this.leftParent = leftParent;
    this.rightParent = rightParent;
  }

  public PairPlanNode<K, V> getLeftParent() {
    return leftParent;
  }

  public PairPlanNode<V, V> getRightParent() {
    return rightParent;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof TransitiveClosurePairNode) {
      TransitiveClosurePairNode o = (TransitiveClosurePairNode) obj;
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
