package org.yago.yago4.converter.plan;

import java.util.Map;

public class JoinPairNode<K, V1, V2> extends PairPlanNode<K, Map.Entry<V1, V2>> {
  private final PairPlanNode<K, V1> leftParent;
  private final PairPlanNode<K, V2> rightParent;

  JoinPairNode(PairPlanNode<K, V1> leftParent, PairPlanNode<K, V2> rightParent) {
    this.leftParent = leftParent;
    this.rightParent = rightParent;
  }

  public PairPlanNode<K, V1> getLeftParent() {
    return leftParent;
  }

  public PairPlanNode<K, V2> getRightParent() {
    return rightParent;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof JoinPairNode) {
      JoinPairNode o = (JoinPairNode) obj;
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
