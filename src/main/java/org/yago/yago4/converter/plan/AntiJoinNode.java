package org.yago.yago4.converter.plan;

import java.util.function.Function;

public class AntiJoinNode<T1, T2> extends PlanNode<T1> {
  private final PlanNode<T1> leftParent;
  private final PlanNode<T2> rightParent;
  private final Function<T1, T2> leftKey;

  AntiJoinNode(PlanNode<T1> leftParent, PlanNode<T2> rightParent, Function<T1, T2> leftKey) {
    this.leftParent = leftParent;
    this.rightParent = rightParent;
    this.leftKey = leftKey;
  }

  public PlanNode<T1> getLeftParent() {
    return leftParent;
  }

  public PlanNode<T2> getRightParent() {
    return rightParent;
  }

  public Function<T1, T2> getLeftKey() {
    return leftKey;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof AntiJoinNode) {
      AntiJoinNode o = (AntiJoinNode) obj;
      return leftParent.equals(o.leftParent) && rightParent.equals(o.rightParent) && leftKey.equals(o.leftKey);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return leftParent.hashCode() ^ rightParent.hashCode() ^ leftKey.hashCode();
  }
}
