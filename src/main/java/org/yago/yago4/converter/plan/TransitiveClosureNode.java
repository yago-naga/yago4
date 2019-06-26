package org.yago.yago4.converter.plan;

import java.util.function.BiFunction;
import java.util.function.Function;

public class TransitiveClosureNode<T1, T2, K> extends PlanNode<T1> {
  private final PlanNode<T1> leftParent;
  private final PlanNode<T2> rightParent;
  private final Function<T1, K> leftKey;
  private final Function<T2, K> rightKey;
  private final BiFunction<T1, T2, T1> mergeFunction;

  TransitiveClosureNode(PlanNode<T1> leftParent, PlanNode<T2> rightParent, Function<T1, K> leftKey, Function<T2, K> rightKey, BiFunction<T1, T2, T1> mergeFunction) {
    this.leftParent = leftParent;
    this.rightParent = rightParent;
    this.leftKey = leftKey;
    this.rightKey = rightKey;
    this.mergeFunction = mergeFunction;
  }

  public PlanNode<T1> getLeftParent() {
    return leftParent;
  }

  public PlanNode<T2> getRightParent() {
    return rightParent;
  }

  public Function<T1, K> getLeftKey() {
    return leftKey;
  }

  public Function<T2, K> getRightKey() {
    return rightKey;
  }

  public BiFunction<T1, T2, T1> getMergeFunction() {
    return mergeFunction;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof TransitiveClosureNode) {
      TransitiveClosureNode o = (TransitiveClosureNode) obj;
      return leftParent.equals(o.leftParent) && rightParent.equals(o.rightParent) && leftKey == o.leftKey && rightKey == o.rightKey && mergeFunction.equals(o.mergeFunction);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return leftParent.hashCode() ^ rightParent.hashCode() ^ leftKey.hashCode() ^ rightKey.hashCode();
  }
}
