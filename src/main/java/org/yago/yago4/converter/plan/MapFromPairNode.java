package org.yago.yago4.converter.plan;

import java.util.function.BiFunction;

public class MapFromPairNode<KI, VI, TO> extends PlanNode<TO> {
  private final PairPlanNode<KI, VI> parent;
  private final BiFunction<KI, VI, TO> function;

  MapFromPairNode(PairPlanNode<KI, VI> parent, BiFunction<KI, VI, TO> function) {
    this.parent = parent;
    this.function = function;
  }

  public PairPlanNode<KI, VI> getParent() {
    return parent;
  }

  public BiFunction<KI, VI, TO> getFunction() {
    return function;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof MapFromPairNode) {
      MapFromPairNode o = (MapFromPairNode) obj;
      return parent.equals(o.parent) && function.equals(o.function);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return parent.hashCode() ^ function.hashCode();
  }
}
