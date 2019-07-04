package org.yago.yago4.converter.plan;

import java.util.Map;

public class MapToPairNode<TI, KO, VO> extends PairPlanNode<KO, VO> {
  private final PlanNode<TI> parent;
  private final SerializableFunction<TI, Map.Entry<KO, VO>> function;

  MapToPairNode(PlanNode<TI> parent, SerializableFunction<TI, Map.Entry<KO, VO>> function) {
    this.parent = parent;
    this.function = function;
  }

  public PlanNode<TI> getParent() {
    return parent;
  }

  public SerializableFunction<TI, Map.Entry<KO, VO>> getFunction() {
    return function;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof MapToPairNode) {
      MapToPairNode o = (MapToPairNode) obj;
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
