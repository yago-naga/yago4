package org.yago.yago4.converter.plan;

import java.util.Map;

public class MapPairNode<KI, VI, KO, VO> extends PairPlanNode<KO, VO> {
  private final PairPlanNode<KI, VI> parent;
  private final SerializableBiFunction<KI, VI, Map.Entry<KO, VO>> function;

  MapPairNode(PairPlanNode<KI, VI> parent, SerializableBiFunction<KI, VI, Map.Entry<KO, VO>> function) {
    this.parent = parent;
    this.function = function;
  }

  public PairPlanNode<KI, VI> getParent() {
    return parent;
  }

  public SerializableBiFunction<KI, VI, Map.Entry<KO, VO>> getFunction() {
    return function;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof MapPairNode) {
      MapPairNode o = (MapPairNode) obj;
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
