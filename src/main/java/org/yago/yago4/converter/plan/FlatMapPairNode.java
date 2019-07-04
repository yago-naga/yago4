package org.yago.yago4.converter.plan;

import java.util.Map;
import java.util.stream.Stream;

public class FlatMapPairNode<KI, VI, KO, VO> extends PairPlanNode<KO, VO> {
  private final PairPlanNode<KI, VI> parent;
  private final SerializableBiFunction<KI, VI, Stream<Map.Entry<KO, VO>>> function;

  FlatMapPairNode(PairPlanNode<KI, VI> parent, SerializableBiFunction<KI, VI, Stream<Map.Entry<KO, VO>>> function) {
    this.parent = parent;
    this.function = function;
  }

  public PairPlanNode<KI, VI> getParent() {
    return parent;
  }

  public SerializableBiFunction<KI, VI, Stream<Map.Entry<KO, VO>>> getFunction() {
    return function;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof FlatMapPairNode) {
      FlatMapPairNode o = (FlatMapPairNode) obj;
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
