package org.yago.yago4.converter.plan;

import java.util.function.Function;
import java.util.stream.Stream;

public class FlatMapNode<TI, TO> extends PlanNode<TO> {
  private final PlanNode<TI> parent;
  private final Function<TI, Stream<TO>> function;

  FlatMapNode(PlanNode<TI> parent, Function<TI, Stream<TO>> function) {
    this.parent = parent;
    this.function = function;
  }

  public PlanNode<TI> getParent() {
    return parent;
  }

  public Function<TI, Stream<TO>> getFunction() {
    return function;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof FlatMapNode) {
      FlatMapNode o = (FlatMapNode) obj;
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
