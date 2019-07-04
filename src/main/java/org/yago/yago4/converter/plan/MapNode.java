package org.yago.yago4.converter.plan;

public class MapNode<TI, TO> extends PlanNode<TO> {
  private final PlanNode<TI> parent;
  private final SerializableFunction<TI, TO> function;

  MapNode(PlanNode<TI> parent, SerializableFunction<TI, TO> function) {
    this.parent = parent;
    this.function = function;
  }

  public PlanNode<TI> getParent() {
    return parent;
  }

  public SerializableFunction<TI, TO> getFunction() {
    return function;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof MapNode) {
      MapNode o = (MapNode) obj;
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
