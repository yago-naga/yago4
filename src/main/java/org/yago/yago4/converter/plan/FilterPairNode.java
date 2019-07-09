package org.yago.yago4.converter.plan;

import java.util.function.BiPredicate;

public class FilterPairNode<K, V> extends PairPlanNode<K, V> {
  private final PairPlanNode<K, V> parent;
  private final BiPredicate<K, V> predicate;

  FilterPairNode(PairPlanNode<K, V> parent, BiPredicate<K, V> predicate) {
    this.parent = parent;
    this.predicate = predicate;
  }

  public PairPlanNode<K, V> getParent() {
    return parent;
  }

  public BiPredicate<K, V> getPredicate() {
    return predicate;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof FilterPairNode) {
      FilterPairNode o = (FilterPairNode) obj;
      return parent.equals(o.parent) && predicate.equals(o.predicate);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return parent.hashCode() ^ predicate.hashCode();
  }
}
