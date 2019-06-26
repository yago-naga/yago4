package org.yago.yago4.converter.plan;

import java.util.Collection;

public class CollectionNode<T> extends PlanNode<T> {
  private final Collection<T> elements;

  CollectionNode(Collection<T> elements) {
    this.elements = elements;
  }

  public Collection<T> getElements() {
    return elements;
  }

  @Override
  public boolean equals(Object obj) {
    return (obj instanceof CollectionNode) && elements.equals(((CollectionNode) obj).elements);
  }

  @Override
  public int hashCode() {
    return elements.hashCode();
  }
}
