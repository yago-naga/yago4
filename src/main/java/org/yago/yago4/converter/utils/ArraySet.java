package org.yago.yago4.converter.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.function.UnaryOperator;

final class ArraySet<E> extends ArrayList<E> {

  ArraySet() {
    super();
  }

  ArraySet(int initialCapacity) {
    super(initialCapacity);
  }

  @Override
  public boolean add(E element) {
    if (contains(element)) {
      return false;
    }
    return super.add(element);
  }

  @Override
  public void add(int index, E element) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean addAll(Collection<? extends E> c) {
    boolean changed = false;
    ensureCapacity(size() + c.size());
    for (E e : c) {
      changed |= add(e);
    }
    return changed;
  }

  @Override
  public boolean addAll(int index, Collection<? extends E> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void replaceAll(UnaryOperator<E> operator) {
    throw new UnsupportedOperationException();
  }

  @Override
  public E set(int index, E element) {
    throw new UnsupportedOperationException();
  }
}
