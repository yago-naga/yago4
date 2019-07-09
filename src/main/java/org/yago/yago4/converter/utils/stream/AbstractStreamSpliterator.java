package org.yago.yago4.converter.utils.stream;

import java.util.Spliterator;
import java.util.function.Consumer;

/**
 * TODO: optimize
 */
abstract class AbstractStreamSpliterator<I, O> implements Spliterator<O>, Consumer<I> {

  private final Spliterator<I> parent;
  private Consumer<? super O> action;

  public AbstractStreamSpliterator(Spliterator<I> parent) {
    this.parent = parent;
  }

  @Override
  public boolean tryAdvance(Consumer<? super O> action) {
    this.action = action;
    return parent.tryAdvance(this);
  }

  @Override
  public void forEachRemaining(Consumer<? super O> action) {
    this.action = action;
    parent.forEachRemaining(this);
  }

  @Override
  public void accept(I input) {
    onElement(input, action);
  }

  protected abstract void onElement(I input, Consumer<? super O> action);

  @Override
  public Spliterator<O> trySplit() {
    Spliterator<I> parentSplit = parent.trySplit();
    if (parentSplit == null) {
      return null;
    }
    return buildSplit(parentSplit);
  }

  protected abstract Spliterator<O> buildSplit(Spliterator<I> parentSplit);

  @Override
  public long estimateSize() {
    return parent.estimateSize(); //TODO: better?
  }

  @Override
  public int characteristics() {
    int characteristics = 0;
    if (parent.hasCharacteristics(ORDERED)) {
      characteristics |= ORDERED;
    }
    if (parent.hasCharacteristics(DISTINCT)) {
      characteristics |= DISTINCT;
    }
    if (parent.hasCharacteristics(SORTED)) {
      characteristics |= SORTED;
    }
    if (parent.hasCharacteristics(NONNULL)) {
      characteristics |= NONNULL;
    }
    if (parent.hasCharacteristics(IMMUTABLE)) {
      characteristics |= IMMUTABLE;
    }
    return characteristics;
  }
}
