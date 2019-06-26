package org.yago.yago4.converter.utils.stream;

import java.util.Set;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * TODO: optimize
 */
public class StreamHashSetAntiJoinSpliterator<L, R> implements Spliterator<L>, Consumer<L> {

  private final Spliterator<L> left;
  private final Set<R> right;
  private final Function<L, R> leftKey;
  private Consumer<? super L> action;

  public StreamHashSetAntiJoinSpliterator(Spliterator<L> left, Set<R> right, Function<L, R> leftKey) {
    this.left = left;
    this.right = right;
    this.leftKey = leftKey;
  }

  @Override
  public void accept(L l) {
    if (!right.contains(leftKey.apply(l))) {
      action.accept(l);
    }
  }

  @Override
  public boolean tryAdvance(Consumer<? super L> action) {
    this.action = action;
    return left.tryAdvance(this);
  }

  @Override
  public void forEachRemaining(Consumer<? super L> action) {
    this.action = action;
    left.forEachRemaining(this);
  }

  @Override
  public StreamHashSetAntiJoinSpliterator<L, R> trySplit() {
    Spliterator<L> leftSplit = left.trySplit();
    if (leftSplit == null) {
      return null;
    }
    return new StreamHashSetAntiJoinSpliterator<>(leftSplit, right, leftKey);
  }

  @Override
  public long estimateSize() {
    return left.estimateSize(); //TODO: better?
  }

  @Override
  public int characteristics() {
    int characteristics = 0;
    if (left.hasCharacteristics(ORDERED)) {
      characteristics |= ORDERED;
    }
    if (left.hasCharacteristics(DISTINCT)) {
      characteristics |= DISTINCT;
    }
    if (left.hasCharacteristics(SORTED)) {
      characteristics |= SORTED;
    }
    if (left.hasCharacteristics(NONNULL)) {
      characteristics |= NONNULL;
    }
    if (left.hasCharacteristics(IMMUTABLE)) {
      characteristics |= IMMUTABLE;
    }
    return characteristics;
  }
}
