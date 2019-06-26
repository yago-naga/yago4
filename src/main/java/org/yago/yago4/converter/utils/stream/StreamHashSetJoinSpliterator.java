package org.yago.yago4.converter.utils.stream;

import java.util.Set;
import java.util.Spliterator;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * TODO: optimize
 */
public class StreamHashSetJoinSpliterator<L, R, O> implements Spliterator<O>, Consumer<L> {

  private final Spliterator<L> left;
  private final Set<R> right;
  private final Function<L, R> leftKey;
  private final BiFunction<L, R, O> combine;
  private Consumer<? super O> action;

  public StreamHashSetJoinSpliterator(Spliterator<L> left, Set<R> right, Function<L, R> leftKey, BiFunction<L, R, O> combine) {
    this.left = left;
    this.right = right;
    this.leftKey = leftKey;
    this.combine = combine;
  }

  @Override
  public void accept(L l) {
    R e = leftKey.apply(l);
    if (right.contains(e)) {
      action.accept(combine.apply(l, e));
    }
  }

  @Override
  public boolean tryAdvance(Consumer<? super O> action) {
    this.action = action;
    return left.tryAdvance(this);
  }

  @Override
  public void forEachRemaining(Consumer<? super O> action) {
    this.action = action;
    left.forEachRemaining(this);
  }

  @Override
  public StreamHashSetJoinSpliterator<L, R, O> trySplit() {
    Spliterator<L> leftSplit = left.trySplit();
    if (leftSplit == null) {
      return null;
    }
    return new StreamHashSetJoinSpliterator<>(leftSplit, right, leftKey, combine);
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
