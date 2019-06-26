package org.yago.yago4.converter.utils.stream;

import com.google.common.collect.Multimap;

import java.util.Spliterator;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * TODO: optimize
 */
public class StreamHashMapJoinSpliterator<L, R, O, K> implements Spliterator<O>, Consumer<L> {

  private final Spliterator<L> left;
  private final Multimap<K, R> right;
  private final Function<L, K> leftKey;
  private final BiFunction<L, R, O> combine;
  private Consumer<? super O> action;

  public StreamHashMapJoinSpliterator(Spliterator<L> left, Multimap<K, R> right, Function<L, K> leftKey, BiFunction<L, R, O> combine) {
    this.left = left;
    this.right = right;
    this.leftKey = leftKey;
    this.combine = combine;
  }

  @Override
  public void accept(L l) {
    right.get(leftKey.apply(l)).forEach(r -> action.accept(combine.apply(l, r)));
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
  public StreamHashMapJoinSpliterator<L, R, O, K> trySplit() {
    Spliterator<L> leftSplit = left.trySplit();
    if (leftSplit == null) {
      return null;
    }
    return new StreamHashMapJoinSpliterator<>(leftSplit, right, leftKey, combine);
  }

  @Override
  public long estimateSize() {
    return left.estimateSize(); //TODO: better?
  }

  @Override
  public int characteristics() {
    return 0; //TODO
  }
}
