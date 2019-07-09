package org.yago.yago4.converter.utils.stream;

import java.util.Set;
import java.util.Spliterator;
import java.util.function.Consumer;

public class StreamSetJoinSpliterator<T> extends AbstractStreamSpliterator<T, T> {

  private final Set<T> right;

  public StreamSetJoinSpliterator(Spliterator<T> left, Set<T> right) {
    super(left);
    this.right = right;
  }

  @Override
  protected void onElement(T input, Consumer<? super T> action) {
    if (right.contains(input)) {
      action.accept(input);
    }
  }

  @Override
  protected Spliterator<T> buildSplit(Spliterator<T> parentSplit) {
    return new StreamSetJoinSpliterator<>(parentSplit, right);
  }
}
