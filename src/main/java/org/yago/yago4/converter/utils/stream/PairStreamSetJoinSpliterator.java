package org.yago.yago4.converter.utils.stream;

import java.util.Map;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.Consumer;

public class PairStreamSetJoinSpliterator<K, V> extends AbstractStreamSpliterator<Map.Entry<K, V>, Map.Entry<K, V>> {

  private final Set<K> right;

  public PairStreamSetJoinSpliterator(Spliterator<Map.Entry<K, V>> left, Set<K> right) {
    super(left);
    this.right = right;
  }

  @Override
  protected void onElement(Map.Entry<K, V> input, Consumer<? super Map.Entry<K, V>> action) {
    if (right.contains(input.getKey())) {
      action.accept(input);
    }
  }

  @Override
  protected Spliterator<Map.Entry<K, V>> buildSplit(Spliterator<Map.Entry<K, V>> parentSplit) {
    return new PairStreamSetJoinSpliterator<>(parentSplit, right);
  }
}
