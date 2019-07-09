package org.yago.yago4.converter.utils.stream;

import com.google.common.collect.Multimap;
import org.yago.yago4.converter.utils.Pair;

import java.util.Map;
import java.util.Spliterator;
import java.util.function.Consumer;

public class StreamMapJoinSpliterator<K, V> extends AbstractStreamSpliterator<K, Map.Entry<K, V>> {

  private final Multimap<K, V> right;

  public StreamMapJoinSpliterator(Spliterator<K> left, Multimap<K, V> right) {
    super(left);
    this.right = right;
  }

  @Override
  protected void onElement(K input, Consumer<? super Map.Entry<K, V>> action) {
    right.get(input).forEach(v -> action.accept(new Pair<>(input, v)));
  }

  @Override
  protected Spliterator<Map.Entry<K, V>> buildSplit(Spliterator<K> parentSplit) {
    return new StreamMapJoinSpliterator<>(parentSplit, right);
  }
}
