package org.yago.yago4.converter.utils.stream;

import com.google.common.collect.Multimap;
import org.yago.yago4.converter.utils.Pair;

import java.util.Map;
import java.util.Spliterator;
import java.util.function.Consumer;

public class PairStreamMapJoinSpliterator<K, V1, V2> extends AbstractStreamSpliterator<Map.Entry<K, V1>, Map.Entry<K, Map.Entry<V1, V2>>> {

  private final Multimap<K, V2> right;

  public PairStreamMapJoinSpliterator(Spliterator<Map.Entry<K, V1>> left, Multimap<K, V2> right) {
    super(left);
    this.right = right;
  }

  @Override
  protected void onElement(Map.Entry<K, V1> input, Consumer<? super Map.Entry<K, Map.Entry<V1, V2>>> action) {
    right.get(input.getKey()).forEach(r -> action.accept(new Pair<>(input.getKey(), new Pair<>(input.getValue(), r))));
  }

  @Override
  protected Spliterator<Map.Entry<K, Map.Entry<V1, V2>>> buildSplit(Spliterator<Map.Entry<K, V1>> parentSplit) {
    return new PairStreamMapJoinSpliterator<>(parentSplit, right);
  }
}
