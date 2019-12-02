package org.yago.yago4.converter.utils;

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;

import java.util.Collection;
import java.util.Map;

public final class ArraySetHashMapMultimap<K, V> extends Multimap<K, V> {
  public ArraySetHashMapMultimap() {
    super();
  }

  public ArraySetHashMapMultimap(Multimap<K, V> from) {
    super(from);
  }

  @Override
  protected Map<K, Collection<V>> createMap() {
    return new Object2ObjectOpenHashMap<>();
  }

  @Override
  protected Map<K, Collection<V>> createMap(int expected) {
    return new Object2ObjectOpenHashMap<>(expected);
  }

  @Override
  protected Collection<V> createCollection() {
    return new ArraySet<>(2);
  }

  @Override
  protected Collection<V> createCollection(Collection<V> from) {
    return new ArraySet<>(from);
  }
}
