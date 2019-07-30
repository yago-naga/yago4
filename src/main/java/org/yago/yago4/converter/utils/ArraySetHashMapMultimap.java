package org.yago.yago4.converter.utils;

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;

import java.util.Collection;
import java.util.Map;

public final class ArraySetHashMapMultimap<K, V> extends Multimap<K, V> {

  @Override
  protected Map<K, Collection<V>> createMap() {
    return new Object2ObjectOpenHashMap<>();
  }

  @Override
  protected Collection<V> createCollection() {
    return new ArraySet<>(2);
  }
}
