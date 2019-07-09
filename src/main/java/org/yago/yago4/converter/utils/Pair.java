package org.yago.yago4.converter.utils;

import java.io.Serializable;
import java.util.Map;

public final class Pair<K, V> implements Map.Entry<K, V>, Serializable {
  private final K key;
  private final V value;

  public Pair(K key, V value) {
    this.key = key;
    this.value = value;
  }

  @Override
  public K getKey() {
    return key;
  }

  @Override
  public V getValue() {
    return value;
  }

  @Override
  public V setValue(V v) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String toString() {
    return "(" + key + ", " + value + ")";
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof Map.Entry)) {
      return false;
    }
    Map.Entry o = (Map.Entry) obj;
    return key.equals(o.getKey()) && value.equals(o.getValue());
  }

  @Override
  public int hashCode() {
    return key.hashCode() ^ value.hashCode();
  }
}