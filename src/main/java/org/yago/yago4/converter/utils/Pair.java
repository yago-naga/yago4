package org.yago.yago4.converter.utils;

import java.io.Serializable;
import java.util.Map;

public final class Pair<K, V> implements Map.Entry<K, V>, Serializable {
  private final K key;
  private V value;

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
    V old = value;
    value = v;
    return old;
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
    if (!(obj instanceof Pair)) {
      return false;
    }
    Pair o = (Pair) obj;
    return key.equals(o.key) && value.equals(o.value);
  }

  @Override
  public int hashCode() {
    return key.hashCode() ^ value.hashCode();
  }
}