package org.yago.yago4.converter.utils;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public abstract class Multimap<K, V> {
  private final Map<K, Collection<V>> inner;
  private final Entries<K, V> entries;

  public Multimap() {
    inner = createMap();
    entries = new Entries<>(inner);
  }

  protected abstract Map<K, Collection<V>> createMap();

  protected abstract Collection<V> createCollection();

  private Collection<V> createCollection(K key) {
    return createCollection();
  }

  public int size() {
    return entries.size();
  }

  public boolean isEmpty() {
    return inner.isEmpty();
  }

  public Collection<V> get(K key) {
    return inner.getOrDefault(key, Collections.emptySet());
  }

  public boolean put(K k, V element) {
    return inner.computeIfAbsent(k, this::createCollection).add(element);
  }

  public void putAll(Multimap<K, V> other) {
    for (Map.Entry<K, Collection<V>> o : other.inner.entrySet()) {
      Collection<V> in = inner.get(o.getKey());
      if (in == null) {
        inner.put(o.getKey(), o.getValue());
      } else {
        in.addAll(o.getValue());
      }
    }
  }

  public void clear() {
    inner.clear();
  }

  public Set<K> keySet() {
    return inner.keySet();
  }

  public Set<Map.Entry<K, V>> entries() {
    return entries;
  }

  private static final class Entries<K, V> implements Set<Map.Entry<K, V>> {

    private final Map<K, Collection<V>> inner;

    private Entries(Map<K, Collection<V>> inner) {
      this.inner = inner;
    }

    @Override
    public int size() {
      int size = 0;
      for (Collection<V> c : inner.values()) {
        size += c.size();
      }
      return size;
    }

    @Override
    public boolean isEmpty() {
      return inner.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
      if (o instanceof Map.Entry) {
        Map.Entry e = (Map.Entry) o;
        return inner.getOrDefault(e.getKey(), Collections.emptySet()).contains(e.getValue());
      } else {
        return false;
      }
    }

    @Override
    public Iterator<Map.Entry<K, V>> iterator() {
      return new EntryIterator<>(inner.entrySet().iterator());
    }

    @Override
    public Object[] toArray() {
      Object[] array = new Object[size()];
      int i = 0;
      for (Map.Entry<K, Collection<V>> e : inner.entrySet()) {
        K k = e.getKey();
        for (V v : e.getValue()) {
          array[i] = Map.entry(k, v);
          i++;
        }
      }
      return array;
    }

    @Override
    public <T> T[] toArray(T[] ts) {
      throw new UnsupportedOperationException();

    }

    @Override
    public boolean add(Map.Entry<K, V> kvEntry) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object o) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsAll(Collection<?> collection) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(Collection<? extends Map.Entry<K, V>> collection) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> collection) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> collection) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean equals(Object o) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int hashCode() {
      throw new UnsupportedOperationException();
    }

    public Spliterator<Map.Entry<K, V>> spliterator() {
      return new EntrySpliterator<>(inner.entrySet().spliterator());
    }

    public Stream<Map.Entry<K, V>> stream() {
      return StreamSupport.stream(spliterator(), false);
    }

    public Stream<Map.Entry<K, V>> parallelStream() {
      return StreamSupport.stream(spliterator(), true);
    }
  }


  private static final class EntrySpliterator<K, V> implements Spliterator<Map.Entry<K, V>> {
    private final Spliterator<Map.Entry<K, Collection<V>>> inner;
    private K currentKey;
    private Spliterator<V> currentSpliterator;
    private Consumer<? super Map.Entry<K, V>> action;

    private EntrySpliterator(Spliterator<Map.Entry<K, Collection<V>>> inner) {
      this.inner = inner;
    }

    @Override
    public boolean tryAdvance(Consumer<? super Map.Entry<K, V>> action) {
      if (currentSpliterator != null) {
        if (currentSpliterator.tryAdvance(this::actionForCurrentKeyValue)) {
          return true;
        } else {
          currentSpliterator = null;
        }
      }

      // Each key has at least one value (TODO: change this is we add value removal)
      return inner.tryAdvance(this::actionForOneKeyValue);
    }

    @Override
    public void forEachRemaining(Consumer<? super Map.Entry<K, V>> action) {
      this.action = action;
      if (currentSpliterator != null) {
        currentSpliterator.forEachRemaining(this::actionForCurrentKeyValue);
      }
      inner.forEachRemaining(this::actionForAllKeyValues);
    }

    private void actionForCurrentKeyValue(V v) {
      action.accept(Map.entry(currentKey, v));
    }

    private void actionForOneKeyValue(Map.Entry<K, Collection<V>> e) {
      currentKey = e.getKey();
      currentSpliterator = e.getValue().spliterator();
      currentSpliterator.tryAdvance(this::actionForCurrentKeyValue);
    }

    private void actionForAllKeyValues(Map.Entry<K, Collection<V>> e) {
      currentKey = e.getKey();
      e.getValue().spliterator().forEachRemaining(this::actionForCurrentKeyValue);
    }

    @Override
    public Spliterator<Map.Entry<K, V>> trySplit() {
      Spliterator<Map.Entry<K, Collection<V>>> split = inner.trySplit();
      return (split == null) ? null : new EntrySpliterator<>(split);
    }

    @Override
    public long estimateSize() {
      return inner.estimateSize();
    }

    @Override
    public int characteristics() {
      return 0; //TODO
    }
  }

  private static final class EntryIterator<K, V> implements Iterator<Map.Entry<K, V>> {
    private final Iterator<Map.Entry<K, Collection<V>>> inner;
    private K currentKey;
    private Iterator<V> currentIterator;

    private EntryIterator(Iterator<Map.Entry<K, Collection<V>>> inner) {
      this.inner = inner;
    }

    @Override
    public boolean hasNext() {
      return inner.hasNext() || (currentIterator != null && currentIterator.hasNext());
    }

    @Override
    public Map.Entry<K, V> next() {
      while (currentIterator == null || !currentIterator.hasNext()) {
        Map.Entry<K, Collection<V>> nextIter = inner.next();
        currentKey = nextIter.getKey();
        currentIterator = nextIter.getValue().iterator();
      }

      V next = currentIterator.next();
      return Map.entry(currentKey, next);
    }
  }
}
