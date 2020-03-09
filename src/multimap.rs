use failure::_core::hash::Hash;
use std::collections::HashMap;
use std::iter::FromIterator;

#[derive(Debug)]
pub struct Multimap<K: Eq + Hash, V: Eq> {
    inner: HashMap<K, Vec<V>>,
}

impl<K: Eq + Hash, V: Eq> Multimap<K, V> {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: HashMap::with_capacity(capacity),
        }
    }

    pub fn get(&self, key: &K) -> Option<&[V]> {
        self.inner.get(key).map(|v| v.as_slice())
    }

    pub fn get_mut(&mut self, key: &K) -> Option<&mut Vec<V>> {
        self.inner.get_mut(key)
    }

    pub fn insert(&mut self, key: K, value: V) {
        self.inner.entry(key).or_insert_with(Vec::new).push(value)
    }

    pub fn iter(&self) -> MultimapIter<'_, K, V> {
        self.into_iter()
    }

    pub fn iter_grouped<'a>(&'a self) -> impl Iterator<Item = (&'a K, &'a Vec<V>)> + 'a {
        self.inner.iter()
    }

    pub fn into_iter_grouped(self) -> impl Iterator<Item = (K, Vec<V>)> {
        self.inner.into_iter()
    }

    pub fn len(&self) -> usize {
        self.inner.values().map(|v| v.len()).sum()
    }
}

impl<K: Eq + Hash, V: Eq> FromIterator<(K, V)> for Multimap<K, V> {
    fn from_iter<I: IntoIterator<Item = (K, V)>>(iter: I) -> Self {
        let iter = iter.into_iter();
        let mut multimap = Multimap::with_capacity(iter.size_hint().0);
        for (k, v) in iter {
            multimap.insert(k, v);
        }
        multimap
    }
}

impl<K: Eq + Hash + Clone, V: Eq> IntoIterator for Multimap<K, V> {
    type Item = (K, V);
    type IntoIter = MultimapIterator<K, V>;

    fn into_iter(self) -> MultimapIterator<K, V> {
        let lower_size = self.inner.len();
        MultimapIterator {
            inner: self.inner.into_iter(),
            key: None,
            values: Vec::default(),
            lower_size,
        }
    }
}

pub struct MultimapIterator<K: Eq + Hash + Clone, V: Eq> {
    inner: <HashMap<K, Vec<V>> as IntoIterator>::IntoIter,
    key: Option<K>,
    values: Vec<V>,
    lower_size: usize,
}

impl<K: Eq + Hash + Clone, V: Eq> Iterator for MultimapIterator<K, V> {
    type Item = (K, V);

    fn next(&mut self) -> Option<(K, V)> {
        loop {
            if let Some(value) = self.values.pop() {
                return Some((self.key.clone().unwrap(), value));
            }
            if let Some((key, values)) = self.inner.next() {
                self.key = Some(key);
                self.values = values;
            } else {
                return None;
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.lower_size, None)
    }
}

impl<'a, K: Eq + Hash, V: Eq> IntoIterator for &'a Multimap<K, V> {
    type Item = (&'a K, &'a V);
    type IntoIter = MultimapIter<'a, K, V>;

    fn into_iter(self) -> MultimapIter<'a, K, V> {
        MultimapIter {
            inner: self.inner.iter(),
            key: None,
            values: Vec::default(),
            lower_size: self.inner.len(),
        }
    }
}

pub struct MultimapIter<'a, K: Eq + Hash, V: Eq> {
    inner: <&'a HashMap<K, Vec<V>> as IntoIterator>::IntoIter,
    key: Option<&'a K>,
    values: Vec<&'a V>,
    lower_size: usize,
}

impl<'a, K: Eq + Hash, V: Eq> Iterator for MultimapIter<'a, K, V> {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<(&'a K, &'a V)> {
        loop {
            if let Some(value) = self.values.pop() {
                return Some((self.key.as_ref().unwrap(), value));
            }
            if let Some((key, values)) = self.inner.next() {
                self.key = Some(key);
                self.values.extend(values);
            } else {
                return None;
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.lower_size, None)
    }
}
