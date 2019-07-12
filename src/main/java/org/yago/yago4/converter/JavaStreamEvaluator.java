package org.yago.yago4.converter;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import org.eclipse.rdf4j.model.Statement;
import org.yago.yago4.converter.plan.*;
import org.yago.yago4.converter.utils.*;
import org.yago.yago4.converter.utils.stream.*;

import java.nio.file.Path;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class JavaStreamEvaluator {

  private final YagoValueFactory valueFactory;
  private final NTriplesReader nTriplesReader;
  private final NTriplesWriter nTriplesWriter;
  private final Map<PlanNode, Set> cache = new HashMap<>();
  private final Map<PairPlanNode, SetMultimap> cachePairs = new HashMap<>();

  public JavaStreamEvaluator(YagoValueFactory valueFactory) {
    this.valueFactory = valueFactory;
    nTriplesReader = new NTriplesReader(valueFactory);
    nTriplesWriter = new NTriplesWriter();
  }

  public void evaluateToNTriples(PlanNode<Statement> plan, Path outputFilePath) {
    nTriplesWriter.write(toStream(plan), outputFilePath);
  }

  public <T> List<T> evaluateToList(PlanNode<T> plan) {
    return toStream(plan).collect(Collectors.toList());
  }

  private <T> Stream<T> toStream(PlanNode<T> plan) {
    Set<T> cachedValue = cache.get(plan);
    if (cachedValue != null) {
      return cachedValue.parallelStream();
    } else if (plan instanceof CacheNode) {
      return toImmutableSet(plan).parallelStream();
    } else if (plan instanceof CollectionNode) {
      return toStream((CollectionNode<T>) plan);
    } else if (plan instanceof FilterNode) {
      return toStream((FilterNode<T>) plan);
    } else if (plan instanceof FlatMapNode) {
      return toStream((FlatMapNode<?, T>) plan);
    } else if (plan instanceof IntersectionNode) {
      return toStream((IntersectionNode<T>) plan);
    } else if (plan instanceof KeysNode) {
      return toStream((KeysNode<T, ?>) plan);
    } else if (plan instanceof MapNode) {
      return toStream((MapNode<?, T>) plan);
    } else if (plan instanceof MapFromPairNode) {
      return toStream((MapFromPairNode<?, ?, T>) plan);
    } else if (plan instanceof NTriplesReaderNode) {
      return (Stream<T>) toStream((NTriplesReaderNode) plan);
    } else if (plan instanceof RDFBinaryReaderNode) {
      return (Stream<T>) toStream((RDFBinaryReaderNode) plan);
    } else if (plan instanceof SubtractNode) {
      return toStream((SubtractNode<T>) plan);
    } else if (plan instanceof UnionNode) {
      return toStream((UnionNode<T>) plan);
    } else if (plan instanceof ValuesNode) {
      return toStream((ValuesNode<?, T>) plan);
    } else {
      throw new EvaluationException("Not supported plan node: " + plan);
    }
  }

  private <T> Stream<T> toStream(CollectionNode<T> plan) {
    return plan.getElements().parallelStream();
  }

  private <T> Stream<T> toStream(FilterNode<T> plan) {
    return toStream(plan.getParent()).filter(plan.getPredicate());
  }

  private <TI, TO> Stream<TO> toStream(FlatMapNode<TI, TO> plan) {
    return toStream(plan.getParent()).flatMap(plan.getFunction());
  }

  private <T> Stream<T> toStream(IntersectionNode<T> plan) {
    return toStream(() -> new StreamSetJoinSpliterator<>(
            toStream(plan.getLeftParent()).spliterator(),
            toImmutableSet(plan.getRightParent())
    ));
  }

  private <K, V> Stream<K> toStream(KeysNode<K, V> plan) {
    return toStream(plan.getParent()).map(Map.Entry::getKey);
  }


  private <TI, TO> Stream<TO> toStream(MapNode<TI, TO> plan) {
    return toStream(plan.getParent()).map(plan.getFunction());
  }

  private <KI, VI, TO> Stream<TO> toStream(MapFromPairNode<KI, VI, TO> plan) {
    BiFunction<KI, VI, TO> fn = plan.getFunction();
    return toStream(plan.getParent()).map(e -> fn.apply(e.getKey(), e.getValue()));
  }

  private Stream<Statement> toStream(NTriplesReaderNode plan) {
    return nTriplesReader.read(plan.getFilePath());
  }

  private Stream<Statement> toStream(RDFBinaryReaderNode plan) {
    return RDFBinaryFormat.read(valueFactory, plan.getFilePath());
  }

  private <T> Stream<T> toStream(SubtractNode<T> plan) {
    return toStream(() -> new StreamSetAntiJoinSpliterator<>(
            toStream(plan.getLeftParent()).spliterator(),
            toImmutableSet(plan.getRightParent())
    ));
  }

  private <T> Stream<T> toStream(UnionNode<T> plan) {
    return plan.getParents().stream().flatMap(this::toStream);
  }


  private <K, V> Stream<V> toStream(ValuesNode<K, V> plan) {
    return toStream(plan.getParent()).map(Map.Entry::getValue);
  }

  private <K, V> Stream<Map.Entry<K, V>> toStream(PairPlanNode<K, V> plan) {
    SetMultimap<K, V> cachedValue = cachePairs.get(plan);
    if (cachedValue != null) {
      return cachedValue.entries().parallelStream();
    } else if (plan instanceof CachePairNode) {
      return toImmutableMap(plan).entries().parallelStream();
    } else if (plan instanceof FilterPairNode) {
      return toStream((FilterPairNode<K, V>) plan);
    } else if (plan instanceof FlatMapPairNode) {
      return toStream((FlatMapPairNode<?, ?, K, V>) plan);
    } else if (plan instanceof IntersectionPairNode) {
      return toStream((IntersectionPairNode<K, V>) plan);
    } else if (plan instanceof JoinNode) {
      return toStream((JoinNode) plan);
    } else if (plan instanceof JoinPairNode) {
      return toStream((JoinPairNode) plan);
    } else if (plan instanceof MapPairNode) {
      return toStream((MapPairNode<?, ?, K, V>) plan);
    } else if (plan instanceof MapToPairNode) {
      return toStream((MapToPairNode<?, K, V>) plan);
    } else if (plan instanceof SubtractPairNode) {
      return toStream((SubtractPairNode<K, V>) plan);
    } else if (plan instanceof TransitiveClosurePairNode) {
      return toMutableMap((TransitiveClosurePairNode<K, V>) plan).entries().parallelStream();
    } else if (plan instanceof UnionPairNode) {
      return toStream((UnionPairNode<K, V>) plan);
    } else {
      throw new EvaluationException("Not supported plan node: " + plan);
    }
  }

  private <K, V> Stream<Map.Entry<K, V>> toStream(FilterPairNode<K, V> plan) {
    BiPredicate<K, V> fn = plan.getPredicate();
    return toStream(plan.getParent()).filter(e -> fn.test(e.getKey(), e.getValue()));
  }

  private <KI, VI, KO, VO> Stream<Map.Entry<KO, VO>> toStream(FlatMapPairNode<KI, VI, KO, VO> plan) {
    BiFunction<KI, VI, Stream<Map.Entry<KO, VO>>> fn = plan.getFunction();
    return toStream(plan.getParent()).flatMap(t -> fn.apply(t.getKey(), t.getValue()));
  }


  private <K, V> Stream<Map.Entry<K, V>> toStream(IntersectionPairNode<K, V> plan) {
    return toStream(() -> new PairStreamSetJoinSpliterator<>(
            toStream(plan.getLeftParent()).spliterator(),
            toImmutableSet(plan.getRightParent())
    ));
  }

  private <K, V> Stream<Map.Entry<K, V>> toStream(JoinNode<K, V> plan) {
    return toStream(() -> new StreamMapJoinSpliterator<>(
            toStream(plan.getLeftParent()).spliterator(),
            toImmutableMap(plan.getRightParent())));
  }

  private <K, V1, V2> Stream<Map.Entry<K, Map.Entry<V1, V2>>> toStream(JoinPairNode<K, V1, V2> plan) {
    return toStream(() -> new PairStreamMapJoinSpliterator<>(
            toStream(plan.getLeftParent()).spliterator(),
            toImmutableMap(plan.getRightParent())));
  }

  private <KI, VI, KO, VO> Stream<Map.Entry<KO, VO>> toStream(MapPairNode<KI, VI, KO, VO> plan) {
    BiFunction<KI, VI, Map.Entry<KO, VO>> fn = plan.getFunction();
    return toStream(plan.getParent()).map(t -> fn.apply(t.getKey(), t.getValue()));
  }

  private <TI, KO, VO> Stream<Map.Entry<KO, VO>> toStream(MapToPairNode<TI, KO, VO> plan) {
    Function<TI, Map.Entry<KO, VO>> fn = plan.getFunction();
    if (fn.equals(Function.identity())) {
      return (Stream<Map.Entry<KO, VO>>) toStream(plan.getParent());
    } else {
      return toStream(plan.getParent()).map(plan.getFunction());
    }
  }

  private <K, V> Stream<Map.Entry<K, V>> toStream(SubtractPairNode<K, V> plan) {
    return toStream(() -> new PairStreamSetAntiJoinSpliterator<>(
            toStream(plan.getLeftParent()).spliterator(),
            toImmutableSet(plan.getRightParent())
    ));
  }

  private <K, V> Stream<Map.Entry<K, V>> toStream(UnionPairNode<K, V> plan) {
    return plan.getParents().stream().flatMap(this::toStream);
  }

  private <T> Set<T> toImmutableSet(PlanNode<T> plan) {
    Set<T> value = cache.get(plan);
    if (value != null) {
      return value;
    } else if (plan instanceof CacheNode) {
      return toImmutableSet((CacheNode<T>) plan);
    } else if (plan instanceof KeysNode) {
      return toImmutableSet((KeysNode<T, ?>) plan);
    } else {
      return toMutableSet(plan);
    }
  }

  private <T> Set<T> toImmutableSet(CacheNode<T> plan) {
    Set<T> value = toImmutableSet(plan.getParent());
    cache.put(plan, value);
    cache.put(plan.getParent(), value); //to allow using the parent value as cache key, avoids impact of a common mistake
    return value;
  }

  private <K, V> Set<K> toImmutableSet(KeysNode<K, V> plan) {
    return toImmutableMap(plan.getParent()).keySet();
  }

  private <T> boolean isAlreadyMutableSet(PlanNode<T> plan) {
    return plan instanceof FilterNode && isAlreadyMutableSet(((FilterNode<T>) plan).getParent()) ||
            plan instanceof CollectionNode;
  }

  private <T> Set<T> toMutableSet(PlanNode<T> plan) {
    if (plan instanceof FilterNode) {
      return toMutableSet((FilterNode<T>) plan);
    } else if (plan instanceof CollectionNode) {
      return toMutableSet((CollectionNode<T>) plan);
    } else if (plan instanceof KeysNode) {
      return toMutableSet((KeysNode<T, ?>) plan);
    } else {
      return toStream(plan).collect(Collectors.toSet());
    }
  }

  private <T> Set<T> toMutableSet(FilterNode<T> plan) {
    if (isAlreadyMutableSet(plan.getParent())) {
      Set<T> elements = toMutableSet(plan.getParent());
      elements.removeIf(plan.getPredicate());
      return elements;
    } else {
      return toStream(plan).collect(Collectors.toSet());
    }
  }

  private <T> Set<T> toMutableSet(CollectionNode<T> plan) {
    Collection<T> elements = plan.getElements();
    if (elements instanceof Set) {
      return (Set<T>) elements;
    } else {
      return new HashSet<>(elements);
    }
  }

  private <K, V> Set<K> toMutableSet(KeysNode<K, V> plan) {
    return toMutableMap(plan.getParent()).keySet();
  }

  private <K, V> SetMultimap<K, V> toImmutableMap(PairPlanNode<K, V> plan) {
    SetMultimap<K, V> value = cachePairs.get(plan);
    if (value != null) {
      return value;
    } else if (plan instanceof CachePairNode) {
      return toImmutableMap((CachePairNode<K, V>) plan);
    } else {
      return toMutableMap(plan);
    }
  }

  private <K, V> SetMultimap<K, V> toImmutableMap(CachePairNode<K, V> plan) {
    SetMultimap<K, V> value = toImmutableMap(plan.getParent());
    cachePairs.put(plan, value);
    cachePairs.put(plan.getParent(), value); //to allow using the parent value as cache key, avoids impact of a common mistake
    return value;
  }

  private <K, V> SetMultimap<K, V> toMutableMap(PairPlanNode<K, V> plan) {
    if (plan instanceof TransitiveClosurePairNode) {
      return toMutableMap((TransitiveClosurePairNode<K, V>) plan);
    } else {
      SetMultimap<K, V> map = Multimaps.synchronizedSetMultimap(HashMultimap.create());
      toStream(plan).forEach(e -> map.put(e.getKey(), e.getValue()));
      return map;
    }
  }

  private <K, V> SetMultimap<K, V> toMutableMap(TransitiveClosurePairNode<K, V> plan) {
    SetMultimap<K, V> closure = toMutableMap(plan.getLeftParent());

    SetMultimap<V, V> right = toImmutableMap(plan.getRightParent());
    List<Map.Entry<K, V>> iteration = new ArrayList<>(closure.entries()); //TODO: avoid list
    while (!iteration.isEmpty()) {
      iteration = StreamSupport.stream(new PairStreamMapJoinSpliterator<>(
              iteration.parallelStream().map(t -> entry(t.getValue(), t.getKey())).spliterator(),
              right
      ), true)
              .map(Map.Entry::getValue)
              .filter(t -> closure.put(t.getKey(), t.getValue()))
              .collect(Collectors.toList());
    }
    return closure;
  }

  private <T> Stream<T> toStream(Supplier<Spliterator<T>> spliterator) {
    return StreamSupport.stream(spliterator, 0, true); //TODO: characteristics
  }

  private <T> void iterate(Set<T> closure, Function<Spliterator<T>, Spliterator<T>> add) {
    //TODO: avoid list creation
    List<T> iteration = new ArrayList<>(closure);
    while (!iteration.isEmpty()) {
      iteration = StreamSupport.stream(add.apply(iteration.spliterator()), true)
              .filter(closure::add)
              .collect(Collectors.toList());
    }
  }

  private static <K, V> Map.Entry<K, V> entry(K key, V value) {
    return new Pair<>(key, value);
  }
}
