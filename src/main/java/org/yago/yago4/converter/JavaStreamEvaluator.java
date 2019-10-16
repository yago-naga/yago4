package org.yago.yago4.converter;

import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import org.eclipse.rdf4j.model.Statement;
import org.yago.yago4.AnnotatedStatement;
import org.yago.yago4.converter.plan.*;
import org.yago.yago4.converter.utils.*;
import org.yago.yago4.converter.utils.stream.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class JavaStreamEvaluator {

  private final NTriplesReader nTriplesReader;
  private final NTriplesWriter nTriplesWriter;
  private final Map<PlanNode, Set> cache = new HashMap<>();
  private final Map<PairPlanNode, Multimap> cachePairs = new HashMap<>();

  public JavaStreamEvaluator(YagoValueFactory valueFactory) {
    nTriplesReader = new NTriplesReader(valueFactory);
    nTriplesWriter = new NTriplesWriter();
  }

  public void evaluateToNTriples(PlanNode<Statement> plan, Path outputFilePath) {
    nTriplesWriter.write(toStream(plan), outputFilePath);
  }

  public void evaluateToNTriplesStar(PlanNode<AnnotatedStatement> plan, Path outputFilePath) {
    nTriplesWriter.writeRdfStar(toStream(plan), outputFilePath);
  }

  public <T> List<T> evaluateToList(PlanNode<T> plan) {
    return toStream(plan).collect(Collectors.toList());
  }

  private <T> Stream<T> toStream(PlanNode<T> plan) {
    Set<T> cachedValue = cache.get(plan);
    if (cachedValue != null) {
      return cachedValue.parallelStream();
    } else if (plan instanceof CacheNode) {
      return toSet((CacheNode<T>) plan).parallelStream();
    } else if (plan instanceof CollectionNode) {
      return toStream((CollectionNode<T>) plan);
    } else if (plan instanceof DistinctNode) {
      return toSet((DistinctNode<T>) plan).parallelStream();
    } else if (plan instanceof FilterNode) {
      return toStream((FilterNode<T>) plan);
    } else if (plan instanceof FlatMapNode) {
      return toStream((FlatMapNode<?, T>) plan);
    } else if (plan instanceof FlatMapFromPairNode) {
      return toStream((FlatMapFromPairNode<?, ?, T>) plan);
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
    } else if (plan instanceof TSVReaderNode) {
      return (Stream<T>) toStream((TSVReaderNode) plan);
    } else if (plan instanceof SubtractNode) {
      return toStream((SubtractNode<T>) plan);
    } else if (plan instanceof TransitiveClosureNode) {
      return toSet((TransitiveClosureNode<T>) plan).parallelStream();
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

  private <KI, VI, TO> Stream<TO> toStream(FlatMapFromPairNode<KI, VI, TO> plan) {
    BiFunction<KI, VI, Stream<TO>> fn = plan.getFunction();
    return toStream(plan.getParent()).flatMap(e -> fn.apply(e.getKey(), e.getValue()));
  }

  private <T> Stream<T> toStream(IntersectionNode<T> plan) {
    return toStream(() -> new StreamSetJoinSpliterator<>(
            toStream(plan.getLeftParent()).spliterator(),
            toSet(plan.getRightParent())
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
    return RDFBinaryFormat.read(plan.getFilePath());
  }

  private Stream<String[]> toStream(TSVReaderNode plan) {
    try {
      return Files.lines(plan.getFilePath())
              .parallel()
              .map(l -> l.split("\t"));
    } catch (IOException e) {
      throw new EvaluationException(e);
    }
  }

  private <T> Stream<T> toStream(SubtractNode<T> plan) {
    return toStream(() -> new StreamSetAntiJoinSpliterator<>(
            toStream(plan.getLeftParent()).spliterator(),
            toSet(plan.getRightParent())
    ));
  }

  private <T> Stream<T> toStream(UnionNode<T> plan) {
    return plan.getParents().stream().flatMap(this::toStream);
  }


  private <K, V> Stream<V> toStream(ValuesNode<K, V> plan) {
    return toStream(plan.getParent()).map(Map.Entry::getValue);
  }

  private <K, V> Stream<Map.Entry<K, V>> toStream(PairPlanNode<K, V> plan) {
    Multimap<K, V> cachedValue = cachePairs.get(plan);
    if (cachedValue != null) {
      return cachedValue.entries().parallelStream();
    } else if (plan instanceof AggregateByKeyNode) {
      return toStream((AggregateByKeyNode) plan);
    } else if (plan instanceof CachePairNode) {
      return toMap((CachePairNode<K, V>) plan).entries().parallelStream();
    } else if (plan instanceof DistinctPairNode) {
      return toMap((DistinctPairNode<K, V>) plan).entries().parallelStream();
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
      return toMap((TransitiveClosurePairNode<K, V>) plan).entries().parallelStream();
    } else if (plan instanceof UnionPairNode) {
      return toStream((UnionPairNode<K, V>) plan);
    } else {
      throw new EvaluationException("Not supported plan node: " + plan);
    }
  }

  private <K, V> Stream<Map.Entry<K, Collection<V>>> toStream(AggregateByKeyNode<K, V> plan) {
    return toMap(plan.getParent()).groups().stream();
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
            toSet(plan.getRightParent())
    ));
  }

  private <K, V> Stream<Map.Entry<K, V>> toStream(JoinNode<K, V> plan) {
    return toStream(() -> new StreamMapJoinSpliterator<>(
            toStream(plan.getLeftParent()).spliterator(),
            toMap(plan.getRightParent())));
  }

  private <K, V1, V2> Stream<Map.Entry<K, Map.Entry<V1, V2>>> toStream(JoinPairNode<K, V1, V2> plan) {
    return toStream(() -> new PairStreamMapJoinSpliterator<>(
            toStream(plan.getLeftParent()).spliterator(),
            toMap(plan.getRightParent())));
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
            toSet(plan.getRightParent())
    ));
  }

  private <K, V> Stream<Map.Entry<K, V>> toStream(UnionPairNode<K, V> plan) {
    return plan.getParents().stream().flatMap(this::toStream);
  }

  private <T> boolean isAlreadySet(PlanNode<T> planNode) {
    return planNode instanceof CacheNode ||
            planNode instanceof CollectionNode ||
            planNode instanceof KeysNode && isAlreadyMap(((KeysNode<T, ?>) planNode).getParent()) ||
            planNode instanceof TransitiveClosureNode ||
            cache.containsKey(planNode);
  }

  private <T> Set<T> toSet(PlanNode<T> plan) {
    Set<T> value = cache.get(plan);
    if (value != null) {
      return value;
    } else if (plan instanceof CacheNode) {
      return toSet((CacheNode<T>) plan);
    } else if (plan instanceof CollectionNode) {
      return toSet((CollectionNode<T>) plan);
    } else if (plan instanceof DistinctNode) {
      return toSet((DistinctNode<T>) plan);
    } else if (plan instanceof KeysNode && isAlreadyMap(((KeysNode<T, ?>) plan).getParent())) {
      return toSet((KeysNode<T, ?>) plan);
    } else if (plan instanceof TransitiveClosureNode) {
      return toSet((TransitiveClosureNode<T>) plan);
    } else {
      return toStream(plan).collect(toSetCollector());
    }
  }

  private <T> Set<T> toSet(CacheNode<T> plan) {
    Set<T> value = toSet(plan.getParent());
    cache.put(plan, value);
    cache.put(plan.getParent(), value); //to allow using the parent value as cache key, avoids impact of a common mistake
    return value;
  }

  private <T> Set<T> toSet(CollectionNode<T> plan) {
    Collection<T> elements = plan.getElements();
    if (elements instanceof Set) {
      return (Set<T>) elements;
    } else {
      return new ObjectOpenHashSet<>(elements);
    }
  }

  private <T> Set<T> toSet(DistinctNode<T> plan) {
    return toSet(plan.getParent());
  }

  private <K, V> Set<K> toSet(KeysNode<K, V> plan) {
    return toMap(plan.getParent()).keySet();
  }

  private <T> Set<T> toSet(TransitiveClosureNode<T> plan) {
    Multimap<T, T> right = toMap(plan.getRightParent());

    Set<T> closure = toStream(plan.getLeftParent()).collect(toSetCollector());
    List<T> toDo = new ArrayList<>(closure);
    while (!toDo.isEmpty()) {
      for (var newElement : right.get(toDo.remove(toDo.size() - 1))) {
        if (closure.add(newElement)) {
          toDo.add(newElement);
        }
      }
    }
    return closure;
  }

  private <K, V> boolean isAlreadyMap(PairPlanNode<K, V> planNode) {
    return planNode instanceof CachePairNode ||
            planNode instanceof TransitiveClosurePairNode ||
            cachePairs.containsKey(planNode);
  }

  private <K, V> Multimap<K, V> toMap(PairPlanNode<K, V> plan) {
    Multimap<K, V> value = cachePairs.get(plan);
    if (value != null) {
      return value;
    } else if (plan instanceof CachePairNode) {
      return toMap((CachePairNode<K, V>) plan);
    } else if (plan instanceof DistinctPairNode) {
      return toMap((DistinctPairNode<K, V>) plan);
    } else if (plan instanceof TransitiveClosurePairNode) {
      return toMap((TransitiveClosurePairNode<K, V>) plan);
    } else {
      return toStream(plan).collect(toMultimapCollector());
    }
  }

  private <K, V> Multimap<K, V> toMap(CachePairNode<K, V> plan) {
    Multimap<K, V> value = toMap(plan.getParent());
    cachePairs.put(plan, value);
    cachePairs.put(plan.getParent(), value); //to allow using the parent value as cache key, avoids impact of a common mistake
    return value;
  }

  private <K, V> Multimap<K, V> toMap(DistinctPairNode<K, V> plan) {
    return toStream(plan.getParent()).collect(toMultimapCollector(ArraySetHashMapMultimap::new));
  }

  private <K, V> Multimap<K, V> toMap(TransitiveClosurePairNode<K, V> plan) {
    Multimap<V, V> right = toMap(plan.getRightParent());

    Multimap<K, V> closure = toStream(plan.getLeftParent()).collect(toMultimapCollector());
    List<Map.Entry<K, V>> toDo = new ArrayList<>(closure.entries());
    while (!toDo.isEmpty()) {
      var oldElement = toDo.remove(toDo.size() - 1);
      for (var rightElement : right.get(oldElement.getValue())) {
        if (closure.put(oldElement.getKey(), rightElement)) {
          toDo.add(Map.entry(oldElement.getKey(), rightElement));
        }
      }
    }
    return closure;
  }

  private static <T> Stream<T> toStream(Supplier<Spliterator<T>> spliterator) {
    return StreamSupport.stream(spliterator, 0, true); //TODO: characteristics
  }

  private static <T> Collector<T, ObjectOpenHashSet<T>, ObjectOpenHashSet<T>> toSetCollector() {
    return Collector.of(
            ObjectOpenHashSet::new,
            ObjectOpenHashSet::add,
            (a, b) -> {
              if (a.size() > b.size()) {
                a.addAll(b);
                return a;
              } else {
                b.addAll(a);
                return b;
              }
            }
    );
  }

  private static <K, V> Collector<Map.Entry<K, V>, Multimap<K, V>, Multimap<K, V>> toMultimapCollector() {
    return toMultimapCollector(ArrayListHashMapMultimap::new);
  }

  private static <K, V> Collector<Map.Entry<K, V>, Multimap<K, V>, Multimap<K, V>> toMultimapCollector(Supplier<Multimap<K, V>> multimapSupplier) {
    return Collector.of(
            multimapSupplier,
            (m, i) -> m.put(i.getKey(), i.getValue()),
            (a, b) -> {
              if (a.size() > b.size()) {
                a.putAll(b);
                return a;
              } else {
                b.putAll(a);
                return b;
              }
            }
    );
  }
}
