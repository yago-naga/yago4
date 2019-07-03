package org.yago.yago4.converter;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.yago.yago4.converter.plan.*;
import org.yago.yago4.converter.utils.NTriplesReader;
import org.yago.yago4.converter.utils.NTriplesWriter;
import org.yago.yago4.converter.utils.RDFBinaryFormat;
import org.yago.yago4.converter.utils.stream.StreamHashMapJoinSpliterator;
import org.yago.yago4.converter.utils.stream.StreamHashSetAntiJoinSpliterator;
import org.yago.yago4.converter.utils.stream.StreamHashSetJoinSpliterator;

import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class JavaStreamEvaluator {

  private final ValueFactory valueFactory;
  private final NTriplesReader nTriplesReader;
  private final NTriplesWriter nTriplesWriter;

  public JavaStreamEvaluator(ValueFactory valueFactory) {
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
    if (plan instanceof AntiJoinNode) {
      return toStream((AntiJoinNode<T, ?>) plan);
    } else if (plan instanceof FilterNode) {
      return toStream((FilterNode<T>) plan);
    } else if (plan instanceof FlatMapNode) {
      return toStream((FlatMapNode<?, T>) plan);
    } else if (plan instanceof JoinNode) {
      return toStream((JoinNode<?, ?, T, ?>) plan);
    } else if (plan instanceof CollectionNode) {
      return toStream((CollectionNode<T>) plan);
    } else if (plan instanceof MapNode) {
      return toStream((MapNode<?, T>) plan);
    } else if (plan instanceof NTriplesReaderNode) {
      return (Stream<T>) toStream((NTriplesReaderNode) plan);
    } else if (plan instanceof RDFBinaryReaderNode) {
      return (Stream<T>) toStream((RDFBinaryReaderNode) plan);
    } else if (plan instanceof TransitiveClosureNode) {
      return toSet((TransitiveClosureNode<T, ?, ?>) plan).stream();
    } else if (plan instanceof UnionNode) {
      return toStream((UnionNode<T>) plan);
    } else {
      throw new EvaluationException("Not supported plan node: " + plan);
    }
  }

  private <T1, T2> Stream<T1> toStream(AntiJoinNode<T1, T2> plan) {
    return toStream(new StreamHashSetAntiJoinSpliterator<>(
            toStream(plan.getLeftParent()).spliterator(),
            toSet(plan.getRightParent()),
            plan.getLeftKey()));
  }

  private <T> Stream<T> toStream(FilterNode<T> plan) {
    return toStream(plan.getParent()).filter(plan.getPredicate());
  }

  private <TI, TO> Stream<TO> toStream(FlatMapNode<TI, TO> plan) {
    return toStream(plan.getParent()).flatMap(plan.getFunction());
  }

  private <T1, T2, TO, K> Stream<TO> toStream(JoinNode<T1, T2, TO, K> plan) {
    Function<T2, K> rightKey = plan.getRightKey();
    if (rightKey == Function.identity()) {
      return toStream(new StreamHashSetJoinSpliterator<>(
              toStream(plan.getLeftParent()).spliterator(),
              toSet(plan.getRightParent()),
              (Function<T1, T2>) plan.getLeftKey(),
              plan.getMergeFunction()));
    } else {
      return toStream(new StreamHashMapJoinSpliterator<>(
              toStream(plan.getLeftParent()).spliterator(),
              toMultimap(toStream(plan.getRightParent()), rightKey),
              plan.getLeftKey(),
              plan.getMergeFunction()));
    }
  }

  private <T> Stream<T> toStream(CollectionNode<T> plan) {
    return plan.getElements().parallelStream();
  }

  private <TI, TO> Stream<TO> toStream(MapNode<TI, TO> plan) {
    return toStream(plan.getParent()).map(plan.getFunction());
  }

  private Stream<Statement> toStream(NTriplesReaderNode plan) {
    return nTriplesReader.read(plan.getFilePath());
  }

  private Stream<Statement> toStream(RDFBinaryReaderNode plan) {
    return RDFBinaryFormat.read(valueFactory, plan.getFilePath());
  }

  private <T> Stream<T> toStream(UnionNode<T> plan) {
    return Stream.concat(toStream(plan.getLeftParent()), toStream(plan.getRightParent()));
  }

  private <T> boolean isAlreadyASet(PlanNode<T> plan) {
    return plan instanceof FilterNode && isAlreadyASet(((FilterNode<T>) plan).getParent()) ||
            plan instanceof CollectionNode ||
            plan instanceof TransitiveClosureNode ||
            plan instanceof UnionNode && (isAlreadyASet(((UnionNode<T>) plan).getLeftParent()) || isAlreadyASet(((UnionNode<T>) plan).getRightParent()));
  }

  private <T> Set<T> toSet(PlanNode<T> plan) {
    if (plan instanceof FilterNode) {
      return toSet((FilterNode<T>) plan);
    } else if (plan instanceof CollectionNode) {
      return toSet((CollectionNode<T>) plan);
    } else if (plan instanceof TransitiveClosureNode) {
      return toSet((TransitiveClosureNode<T, ?, ?>) plan);
    } else if (plan instanceof UnionNode) {
      return toSet((UnionNode<T>) plan);
    } else {
      return toStream(plan).collect(Collectors.toCollection(ConcurrentHashMap::newKeySet));
    }
  }

  private <T> Set<T> toSet(FilterNode<T> plan) {
    if (isAlreadyASet(plan.getParent())) {
      Set<T> elements = toSet(plan.getParent());
      elements.removeIf(plan.getPredicate());
      return elements;
    } else {
      return toStream(plan).collect(Collectors.toCollection(ConcurrentHashMap::newKeySet));
    }
  }

  private <T> Set<T> toSet(CollectionNode<T> plan) {
    Collection<T> elements = plan.getElements();
    if (elements instanceof Set) {
      return (Set<T>) elements;
    } else {
      return elements.stream().collect(Collectors.toCollection(ConcurrentHashMap::newKeySet));
    }
  }

  private <T1, T2, K> Set<T1> toSet(TransitiveClosureNode<T1, T2, K> plan) {
    Set<T1> closure = toSet(plan.getLeftParent());

    Function<T2, K> rightKey = plan.getRightKey();
    if (rightKey == Function.identity()) {
      Set<T2> right = toSet(plan.getRightParent());
      iterate(closure, iteration -> new StreamHashSetJoinSpliterator<>(
              iteration,
              right,
              (Function<T1, T2>) plan.getLeftKey(),
              plan.getMergeFunction()
      ));
    } else {
      Multimap<K, T2> right = toMultimap(toStream(plan.getRightParent()), rightKey);
      iterate(closure, iteration -> new StreamHashMapJoinSpliterator<>(
              iteration,
              right,
              plan.getLeftKey(),
              plan.getMergeFunction()
      ));
    }
    return closure;
  }

  private <T> Set<T> toSet(UnionNode<T> plan) {
    if (isAlreadyASet(plan.getLeftParent())) {
      Set<T> elements = toSet(plan.getLeftParent());
      toStream(plan.getRightParent()).forEach(elements::add);
      return elements;
    } else if (isAlreadyASet(plan.getRightParent())) {
      Set<T> elements = toSet(plan.getRightParent());
      toStream(plan.getLeftParent()).forEach(elements::add);
      return elements;
    } else {
      return toStream(plan).collect(Collectors.toCollection(ConcurrentHashMap::newKeySet));
    }
  }

  private <T> Stream<T> toStream(Spliterator<T> spliterator) {
    return StreamSupport.stream(spliterator, true);
  }

  private <K, V> Multimap<K, V> toMultimap(Stream<V> s, Function<V, K> computeKey) {
    Multimap<K, V> map = Multimaps.synchronizedMultimap(ArrayListMultimap.create());
    s.forEach(t -> map.put(computeKey.apply(t), t));
    return map;
  }

  private <T> void iterate(Set<T> closure, Function<Spliterator<T>, Spliterator<T>> add) {
    //TODO: avoid list creation
    List<T> iteration = new ArrayList<>(closure);
    while (!iteration.isEmpty()) {
      iteration = toStream(add.apply(iteration.spliterator()))
              .filter(closure::add)
              .collect(Collectors.toList());
    }
  }
}
