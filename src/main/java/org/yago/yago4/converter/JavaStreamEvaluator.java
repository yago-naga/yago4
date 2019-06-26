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

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
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
    return StreamSupport.stream(new StreamHashSetAntiJoinSpliterator<>(
                    toStream(plan.getLeftParent()).spliterator(),
                    toSet(plan.getRightParent()),
                    plan.getLeftKey()),
            true
    );
  }

  private <T> Stream<T> toStream(FilterNode<T> plan) {
    return toStream(plan.getParent()).filter(plan.getPredicate());
  }

  private <TI, TO> Stream<TO> toStream(FlatMapNode<TI, TO> plan) {
    return toStream(plan.getParent()).flatMap(plan.getFunction());
  }

  private <T1, T2, TO, K> Stream<TO> toStream(JoinNode<T1, T2, TO, K> plan) {
    Multimap<K, T2> right = Multimaps.synchronizedMultimap(ArrayListMultimap.create());
    Function<T2, K> rightKey = plan.getRightKey();
    toStream(plan.getRightParent()).forEach(t -> right.put(rightKey.apply(t), t));

    return join(toStream(plan.getLeftParent()), right, plan.getLeftKey(), plan.getMergeFunction());
  }

  private <T1, T2, TO, K> Stream<TO> join(Stream<T1> left, Multimap<K, T2> right, Function<T1, K> leftKey, BiFunction<T1, T2, TO> merge) {
    return StreamSupport.stream(new StreamHashMapJoinSpliterator<>(left.spliterator(), right, leftKey, merge), true); //TODO: closure?
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

  private <T> boolean isAlreadySet(PlanNode<T> plan) {
    return plan instanceof FilterNode && isAlreadySet(((FilterNode<T>) plan).getParent()) ||
            plan instanceof CollectionNode ||
            plan instanceof TransitiveClosureNode;
  }

  private <T> Set<T> toSet(FilterNode<T> plan) {
    if (isAlreadySet(plan.getParent())) {
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
    Multimap<K, T2> right = ArrayListMultimap.create();
    Function<T2, K> rightKey = plan.getRightKey();
    toStream(plan.getRightParent()).forEachOrdered(t -> right.put(rightKey.apply(t), t)); //TODO: concurrent

    Set<T1> closure = toSet(plan.getLeftParent());

    //TODO: avoid list creation
    List<T1> iteration = new ArrayList<>(closure);
    while (!iteration.isEmpty()) {
      iteration = join(iteration.stream(), right, plan.getLeftKey(), plan.getMergeFunction())
              .filter(t -> !closure.contains(t))
              .peek(closure::add)
              .collect(Collectors.toList());
    }
    return closure;
  }

  private <T> Set<T> toSet(UnionNode<T> plan) {
    if (isAlreadySet(plan.getLeftParent())) {
      Set<T> elements = toSet(plan.getLeftParent());
      toStream(plan.getRightParent()).forEach(elements::add);
      return elements;
    } else if (isAlreadySet(plan.getRightParent())) {
      Set<T> elements = toSet(plan.getRightParent());
      toStream(plan.getLeftParent()).forEach(elements::add);
      return elements;
    } else {
      return toStream(plan).collect(Collectors.toCollection(ConcurrentHashMap::newKeySet));
    }
  }
}
