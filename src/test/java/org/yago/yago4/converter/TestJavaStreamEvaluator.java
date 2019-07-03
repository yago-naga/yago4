package org.yago.yago4.converter;

import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.junit.jupiter.api.Test;
import org.yago.yago4.converter.plan.PlanNode;
import org.yago.yago4.converter.utils.Pair;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TestJavaStreamEvaluator {

  private static final JavaStreamEvaluator evaluator = new JavaStreamEvaluator(SimpleValueFactory.getInstance());

  @Test
  void testFilter() {
    assertEquals(
            Arrays.asList(1, 2),
            evaluator.evaluateToList(PlanNode.fromCollection(Arrays.asList(1, 2, 3))
                    .filter(t -> t < 3))
    );
  }

  @Test
  void testMap() {
    assertEquals(
            Arrays.asList(2, 4, 6),
            evaluator.evaluateToList(PlanNode.fromCollection(Arrays.asList(1, 2, 3))
                    .map(t -> 2 * t))
    );
  }

  @Test
  void testFlatMap() {
    assertEquals(
            Arrays.asList(1, 2, 2, 4, 3, 6),
            evaluator.evaluateToList(PlanNode.fromCollection(Arrays.asList(1, 2, 3))
                    .flatMap(t -> Stream.of(t, 2 * t)))
    );
  }

  @Test
  void testJoin() {
    assertEquals(
            Arrays.asList(5, 10),
            evaluator.evaluateToList(PlanNode.fromCollection(Arrays.asList(1, 2))
                    .join(PlanNode.fromCollection(Arrays.asList(4, 8)), t -> t, t -> t / 4, Integer::sum))
    );
  }

  @Test
  void testAntiJoin() {
    assertEquals(
            Arrays.asList(2, 8),
            evaluator.evaluateToList(PlanNode.fromCollection(Arrays.asList(2, 4, 6, 8))
                    .antiJoin(PlanNode.fromCollection(Arrays.asList(2, 3)), t -> t / 2))
    );
  }

  @Test
  void testTransitiveClosure() {
    assertEquals(
            Arrays.asList(1, 2, 3, 4),
            evaluator.evaluateToList(PlanNode.fromCollection(Collections.singletonList(1))
                    .transitiveClosure(PlanNode.fromCollection(Arrays.asList(new Pair<>(1, 2), new Pair<>(2, 3), new Pair<>(1, 4), new Pair<>(5, 6))), t -> t, Pair::getKey, (t1, t2) -> t2.getValue()))
    );
  }

  @Test
  void testUnion() {
    assertEquals(
            Arrays.asList(1, 2, 9, 10),
            evaluator.evaluateToList(PlanNode.fromCollection(Arrays.asList(1, 2))
                    .union(PlanNode.fromCollection(Arrays.asList(9, 10))))
    );
  }

  @Test
  void testCache() {
    AtomicInteger counter = new AtomicInteger(0);
    var cached = PlanNode.fromCollection(Arrays.asList(0, 0)).map(t -> t + counter.addAndGet(1)).cache();
    assertEquals(
            Arrays.asList(1, 2, 1, 2),
            evaluator.evaluateToList(cached.union(cached))
    );
  }

  @Test
  void testNoCache() {
    AtomicInteger counter = new AtomicInteger(0);
    var stream = PlanNode.fromCollection(Arrays.asList(0, 0)).map(t -> t + counter.addAndGet(1));
    var results = evaluator.evaluateToList(stream.union(stream));
    results.sort(Integer::compareTo);
    assertEquals(Arrays.asList(1, 2, 3, 4), results);
  }
}
