package org.yago.yago4.converter;

import org.junit.jupiter.api.Test;
import org.yago.yago4.converter.plan.PlanNode;
import org.yago.yago4.converter.utils.Pair;
import org.yago.yago4.converter.utils.YagoValueFactory;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TestJavaStreamEvaluator {

  private static final JavaStreamEvaluator evaluator = new JavaStreamEvaluator(YagoValueFactory.getInstance());

  @Test
  void testFilter() {
    assertEquals(
            List.of(1, 2),
            evaluator.evaluateToList(PlanNode.fromCollection(List.of(1, 2, 3))
                    .filter(t -> t < 3))
    );
  }

  @Test
  void testMap() {
    assertEquals(
            List.of(2, 4, 6),
            evaluator.evaluateToList(PlanNode.fromCollection(List.of(1, 2, 3))
                    .map(t -> 2 * t))
    );
  }

  @Test
  void testFlatMap() {
    assertEquals(
            List.of(1, 2, 2, 4, 3, 6),
            evaluator.evaluateToList(PlanNode.fromCollection(List.of(1, 2, 3))
                    .flatMap(t -> Stream.of(t, 2 * t)))
    );
  }

  @Test
  void testIntersection() {
    assertEquals(
            List.of(1, 3),
            evaluator.evaluateToList(PlanNode.fromCollection(List.of(1, 2, 3))
                    .intersection(PlanNode.fromCollection(List.of(1, 3, 4))))
    );
  }

  @Test
  void testJoinPairPair() {
    assertEquals(
            List.of(5, 10),
            evaluator.evaluateToList(PlanNode.fromCollection(List.of(1, 2))
                    .mapToPair(t -> new Pair<>(t * 2, t))
                    .join(PlanNode.fromCollection(List.of(4, 8)).mapToPair(t -> new Pair<>(t / 2, t)))
                    .mapValue(e -> e.getKey() + e.getValue())
                    .values()
            )
    );
  }

  @Test
  void testSubtract() {
    assertEquals(
            List.of(2, 8),
            evaluator.evaluateToList(PlanNode.fromCollection(List.of(2, 4, 6, 8))
                    .subtract(PlanNode.fromCollection(List.of(4, 6))))
    );
  }

  @Test
  void testSubtractPair() {
    assertEquals(
            List.of(2, 8),
            evaluator.evaluateToList(PlanNode.fromCollection(List.of(2, 4, 6, 8))
                    .mapToPair(t -> new Pair<>(t / 2, t))
                    .subtract(PlanNode.fromCollection(List.of(2, 3)))
                    .values())
    );
  }

  @Test
  void testTransitiveClosure() {
    assertEquals(
            List.of(1, 2, 3, 4),
            evaluator.evaluateToList(PlanNode.fromCollection(Collections.singletonList(1))
                    .transitiveClosure(PlanNode.fromCollection(List.of(new Pair<>(1, 2), new Pair<>(2, 3), new Pair<>(1, 4), new Pair<>(5, 6))).mapToPair(t -> t)))
    );
  }

  @Test
  void testPairTransitiveClosure() {
    assertEquals(
            List.of(1, 2, 4, 3),
            evaluator.evaluateToList(PlanNode.fromCollection(Collections.singletonList(1))
                    .mapToPair(t -> new Pair<>(t, t))
                    .transitiveClosure(PlanNode.fromCollection(List.of(new Pair<>(1, 2), new Pair<>(2, 3), new Pair<>(1, 4), new Pair<>(5, 6))).mapToPair(t -> t))
                    .values())
    );
  }

  @Test
  void testUnion() {
    assertEquals(
            List.of(1, 2, 9, 10),
            evaluator.evaluateToList(PlanNode.fromCollection(List.of(1, 2))
                    .union(PlanNode.fromCollection(List.of(9, 10))))
    );
  }

  @Test
  void testCache() {
    AtomicInteger counter = new AtomicInteger(0);
    var cached = PlanNode.fromCollection(List.of(0, 0)).map(t -> t + counter.addAndGet(1)).cache();
    assertEquals(
            List.of(1, 2, 1, 2),
            evaluator.evaluateToList(cached.union(cached))
    );
  }

  @Test
  void testNoCache() {
    AtomicInteger counter = new AtomicInteger(0);
    var stream = PlanNode.fromCollection(List.of(0, 0)).map(t -> t + counter.addAndGet(1));
    var results = evaluator.evaluateToList(stream.union(stream));
    results.sort(Integer::compareTo);
    assertEquals(List.of(1, 2, 3, 4), results);
  }
}
