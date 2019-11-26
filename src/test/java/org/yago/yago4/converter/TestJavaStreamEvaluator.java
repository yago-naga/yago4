package org.yago.yago4.converter;

import org.junit.jupiter.api.Test;
import org.yago.yago4.converter.plan.PairPlanNode;
import org.yago.yago4.converter.plan.PlanNode;
import org.yago.yago4.converter.utils.YagoValueFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TestJavaStreamEvaluator {

  private static final JavaStreamEvaluator evaluator = new JavaStreamEvaluator(YagoValueFactory.getInstance());

  private <T> List<T> evaluate(PlanNode<T> plan) {
    return evaluator.evaluateToList(plan);
  }

  private <T extends Comparable<T>> void assertEq(List<T> expected, List<T> actual) {
    actual = new ArrayList<>(actual);
    actual.sort(Comparator.naturalOrder());
    assertEquals(expected, actual);
  }

  @Test
  void testDistinct() {
    assertEq(
            List.of(1, 2),
            evaluate(PlanNode.fromCollection(List.of(1, 2, 1, 2, 2)).distinct())
    );
  }

  @Test
  void testDistinctPairs() {
    assertEq(
            List.of(1, 2),
            evaluate(PairPlanNode.fromCollection(List.of(Map.entry(1, 'a'), Map.entry(2, 'b'), Map.entry(1, 'a'))).distinct().keys())
    );
  }

  @Test
  void testFilter() {
    assertEq(
            List.of(1, 2),
            evaluate(PlanNode.fromCollection(List.of(1, 2, 3))
                    .filter(t -> t < 3))
    );
  }


  @Test
  void testFilterPair() {
    assertEq(
            List.of(2),
            evaluate(PairPlanNode.fromCollection(List.of(Map.entry(1, 2), Map.entry(3, 4)))
                    .filter((k, v) -> k == 1)
                    .values())
    );
  }

  @Test
  void testFilterKey() {
    assertEq(
            List.of(2),
            evaluate(PairPlanNode.fromCollection(List.of(Map.entry(1, 2), Map.entry(3, 4)))
                    .filterKey(k -> k == 1)
                    .values())
    );
  }

  @Test
  void testFilterValue() {
    assertEq(
            List.of(2),
            evaluate(PairPlanNode.fromCollection(List.of(Map.entry(1, 2), Map.entry(3, 4)))
                    .filterValue(v -> v == 2)
                    .values())
    );
  }

  @Test
  void testMap() {
    assertEq(
            List.of(2, 4, 6),
            evaluate(PlanNode.fromCollection(List.of(1, 2, 3))
                    .map(t -> 2 * t))
    );
  }

  @Test
  void testFlatMap() {
    assertEq(
            List.of(1, 2, 2, 3, 4, 6),
            evaluate(PlanNode.fromCollection(List.of(1, 2, 3))
                    .flatMap(t -> Stream.of(t, 2 * t)))
    );
  }

  @Test
  void testFlatMapFromPair() {
    assertEq(
            List.of(2, 3, 7, 12),
            evaluate(PairPlanNode.fromCollection(List.of(Map.entry(1, 2), Map.entry(3, 4)))
                    .flatMap((k, v) -> Stream.of(k + v, k * v)))
    );
  }

  @Test
  void testFlatMapPair() {
    assertEq(
            List.of(2, 3, 7, 12),
            evaluate(PairPlanNode.fromCollection(List.of(Map.entry(1, 2), Map.entry(3, 4)))
                    .flatMapPair((k, v) -> Stream.of(Map.entry(k, k + v), Map.entry(k, k * v)))
                    .values())
    );
  }

  @Test
  void testIntersection() {
    assertEq(
            List.of(1, 3),
            evaluate(PlanNode.fromCollection(List.of(1, 2, 3))
                    .intersection(PlanNode.fromCollection(List.of(1, 3, 4))))
    );
  }

  @Test
  void testIntersectionPair() {
    assertEq(
            List.of('a', 'c'),
            evaluate(PairPlanNode.fromCollection(List.of(Map.entry(1, 'a'), Map.entry(2, 'b'), Map.entry(3, 'c')))
                    .intersection(PlanNode.fromCollection(List.of(1, 3, 4)))
                    .values())
    );
  }

  @Test
  void testJoinValuePair() {
    assertEq(
            List.of(2, 4),
            evaluate(PlanNode.fromCollection(List.of(1, 3))
                    .join(PairPlanNode.fromCollection(List.of(Map.entry(1, 2), Map.entry(3, 4))))
                    .values()
            )
    );
  }

  @Test
  void testJoinPairPair() {
    assertEq(
            List.of(5, 10),
            evaluate(PlanNode.fromCollection(List.of(1, 2))
                    .mapToPair(t -> Map.entry(t * 2, t))
                    .join(PlanNode.fromCollection(List.of(4, 8)).mapToPair(t -> Map.entry(t / 2, t)))
                    .mapValue(e -> e.getKey() + e.getValue())
                    .values()
            )
    );
  }

  @Test
  void testSubtract() {
    assertEq(
            List.of(2, 8),
            evaluate(PlanNode.fromCollection(List.of(2, 4, 6, 8))
                    .subtract(PlanNode.fromCollection(List.of(4, 6))))
    );
  }

  @Test
  void testSubtractPair() {
    assertEq(
            List.of(2, 8),
            evaluate(PlanNode.fromCollection(List.of(2, 4, 6, 8))
                    .mapToPair(t -> Map.entry(t / 2, t))
                    .subtract(PlanNode.fromCollection(List.of(2, 3)))
                    .values())
    );
  }

  @Test
  void testTransitiveClosure() {
    assertEq(
            List.of(1, 2, 3, 4),
            evaluate(PlanNode.fromCollection(Collections.singletonList(1))
                    .transitiveClosure(PairPlanNode.fromCollection(List.of(Map.entry(1, 2), Map.entry(2, 3), Map.entry(1, 4), Map.entry(5, 6)))))
    );
  }

  @Test
  void testPairTransitiveClosure() {
    assertEq(
            List.of(1, 2, 3, 4),
            evaluate(PlanNode.fromCollection(Collections.singletonList(1))
                    .mapToPair(t -> Map.entry(t, t))
                    .transitiveClosure(PairPlanNode.fromCollection(List.of(Map.entry(1, 2), Map.entry(2, 3), Map.entry(1, 4), Map.entry(5, 6))))
                    .values())
    );
  }

  @Test
  void testUnion() {
    assertEq(
            List.of(1, 2, 9, 10),
            evaluate(PlanNode.fromCollection(List.of(1, 2))
                    .union(PlanNode.fromCollection(List.of(9, 10))))
    );
  }

  @Test
  void testUnionPair() {
    assertEq(
            List.of(2, 10),
            evaluate(PairPlanNode.fromCollection(List.of(Map.entry(1, 2)))
                    .union(PairPlanNode.fromCollection(List.of(Map.entry(9, 10))))
                    .values())
    );
  }

  @Test
  void testCache() {
    AtomicInteger counter = new AtomicInteger(0);
    var cached = PlanNode.fromCollection(List.of(0, 0)).map(t -> t + counter.addAndGet(1)).cache();
    assertEq(
            List.of(1, 1, 2, 2),
            evaluate(cached.union(cached))
    );
  }

  @Test
  void testNoCache() {
    AtomicInteger counter = new AtomicInteger(0);
    var stream = PlanNode.fromCollection(List.of(0, 0)).map(t -> t + counter.addAndGet(1));
    assertEq(List.of(1, 2, 3, 4), evaluate(stream.union(stream)));
  }

  @Test
  void testAggregateByKey() {
    assertEq(
            List.of(1, 2),
            evaluate(PairPlanNode.fromCollection(List.of(Map.entry(1, 'a'), Map.entry(2, 'b'), Map.entry(1, 'b'))).aggregateByKey().map((k, v) -> v.size()))
    );
  }
}
