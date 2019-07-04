package org.yago.yago4.converter;

import com.google.common.collect.Maps;
import org.junit.jupiter.api.Test;
import org.yago.yago4.converter.plan.PlanNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

abstract class AbstractTestEvaluator {

  abstract <T> List<T> evaluate(PlanNode<T> plan);

  protected <T extends Comparable<T>> void assertEq(List<T> expected, List<T> actual) {
    actual = new ArrayList<>(actual);
    actual.sort(Comparator.naturalOrder());
    assertEquals(expected, actual);
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
            evaluate(PlanNode.fromCollection(List.of(Maps.immutableEntry(1, 'a'), Maps.immutableEntry(2, 'b'), Maps.immutableEntry(3, 'c')))
                    .mapToPair(t -> t)
                    .intersection(PlanNode.fromCollection(List.of(1, 3, 4)))
                    .values())
    );
  }

  @Test
  void testJoinPairPair() {
    assertEq(
            List.of(5, 10),
            evaluate(PlanNode.fromCollection(List.of(1, 2))
                    .mapToPair(t -> Maps.immutableEntry(t * 2, t))
                    .join(PlanNode.fromCollection(List.of(4, 8)).mapToPair(t -> Maps.immutableEntry(t / 2, t)))
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
                    .mapToPair(t -> Maps.immutableEntry(t / 2, t))
                    .subtract(PlanNode.fromCollection(List.of(2, 3)))
                    .values())
    );
  }

  @Test
  void testTransitiveClosure() {
    assertEq(
            List.of(1, 2, 3, 4),
            evaluate(PlanNode.fromCollection(Collections.singletonList(1))
                    .transitiveClosure(PlanNode.fromCollection(List.of(Maps.immutableEntry(1, 2), Maps.immutableEntry(2, 3), Maps.immutableEntry(1, 4), Maps.immutableEntry(5, 6))).mapToPair(t -> t)))
    );
  }

  @Test
  void testPairTransitiveClosure() {
    assertEq(
            List.of(1, 2, 3, 4),
            evaluate(PlanNode.fromCollection(Collections.singletonList(1))
                    .mapToPair(t -> Maps.immutableEntry(t, t))
                    .transitiveClosure(PlanNode.fromCollection(List.of(Maps.immutableEntry(1, 2), Maps.immutableEntry(2, 3), Maps.immutableEntry(1, 4), Maps.immutableEntry(5, 6))).mapToPair(t -> t))
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
}
