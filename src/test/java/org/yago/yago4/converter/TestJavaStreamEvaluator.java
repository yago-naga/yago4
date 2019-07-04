package org.yago.yago4.converter;

import org.junit.jupiter.api.Test;
import org.yago.yago4.converter.plan.PlanNode;
import org.yago.yago4.converter.utils.YagoValueFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

class TestJavaStreamEvaluator extends AbstractTestEvaluator {

  private static final JavaStreamEvaluator evaluator = new JavaStreamEvaluator(YagoValueFactory.getInstance());

  @Override
  <T> List<T> evaluate(PlanNode<T> plan) {
    return evaluator.evaluateToList(plan);
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
}
