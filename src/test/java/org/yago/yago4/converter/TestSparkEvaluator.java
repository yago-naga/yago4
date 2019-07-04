package org.yago.yago4.converter;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.yago.yago4.converter.plan.PlanNode;
import org.yago.yago4.converter.utils.YagoValueFactory;

import java.util.List;

class TestSparkEvaluator extends AbstractTestEvaluator {

  private static final SparkEvaluator evaluator = new SparkEvaluator(YagoValueFactory.getInstance(), new JavaSparkContext(new SparkConf().setAppName("test").setMaster("local[4]")));

  @Override
  <T> List<T> evaluate(PlanNode<T> plan) {
    return evaluator.evaluateToList(plan);
  }
}
