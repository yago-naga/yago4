package org.yago.yago4.converter;

import com.google.common.collect.Maps;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.eclipse.rdf4j.model.Statement;
import org.yago.yago4.converter.plan.*;
import org.yago.yago4.converter.utils.NTriplesReader;
import org.yago.yago4.converter.utils.NTriplesWriter;
import org.yago.yago4.converter.utils.RDFBinaryFormat;
import org.yago.yago4.converter.utils.YagoValueFactory;
import scala.Tuple2;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class SparkEvaluator {

  private final YagoValueFactory valueFactory;
  private final JavaSparkContext sparkContext;
  private final NTriplesReader nTriplesReader;
  private final NTriplesWriter nTriplesWriter;

  public SparkEvaluator(YagoValueFactory valueFactory, JavaSparkContext sparkContext) {
    this.valueFactory = valueFactory;
    this.sparkContext = sparkContext;
    nTriplesReader = new NTriplesReader(valueFactory);
    nTriplesWriter = new NTriplesWriter();
  }

  public void evaluateToNTriples(PlanNode<Statement> plan, Path outputFilePath) {
    toRDD(plan).map(nTriplesWriter::toString).saveAsTextFile(outputFilePath.toAbsolutePath().toString());
  }

  public <T> List<T> evaluateToList(PlanNode<T> plan) {
    return toRDD(plan).collect();
  }

  private <T> JavaRDD<T> toRDD(PlanNode<T> plan) {
    if (plan instanceof CacheNode) {
      return toRDD((CacheNode<T>) plan);
    } else if (plan instanceof CollectionNode) {
      return toRDD((CollectionNode<T>) plan);
    } else if (plan instanceof FilterNode) {
      return toRDD((FilterNode<T>) plan);
    } else if (plan instanceof FlatMapNode) {
      return toRDD((FlatMapNode<?, T>) plan);
    } else if (plan instanceof IntersectionNode) {
      return toRDD((IntersectionNode<T>) plan);
    } else if (plan instanceof KeysNode) {
      return toRDD((KeysNode<T, ?>) plan);
    } else if (plan instanceof MapNode) {
      return toRDD((MapNode<?, T>) plan);
    } else if (plan instanceof MapFromPairNode) {
      return toRDD((MapFromPairNode<?, ?, T>) plan);
    } else if (plan instanceof NTriplesReaderNode) {
      return (JavaRDD<T>) toRDD((NTriplesReaderNode) plan);
    } else if (plan instanceof RDFBinaryReaderNode) {
      return (JavaRDD<T>) toRDD((RDFBinaryReaderNode) plan);
    } else if (plan instanceof SubtractNode) {
      return toRDD((SubtractNode<T>) plan);
    } else if (plan instanceof TransitiveClosureNode) {
      return toRDD((TransitiveClosureNode<T>) plan);
    } else if (plan instanceof UnionNode) {
      return toRDD((UnionNode<T>) plan);
    } else if (plan instanceof ValuesNode) {
      return toRDD((ValuesNode<?, T>) plan);
    } else {
      throw new EvaluationException("Not supported plan node: " + plan);
    }
  }

  private <T> JavaRDD<T> toRDD(CacheNode<T> plan) {
    return toRDD(plan.getParent()).cache();
  }

  private <T> JavaRDD<T> toRDD(CollectionNode<T> plan) {
    var elements = plan.getElements();
    return sparkContext.parallelize(elements instanceof List ? (List<T>) elements : new ArrayList<>(elements));
  }

  private <T> JavaRDD<T> toRDD(FilterNode<T> plan) {
    return toRDD(plan.getParent()).filter(plan.getPredicate()::test);
  }

  private <TI, TO> JavaRDD<TO> toRDD(FlatMapNode<TI, TO> plan) {
    var fn = plan.getFunction();
    return toRDD(plan.getParent()).flatMap(t -> fn.apply(t).iterator());
  }

  private <T> JavaRDD<T> toRDD(IntersectionNode<T> plan) {
    return toRDD(plan.getLeftParent()).intersection(toRDD(plan.getRightParent()));
  }

  private <K, V> JavaRDD<K> toRDD(KeysNode<K, V> plan) {
    return toRDD(plan.getParent()).keys();
  }


  private <TI, TO> JavaRDD<TO> toRDD(MapNode<TI, TO> plan) {
    return toRDD(plan.getParent()).map(plan.getFunction()::apply);
  }

  private <KI, VI, TO> JavaRDD<TO> toRDD(MapFromPairNode<KI, VI, TO> plan) {
    var fn = plan.getFunction();
    return toRDD(plan.getParent()).map(e -> fn.apply(e._1, e._2));
  }

  private JavaRDD<Statement> toRDD(NTriplesReaderNode plan) {
    return sparkContext.textFile(plan.getFilePath().toAbsolutePath().toString())
            .flatMap(t -> nTriplesReader.parseNTriplesLineSafe(t).iterator());
  }

  private JavaRDD<Statement> toRDD(RDFBinaryReaderNode plan) {
    return sparkContext.binaryFiles(plan.getFilePath().toAbsolutePath().toString())
            .values()
            .flatMap(t -> {
              DataInputStream dataInputStream = t.open();
              return RDFBinaryFormat.read(YagoValueFactory.getInstance(), dataInputStream).onClose(() -> {
                try {
                  dataInputStream.close();
                } catch (IOException e) {
                  throw new EvaluationException(e);
                }
              }).iterator();
            });
  }

  private <T> JavaRDD<T> toRDD(SubtractNode<T> plan) {
    return toRDD(plan.getLeftParent()).subtract(toRDD(plan.getRightParent()));
  }

  private <T> JavaRDD<T> toRDD(TransitiveClosureNode<T> plan) {
    return evaluateClosure(toRDD(plan.getLeftParent())
            .mapToPair(SparkEvaluator::toTuple2), toRDD(plan.getRightParent()))
            .values();
  }

  private <T> JavaRDD<T> toRDD(UnionNode<T> plan) {
    return sparkContext.union((JavaRDD<T>[]) plan.getParents().stream().map(this::toRDD).toArray(JavaRDD[]::new));
  }

  private <K, V> JavaRDD<V> toRDD(ValuesNode<K, V> plan) {
    return toRDD(plan.getParent()).values();
  }

  private <K, V> JavaPairRDD<K, V> toRDD(PairPlanNode<K, V> plan) {
    if (plan instanceof CachePairNode) {
      return toRDD((CachePairNode<K, V>) plan);
    } else if (plan instanceof FilterPairNode) {
      return toRDD((FilterPairNode<K, V>) plan);
    } else if (plan instanceof FlatMapPairNode) {
      return toRDD((FlatMapPairNode<?, ?, K, V>) plan);
    } else if (plan instanceof IntersectionPairNode) {
      return toRDD((IntersectionPairNode<K, V>) plan);
    } else if (plan instanceof JoinNode) {
      return toRDD((JoinNode<K, V>) plan);
    } else if (plan instanceof JoinPairNode) {
      return toRDD((JoinPairNode) plan);
    } else if (plan instanceof MapPairNode) {
      return toRDD((MapPairNode<?, ?, K, V>) plan);
    } else if (plan instanceof MapToPairNode) {
      return toRDD((MapToPairNode<?, K, V>) plan);
    } else if (plan instanceof SubtractPairNode) {
      return toRDD((SubtractPairNode<K, V>) plan);
    } else if (plan instanceof TransitiveClosurePairNode) {
      return toRDD((TransitiveClosurePairNode<K, V>) plan);
    } else if (plan instanceof UnionPairNode) {
      return toRDD((UnionPairNode<K, V>) plan);
    } else {
      throw new EvaluationException("Not supported plan node: " + plan);
    }
  }

  private <K, V> JavaPairRDD<K, V> toRDD(CachePairNode<K, V> plan) {
    return toRDD(plan.getParent()).cache();
  }

  private <K, V> JavaPairRDD<K, V> toRDD(FilterPairNode<K, V> plan) {
    var fn = plan.getPredicate();
    return toRDD(plan.getParent()).filter(t -> fn.test(t._1, t._2));
  }

  private <KI, VI, KO, VO> JavaPairRDD<KO, VO> toRDD(FlatMapPairNode<KI, VI, KO, VO> plan) {
    var fn = plan.getFunction();
    return toRDD(plan.getParent())
            .flatMapToPair(t -> fn.apply(t._1, t._2).map(SparkEvaluator::toTuple2).iterator());
  }


  private <K, V> JavaPairRDD<K, V> toRDD(IntersectionPairNode<K, V> plan) {
    return toRDD(plan.getLeftParent()).join(toRDD(plan.getRightParent()).mapToPair(SparkEvaluator::toTuple2))
            .mapToPair(t -> new Tuple2<>(t._1, t._2._1));
  }

  private <K, V> JavaPairRDD<K, V> toRDD(JoinNode<K, V> plan) {
    return toRDD(plan.getLeftParent())
            .mapToPair(SparkEvaluator::toTuple2)
            .join(toRDD(plan.getRightParent()))
            .mapToPair(t -> t._2);
  }

  private <K, V1, V2> JavaPairRDD<K, Map.Entry<V1, V2>> toRDD(JoinPairNode<K, V1, V2> plan) {
    return toRDD(plan.getLeftParent())
            .join(toRDD(plan.getRightParent()))
            .mapValues(t -> Maps.immutableEntry(t._1, t._2));
  }

  private <KI, VI, KO, VO> JavaPairRDD<KO, VO> toRDD(MapPairNode<KI, VI, KO, VO> plan) {
    var fn = plan.getFunction();
    return toRDD(plan.getParent()).mapToPair(t -> toTuple2(fn.apply(t._1, t._2)));
  }

  private <TI, KO, VO> JavaPairRDD<KO, VO> toRDD(MapToPairNode<TI, KO, VO> plan) {
    var fn = plan.getFunction();
    return toRDD(plan.getParent()).mapToPair(t -> toTuple2(fn.apply(t)));
  }

  private <K, V> JavaPairRDD<K, V> toRDD(SubtractPairNode<K, V> plan) {
    return toRDD(plan.getLeftParent())
            .leftOuterJoin(toRDD(plan.getRightParent()).mapToPair(SparkEvaluator::toTuple2))
            .flatMapToPair(e -> {
              if (e._2._2.isPresent()) {
                return Collections.emptyIterator();
              } else {
                return Collections.singleton(new Tuple2<>(e._1, e._2._1)).iterator();
              }
            });
  }

  private <K, V> JavaPairRDD<K, V> toRDD(TransitiveClosurePairNode<K, V> plan) {
    return evaluateClosure(toRDD(plan.getLeftParent()), toRDD(plan.getRightParent()));
  }

  private <K, V> JavaPairRDD<K, V> toRDD(UnionPairNode<K, V> plan) {
    return sparkContext.union(plan.getParents().stream().map(this::toRDD).toArray(JavaPairRDD[]::new));
  }

  private <K, V> JavaPairRDD<K, V> evaluateClosure(JavaPairRDD<K, V> input, JavaPairRDD<V, V> additions) {
    JavaPairRDD<V, K> closure = input.mapToPair(t -> new Tuple2<>(t._2, t._1));
    long oldCount = 0;
    var nextCount = closure.count();

    do {
      oldCount = nextCount;
      closure = closure.union(closure.join(additions).mapToPair(t -> new Tuple2<>(t._2._2, t._2._1))).distinct().cache();
      nextCount = closure.count();
    } while (nextCount != oldCount);
    return closure.mapToPair(t -> new Tuple2<>(t._2, t._1));
  }

  private static <K, V> Tuple2<K, V> toTuple2(Map.Entry<K, V> e) {
    return new Tuple2<>(e.getKey(), e.getValue());
  }

  private static <T> Tuple2<T, T> toTuple2(T e) {
    return new Tuple2<>(e, e);
  }
}
