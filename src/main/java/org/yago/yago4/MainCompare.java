package org.yago.yago4;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.rio.ntriples.NTriplesUtil;
import org.yago.yago4.converter.JavaStreamEvaluator;
import org.yago.yago4.converter.plan.PlanNode;
import org.yago.yago4.converter.utils.YagoValueFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Code to compare Yago 3 and Yago 4
 */
public class MainCompare {
  private static final YagoValueFactory VALUE_FACTORY = YagoValueFactory.getInstance();
  private static final JavaStreamEvaluator evaluator = new JavaStreamEvaluator(VALUE_FACTORY);


  public static void main(String[] args) throws ParseException, IOException {
    Options options = new Options();
    options.addRequiredOption("yago3", "yago3File", true, "Yago 3 uncompressed TSV file");
    options.addRequiredOption("yago4", "yago4File", true, "Yago 4 NTriples file");
    options.addRequiredOption("mapping", "yago3To4Mapping", true, "Yago 3 to 4 mapping as a TSV File");

    CommandLine params = (new DefaultParser()).parse(options, args);

    var yago3 = PlanNode.readTSV(Path.of(params.getOptionValue("yago3File")));
    var yago3SPtoTriple = yago3
            .flatMap(l -> {
              if (l.length > 3 && l[2].startsWith("<")) {
                return Stream.of(Map.entry(Map.entry(NTriplesUtil.parseURI(l[1], VALUE_FACTORY).stringValue(), NTriplesUtil.parseURI(l[2], VALUE_FACTORY).stringValue()), VALUE_FACTORY.createStatement(
                        NTriplesUtil.parseResource(l[1], VALUE_FACTORY),
                        NTriplesUtil.parseURI(l[2], VALUE_FACTORY),
                        NTriplesUtil.parseValue(l[3]
                                        .replace("xsd:date", "<http://www.w3.org/2001/XMLSchema#date>")
                                        .replace("xsd:integer", "<http://www.w3.org/2001/XMLSchema#integer>")
                                        .replace("xsd:decimal", "<http://www.w3.org/2001/XMLSchema#decimal>"),
                                VALUE_FACTORY)
                )));
              } else {
                return Stream.empty();
              }
            })
            .mapToPair(t -> t);

    var yago4PtoS = PlanNode.readNTriples(Path.of(params.getOptionValue("yago4File")))
            .filter(s -> s.getSubject() instanceof IRI)
            .mapToPair(s -> Map.entry(s.getPredicate().stringValue(), ((IRI) s.getSubject()).getLocalName()));
    var mapping4To3 = PlanNode.readTSV(Path.of(params.getOptionValue("yago3To4Mapping"))).flatMap(l -> {
      if (l.length > 1) {
        return Stream.of(Map.entry(l[1].strip(), l[0].strip()));
      } else {
        return Stream.empty();
      }
    }).mapToPair(t -> t);

    evaluator.evaluateToList(mapping4To3.map(Map::entry)).forEach(System.out::println);

    var existingSP = yago4PtoS
            .join(mapping4To3)
            .values();

    var factsMissingInYago4 = yago3SPtoTriple.subtract(existingSP);

    evaluator.evaluateToNTriples(factsMissingInYago4.values(), Path.of("output.nt"));
  }
}
