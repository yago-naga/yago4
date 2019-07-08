package org.yago.yago4.converter.utils;

import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sparql.SPARQLRepository;
import org.yago.yago4.ShaclSchema;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class CompareSchemaWithWikidata implements AutoCloseable {

  private static final String WDQS_ENDPOINT = "https://query.wikidata.org/sparql";

  public static void main(String[] args) {
    try (var self = new CompareSchemaWithWikidata()) {
      self.compareProperties();
      self.compareClasses();
    }
  }

  private final SPARQLRepository repository;

  private CompareSchemaWithWikidata() {
    repository = new SPARQLRepository(WDQS_ENDPOINT);
    repository.setAdditionalHttpHeaders(Collections.singletonMap("User-Agent", "Yago/4.0 (http://yago-knowledge.org)"));
    repository.init();
  }

  @Override
  public void close() {
    repository.shutDown();
  }

  private void compareProperties() {
    var wikidataMapping = getEquivalentProperties();
    var yagoMapping = ShaclSchema.getSchema().getPropertyShapes()
            .flatMap(shape -> shape.getFromProperties().map(wdp -> new Pair<>(wdp.stringValue(), shape.getProperty().stringValue())))
            .collect(Collectors.toSet());
    System.out.println();
    System.out.println("==== PROPERTIES ====");
    printDiff(wikidataMapping, yagoMapping);
    System.out.println();
  }

  private void compareClasses() {
    var wikidataMapping = getEquivalentsClasses();
    var yagoMapping = ShaclSchema.getSchema().getNodeShapes()
            .flatMap(shape -> shape.getFromClasses().flatMap(wdcl -> shape.getClasses().map(ycl -> new Pair<>(wdcl.stringValue(), ycl.stringValue()))))
            .collect(Collectors.toSet());
    System.out.println();
    System.out.println("==== CLASSES ====");
    printDiff(wikidataMapping, yagoMapping);
    System.out.println();
  }

  private Set<Pair<String, String>> getEquivalentProperties() {
    Set<Pair<String, String>> out = new HashSet<>();
    try (RepositoryConnection connection = repository.getConnection()) {
      try (var results = connection.prepareTupleQuery("SELECT DISTINCT ?wd ?yago WHERE {\n" +
              "  ?p wdt:P1628|wdt:P2235|wdt:P2236 ?yago .\n" +
              "  ?p wikibase:directClaim ?wd .\n" +
              "  FILTER(STRSTARTS(STR(?yago), \"http://schema.org/\"))\n" +
              "}").evaluate()) {
        while (results.hasNext()) {  // iterate over the result
          var bindingSet = results.next();
          out.add(new Pair<>(bindingSet.getValue("wd").stringValue(), bindingSet.getValue("yago").stringValue()));
        }
      }
    }
    return out;
  }

  private Set<Pair<String, String>> getEquivalentsClasses() {
    Set<Pair<String, String>> out = new HashSet<>();
    try (RepositoryConnection connection = repository.getConnection()) {
      try (var results = connection.prepareTupleQuery("SELECT DISTINCT ?wd ?yago WHERE {\n" +
              "  ?wd wdt:P1709|wdt:P3950 ?yago .\n" +
              "  FILTER(STRSTARTS(STR(?yago), \"http://schema.org/\"))\n" +
              "}").evaluate()) {
        while (results.hasNext()) {  // iterate over the result
          var bindingSet = results.next();
          out.add(new Pair<>(bindingSet.getValue("wd").stringValue(), bindingSet.getValue("yago").stringValue()));
        }
      }
    }
    return out;
  }

  private <T> void printDiff(Set<T> wikidata, Set<T> yago) {
    System.out.println("= not in Wikidata =");
    for (var v : yago) {
      if (!wikidata.contains(v)) {
        System.out.println(v);
      }
    }
    System.out.println();

    System.out.println("= not in Yago =");
    for (var v : wikidata) {
      if (!yago.contains(v)) {
        System.out.println(v);
      }
    }
    System.out.println();
  }
}
