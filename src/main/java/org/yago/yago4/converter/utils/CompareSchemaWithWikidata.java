package org.yago.yago4.converter.utils;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sparql.SPARQLRepository;
import org.yago.yago4.ShaclSchema;

import java.util.*;
import java.util.stream.Collectors;

public class CompareSchemaWithWikidata implements AutoCloseable {

  private static final String WDQS_ENDPOINT = "https://query.wikidata.org/sparql";

  public static void main(String[] args) {
    try (var self = new CompareSchemaWithWikidata()) {
      self.compareProperties();
      self.compareClasses();
      self.compareFunctionals();
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
            .flatMap(shape -> shape.getFromProperties().map(wdp -> Map.entry(wdp.stringValue(), shape.getProperty().stringValue())))
            .collect(Collectors.toSet());
    System.out.println();
    System.out.println("==== PROPERTIES ====");
    printPairDiff(wikidataMapping, yagoMapping);
    System.out.println();
  }

  private void compareClasses() {
    var wikidataMapping = getEquivalentsClasses();
    var yagoMapping = ShaclSchema.getSchema().getNodeShapes()
            .flatMap(shape -> shape.getFromClasses().flatMap(wdcl -> shape.getClasses().map(ycl -> Map.entry(wdcl.stringValue(), ycl.stringValue()))))
            .collect(Collectors.toSet());
    System.out.println();
    System.out.println("==== CLASSES ====");
    printPairDiff(wikidataMapping, yagoMapping);
    System.out.println();
  }

  private void compareFunctionals() {
    var wikidataFunctionals = getFunctionalProperties();
    var yagoFunctionals = ShaclSchema.getSchema().getPropertyShapes()
            .filter(shape -> shape.getMaxCount().equals(OptionalInt.of(1)))
            .flatMap(ShaclSchema.PropertyShape::getFromProperties)
            .map(Value::stringValue)
            .collect(Collectors.toSet());
    System.out.println();
    System.out.println("==== FUNCTIONALS ====");
    printDiff(wikidataFunctionals, yagoFunctionals);
    System.out.println();
  }

  private Set<Map.Entry<String, String>> getEquivalentProperties() {
    Set<Map.Entry<String, String>> out = new HashSet<>();
    try (RepositoryConnection connection = repository.getConnection()) {
      try (var results = connection.prepareTupleQuery("SELECT DISTINCT ?wd ?yago WHERE {\n" +
              "  ?p wdt:P1628|wdt:P2235|wdt:P2236 ?yago .\n" +
              "  ?p wikibase:directClaim ?wd .\n" +
              "  FILTER(STRSTARTS(STR(?yago), \"http://schema.org/\") || STRSTARTS(STR(?yago), \"http://bioschemas.org/\"))\n" +
              "}").evaluate()) {
        while (results.hasNext()) {  // iterate over the result
          var bindingSet = results.next();
          out.add(Map.entry(bindingSet.getValue("wd").stringValue(), bindingSet.getValue("yago").stringValue()));
        }
      }
    }
    return out;
  }

  private Set<Map.Entry<String, String>> getEquivalentsClasses() {
    Set<Map.Entry<String, String>> out = new HashSet<>();
    try (RepositoryConnection connection = repository.getConnection()) {
      try (var results = connection.prepareTupleQuery("SELECT DISTINCT ?wd ?yago WHERE {\n" +
              "  ?wd wdt:P1709|wdt:P3950 ?yago .\n" +
              "  FILTER(STRSTARTS(STR(?yago), \"http://schema.org/\") || STRSTARTS(STR(?yago), \"http://bioschemas.org/\"))\n" +
              "}").evaluate()) {
        while (results.hasNext()) {  // iterate over the result
          var bindingSet = results.next();
          out.add(Map.entry(bindingSet.getValue("wd").stringValue(), bindingSet.getValue("yago").stringValue()));
        }
      }
    }
    return out;
  }

  private Set<String> getFunctionalProperties() {
    var allYago = ShaclSchema.getSchema().getPropertyShapes()
            .flatMap(ShaclSchema.PropertyShape::getFromProperties)
            .map(Value::stringValue)
            .collect(Collectors.toSet());
    System.out.println(allYago);
    Set<String> out = new HashSet<>();
    try (RepositoryConnection connection = repository.getConnection()) {
      try (var results = connection.prepareTupleQuery("SELECT DISTINCT ?wd WHERE { ?p wdt:P2302 wd:Q19474404 ; wikibase:directClaim ?wd }").evaluate()) {
        while (results.hasNext()) {  // iterate over the result
          var bindingSet = results.next();
          var val = bindingSet.getValue("wd").stringValue();
          if (allYago.contains(val)) {
            out.add(val);
          }
        }
      }
    }
    return out;
  }

  private <K, V> void printPairDiff(Set<Map.Entry<K, V>> wikidata, Set<Map.Entry<K, V>> yago) {
    System.out.println("= not in Wikidata =");
    for (var v : yago) {
      if (!wikidata.contains(v)) {
        System.out.println(v.getKey() + " -> " + v.getValue());
      }
    }
    System.out.println();

    System.out.println("= not in Yago =");
    for (var v : wikidata) {
      if (!yago.contains(v)) {
        System.out.println(v.getKey() + " -> " + v.getValue());
      }
    }
    System.out.println();
  }

  private <E> void printDiff(Set<E> wikidata, Set<E> yago) {
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
