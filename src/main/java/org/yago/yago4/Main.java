package org.yago.yago4;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.vocabulary.OWL;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.yago.yago4.converter.JavaStreamEvaluator;
import org.yago.yago4.converter.plan.PairPlanNode;
import org.yago.yago4.converter.plan.PlanNode;
import org.yago.yago4.converter.utils.NTriplesReader;
import org.yago.yago4.converter.utils.Pair;
import org.yago.yago4.converter.utils.YagoValueFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Main {
  private static final YagoValueFactory VALUE_FACTORY = YagoValueFactory.getInstance();

  private static final String WD_PREFIX = "http://www.wikidata.org/entity/";
  private static final String WDT_PREFIX = "http://www.wikidata.org/prop/direct/";
  private static final String SCHEMA_PREFIX = "http://schema.org/";
  private static final String YAGO_RESOURCE_PREFIX = "http://yago-knowledge.org/resource/";

  private static final IRI WIKIBASE_ITEM = VALUE_FACTORY.createIRI("http://wikiba.se/ontology#Item");
  private static final IRI WDT_P31 = VALUE_FACTORY.createIRI(WDT_PREFIX, "P31");
  private static final IRI WDT_P279 = VALUE_FACTORY.createIRI(WDT_PREFIX, "P279");
  private static final IRI SCHEMA_THING = VALUE_FACTORY.createIRI(SCHEMA_PREFIX, "Thing");
  private static final IRI SCHEMA_GEO_COORDINATES = VALUE_FACTORY.createIRI(SCHEMA_PREFIX, "GeoCoordinates");
  private static final IRI SCHEMA_ABOUT = VALUE_FACTORY.createIRI(SCHEMA_PREFIX, "about");

  private static final Pattern WKT_COORDINATES_PATTERN = Pattern.compile("^POINT\\(([0-9.]+) +([0-9.]+)\\)$", Pattern.CASE_INSENSITIVE);

  private static final List<String> WD_BAD_TYPES = List.of(
          "Q17379835", //Wikimedia page outside the main knowledge tree
          "Q17442446", //Wikimedia internal stuff
          "Q4167410", //disambiguation page
          "Q13406463", //list article
          "Q17524420", //aspect of history
          "Q18340514" //article about events in a specific year or time period
  );
  private static final Set<Resource> SCHEMA_BLACKLIST = Set.of(
          // Some schema class we do not want to emit even if they are super classes from the existing classes
          SCHEMA_THING,
          VALUE_FACTORY.createIRI(SCHEMA_PREFIX, "Intangible"),
          VALUE_FACTORY.createIRI(SCHEMA_PREFIX, "StructuredValue")

  );
  private static final Set<IRI> LABEL_IRIS = Set.of(
          VALUE_FACTORY.createIRI(SCHEMA_PREFIX, "name"),
          VALUE_FACTORY.createIRI(SCHEMA_PREFIX, "alternateName"),
          VALUE_FACTORY.createIRI(SCHEMA_PREFIX, "description")
  );


  public static void main(String[] args) throws ParseException, IOException {
    Options options = new Options();
    options.addRequiredOption("dir", "workingDirectory", true, "Working directory where the partition should be stored");

    // Partitioning
    options.addOption("partition", "partition", false, "Partition the dumps");
    options.addOption("wdDump", "wdDump", true, "Wikidata NTriples Truthy dump in bz2");

    // Processing
    options.addOption("yago", "buildYago", false, "Build Yago");
    options.addOption("yagoDir", "yagoDir", true, "Path to the directory where Yago N-Triples files should be built");

    CommandLine params = (new DefaultParser()).parse(options, args);

    Path workingDir = Path.of(params.getOptionValue("dir"));
    PartitionedStatements partitionedStatements = new PartitionedStatements(workingDir);

    if (params.hasOption("partition")) {
      Path wdDump = Path.of(params.getOptionValue("wdDump"));

      System.out.println("Partitioning Wikidata dump " + wdDump + " to " + workingDir);
      doPartition(partitionedStatements, wdDump);
    }

    if (params.hasOption("yago")) {
      Path yagoDir = Path.of(params.getOptionValue("yagoDir"));

      System.out.println("Generating Yago N-Triples dump to " + yagoDir);
      buildYago(partitionedStatements, yagoDir);
    }
  }

  private static void doPartition(PartitionedStatements partitionedStatements, Path wdDump) {
    NTriplesReader reader = new NTriplesReader(VALUE_FACTORY);
    try (PartitionedStatements.Writer writer = partitionedStatements.getWriter(t -> keyForIri(t.getPredicate()))) {
      writer.writeAll(reader.read(wdDump));
    }
  }

  private static String keyForIri(IRI iri) {
    return IRIShortener.shortened(iri).replace('/', '-').replace(':', '/');
  }

  private static void buildYago(PartitionedStatements partitionedStatements, Path outputDir) throws IOException {
    Files.createDirectories(outputDir);

    var evaluator = new JavaStreamEvaluator(VALUE_FACTORY);

    var wikidataInstanceOf = partitionedStatements.getForKey(keyForIri(WDT_P31))
            .mapToPair(t -> new Pair<>((Resource) t.getObject(), t.getSubject()));
    var wikidataSuperClassOf = partitionedStatements.getForKey(keyForIri(WDT_P279))
            .mapToPair(t -> new Pair<>((Resource) t.getObject(), t.getSubject()));
    wikidataSuperClassOf = wikidataSuperClassOf.transitiveClosure(wikidataSuperClassOf);

    var wikidataToYagoUrisMapping = wikidataToYagoUrisMapping(partitionedStatements, wikidataInstanceOf, wikidataSuperClassOf);
    evaluator.evaluateToNTriples(
            wikidataToYagoUrisMapping
                    .map((wd, yago) -> VALUE_FACTORY.createStatement(yago, OWL.SAMEAS, wd)),
            outputDir.resolve("yago-wd-sameAs.nt")
    );

    var classInstances = classesFromSchema(wikidataToYagoUrisMapping, wikidataInstanceOf, wikidataSuperClassOf);
    var yagoClasses = classInstances.entrySet().stream().map(e -> {
      var yagoClass = e.getKey();
      return e.getValue().map(i -> VALUE_FACTORY.createStatement(i, RDF.TYPE, yagoClass));
    }).reduce(PlanNode::union).orElseGet(PlanNode::empty);
    evaluator.evaluateToNTriples(yagoClasses, outputDir.resolve("yago-wd-types.nt"));

    evaluator.evaluateToNTriples(
            propertiesFromSchema(partitionedStatements, classInstances, wikidataToYagoUrisMapping, LABEL_IRIS, null),
            outputDir.resolve("yago-wd-labels.nt")
    );
    evaluator.evaluateToNTriples(
            propertiesFromSchema(partitionedStatements, classInstances, wikidataToYagoUrisMapping, null, LABEL_IRIS),
            outputDir.resolve("yago-wd-facts.nt")
    );
  }

  private static PairPlanNode<Resource, Resource> wikidataToYagoUrisMapping(PartitionedStatements partitionedStatements, PairPlanNode<Resource, Resource> wikidataInstanceOf, PairPlanNode<Resource, Resource> wikidataSuperClassOf) {
    var wikidataItems = partitionedStatements.getForKey(keyForIri(RDF.TYPE))
            .filter(t -> WIKIBASE_ITEM.equals(t.getObject()))
            .map(Statement::getSubject);

    var badWikidataClasses = joinWithSuperClassOfClosure(PlanNode.fromCollection(WD_BAD_TYPES).map(t -> (Resource) VALUE_FACTORY.createIRI(WD_PREFIX + t)), wikidataSuperClassOf);

    var badWikidataItems = wikidataInstanceOf.intersection(badWikidataClasses).values();

    var goodWikidataItems = wikidataItems.subtract(badWikidataItems);

    var mappingFromEnWikipedia = partitionedStatements.getForKey(keyForIri(SCHEMA_ABOUT))
            .filter(t -> t.getSubject().stringValue().startsWith("https://en.wikipedia.org/wiki/"))
            .mapToPair(s -> new Pair<>((Resource) s.getObject(), s.getSubject()))
            .intersection(goodWikidataItems)
            .mapPair((wikidata, wikipedia) -> new Pair<>(wikidata, (Resource) VALUE_FACTORY.createIRI(wikipedia.stringValue().replace("https://en.wikipedia.org/wiki/", YAGO_RESOURCE_PREFIX))));

    var mappingOthers = goodWikidataItems
            .subtract(mappingFromEnWikipedia.keys())
            .mapToPair(e -> new Pair<>(e, (Resource) VALUE_FACTORY.createIRI(YAGO_RESOURCE_PREFIX, "wikidata_" + ((IRI) e).getLocalName())));
    return mappingFromEnWikipedia.union(mappingOthers).cache();
  }

  private static Map<Resource, PlanNode<Resource>> classesFromSchema(PairPlanNode<Resource, Resource> wikidataToYagoUrisMapping, PairPlanNode<Resource, Resource> wikidataInstanceOf, PairPlanNode<Resource, Resource> wikidataSuperClassOf) {
    var schemaThings = wikidataToYagoUrisMapping.values().cache();

    Map<Resource, PlanNode<Resource>> instancesSet = new HashMap<>();
    instancesSet.put(SCHEMA_THING, schemaThings);

    Multimap<Resource, Resource> classMapping = HashMultimap.create();
    ShaclSchema schema = ShaclSchema.getSchema();
    schema.getNodeShapes().forEach(nodeShape ->
            nodeShape.getFromClasses().forEach(sourceClass ->
                    nodeShape.getClasses()
                            .flatMap(schema::getSuperClasses)
                            .forEach(yagoClass -> classMapping.put(yagoClass, sourceClass))));

    for (Map.Entry<Resource, Collection<Resource>> entry : classMapping.asMap().entrySet()) {
      Resource yagoClass = entry.getKey();
      if (SCHEMA_BLACKLIST.contains(yagoClass)) {
        continue; // We ignore the blacklisted classes
      }
      var sourceClasses = joinWithSuperClassOfClosure(PlanNode.fromCollection(entry.getValue()), wikidataSuperClassOf);
      var instances = wikidataInstanceOf.intersection(sourceClasses).values();

      var mappedInstance = mapToYago(instances, wikidataToYagoUrisMapping)
              .intersection(schemaThings)
              .cache();
      instancesSet.put(yagoClass, mappedInstance);
    }

    return instancesSet;
  }

  private static PlanNode<Resource> joinWithSuperClassOfClosure(PlanNode<Resource> classes, PairPlanNode<Resource, Resource> superClassOf) {
    return superClassOf
            .intersection(classes)
            .values()
            .union(classes);
  }

  private static PlanNode<Statement> propertiesFromSchema(
          PartitionedStatements partitionedStatements,
          Map<Resource, PlanNode<Resource>> classInstances,
          PairPlanNode<Resource, Resource> wikidataToYagoUrisMapping,
          Set<IRI> onlyProperties,
          Set<IRI> excludeProperties
  ) {
    return ShaclSchema.getSchema().getPropertyShapes().map(propertyShape -> {
      IRI yagoProperty = propertyShape.getProperty();
      if (onlyProperties != null && !onlyProperties.contains(yagoProperty)) {
        return PlanNode.<Statement>empty();
      }
      if (excludeProperties != null && excludeProperties.contains(yagoProperty)) {
        return PlanNode.<Statement>empty();
      }

      var subjectObjects = propertyShape.getFromProperties()
              .map(wikidataProperty -> partitionedStatements.getForKey(keyForIri(wikidataProperty)))
              .reduce(PlanNode::union).orElseGet(PlanNode::empty)
              .mapToPair(t -> new Pair<>(t.getSubject(), t.getObject()));

      // Datatype filter
      if (propertyShape.getDatatypes().isPresent()) {
        Set<IRI> dts = propertyShape.getDatatypes().get();

        //We map IRIs to xsd:anyUri
        if (dts.contains(XMLSchema.ANYURI)) {
          subjectObjects = subjectObjects.flatMapValue(object -> {
            if (object instanceof IRI || (object instanceof Literal && XMLSchema.ANYURI.equals(((Literal) object).getDatatype()))) {
              return normalizeUri(object.stringValue())
                      .map(o -> VALUE_FACTORY.createLiteral(o, XMLSchema.ANYURI));
            } else {
              return Stream.of(object);
            }
          });
        }
        //TODO: time and quantity values
        subjectObjects = subjectObjects.filterValue(object -> object instanceof Literal && dts.contains(((Literal) object).getDatatype()));
      }

      // Range type filter
      if (propertyShape.getNodeShape().isPresent()) {
        ShaclSchema.NodeShape nodeShape = propertyShape.getNodeShape().get();
        Set<Resource> expectedClasses = nodeShape.getClasses().collect(Collectors.toSet());
        if (Collections.singleton(SCHEMA_GEO_COORDINATES).equals(expectedClasses)) {
          subjectObjects = subjectObjects.flatMapValue(object -> {
            //TODO: precision
            Matcher matcher = WKT_COORDINATES_PATTERN.matcher(object.stringValue());
            if (!matcher.matches()) {
              return Stream.empty();
            }
            double longitude = Float.parseFloat(matcher.group(1));
            double latitude = Float.parseFloat(matcher.group(2));
            return Stream.of(VALUE_FACTORY.createIRI("geo:" + latitude + "," + longitude)); //TODO: description of geocoordinates
          });
        } else {
          var objectSubjectsForRange = subjectObjects.mapPair((k, v) -> new Pair<>((Resource) v, k));
          objectSubjectsForRange = mapKeyToYago(objectSubjectsForRange, wikidataToYagoUrisMapping);
          subjectObjects = nodeShape.getClasses()
                  .distinct()
                  .flatMap(cls -> Stream.ofNullable(classInstances.get(cls)))
                  .map(objectSubjectsForRange::intersection)
                  .reduce(PairPlanNode::union).orElseGet(PairPlanNode::empty)
                  .mapPair((k, v) -> new Pair<>(v, k));
        }
      }

      //Regex
      if (propertyShape.getPattern().isPresent()) {
        Pattern pattern = propertyShape.getPattern().get();
        subjectObjects = subjectObjects.filterValue(object -> pattern.matcher(object.stringValue()).matches());
      }

      // Domain type filter
      var subjectObjectsForDomain = subjectObjects;
      subjectObjectsForDomain = mapKeyToYago(subjectObjectsForDomain, wikidataToYagoUrisMapping);
      subjectObjects = propertyShape.getParentShapes().stream()
              .flatMap(Collection::stream)
              .flatMap(ShaclSchema.NodeShape::getClasses)
              .distinct()
              .flatMap(cls -> Stream.ofNullable(classInstances.get(cls)))
              .map(subjectObjectsForDomain::intersection)
              .reduce(PairPlanNode::union)
              .orElseGet(PairPlanNode::empty);

      return subjectObjects.map((s, o) -> VALUE_FACTORY.createStatement(s, yagoProperty, o));
    }).reduce(PlanNode::union).orElseGet(PlanNode::empty);
  }

  private static PlanNode<Resource> mapToYago(PlanNode<Resource> facts, PairPlanNode<Resource, Resource> wikidataToYagoUrisMapping) {
    return facts
            .join(wikidataToYagoUrisMapping)
            .values();
  }

  private static <V> PairPlanNode<Resource, V> mapKeyToYago(PairPlanNode<Resource, V> facts, PairPlanNode<Resource, Resource> wikidataToYagoUrisMapping) {
    return facts
            .join(wikidataToYagoUrisMapping)
            .mapPair((subject, pair) -> new Pair<>(pair.getValue(), pair.getKey()));
  }

  private static Stream<String> normalizeUri(String uri) {
    try {
      URI parsedURI = new URI(uri).normalize();
      if ((parsedURI.getScheme().equals("http") || parsedURI.getScheme().equals("https")) && parsedURI.getPath().isEmpty()) {
        parsedURI = parsedURI.resolve("/"); //We make sure there is always a path
      }
      return Stream.of(parsedURI.toString());
    } catch (URISyntaxException e) {
      return Stream.empty();
    }
  }
}
