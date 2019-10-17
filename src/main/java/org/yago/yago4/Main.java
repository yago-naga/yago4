package org.yago.yago4;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
import org.eclipse.rdf4j.common.net.ParsedIRI;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.model.impl.SimpleNamespace;
import org.eclipse.rdf4j.model.vocabulary.*;
import org.yago.yago4.converter.JavaStreamEvaluator;
import org.yago.yago4.converter.plan.PairPlanNode;
import org.yago.yago4.converter.plan.PlanNode;
import org.yago.yago4.converter.utils.NTriplesReader;
import org.yago.yago4.converter.utils.YagoValueFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.SignStyle;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Main {
  private static final YagoValueFactory VALUE_FACTORY = YagoValueFactory.getInstance();
  private static final JavaStreamEvaluator evaluator = new JavaStreamEvaluator(VALUE_FACTORY);

  private static final DateTimeFormatter WIKIBASE_TIMESTAMP_FORMATTER = (new DateTimeFormatterBuilder())
          .appendValue(ChronoField.YEAR, 4, 19, SignStyle.NORMAL)
          .appendLiteral('-')
          .appendValue(ChronoField.MONTH_OF_YEAR, 2, 2, SignStyle.NORMAL)
          .appendLiteral('-')
          .appendValue(ChronoField.DAY_OF_MONTH, 2, 2, SignStyle.NORMAL)
          .appendLiteral('T')
          .appendValue(ChronoField.HOUR_OF_DAY, 2, 2, SignStyle.NORMAL)
          .appendLiteral(':')
          .appendValue(ChronoField.MINUTE_OF_HOUR, 2, 2, SignStyle.NORMAL)
          .appendLiteral(':')
          .appendValue(ChronoField.SECOND_OF_MINUTE, 2, 2, SignStyle.NORMAL)
          .appendOffsetId()
          .toFormatter();

  private static final String WD_PREFIX = "http://www.wikidata.org/entity/";
  private static final String WDT_PREFIX = "http://www.wikidata.org/prop/direct/";
  private static final String P_PREFIX = "http://www.wikidata.org/prop/";
  private static final String PS_PREFIX = "http://www.wikidata.org/prop/statement/";
  private static final String PSV_PREFIX = "http://www.wikidata.org/prop/statement/value/";
  private static final String PQ_PREFIX = "http://www.wikidata.org/prop/qualifier/";
  private static final String PQV_PREFIX = "http://www.wikidata.org/prop/qualifier/value/";
  private static final String SCHEMA_PREFIX = "http://schema.org/";
  private static final String WIKIBASE_PREFIX = "http://wikiba.se/ontology#";
  private static final String YAGO_RESOURCE_PREFIX = "http://yago-knowledge.org/resource/";
  private static final String YAGO_VALUE_PREFIX = "http://yago-knowledge.org/value/";


  private static final Set<Namespace> NAMESPACES = Set.of(
          RDF.NS, RDFS.NS, XMLSchema.NS, OWL.NS, SHACL.NS, SKOS.NS,
          new SimpleNamespace("schema", SCHEMA_PREFIX),
          new SimpleNamespace("yago", YAGO_RESOURCE_PREFIX),
          new SimpleNamespace("wd", WD_PREFIX)
  );

  private static final IRI WIKIBASE_ITEM = VALUE_FACTORY.createIRI(WIKIBASE_PREFIX, "Item");
  private static final IRI WIKIBASE_BEST_RANK = VALUE_FACTORY.createIRI(WIKIBASE_PREFIX, "BestRank");
  private static final IRI WIKIBASE_TIME_VALUE = VALUE_FACTORY.createIRI(WIKIBASE_PREFIX, "timeValue");
  private static final IRI WIKIBASE_TIME_PRECISION = VALUE_FACTORY.createIRI(WIKIBASE_PREFIX, "timePrecision");
  private static final IRI WIKIBASE_TIME_CALENDAR_MODEL = VALUE_FACTORY.createIRI(WIKIBASE_PREFIX, "timeCalendarModel");
  private static final IRI WIKIBASE_GEO_LATITUDE = VALUE_FACTORY.createIRI(WIKIBASE_PREFIX, "geoLatitude");
  private static final IRI WIKIBASE_GEO_LONGITUDE = VALUE_FACTORY.createIRI(WIKIBASE_PREFIX, "geoLongitude");
  private static final IRI WIKIBASE_GEO_PRECISION = VALUE_FACTORY.createIRI(WIKIBASE_PREFIX, "geoPrecision");
  private static final IRI WIKIBASE_GEO_GLOBE = VALUE_FACTORY.createIRI(WIKIBASE_PREFIX, "geoGlobe");
  private static final IRI WIKIBASE_QUANTITY_AMOUNT = VALUE_FACTORY.createIRI(WIKIBASE_PREFIX, "quantityAmount");
  private static final IRI WIKIBASE_QUANTITY_UPPER_BOUND = VALUE_FACTORY.createIRI(WIKIBASE_PREFIX, "quantityUpperBound");
  private static final IRI WIKIBASE_QUANTITY_LOWER_BOUND = VALUE_FACTORY.createIRI(WIKIBASE_PREFIX, "quantityLowerBound");
  private static final IRI WIKIBASE_QUANTITY_UNIT = VALUE_FACTORY.createIRI(WIKIBASE_PREFIX, "quantityUnit");
  private static final IRI WDT_P31 = VALUE_FACTORY.createIRI(WDT_PREFIX, "P31");
  private static final IRI WDT_P279 = VALUE_FACTORY.createIRI(WDT_PREFIX, "P279");
  private static final IRI WDT_P646 = VALUE_FACTORY.createIRI(WDT_PREFIX, "P646");
  private static final IRI WD_Q7727 = VALUE_FACTORY.createIRI(WD_PREFIX, "Q7727");
  private static final Value WD_Q11574 = VALUE_FACTORY.createIRI(WD_PREFIX, "Q11574");
  private static final Value WD_Q25235 = VALUE_FACTORY.createIRI(WD_PREFIX, "Q25235");
  private static final Value WD_Q573 = VALUE_FACTORY.createIRI(WD_PREFIX, "Q573");
  private static final Value WD_Q199 = VALUE_FACTORY.createIRI(WD_PREFIX, "Q199");
  private static final Value WD_Q2 = VALUE_FACTORY.createIRI(WD_PREFIX, "Q2");
  private static final Value WD_Q1985727 = VALUE_FACTORY.createIRI(WD_PREFIX, "Q1985727");
  private static final IRI SCHEMA_THING = VALUE_FACTORY.createIRI(SCHEMA_PREFIX, "Thing");
  private static final IRI SCHEMA_INTANGIBLE = VALUE_FACTORY.createIRI(SCHEMA_PREFIX, "Intangible");
  private static final IRI SCHEMA_STRUCTURED_VALUE = VALUE_FACTORY.createIRI(SCHEMA_PREFIX, "StructuredValue");
  private static final IRI SCHEMA_GEO_COORDINATES = VALUE_FACTORY.createIRI(SCHEMA_PREFIX, "GeoCoordinates");
  private static final IRI SCHEMA_QUANTITATIVE_VALUE = VALUE_FACTORY.createIRI(SCHEMA_PREFIX, "QuantitativeValue");
  private static final IRI SCHEMA_IMAGE_OBJECT = VALUE_FACTORY.createIRI(SCHEMA_PREFIX, "ImageObject");
  private static final IRI SCHEMA_ABOUT = VALUE_FACTORY.createIRI(SCHEMA_PREFIX, "about");
  private static final IRI SCHEMA_ALTERNATE_NAME = VALUE_FACTORY.createIRI(SCHEMA_PREFIX, "alternateName");
  private static final IRI SCHEMA_DESCRIPTION = VALUE_FACTORY.createIRI(SCHEMA_PREFIX, "description");
  private static final IRI SCHEMA_SAME_AS = VALUE_FACTORY.createIRI(SCHEMA_PREFIX, "sameAs");
  private static final IRI SCHEMA_MAX_VALUE = VALUE_FACTORY.createIRI(SCHEMA_PREFIX, "maxValue");
  private static final IRI SCHEMA_MIN_VALUE = VALUE_FACTORY.createIRI(SCHEMA_PREFIX, "minValue");
  private static final IRI SCHEMA_UNIT_CODE = VALUE_FACTORY.createIRI(SCHEMA_PREFIX, "unitCode");
  private static final IRI SCHEMA_LATITUDE = VALUE_FACTORY.createIRI(SCHEMA_PREFIX, "latitude");
  private static final IRI SCHEMA_LONGITUDE = VALUE_FACTORY.createIRI(SCHEMA_PREFIX, "longitude");
  private static final IRI SCHEMA_VALUE = VALUE_FACTORY.createIRI(SCHEMA_PREFIX, "value");
  private static final Pattern WIKIDATA_PROPERTY_IRI_PATTERN = Pattern.compile("^http://www.wikidata.org/prop/[a-z\\-/]*P\\d+$");

  private static final Set<IRI> CALENDAR_DT_SET = Set.of(XMLSchema.GYEAR, XMLSchema.GYEARMONTH, XMLSchema.DATE, XMLSchema.DATETIME);

  private static final List<String> WD_BAD_CLASSES = List.of(
          "Q17379835", //Wikimedia page outside the main knowledge tree
          "Q17442446", //Wikimedia internal stuff
          "Q4167410", //disambiguation page
          "Q13406463", //list article
          "Q17524420", //aspect of history
          "Q18340514" //article about events in a specific year or time period
  );
  private static final Set<IRI> LABEL_IRIS = Set.of(RDFS.LABEL, RDFS.COMMENT, SCHEMA_ALTERNATE_NAME);
  private static final int MIN_NUMBER_OF_INSTANCES = 10;

  public static void main(String[] args) throws ParseException, IOException {
    Options options = new Options();
    options.addRequiredOption("dir", "workingDirectory", true, "Working directory where the partition should be stored");

    // Partitioning
    options.addOption("partition", "partition", false, "Partition the dumps");
    options.addOption("wdDump", "wdDump", true, "Wikidata NTriples Truthy dump in bz2");

    // Processing
    options.addOption("yago", "buildYago", false, "Build Yago");
    options.addOption("enWiki", "onlyEnWikipedia", false, "Only introduces resources that are mapped to English Wikipedia");
    options.addOption("wikis", "onlyWikipedia", false, "Only introduces resources that are mapped to a Wikipedia");
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
      buildYago(partitionedStatements, yagoDir, params.hasOption("onlyEnWikipedia"), params.hasOption("onlyWikipedia"));
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

  private static void buildYago(PartitionedStatements partitionedStatements, Path outputDir, boolean enWikipediaOnly, boolean wikipediaOnly) throws IOException {
    Files.createDirectories(outputDir);

    var wikidataToYagoUrisMapping = wikidataToYagoUrisMapping(partitionedStatements);

    var t = buildYagoClassesAndSuperClassOf(partitionedStatements, wikidataToYagoUrisMapping);
    var yagoClasses = t.getKey();
    var yagoSuperClassOf = t.getValue();

    var yagoShapeInstances = yagoShapeInstances(partitionedStatements, yagoSuperClassOf, yagoClasses,
            enWikipediaOnly
                    ? enWikipediaElements(partitionedStatements, wikidataToYagoUrisMapping)
                    : (wikipediaOnly ? wikipediaElements(partitionedStatements, wikidataToYagoUrisMapping) : null),
            wikidataToYagoUrisMapping);

    generateNTFile(
            buildClassesDescription(yagoClasses, yagoSuperClassOf, partitionedStatements, wikidataToYagoUrisMapping),
            outputDir, "yago-wd-class.nt"
    );

    generateNTFile(buildSimpleInstanceOf(yagoShapeInstances), outputDir, "yago-wd-simple-types.nt");

    generateNTFile(
            buildFullInstanceOf(yagoShapeInstances.get(SCHEMA_THING), yagoClasses, partitionedStatements, wikidataToYagoUrisMapping),
            outputDir, "yago-wd-full-types.nt"
    );

    generateNTFile(
            buildFactsFromRdfProperty(partitionedStatements, yagoShapeInstances, wikidataToYagoUrisMapping, LABEL_IRIS, null),
            outputDir, "yago-wd-labels.nt"
    );

    var annotatedFacts = buildPropertiesFromSchema(partitionedStatements, yagoShapeInstances, wikidataToYagoUrisMapping, null, LABEL_IRIS);
    generateNTFile(annotatedFacts.getKey(), outputDir, "yago-wd-facts.nt");
    generateNTStarFile(annotatedFacts.getValue(), outputDir, "yago-wd-facts-annotations.ntx");

    generateNTFile(
            buildSameAs(partitionedStatements, yagoShapeInstances.get(SCHEMA_THING), wikidataToYagoUrisMapping),
            outputDir, "yago-wd-sameAs.nt"
    );

    generateNTFile(buildYagoSchema(), outputDir, "yago-wd-schema.nt");

    generateNTFile(buildYagoShapes(), outputDir, "yago-wd-shapes.nt");
  }

  private static void generateNTFile(PlanNode<Statement> stream, Path outputDir, String fileName) {
    System.out.println("Generating N-Triples file " + fileName);
    var start = LocalDateTime.now();
    evaluator.evaluateToNTriples(stream, outputDir.resolve(fileName + ".gz"));
    var end = LocalDateTime.now();
    System.out.println("Generation of " + fileName + " done in " + Duration.between(start, end));
  }

  private static void generateNTStarFile(PlanNode<AnnotatedStatement> stream, Path outputDir, String fileName) {
    System.out.println("Generating N-Triples* file " + fileName);
    var start = LocalDateTime.now();
    evaluator.evaluateToNTriplesStar(stream, outputDir.resolve(fileName + ".gz"));
    var end = LocalDateTime.now();
    System.out.println("Generation of " + fileName + " done in " + Duration.between(start, end));
  }


  /**
   * Converts Wikidata URI to Yago URIs based on en.wikipedia article titles
   */
  private static PairPlanNode<Resource, Resource> wikidataToYagoUrisMapping(PartitionedStatements partitionedStatements) {
    var fromSchemaMapping = PairPlanNode.fromStream(ShaclSchema.getSchema()
            .getNodeShapes()
            .flatMap(shape -> shape.getClasses().flatMap(toCls -> shape.getFromClasses().map(fromCls -> Map.entry((Resource) fromCls, toCls)))))
            .cache();

    var fromWikipediaMapping = partitionedStatements.getForKey(keyForIri(SCHEMA_ABOUT))
            .filter(t -> t.getSubject().stringValue().startsWith("https://en.wikipedia.org/wiki/"))
            .mapToPair(s -> Map.entry((Resource) s.getObject(), s.getSubject()))
            .subtract(fromSchemaMapping.keys())
            .flatMapValue(wikipedia -> normalizeIri(wikipedia.stringValue()))
            .mapPair((wikidata, wikipedia) -> Map.entry(wikidata, (Resource) VALUE_FACTORY.createIRI(wikipedia.replace("https://en.wikipedia.org/wiki/", YAGO_RESOURCE_PREFIX))))
            .cache();

    var optEn = Optional.of("en");
    var fromLabelMapping = partitionedStatements.getForKey(keyForIri(SKOS.PREF_LABEL))
            .filter(t -> t.getObject() instanceof Literal && ((Literal) t.getObject()).getLanguage().equals(optEn))
            .mapToPair(s -> Map.entry(s.getSubject(), s.getObject().stringValue()))
            .subtract(fromSchemaMapping.keys())
            .subtract(fromWikipediaMapping.keys())
            .flatMapPair((wd, label) -> normalizeIri(YAGO_RESOURCE_PREFIX + URLEncoder.encode(StringUtils.capitalize(label.replace(' ', '_')), StandardCharsets.UTF_8) + '_' + ((IRI) wd).getLocalName()).map(yago -> Map.entry(wd, (Resource) VALUE_FACTORY.createIRI(yago))))
            .cache();

    var wikidataItems = partitionedStatements.getForKey(keyForIri(RDF.TYPE))
            .filter(t -> WIKIBASE_ITEM.equals(t.getObject()))
            .map(Statement::getSubject);

    /*TODO: bad hack for tests
    wikidataItems = wikidataItems.union(partitionedStatements.getForKey(keyForIri(WDT_P31))
            .filter(t -> t.getObject() instanceof IRI)
            .map(t -> (Resource) t.getObject()));
    wikidataItems = wikidataItems.union(partitionedStatements.getForKey(keyForIri(WDT_P279))
            .filter(t -> t.getObject() instanceof IRI)
            .map(t -> (Resource) t.getObject()));
    wikidataItems = wikidataItems.union(partitionedStatements.getForKey(keyForIri(WDT_P279))
            .filter(t -> t.getSubject() instanceof IRI)
            .map(Statement::getSubject));
    wikidataItems = wikidataItems.distinct();
    */

    var fallbackMapping = wikidataItems
            .subtract(fromSchemaMapping.keys())
            .subtract(fromWikipediaMapping.keys())
            .subtract(fromLabelMapping.keys())
            .mapToPair(e -> Map.entry(e, (Resource) VALUE_FACTORY.createIRI(YAGO_RESOURCE_PREFIX, "_" + ((IRI) e).getLocalName())));

    return fromSchemaMapping.union(fromWikipediaMapping).union(fromLabelMapping).union(fallbackMapping).cache();
  }

  /**
   * Builds the class set and class hierarchy from Wikidata, schema.org ontology and shapes
   * <p>
   * Algorithm:
   * 1. Take all subClassOf (P279) from Wikidata
   * 2. Only keep the classes that are sub class of a Yago defined class
   * 3. Only keep the elements that have/have a subclass with at least 10 instances
   * 4. Remove the bad classes
   * 5. Remove the classes that are sub class of two disjoint classes
   */
  private static Map.Entry<PlanNode<Resource>, PairPlanNode<Resource, Resource>> buildYagoClassesAndSuperClassOf(PartitionedStatements partitionedStatements, PairPlanNode<Resource, Resource> wikidataToYagoUrisMapping) {
    var yagoSchemaClasses = PlanNode.fromStream(ShaclSchema.getSchema().getNodeShapes().flatMap(ShaclSchema.NodeShape::getClasses));
    var yagoSchemaFromClasses = PlanNode.fromStream(ShaclSchema.getSchema().getNodeShapes().flatMap(ShaclSchema.NodeShape::getFromClasses).map(c -> (Resource) c));

    var wikidataSubClassOf = partitionedStatements.getForKey(keyForIri(WDT_P279))
            .mapToPair(t -> Map.entry(t.getSubject(), (Resource) t.getObject()))
            .subtract(yagoSchemaFromClasses)
            .cache(); // Yago shape classes only have super classes which are shapes
    var wikidataSuperClassOf = wikidataSubClassOf.swap().cache();

    var possibleSuperClassOfFromWikidata = mapKeyToYago(mapKeyToYago(wikidataSuperClassOf, wikidataToYagoUrisMapping).swap(), wikidataToYagoUrisMapping).swap();
    var superClassOfFromSchema = subClassOfFromYagoSchema().swap();
    var possibleSuperClassOf = possibleSuperClassOfFromWikidata.union(superClassOfFromSchema).cache();

    var wikidataBadClasses = PlanNode.fromCollection(WD_BAD_CLASSES).map(c -> (Resource) VALUE_FACTORY.createIRI(WD_PREFIX, c))
            .transitiveClosure(wikidataSuperClassOf);

    var wikidataClassesWithAtLeastMinCountInstances = partitionedStatements.getForKey(keyForIri(WDT_P31))
            .mapToPair(t -> Map.entry((Resource) t.getObject(), t.getSubject()))
            .aggregateByKey()
            .filterValue(v -> v.size() >= MIN_NUMBER_OF_INSTANCES)
            .keys()
            .transitiveClosure(wikidataSubClassOf);

    var yagoClassesSubClasses = yagoSchemaFromClasses.transitiveClosure(wikidataSuperClassOf);

    var wikidataClassesToKeep = yagoClassesSubClasses
            .intersection(wikidataClassesWithAtLeastMinCountInstances)
            .subtract(wikidataBadClasses);


    var subclassesOfDisjoint = ShaclSchema.getSchema().getClasses()
            .flatMap(cls1 -> cls1.getDisjointedClasses().map(c2 -> Map.entry(cls1.getTerm(), c2)))
            .map(e ->
                    PlanNode.fromCollection(List.of(e.getKey())).transitiveClosure(possibleSuperClassOf)
                            .intersection(PlanNode.fromCollection(List.of(e.getValue())).transitiveClosure(possibleSuperClassOf))
            ).reduce(PlanNode::union).orElseGet(PlanNode::empty);

    var yagoClasses = mapToYago(wikidataClassesToKeep, wikidataToYagoUrisMapping)
            .union(yagoSchemaClasses)
            .subtract(subclassesOfDisjoint)
            .cache();

    var yagoSuperClassOf = possibleSuperClassOf
            .intersection(yagoClasses)
            .swap()
            .intersection(yagoClasses)
            .swap()
            .cache();

    return Map.entry(yagoClasses, yagoSuperClassOf);
  }

  private static PlanNode<Resource> wikipediaElements(PartitionedStatements partitionedStatements, PairPlanNode<Resource, Resource> wikidataToYagoUrisMapping) {
    var wikidataLinkedToEnWikipedia = partitionedStatements.getForKey(keyForIri(SCHEMA_ABOUT))
            .filter(t -> t.getSubject().stringValue().contains(".wikipedia.org/wiki/"))
            .map(t -> (Resource) t.getObject());
    return mapToYago(wikidataLinkedToEnWikipedia, wikidataToYagoUrisMapping);
  }

  private static PlanNode<Resource> enWikipediaElements(PartitionedStatements partitionedStatements, PairPlanNode<Resource, Resource> wikidataToYagoUrisMapping) {
    var wikidataLinkedToEnWikipedia = partitionedStatements.getForKey(keyForIri(SCHEMA_ABOUT))
            .filter(t -> t.getSubject().stringValue().startsWith("https://en.wikipedia.org/wiki/"))
            .map(t -> (Resource) t.getObject());
    return mapToYago(wikidataLinkedToEnWikipedia, wikidataToYagoUrisMapping);
  }

  private static Map<Resource, PlanNode<Resource>> yagoShapeInstances(PartitionedStatements partitionedStatements, PairPlanNode<Resource, Resource> yagoSuperClassOf, PlanNode<Resource> yagoClasses, PlanNode<Resource> optionalThingSuperset, PairPlanNode<Resource, Resource> wikidataToYagoUrisMapping) {
    var schema = ShaclSchema.getSchema();
    var wikidataInstancesForYagoClass = mapKeyToYago(
            partitionedStatements.getForKey(keyForIri(WDT_P31))
                    .mapToPair(t -> Map.entry((Resource) t.getObject(), t.getSubject())),
            wikidataToYagoUrisMapping
    );

    var optionalThingSupersetWithoutClasses = optionalThingSuperset == null
            ? null
            : optionalThingSuperset.subtract(yagoClasses); // We do not want classes

    var instancesWithoutIntersectionRemoval = schema.getNodeShapes().flatMap(nodeShape -> {
      var fromYagoClasses = PlanNode.fromStream(nodeShape.getClasses()).transitiveClosure(yagoSuperClassOf);

      var wdInstances = wikidataInstancesForYagoClass
              .intersection(fromYagoClasses)
              .values();
      var instances = optionalThingSupersetWithoutClasses == null
              ? mapToYago(wdInstances, wikidataToYagoUrisMapping).subtract(yagoClasses) // We do not want classes
              : mapToYago(wdInstances, wikidataToYagoUrisMapping).intersection(optionalThingSupersetWithoutClasses);

      return nodeShape.getClasses().map(c -> Map.entry(c, instances));
    }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    var instancesInDisjointIntersections = schema.getClasses()
            .flatMap(cls1 -> cls1.getDisjointedClasses().map(c2 -> Map.entry(cls1.getTerm(), c2)))
            .map(e -> instancesWithoutIntersectionRemoval.get(e.getKey()).intersection(instancesWithoutIntersectionRemoval.get(e.getValue())))
            .reduce(PlanNode::union).orElseGet(PlanNode::empty)
            .cache();

    return instancesWithoutIntersectionRemoval.entrySet().stream()
            .map(e -> Map.entry(e.getKey(), e.getValue().subtract(instancesInDisjointIntersections).cache()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private static PlanNode<Statement> buildSimpleInstanceOf(Map<Resource, PlanNode<Resource>> yagoShapeInstances) {
    return yagoShapeInstances.entrySet().stream().map(e -> {
      Resource object = e.getKey();
      return e.getValue().map(subject -> VALUE_FACTORY.createStatement(subject, RDF.TYPE, object));
    }).reduce(PlanNode::union).orElseGet(PlanNode::empty);
  }

  private static PlanNode<Statement> buildFullInstanceOf(PlanNode<Resource> yagoThings, PlanNode<Resource> yagoClasses, PartitionedStatements partitionedStatements, PairPlanNode<Resource, Resource> wikidataToYagoUrisMapping) {
    var wikidataInstanceOf = partitionedStatements.getForKey(keyForIri(WDT_P31))
            .mapToPair(t -> Map.entry(t.getSubject(), (Resource) t.getObject()));

    var instanceOfSubjectFiltered = mapKeyToYago(wikidataInstanceOf, wikidataToYagoUrisMapping)
            .intersection(yagoThings);

    return mapKeyToYago(instanceOfSubjectFiltered.swap(), wikidataToYagoUrisMapping)
            .intersection(yagoClasses)
            .map((o, s) -> VALUE_FACTORY.createStatement(s, RDF.TYPE, o));
  }

  private static PlanNode<Statement> buildClassesDescription(PlanNode<Resource> yagoClasses, PairPlanNode<Resource, Resource> yagoSuperClassOf, PartitionedStatements partitionedStatements, PairPlanNode<Resource, Resource> wikidataToYagoUrisMapping) {
    var yagoOwlClassTriple = yagoClasses.map(c -> VALUE_FACTORY.createStatement(c, RDF.TYPE, OWL.CLASS));

    var yagoSubClassOf = yagoSuperClassOf.map((o, s) -> VALUE_FACTORY.createStatement(s, RDFS.SUBCLASSOF, o));

    var wikidataLabel = partitionedStatements.getForKey(keyForIri(SKOS.PREF_LABEL))
            .mapToPair(t -> Map.entry(t.getSubject(), t.getObject()));
    var rdfsLabel = mapKeyToYago(wikidataLabel, wikidataToYagoUrisMapping)
            .intersection(yagoClasses)
            .map((s, o) -> VALUE_FACTORY.createStatement(s, RDFS.LABEL, o));

    var wikidataDescription = partitionedStatements.getForKey(keyForIri(SCHEMA_DESCRIPTION))
            .mapToPair(t -> Map.entry(t.getSubject(), t.getObject()));
    var rdfsComment = mapKeyToYago(wikidataDescription, wikidataToYagoUrisMapping)
            .intersection(yagoClasses)
            .map((s, o) -> VALUE_FACTORY.createStatement(s, RDFS.COMMENT, o));

    return yagoSubClassOf.union(yagoOwlClassTriple).union(rdfsLabel).union(rdfsComment);
  }

  /**
   * Builds yago facts from direct properties like rdfs:label
   */
  private static PlanNode<Statement> buildFactsFromRdfProperty(
          PartitionedStatements partitionedStatements,
          Map<Resource, PlanNode<Resource>> yagoShapeInstances,
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

      PairPlanNode<Resource, Value> subjectObjects;
      if (propertyShape.getDatatypes().isPresent()) {
        var datatypes = propertyShape.getDatatypes().orElseGet(Collections::emptySet);
        subjectObjects = getPropertyValues(partitionedStatements, propertyShape)
                .filterValue(object -> object instanceof Literal && datatypes.contains(((Literal) object).getDatatype()));
      } else if (propertyShape.getNodeShape().isPresent()) {
        subjectObjects = filterObjectRange(
                mapKeyToYago(getPropertyValues(partitionedStatements, propertyShape).mapPair((s, o) -> Map.entry((Resource) o, s)), wikidataToYagoUrisMapping),
                yagoShapeInstances,
                propertyShape
        );
      } else {
        System.err.println("No range constraint found for property " + propertyShape.getProperty() + ". Ignoring it.");
        return PlanNode.<Statement>empty();
      }

      //Regex
      if (propertyShape.getPattern().isPresent()) {
        Pattern pattern = propertyShape.getPattern().get();
        subjectObjects = subjectObjects.filterValue(object -> pattern.matcher(object.stringValue()).matches());
      }

      // Domain type filter
      subjectObjects = filterDomain(
              mapKeyToYago(subjectObjects, wikidataToYagoUrisMapping),
              yagoShapeInstances,
              propertyShape
      );

      return subjectObjects.map((s, o) -> VALUE_FACTORY.createStatement(s, yagoProperty, o));
    }).reduce(PlanNode::union).orElseGet(PlanNode::empty);
  }

  private static PairPlanNode<Resource, Value> getPropertyValues(PartitionedStatements partitionedStatements, ShaclSchema.PropertyShape propertyShape) {
    return propertyShape.getFromProperties()
            .map(wikidataProperty -> partitionedStatements.getForKey(keyForIri(wikidataProperty)))
            .reduce(PlanNode::union).orElseGet(PlanNode::empty)
            .mapToPair(t -> Map.entry(t.getSubject(), t.getObject()));
  }

  private static Map.Entry<PlanNode<Statement>, PlanNode<AnnotatedStatement>> buildPropertiesFromSchema(
          PartitionedStatements partitionedStatements,
          Map<Resource, PlanNode<Resource>> yagoShapeInstances,
          PairPlanNode<Resource, Resource> wikidataToYagoUrisMapping,
          Set<IRI> onlyProperties,
          Set<IRI> excludeProperties
  ) {
    // Some utility plans
    PlanNode<Resource> bestRanks = partitionedStatements.getForKey(keyForIri(RDF.TYPE))
            .filter(s -> s.getObject().equals(WIKIBASE_BEST_RANK))
            .map(Statement::getSubject).cache();

    PairPlanNode<Resource, Value> cleanTimes = partitionedStatements.getForKey(keyForIri(WIKIBASE_TIME_VALUE))
            .mapToPair(s -> Map.entry(s.getSubject(), s.getObject()))
            .join(partitionedStatements.getForKey(keyForIri(WIKIBASE_TIME_PRECISION)).mapToPair(s -> Map.entry(s.getSubject(), s.getObject())))
            .join(partitionedStatements.getForKey(keyForIri(WIKIBASE_TIME_CALENDAR_MODEL)).mapToPair(s -> Map.entry(s.getSubject(), s.getObject())))
            .flatMapPair((k, e) -> convertTime(e.getKey().getKey(), e.getKey().getValue(), e.getValue()).map(t -> Map.entry(k, t)))
            .distinct()
            .cache();

    PairPlanNode<Resource, Map.Entry<Value, List<Statement>>> cleanCoordinates = partitionedStatements.getForKey(keyForIri(WIKIBASE_GEO_LATITUDE))
            .mapToPair(s -> Map.entry(s.getSubject(), s.getObject()))
            .join(partitionedStatements.getForKey(keyForIri(WIKIBASE_GEO_LONGITUDE)).mapToPair(s -> Map.entry(s.getSubject(), s.getObject())))
            .join(partitionedStatements.getForKey(keyForIri(WIKIBASE_GEO_PRECISION)).mapToPair(s -> Map.entry(s.getSubject(), s.getObject())))
            .join(partitionedStatements.getForKey(keyForIri(WIKIBASE_GEO_GLOBE)).mapToPair(s -> Map.entry(s.getSubject(), s.getObject())))
            .flatMapPair((k, e) -> convertGlobeCoordinates(e.getKey().getKey().getKey(), e.getKey().getKey().getValue(), e.getKey().getValue(), e.getValue()).map(t -> Map.entry(k, t)))
            .distinct()
            .cache();

    var quantityAmountAndUnit = partitionedStatements.getForKey(keyForIri(WIKIBASE_QUANTITY_AMOUNT))
            .mapToPair(s -> Map.entry(s.getSubject(), s.getObject()))
            .join(partitionedStatements.getForKey(keyForIri(WIKIBASE_QUANTITY_UNIT)).mapToPair(s -> Map.entry(s.getSubject(), s.getObject())));

    PairPlanNode<Resource, Value> cleanDurations = quantityAmountAndUnit
            .flatMapPair((k, e) -> convertDurationQuantity(e.getKey(), e.getValue()).map(t -> Map.entry(k, t)))
            .distinct()
            .cache();

    PairPlanNode<Resource, Value> cleanIntegers = quantityAmountAndUnit
            .flatMapPair((k, e) -> convertIntegerQuantity(e.getKey(), e.getValue()).map(t -> Map.entry(k, t)))
            .distinct()
            .cache();

    PairPlanNode<Resource, Map.Entry<Value, List<Statement>>> cleanQuantities = mapKeyToYago(partitionedStatements.getForKey(keyForIri(WIKIBASE_QUANTITY_UNIT))
            .mapToPair(s -> Map.entry((Resource) s.getObject(), s.getSubject())), wikidataToYagoUrisMapping)
            .swap()
            .join(partitionedStatements.getForKey(keyForIri(WIKIBASE_QUANTITY_AMOUNT)).mapToPair(s -> Map.entry(s.getSubject(), s.getObject())))
            .join(partitionedStatements.getForKey(keyForIri(WIKIBASE_QUANTITY_LOWER_BOUND)).mapToPair(s -> Map.entry(s.getSubject(), s.getObject())))
            .join(partitionedStatements.getForKey(keyForIri(WIKIBASE_QUANTITY_UPPER_BOUND)).mapToPair(s -> Map.entry(s.getSubject(), s.getObject())))
            .mapPair((k, e) -> Map.entry(k, convertQuantity(k, e.getKey().getKey().getKey(), e.getKey().getKey().getValue(), e.getKey().getValue(), e.getValue())))
            .distinct()
            .cache();

    var statementsWithAnnotations = ShaclSchema.getSchema().getAnnotationPropertyShapes().map(annotationShape ->
            mapWikidataPropertyValue(annotationShape,
                    partitionedStatements, yagoShapeInstances, wikidataToYagoUrisMapping,
                    cleanTimes, cleanDurations, cleanIntegers, cleanQuantities, cleanCoordinates,
                    PQ_PREFIX, PQV_PREFIX
            ).mapValue(v -> Map.entry(annotationShape.getProperty(), v))
    ).reduce(PairPlanNode::union).orElseGet(PairPlanNode::empty);

    return ShaclSchema.getSchema().getPropertyShapes().map(propertyShape -> {
      IRI yagoProperty = propertyShape.getProperty();
      if (onlyProperties != null && !onlyProperties.contains(yagoProperty)) {
        return Map.entry(PlanNode.<Statement>empty(), PlanNode.<AnnotatedStatement>empty());
      }
      if (excludeProperties != null && excludeProperties.contains(yagoProperty)) {
        return Map.entry(PlanNode.<Statement>empty(), PlanNode.<AnnotatedStatement>empty());
      }

      // We map the statement -> object relation
      var statementObject = mapWikidataPropertyValue(propertyShape,
              partitionedStatements, yagoShapeInstances, wikidataToYagoUrisMapping,
              cleanTimes, cleanDurations, cleanIntegers, cleanQuantities, cleanCoordinates,
              PS_PREFIX, PSV_PREFIX
      );

      // We map the subject -> statement relation with domain filter
      var subjectStatement = filterDomain(
              mapKeyToYago(getSubjectStatement(partitionedStatements, propertyShape), wikidataToYagoUrisMapping),
              yagoShapeInstances,
              propertyShape
      ).mapValue(s -> (Resource) s);

      var statementTriple = statementObject
              .join(subjectStatement.swap())
              .mapValue(e -> Map.entry(VALUE_FACTORY.createStatement(e.getValue(), yagoProperty, e.getKey().getKey()), e.getKey().getValue()));

      var bestMainFacts = statementTriple.intersection(bestRanks).values();  // We keep only best ranks

      if (propertyShape.getMaxCount().isPresent()) {
        var maxCount = propertyShape.getMaxCount().getAsInt();
        bestMainFacts = bestMainFacts.mapToPair(s -> Map.entry(s.getKey().getSubject(), s)).aggregateByKey().flatMap((k, values) -> {
          if (values.size() <= maxCount) {
            return values.stream();
          } else {
            return Stream.empty();
          }
        });
      }

      var mainFacts = bestMainFacts.flatMap(e -> {
        if (e.getValue().isEmpty()) {
          return Stream.of(e.getKey());
        } else {
          return Stream.concat(Stream.of(e.getKey()), e.getValue().stream());
        }
      });

      // Annotations
      //TODO: emit object annotations
      //TODO maxCount on annotations
      var annotations = statementTriple
              .join(statementsWithAnnotations)
              .map((s, e) -> new AnnotatedStatement(e.getKey().getKey(), e.getValue().getKey(), e.getValue().getValue().getKey()));

      return Map.entry(mainFacts, annotations);
    }).reduce((p1, p2) -> Map.entry(p1.getKey().union(p2.getKey()), p1.getValue().union(p2.getValue())))
            .orElseGet(() -> Map.entry(PlanNode.empty(), PlanNode.empty()));
  }

  private static PairPlanNode<Resource, Map.Entry<Value, List<Statement>>> mapWikidataPropertyValue(
          ShaclSchema.PropertyShape propertyShape,
          PartitionedStatements partitionedStatements,
          Map<Resource, PlanNode<Resource>> yagoShapeInstances, PairPlanNode<Resource, Resource> wikidataToYagoUrisMapping,
          PairPlanNode<Resource, Value> cleanTimes, PairPlanNode<Resource, Value> cleanDurations, PairPlanNode<Resource, Value> cleanIntegers,
          PairPlanNode<Resource, Map.Entry<Value, List<Statement>>> cleanQuantities,
          PairPlanNode<Resource, Map.Entry<Value, List<Statement>>> cleanCoordinates,
          String simpleValuePrefix, String complexValuePrefix
  ) {
    PairPlanNode<Resource, Map.Entry<Value, List<Statement>>> statementObject;
    if (propertyShape.getDatatypes().isPresent()) {
      if (propertyShape.getNodeShape().isPresent()) {
        System.err.println("The property " + propertyShape.getProperty() + " could not have both a datatype domain and a node domain. Ignoring it.");
        return PairPlanNode.empty();
      }
      // Datatype filter
      Set<IRI> dts = propertyShape.getDatatypes().get();

      if (dts.equals(Collections.singleton(XMLSchema.ANYURI))) {
        //We map IRIs to xsd:anyUri
        statementObject = getTriplesFromWikidataPropertyRelation(partitionedStatements, propertyShape, simpleValuePrefix).flatMapValue(object -> {
          if (object instanceof IRI || (object instanceof Literal && XMLSchema.ANYURI.equals(((Literal) object).getDatatype()))) {
            return normalizeIri(object.stringValue())
                    .map(o -> Map.entry(VALUE_FACTORY.createLiteral(o, XMLSchema.ANYURI), Collections.emptyList()));
          } else {
            return Stream.of(Map.entry(object, Collections.emptyList()));
          }
        });
      } else if (CALENDAR_DT_SET.containsAll(dts)) {
        //We clean up times by retrieving their full representation
        statementObject = getAndConvertStatementsComplexValue(partitionedStatements, propertyShape, cleanTimes, complexValuePrefix);
      } else if (dts.equals(Collections.singleton(XMLSchema.DURATION))) {
        //We clean up durations form Wikibase quantities by retrieving their full representation
        statementObject = getAndConvertStatementsComplexValue(partitionedStatements, propertyShape, cleanDurations, complexValuePrefix);
      } else if (dts.equals(Collections.singleton(XMLSchema.INTEGER))) {
        //We clean up durations form Wikibase quantities by retrieving their full representation
        statementObject = getAndConvertStatementsComplexValue(partitionedStatements, propertyShape, cleanIntegers, complexValuePrefix);
      } else {
        statementObject = getTriplesFromWikidataPropertyRelation(partitionedStatements, propertyShape, simpleValuePrefix)
                .filterValue(object -> object instanceof Literal && dts.contains(((Literal) object).getDatatype()))
                .mapValue(o -> Map.entry(o, Collections.emptyList()));
      }
    } else if (propertyShape.getNodeShape().isPresent()) {
      // Range type filter
      Set<Resource> expectedClasses = propertyShape.getNodeShape()
              .map(ShaclSchema.NodeShape::getClasses)
              .orElseGet(Stream::empty)
              .collect(Collectors.toSet());
      if (Collections.singleton(SCHEMA_GEO_COORDINATES).equals(expectedClasses)) {
        //We clean up globe coordinates by retrieving their full representation
        statementObject = getAndConvertStatementsAnnotatedComplexValue(partitionedStatements, propertyShape, cleanCoordinates, complexValuePrefix);
      } else if (Collections.singleton(SCHEMA_QUANTITATIVE_VALUE).equals(expectedClasses)) {
        statementObject = getAndConvertStatementsAnnotatedComplexValue(partitionedStatements, propertyShape, cleanQuantities, complexValuePrefix);
      } else if (Collections.singleton(SCHEMA_IMAGE_OBJECT).equals(expectedClasses)) {
        //We clean up image by retrieving their full representation
        statementObject = getTriplesFromWikidataPropertyRelation(partitionedStatements, propertyShape, simpleValuePrefix)
                .filterValue(v -> v.stringValue().startsWith("http://commons.wikimedia.org/wiki/Special:FilePath/"))
                .mapValue(o -> Map.entry(o, Collections.emptyList()));
        //TODO: image descriptions
      } else {
        statementObject = filterObjectRange(
                mapKeyToYago(getTriplesFromWikidataPropertyRelation(partitionedStatements, propertyShape, simpleValuePrefix).mapPair((s, o) -> Map.entry((Resource) o, s)), wikidataToYagoUrisMapping),
                yagoShapeInstances,
                propertyShape
        ).mapValue(o -> Map.entry(o, Collections.emptyList()));
      }
    } else {
      System.err.println("No range constraint found for property " + propertyShape.getProperty() + ". Ignoring it.");
      return PairPlanNode.empty();
    }

    //Regex
    if (propertyShape.getPattern().isPresent()) {
      Pattern pattern = propertyShape.getPattern().get();
      statementObject = statementObject.filterValue(o -> pattern.matcher(o.getKey().stringValue()).matches());
    }

    return statementObject;
  }

  private static PairPlanNode<Resource, Map.Entry<Value, List<Statement>>> getAndConvertStatementsComplexValue(PartitionedStatements partitionedStatements, ShaclSchema.PropertyShape propertyShape, PairPlanNode<Resource, Value> clean, String complexValuePrefix) {
    return getTriplesFromWikidataPropertyRelation(partitionedStatements, propertyShape, complexValuePrefix)
            .mapPair((k, v) -> Map.entry((Resource) v, k))
            .join(clean)
            .values()
            .mapToPair(t -> Map.entry(t.getKey(), Map.entry(t.getValue(), Collections.emptyList())));
  }

  private static PairPlanNode<Resource, Map.Entry<Value, List<Statement>>> getAndConvertStatementsAnnotatedComplexValue(PartitionedStatements partitionedStatements, ShaclSchema.PropertyShape propertyShape, PairPlanNode<Resource, Map.Entry<Value, List<Statement>>> clean, String complexValuePrefix) {
    return getTriplesFromWikidataPropertyRelation(partitionedStatements, propertyShape, complexValuePrefix)
            .mapPair((k, v) -> Map.entry((Resource) v, k))
            .join(clean)
            .values()
            .mapToPair(t -> t);
  }

  private static PairPlanNode<Resource, Value> getSubjectStatement(PartitionedStatements partitionedStatements, ShaclSchema.PropertyShape propertyShape) {
    return getTriplesFromWikidataPropertyRelation(partitionedStatements, propertyShape, P_PREFIX);
  }

  private static PairPlanNode<Resource, Value> getTriplesFromWikidataPropertyRelation(PartitionedStatements partitionedStatements, ShaclSchema.PropertyShape propertyShape, String prefix) {
    return propertyShape.getFromProperties().map(wikidataProperty -> {
      if (WIKIDATA_PROPERTY_IRI_PATTERN.matcher(wikidataProperty.stringValue()).matches()) {
        return partitionedStatements.getForKey(keyForIri(VALUE_FACTORY.createIRI(prefix, wikidataProperty.getLocalName())));
      } else {
        System.err.println("Invalid Wikidata property IRI: " + wikidataProperty);
        return PlanNode.<Statement>empty();
      }
    }).reduce(PlanNode::union).orElseGet(PlanNode::empty).mapToPair(t -> Map.entry(t.getSubject(), t.getObject()));
  }

  private static PairPlanNode<Resource, Value> filterDomain(PairPlanNode<Resource, Value> subjectObjects, Map<Resource, PlanNode<Resource>> yagoShapeInstances, ShaclSchema.PropertyShape propertyShape) {
    return propertyShape.getParentShape().stream()
            .flatMap(ShaclSchema.NodeShape::getClasses)
            .distinct()
            .flatMap(cls -> Stream.ofNullable(yagoShapeInstances.get(cls)))
            .map(subjectObjects::intersection)
            .reduce(PairPlanNode::union)
            .orElseGet(PairPlanNode::empty);
  }

  private static PairPlanNode<Resource, Value> filterObjectRange(PairPlanNode<Resource, Resource> objectSubjects, Map<Resource, PlanNode<Resource>> yagoShapeInstances, ShaclSchema.PropertyShape propertyShape) {
    return propertyShape.getNodeShape()
            .map(ShaclSchema.NodeShape::getClasses)
            .orElseGet(Stream::empty)
            .distinct()
            .flatMap(cls -> Stream.ofNullable(yagoShapeInstances.get(cls)))
            .map(objectSubjects::intersection)
            .reduce(PairPlanNode::union).orElseGet(PairPlanNode::empty)
            .mapPair((k, v) -> Map.entry(v, k));
  }

  private static PlanNode<Statement> buildSameAs(PartitionedStatements partitionedStatements, PlanNode<Resource> yagoThings, PairPlanNode<Resource, Resource> wikidataToYagoUrisMapping) {
    // Wikidata
    PlanNode<Statement> wikidata = wikidataToYagoUrisMapping
            .swap()
            .intersection(yagoThings)
            .map((yago, wd) -> VALUE_FACTORY.createStatement(yago, OWL.SAMEAS, wd));

    //dbPedia
    PlanNode<Statement> dbPedia = mapKeyToYago(partitionedStatements.getForKey(keyForIri(SCHEMA_ABOUT))
            .filter(t -> t.getSubject().stringValue().startsWith("https://en.wikipedia.org/wiki/"))
            .mapToPair(s -> Map.entry((Resource) s.getObject(), s.getSubject())), wikidataToYagoUrisMapping)
            .intersection(yagoThings)
            .mapValue(wikipedia -> VALUE_FACTORY.createIRI(wikipedia.stringValue().replace("https://en.wikipedia.org/wiki/", "http://dbpedia.org/resource/")))
            .map((yago, dbpedia) -> VALUE_FACTORY.createStatement(yago, OWL.SAMEAS, dbpedia));

    //Freebase
    Pattern freebaseIdPattern = Pattern.compile("/m/0([0-9a-z_]{2,6}|1[0123][0-9a-z_]{5})$");
    PlanNode<Statement> freebase = mapKeyToYago(partitionedStatements.getForKey(keyForIri(WDT_P646)).mapToPair(s -> Map.entry(s.getSubject(), s.getObject())), wikidataToYagoUrisMapping)
            .intersection(yagoThings)
            .filterValue(fb -> freebaseIdPattern.matcher(fb.stringValue()).matches())
            .mapValue(fb -> VALUE_FACTORY.createIRI("http://rdf.freebase.com/ns/", fb.stringValue().substring(1).replace("/", ".")))
            .map((yago, fp) -> VALUE_FACTORY.createStatement(yago, OWL.SAMEAS, fp));

    //Wikipedia
    PlanNode<Statement> wikipedia = mapKeyToYago(partitionedStatements.getForKey(keyForIri(SCHEMA_ABOUT))
            .filter(t -> t.getSubject().stringValue().contains(".wikipedia.org/wiki/"))
            .mapToPair(s -> Map.entry((Resource) s.getObject(), s.getSubject())), wikidataToYagoUrisMapping)
            .intersection(yagoThings)
            .flatMapValue(iri -> normalizeIri(iri.stringValue()))
            .map((yago, wp) -> VALUE_FACTORY.createStatement(yago, SCHEMA_SAME_AS, VALUE_FACTORY.createLiteral(wp, XMLSchema.ANYURI)));

    return wikidata.union(dbPedia).union(freebase).union(wikipedia);
  }

  private static PlanNode<Statement> buildYagoSchema() {
    Model yagoStatements = new LinkedHashModel(NAMESPACES);
    ShaclSchema schema = ShaclSchema.getSchema();

    Map<Resource, Set<Resource>> domains = new HashMap<>();
    Map<Resource, Set<Resource>> objectRanges = new HashMap<>();
    Map<Resource, Set<Resource>> datatypeRanges = new HashMap<>();

    // Classes
    schema.getNodeShapes()
            .flatMap(ShaclSchema.NodeShape::getClasses)
            .flatMap(c -> schema.getClass(c).stream())
            .forEach(c -> {
              yagoStatements.add(c.getTerm(), RDF.TYPE, OWL.CLASS);
              c.getLabels().forEach(l -> yagoStatements.add(c.getTerm(), RDFS.LABEL, VALUE_FACTORY.createLiteral(camlCaseToRegular(l.stringValue()), "en")));
              c.getComments().forEach(l -> yagoStatements.add(c.getTerm(), RDFS.COMMENT, VALUE_FACTORY.createLiteral(l.stringValue().replaceAll("<[^>]+>", ""), "en")));
              c.getSuperClasses().forEach(cp -> {
                if (cp.equals(SCHEMA_INTANGIBLE)) {
                  //We ignore schema:Intangible
                  yagoStatements.add(c.getTerm(), RDFS.SUBCLASSOF, SCHEMA_THING);
                } else if (cp.equals(SCHEMA_STRUCTURED_VALUE)) {
                  //schema:StructuredValue are not schema:Thing
                  //TODO: breaks Blazegraph yagoStatements.add(c.getTerm(), RDFS.SUBCLASSOF, OWL.THING);
                } else { //We ignore schema:Intangible
                  yagoStatements.add(c.getTerm(), RDFS.SUBCLASSOF, cp);
                }
              });
              c.getDisjointedClasses().forEach(dc -> yagoStatements.add(c.getTerm(), OWL.DISJOINTWITH, dc));
            });

    // Properties
    schema.getPropertyShapes()
            .forEach(shape -> schema.getProperty(shape.getProperty()).ifPresent(p -> {
              if (shape.getNodeShape().isPresent() && shape.getDatatypes().isEmpty()) {
                yagoStatements.add(p.getTerm(), RDF.TYPE, OWL.OBJECTPROPERTY);
              } else if (shape.getNodeShape().isEmpty() && shape.getDatatypes().isPresent()) {
                yagoStatements.add(p.getTerm(), RDF.TYPE, OWL.DATATYPEPROPERTY);
              } else {
                System.err.println("Not sure if " + p.getTerm() + " is an object or a datatype property.");
              }
              p.getLabels().forEach(l -> yagoStatements.add(p.getTerm(), RDFS.LABEL, VALUE_FACTORY.createLiteral(camlCaseToRegular(l.stringValue()), "en")));
              p.getComments().forEach(l -> yagoStatements.add(p.getTerm(), RDFS.COMMENT, VALUE_FACTORY.createLiteral(l.stringValue().replaceAll("<[^>]+>", ""), "en")));
              p.getSuperProperties().forEach(cp -> yagoStatements.add(p.getTerm(), RDFS.SUBPROPERTYOF, cp));
              p.getInverseProperties().forEach(cp -> yagoStatements.add(p.getTerm(), OWL.INVERSEOF, cp));
              shape.getMaxCount().ifPresent(maxCount -> {
                if (maxCount == 1) {
                  yagoStatements.add(p.getTerm(), RDF.TYPE, OWL.FUNCTIONALPROPERTY);
                }
                //TODO: owl:maxCardinality
              });

              shape.getParentShape().ifPresent(subjectShape -> {
                Set<Resource> target = domains.computeIfAbsent(p.getTerm(), (k) -> new HashSet<>());
                subjectShape.getClasses().forEach(target::add);
              });
              shape.getNodeShape().ifPresent(objectShape -> {
                Set<Resource> target = objectRanges.computeIfAbsent(p.getTerm(), (k) -> new HashSet<>());
                objectShape.getClasses().forEach(target::add);
              });
              shape.getDatatypes().ifPresent(datatypes -> datatypeRanges.computeIfAbsent(p.getTerm(), (k) -> new HashSet<>()).addAll(datatypes));
            }));

    // Domains
    for (Map.Entry<Resource, Set<Resource>> e : domains.entrySet()) {
      addUnionOfObject(yagoStatements, e.getKey(), RDFS.DOMAIN, e.getValue(), OWL.CLASS);
    }
    // Ranges
    for (Map.Entry<Resource, Set<Resource>> e : objectRanges.entrySet()) {
      addUnionOfObject(yagoStatements, e.getKey(), RDFS.RANGE, e.getValue(), OWL.CLASS);
    }
    for (Map.Entry<Resource, Set<Resource>> e : datatypeRanges.entrySet()) {
      addUnionOfObject(yagoStatements, e.getKey(), RDFS.RANGE, e.getValue(), RDFS.DATATYPE);
    }

    // Some hardcoded triples
    //TODO: breaks Blazegraph yagoStatements.add(SCHEMA_THING, RDFS.SUBCLASSOF, OWL.THING);
    yagoStatements.add(RDF.LANGSTRING, RDF.TYPE, RDFS.DATATYPE);

    return PlanNode.fromCollection(yagoStatements);
  }

  private static PlanNode<Statement> buildYagoShapes() {
    Model yagoStatements = new LinkedHashModel(NAMESPACES);

    ShaclSchema.getSchema().getNodeShapes().forEach(shape -> shape.getClasses().forEach(cls -> {
              yagoStatements.add(cls, RDF.TYPE, SHACL.NODE_SHAPE);
              shape.getProperties().forEach(prop -> {
                Resource propShapeSubject = VALUE_FACTORY.createIRI(YAGO_RESOURCE_PREFIX, "shape-prop-" + stringName(yagoStatements, cls) + "-" + stringName(yagoStatements, prop.getProperty()));
                yagoStatements.add(cls, SHACL.PROPERTY, propShapeSubject);
                yagoStatements.add(propShapeSubject, RDF.TYPE, SHACL.PROPERTY_SHAPE);
                yagoStatements.add(propShapeSubject, SHACL.PATH, prop.getProperty());

                prop.getDatatypes().ifPresent(datatypes -> {
                  if (datatypes.size() <= 1) {
                    for (IRI datatype : datatypes) {
                      yagoStatements.add(propShapeSubject, SHACL.DATATYPE, datatype);
                    }
                  } else {
                    addListObject(yagoStatements, propShapeSubject, SHACL.OR, datatypes.stream().map(datatype -> {
                      Resource subject = VALUE_FACTORY.createIRI(YAGO_RESOURCE_PREFIX, "sh:datatype-" + stringName(yagoStatements, datatype));
                      yagoStatements.add(subject, SHACL.DATATYPE, datatype);
                      return subject;
                    }).sorted(Comparator.comparing(Value::stringValue)).collect(Collectors.toList()));
                  }
                });

                prop.getNodeShape().ifPresent(nodeShape -> {
                  List<Resource> nodes = nodeShape.getClasses().collect(Collectors.toList());
                  if (nodes.size() <= 1) {
                    for (Resource node : nodes) {
                      yagoStatements.add(propShapeSubject, SHACL.NODE, node);
                    }
                  } else {
                    addListObject(yagoStatements, propShapeSubject, SHACL.OR, nodes.stream().map(node -> {
                      Resource subject = VALUE_FACTORY.createIRI(YAGO_RESOURCE_PREFIX, "sh:node-" + stringName(yagoStatements, node));
                      yagoStatements.add(subject, SHACL.NODE, node);
                      return subject;
                    }).sorted(Comparator.comparing(Value::stringValue)).collect(Collectors.toList()));
                  }
                });

                prop.getPattern().ifPresent(pattern -> yagoStatements.add(propShapeSubject, SHACL.PATTERN, VALUE_FACTORY.createLiteral(pattern.toString())));
                prop.getMaxCount().ifPresent(maxCount -> yagoStatements.add(propShapeSubject, SHACL.MAX_COUNT, VALUE_FACTORY.createLiteral(maxCount)));
              });
            })
    );
    return PlanNode.fromCollection(yagoStatements);
  }

  private static PairPlanNode<Resource, Resource> subClassOfFromYagoSchema() {
    ShaclSchema schema = ShaclSchema.getSchema();
    return PairPlanNode.fromCollection(schema.getNodeShapes()
            .flatMap(ShaclSchema.NodeShape::getClasses)
            .flatMap(c -> schema.getClass(c).stream())
            .flatMap(c -> c.getSuperClasses().flatMap(cp -> {
              if (cp.equals(SCHEMA_INTANGIBLE)) {
                //We ignore schema:Intangible
                return Stream.of(Map.entry(c.getTerm(), (Resource) SCHEMA_THING));
              } else if (cp.equals(SCHEMA_STRUCTURED_VALUE)) {
                //schema:StructuredValue are not schema:Thing
                return Stream.empty();
              } else { //We ignore schema:Intangible
                return Stream.of(Map.entry(c.getTerm(), cp));
              }
            })).collect(Collectors.toSet()));
  }

  private static String camlCaseToRegular(String txt) {
    return StringUtils.join(StringUtils.splitByCharacterTypeCamelCase(txt), " ").toLowerCase().replaceAll(" ", " ").replaceAll("  ", " ");
  }

  private static PlanNode<Resource> mapToYago(PlanNode<Resource> facts, PairPlanNode<Resource, Resource> wikidataToYagoUrisMapping) {
    return facts
            .join(wikidataToYagoUrisMapping)
            .values();
  }

  private static <V> PairPlanNode<Resource, V> mapKeyToYago(PairPlanNode<Resource, V> facts, PairPlanNode<Resource, Resource> wikidataToYagoUrisMapping) {
    return facts
            .join(wikidataToYagoUrisMapping)
            .mapPair((subject, pair) -> Map.entry(pair.getValue(), pair.getKey()));
  }

  private static Stream<String> normalizeIri(String uri) {
    try {
      return Stream.of(new ParsedIRI(uri).normalize().toString());
    } catch (URISyntaxException e) {
      return Stream.empty();
    }
  }

  private static Stream<Value> convertTime(Value value, Value precision, Value calendarModel) {
    if (!(value instanceof Literal) || !(precision instanceof Literal)) {
      return Stream.empty();
    }
    if (!WD_Q1985727.equals(calendarModel)) {
      return Stream.empty(); //TODO: add julian calendar support
    }

    try {
      TemporalAccessor input = WIKIBASE_TIMESTAMP_FORMATTER.parse(value.stringValue());
      if (input.get(ChronoField.YEAR) <= 0) {
        return Stream.empty(); //TODO: add negative years support
      }
      int p = ((Literal) precision).intValue();
      switch (p) {
        case 9:
          return Stream.of(VALUE_FACTORY.createLiteral(Year.from(input)));
        case 10:
          return Stream.of(VALUE_FACTORY.createLiteral(YearMonth.from(input)));
        case 11:
          return Stream.of(VALUE_FACTORY.createLiteral(LocalDate.from(input)));
        case 14:
          return Stream.of(VALUE_FACTORY.createLiteral(OffsetDateTime.from(input)));
        default:
          return Stream.empty();
      }
    } catch (DateTimeException | IllegalArgumentException e) {
      return Stream.empty();
    }
  }

  private static Stream<Map.Entry<Value, List<Statement>>> convertGlobeCoordinates(Value latitude, Value longitude, Value precision, Value globe) {
    if (!globe.equals(WD_Q2)) {
      return Stream.empty(); //Not earth
    }
    if (!(latitude instanceof Literal && longitude instanceof Literal && precision instanceof Literal)) {
      return Stream.empty();
    }

    double lat = ((Literal) latitude).doubleValue();
    double lon = ((Literal) longitude).doubleValue();
    double prec = ((Literal) precision).doubleValue();

    double roundedLatitude = roundDegrees(lat, prec);
    double roundedLongitude = roundDegrees(lon, prec);
    IRI coordinates = VALUE_FACTORY.createIRI("geo:" + roundedLatitude + "," + roundedLongitude);

    return Stream.of(Map.entry(
            coordinates,
            List.of(
                    VALUE_FACTORY.createStatement(coordinates, RDF.TYPE, SCHEMA_GEO_COORDINATES),
                    VALUE_FACTORY.createStatement(coordinates, SCHEMA_LATITUDE, VALUE_FACTORY.createLiteral(roundedLatitude)),
                    VALUE_FACTORY.createStatement(coordinates, SCHEMA_LONGITUDE, VALUE_FACTORY.createLiteral(roundedLongitude))
            )
    ));
  }

  /**
   * From https://github.com/DataValues/Geo/blob/master/src/Formatters/LatLongFormatter.php
   */
  private static double roundDegrees(double degrees, double precision) {
    double sign = (degrees > 0) ? 1 : -1;
    long reduced = Math.round(Math.abs(degrees) / precision);
    double expended = reduced * precision;
    return sign * expended;
  }

  private static Stream<Value> convertDurationQuantity(Value amountNode, Value unitNode) {
    if (!(amountNode instanceof Literal)) {
      return Stream.empty();
    }
    try {
      long value = ((Literal) amountNode).decimalValue().longValueExact();
      if (unitNode.equals(WD_Q11574)) {
        return Stream.of(VALUE_FACTORY.createLiteral(Duration.ofSeconds(value)));
      } else if (unitNode.equals(WD_Q7727)) {
        return Stream.of(VALUE_FACTORY.createLiteral(Duration.ofMinutes(value)));
      } else if (unitNode.equals(WD_Q25235)) {
        return Stream.of(VALUE_FACTORY.createLiteral(Duration.ofHours(value)));
      } else if (unitNode.equals(WD_Q573)) {
        return Stream.of(VALUE_FACTORY.createLiteral(Duration.ofDays(value)));
      } else {
        return Stream.empty();
      }
    } catch (ArithmeticException | NumberFormatException e) {
      return Stream.empty();
    }
  }

  private static Stream<Value> convertIntegerQuantity(Value amountNode, Value unitNode) {
    if (!unitNode.equals(WD_Q199) || !(amountNode instanceof Literal)) {
      return Stream.empty();
    }
    try {
      long value = ((Literal) amountNode).decimalValue().longValueExact();
      return Stream.of(VALUE_FACTORY.createLiteral(value));
    } catch (ArithmeticException | NumberFormatException e) {
      return Stream.empty();
    }
  }

  private static Map.Entry<Value, List<Statement>> convertQuantity(Value subject, Resource unit, Value amount, Value lowerBound, Value upperBound) {
    IRI quantity = VALUE_FACTORY.createIRI(YAGO_VALUE_PREFIX, ((IRI) subject).getLocalName());
    return Map.entry(
            quantity,
            List.of(
                    VALUE_FACTORY.createStatement(quantity, RDF.TYPE, SCHEMA_QUANTITATIVE_VALUE),
                    VALUE_FACTORY.createStatement(quantity, SCHEMA_VALUE, amount),
                    VALUE_FACTORY.createStatement(quantity, SCHEMA_MIN_VALUE, lowerBound),
                    VALUE_FACTORY.createStatement(quantity, SCHEMA_MAX_VALUE, upperBound),
                    VALUE_FACTORY.createStatement(quantity, SCHEMA_UNIT_CODE, unit)
            )
    );
  }

  private static void addUnionOfObject(Model model, Resource subject, IRI predicate, Collection<? extends Resource> objects, IRI type) {
    if (objects.size() == 1) {
      model.add(subject, predicate, objects.iterator().next());
    } else {
      List<Value> list = objects.stream().sorted(Comparator.comparing(Value::stringValue)).collect(Collectors.toList());
      Resource union = VALUE_FACTORY.createIRI(YAGO_RESOURCE_PREFIX, "owl:unionOf-" + stringName(model, list));
      model.add(subject, predicate, union);
      model.add(union, RDF.TYPE, type);
      addListObject(model, union, OWL.UNIONOF, list);
    }
  }

  private static void addListObject(Model model, Resource subject, IRI predicate, Collection<? extends Value> objects) {
    List<Value> list = new ArrayList<>(objects);
    Resource current = RDF.NIL;
    for (int i = list.size() - 1; i >= 0; i--) {
      Resource newCurrent = VALUE_FACTORY.createIRI(YAGO_RESOURCE_PREFIX, "list-" + stringName(model, list.subList(i, list.size())));
      model.add(newCurrent, RDF.REST, current);
      model.add(newCurrent, RDF.FIRST, list.get(i));
      current = newCurrent;
    }
    model.add(subject, predicate, current);
  }

  private static String stringName(Model model, Value value) {
    if (value instanceof IRI) {
      var iri = (IRI) value;
      return model.getNamespaces().stream()
              .filter(t -> t.getName().equals(iri.getNamespace()))
              .findAny()
              .map(ns -> ns.getPrefix() + ":" + iri.getLocalName())
              .orElseGet(iri::stringValue);
    } else if (value instanceof BNode) {
      return ((BNode) value).getID();
    } else if (value instanceof Literal) {
      return value.stringValue(); //TODO: improve
    } else {
      throw new IllegalArgumentException("Invalid resource: " + value);
    }
  }

  private static String stringName(Model model, Collection<? extends Value> values) {
    return values.stream().map(v -> stringName(model, v)).collect(Collectors.joining("-"));
  }
}
