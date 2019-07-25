package org.yago.yago4;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.model.vocabulary.*;
import org.yago.yago4.converter.JavaStreamEvaluator;
import org.yago.yago4.converter.plan.PairPlanNode;
import org.yago.yago4.converter.plan.PlanNode;
import org.yago.yago4.converter.utils.NTriplesReader;
import org.yago.yago4.converter.utils.YagoValueFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Main {
  private static final YagoValueFactory VALUE_FACTORY = YagoValueFactory.getInstance();

  private static final String WD_PREFIX = "http://www.wikidata.org/entity/";
  private static final String WDT_PREFIX = "http://www.wikidata.org/prop/direct/";
  private static final String P_PREFIX = "http://www.wikidata.org/prop/";
  private static final String PSV_PREFIX = "http://www.wikidata.org/prop/statement/value/";
  private static final String SCHEMA_PREFIX = "http://schema.org/";
  private static final String WIKIBASE_PREFIX = "http://wikiba.se/ontology#";
  private static final String YAGO_RESOURCE_PREFIX = "http://yago-knowledge.org/resource/";

  private static final IRI WIKIBASE_ITEM = VALUE_FACTORY.createIRI(WIKIBASE_PREFIX, "Item");
  private static final IRI WIKIBASE_BEST_RANK = VALUE_FACTORY.createIRI(WIKIBASE_PREFIX, "BestRank");
  private static final IRI WIKIBASE_TIME_VALUE = VALUE_FACTORY.createIRI(WIKIBASE_PREFIX, "timeValue");
  private static final IRI WIKIBASE_TIME_PRECISION = VALUE_FACTORY.createIRI(WIKIBASE_PREFIX, "timePrecision");
  private static final IRI WDT_P31 = VALUE_FACTORY.createIRI(WDT_PREFIX, "P31");
  private static final IRI WDT_P279 = VALUE_FACTORY.createIRI(WDT_PREFIX, "P279");
  private static final IRI WDT_P646 = VALUE_FACTORY.createIRI(WDT_PREFIX, "P646");
  private static final IRI SCHEMA_THING = VALUE_FACTORY.createIRI(SCHEMA_PREFIX, "Thing");
  private static final IRI SCHEMA_INTANGIBLE = VALUE_FACTORY.createIRI(SCHEMA_PREFIX, "Intangible");
  private static final IRI SCHEMA_STRUCTURED_VALUE = VALUE_FACTORY.createIRI(SCHEMA_PREFIX, "StructuredValue");
  private static final IRI SCHEMA_GEO_COORDINATES = VALUE_FACTORY.createIRI(SCHEMA_PREFIX, "GeoCoordinates");
  private static final IRI SCHEMA_ABOUT = VALUE_FACTORY.createIRI(SCHEMA_PREFIX, "about");
  private static final IRI SCHEMA_NAME = VALUE_FACTORY.createIRI(SCHEMA_PREFIX, "name");
  private static final IRI SCHEMA_ALTERNATE_NAME = VALUE_FACTORY.createIRI(SCHEMA_PREFIX, "alternateName");
  private static final IRI SCHEMA_DESCRIPTION = VALUE_FACTORY.createIRI(SCHEMA_PREFIX, "description");

  private static final Set<IRI> CALENDAR_DT_SET = Set.of(XMLSchema.GYEAR, XMLSchema.GYEARMONTH, XMLSchema.DATE, XMLSchema.DATETIME);

  private static final Pattern WKT_COORDINATES_PATTERN = Pattern.compile("^POINT\\(([0-9.]+) +([0-9.]+)\\)$", Pattern.CASE_INSENSITIVE);

  private static final List<String> WD_BAD_CLASSES = List.of(
          "Q17379835", //Wikimedia page outside the main knowledge tree
          "Q17442446", //Wikimedia internal stuff
          "Q4167410", //disambiguation page
          "Q13406463", //list article
          "Q17524420", //aspect of history
          "Q18340514" //article about events in a specific year or time period
  );
  private static final Set<IRI> LABEL_IRIS = Set.of(SCHEMA_NAME, SCHEMA_ALTERNATE_NAME, SCHEMA_DESCRIPTION);


  public static void main(String[] args) throws ParseException, IOException {
    Options options = new Options();
    options.addRequiredOption("dir", "workingDirectory", true, "Working directory where the partition should be stored");

    // Partitioning
    options.addOption("partition", "partition", false, "Partition the dumps");
    options.addOption("wdDump", "wdDump", true, "Wikidata NTriples Truthy dump in bz2");

    // Processing
    options.addOption("yago", "buildYago", false, "Build Yago");
    options.addOption("small", "smallOnly", false, "Only introduces resources that are mapped to English Wikipedia");
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
      buildYago(partitionedStatements, yagoDir, params.hasOption("smallOnly"));
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

  private static void buildYago(PartitionedStatements partitionedStatements, Path outputDir, boolean enWikipediaOnly) throws IOException {
    Files.createDirectories(outputDir);

    var evaluator = new JavaStreamEvaluator(VALUE_FACTORY);

    var wikidataToYagoUrisMapping = wikidataToYagoUrisMapping(partitionedStatements);

    var t = buildYagoClassesAndSuperClassOf(partitionedStatements, wikidataToYagoUrisMapping);
    var yagoClasses = t.getKey();
    var yagoSuperClassOf = t.getValue();

    var yagoShapeInstances = yagoShapeInstances(partitionedStatements, yagoSuperClassOf, yagoClasses, wikidataToYagoUrisMapping);

    evaluator.evaluateToNTriples(
            buildClassesDescription(yagoClasses, yagoSuperClassOf, partitionedStatements, wikidataToYagoUrisMapping),
            outputDir.resolve("yago-wd-class.nt")
    );

    evaluator.evaluateToNTriples(
            buildInstanceOf(yagoShapeInstances.get(SCHEMA_THING), yagoClasses, partitionedStatements, wikidataToYagoUrisMapping),
            outputDir.resolve("yago-wd-types.nt")
    );

    evaluator.evaluateToNTriples(
            buildPropertiesFromSchema(partitionedStatements, yagoShapeInstances, wikidataToYagoUrisMapping, LABEL_IRIS, null),
            outputDir.resolve("yago-wd-labels.nt")
    );
    evaluator.evaluateToNTriples(
            buildPropertiesFromSchema(partitionedStatements, yagoShapeInstances, wikidataToYagoUrisMapping, null, LABEL_IRIS),
            outputDir.resolve("yago-wd-facts.nt")
    );

    evaluator.evaluateToNTriples(
            buildSameAs(partitionedStatements, yagoShapeInstances.get(SCHEMA_THING), wikidataToYagoUrisMapping),
            outputDir.resolve("yago-wd-sameAs.nt")
    );

    evaluator.evaluateToNTriples(buildYagoSchema(), outputDir.resolve("yago-wd-schema.nt"));
  }

  /**
   * Converts Wikidata URI to Yago URIs based on en.wikipedia article titles
   */
  private static PairPlanNode<Resource, Resource> wikidataToYagoUrisMapping(PartitionedStatements partitionedStatements) {
    var mapping = partitionedStatements.getForKey(keyForIri(SCHEMA_ABOUT))
            .filter(t -> t.getSubject().stringValue().startsWith("https://en.wikipedia.org/wiki/"))
            .mapToPair(s -> Map.entry((Resource) s.getObject(), s.getSubject()))
            .mapPair((wikidata, wikipedia) -> Map.entry(wikidata, (Resource) VALUE_FACTORY.createIRI(wikipedia.stringValue().replace("https://en.wikipedia.org/wiki/", YAGO_RESOURCE_PREFIX))));

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
    wikidataItems = wikidataItems.cache(); //TODO
    */

    var mappingForNotLinkedToEnWikipedia = wikidataItems
            .subtract(mapping.keys())
            .mapToPair(e -> Map.entry(e, (Resource) VALUE_FACTORY.createIRI(YAGO_RESOURCE_PREFIX, "wikidata_" + ((IRI) e).getLocalName())));

    return mapping.union(mappingForNotLinkedToEnWikipedia).cache();
  }

  /**
   * Builds the class set and class hierarchy from Wikidata, schema.org ontology and shapes
   * <p>
   * Algorithm:
   * 1. take all subClassOf (P279) from Wikidata and maps URIs to Yago
   * 2. take all subClassOf from schema.org ontology and shapes mapping
   * 3. remove from them the elements and subclasses of WD_BAD_CLASSES
   * 4. construct class set by only keeping the classes that have instance or subclass of and are transitively subclasses of schema:Thing
   */
  private static Map.Entry<PlanNode<Resource>, PairPlanNode<Resource, Resource>> buildYagoClassesAndSuperClassOf(PartitionedStatements partitionedStatements, PairPlanNode<Resource, Resource> wikidataToYagoUrisMapping) {
    var wikidataSubClassOf = partitionedStatements.getForKey(keyForIri(WDT_P279))
            .mapToPair(t -> Map.entry(t.getSubject(), (Resource) t.getObject()));
    var possibleSuperClassOfFromWikidata = mapKeyToYago(mapKeyToYago(wikidataSubClassOf, wikidataToYagoUrisMapping).swap(), wikidataToYagoUrisMapping);

    ShaclSchema schema = ShaclSchema.getSchema();
    var wikidataToSchemaSubClassOf = PairPlanNode.fromStream(schema.getNodeShapes().flatMap(nodeShape ->
            nodeShape.getFromClasses().flatMap(sourceClass ->
                    nodeShape.getClasses()
                            .map(yagoClass -> Map.entry((Resource) sourceClass, yagoClass)))));
    var superClassOfFromSchema = mapKeyToYago(wikidataToSchemaSubClassOf, wikidataToYagoUrisMapping)
            .union(subClassOfFromYagoSchema())
            .swap();

    var possibleSuperClassOf = possibleSuperClassOfFromWikidata.union(superClassOfFromSchema).cache(); //TODO: is it smart?

    var schemaThingSubClasses = PlanNode.fromCollection(Collections.singleton((Resource) SCHEMA_THING))
            .transitiveClosure(possibleSuperClassOf);

    var badClasses = PlanNode.fromCollection(WD_BAD_CLASSES)
            .map(c -> (Resource) VALUE_FACTORY.createIRI(WD_PREFIX, c))
            .transitiveClosure(possibleSuperClassOf);

    var instanceOfObjects = mapToYago(
            partitionedStatements.getForKey(keyForIri(WDT_P31)).map(t -> (Resource) t.getObject()),
            wikidataToYagoUrisMapping
    );

    //TODO: change this to only keep classes that have instances recursively
    var yagoClasses = instanceOfObjects
            .union(possibleSuperClassOf.keys())
            .intersection(schemaThingSubClasses)
            .subtract(badClasses)
            .cache();

    var yagoSuperClassOf = possibleSuperClassOf
            .intersection(yagoClasses)
            .swap()
            .intersection(yagoClasses)
            .swap()
            .cache();

    return Map.entry(yagoClasses, yagoSuperClassOf);
  }

  private static Map<Resource, PlanNode<Resource>> yagoShapeInstances(PartitionedStatements partitionedStatements, PairPlanNode<Resource, Resource> yagoSuperClassOf, PlanNode<Resource> yagoClasses, PairPlanNode<Resource, Resource> wikidataToYagoUrisMapping) {
    var wikidataInstancesForYagoClass = mapKeyToYago(
            partitionedStatements.getForKey(keyForIri(WDT_P31))
                    .mapToPair(t -> Map.entry((Resource) t.getObject(), t.getSubject())),
            wikidataToYagoUrisMapping
    ); //TODO: cache?

    return ShaclSchema.getSchema().getNodeShapes().flatMap(nodeShape -> {
      var fromYagoClasses = PlanNode.fromStream(Stream.concat(
              nodeShape.getClasses(),
              nodeShape.getFromClasses().map(t -> (Resource) t)
      )).transitiveClosure(yagoSuperClassOf);

      var wdInstances = wikidataInstancesForYagoClass
              .intersection(fromYagoClasses)
              .values();
      var instances = mapToYago(wdInstances, wikidataToYagoUrisMapping)
              .subtract(yagoClasses) // We do not want classes
              .cache();
      return nodeShape.getClasses().map(c -> Map.entry(c, instances));
    }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private static PlanNode<Statement> buildInstanceOf(PlanNode<Resource> yagoThings, PlanNode<Resource> yagoClasses, PartitionedStatements partitionedStatements, PairPlanNode<Resource, Resource> wikidataToYagoUrisMapping) {
    var wikidataInstanceOf = partitionedStatements.getForKey(keyForIri(WDT_P31))
            .mapToPair(t -> Map.entry(t.getSubject(), (Resource) t.getObject()));

    var instanceOfSubjectFiltered = mapKeyToYago(wikidataInstanceOf, wikidataToYagoUrisMapping)
            .intersection(yagoThings);

    return mapKeyToYago(instanceOfSubjectFiltered.swap(), wikidataToYagoUrisMapping)
            .intersection(yagoClasses)
            .map((o, s) -> VALUE_FACTORY.createStatement(s, RDF.TYPE, o));
  }

  private static PlanNode<Statement> buildClassesDescription(PlanNode<Resource> yagoClasses, PairPlanNode<Resource, Resource> yagoSuperClassOf, PartitionedStatements partitionedStatements, PairPlanNode<Resource, Resource> wikidataToYagoUrisMapping) {
    var yagoRdfsClassTriple = yagoClasses.map(c -> VALUE_FACTORY.createStatement(c, RDF.TYPE, RDFS.CLASS));
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

    return yagoSubClassOf.union(yagoRdfsClassTriple).union(yagoOwlClassTriple).union(rdfsLabel).union(rdfsComment);
  }

  private static PlanNode<Statement> buildPropertiesFromSchema(
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

    return ShaclSchema.getSchema().getPropertyShapes().map(propertyShape -> {
      IRI yagoProperty = propertyShape.getProperty();
      if (onlyProperties != null && !onlyProperties.contains(yagoProperty)) {
        return PlanNode.<Statement>empty();
      }
      if (excludeProperties != null && excludeProperties.contains(yagoProperty)) {
        return PlanNode.<Statement>empty();
      }

      PairPlanNode<Resource, Value> subjectObjects = PairPlanNode.empty();

      if (propertyShape.getDatatypes().isPresent()) {
        if (propertyShape.getNodeShape().isPresent()) {
          System.err.println("The property " + propertyShape.getProperty() + " could not have both a datatype domain and a node domain. Ignoring it.");
          return PlanNode.<Statement>empty();
        }
        // Datatype filter
        Set<IRI> dts = propertyShape.getDatatypes().get();

        if (dts.equals(Collections.singleton(XMLSchema.ANYURI))) {
          //We map IRIs to xsd:anyUri
          subjectObjects = getPropertyValues(partitionedStatements, propertyShape).flatMapValue(object -> {
            if (object instanceof IRI || (object instanceof Literal && XMLSchema.ANYURI.equals(((Literal) object).getDatatype()))) {
              return normalizeUri(object.stringValue())
                      .map(o -> VALUE_FACTORY.createLiteral(o, XMLSchema.ANYURI));
            } else {
              return Stream.of(object);
            }
          });
        } else if (CALENDAR_DT_SET.containsAll(dts)) {
          //We clean up times by retrieving their full representation
          subjectObjects = getBestMainSnakComplexValues(partitionedStatements, propertyShape, bestRanks)
                          .swap()
                          .join(partitionedStatements.getForKey(keyForIri(WIKIBASE_TIME_VALUE)).mapToPair(s -> Map.entry(s.getSubject(), s.getObject())))
                          .join(partitionedStatements.getForKey(keyForIri(WIKIBASE_TIME_PRECISION)).mapToPair(s -> Map.entry(s.getSubject(), s.getObject())))
                  .flatMapPair((k, e) -> cleanupTime(e.getKey().getValue(), e.getValue()).map(t -> Map.entry(e.getKey().getKey(), t)));
        } else {
          subjectObjects = getPropertyValues(partitionedStatements, propertyShape)
                  .filterValue(object -> object instanceof Literal && dts.contains(((Literal) object).getDatatype()));
        }
        //TODO: quantity values
      } else if (propertyShape.getNodeShape().isPresent()) {
        // Range type filter
        var innerSubjectObjects = getPropertyValues(partitionedStatements, propertyShape);

        ShaclSchema.NodeShape nodeShape = propertyShape.getNodeShape().get();
        Set<Resource> expectedClasses = nodeShape.getClasses().collect(Collectors.toSet());
        if (Collections.singleton(SCHEMA_GEO_COORDINATES).equals(expectedClasses)) {
          subjectObjects = innerSubjectObjects.flatMapValue(object -> {
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
          var objectSubjectsForRange = innerSubjectObjects.mapPair((k, v) -> Map.entry((Resource) v, k));
          objectSubjectsForRange = mapKeyToYago(objectSubjectsForRange, wikidataToYagoUrisMapping);
          subjectObjects = nodeShape.getClasses()
                  .distinct()
                  .flatMap(cls -> Stream.ofNullable(yagoShapeInstances.get(cls)))
                  .map(objectSubjectsForRange::intersection)
                  .reduce(PairPlanNode::union).orElseGet(PairPlanNode::empty)
                  .mapPair((k, v) -> Map.entry(v, k));
        }
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
      var subjectObjectsForDomain = subjectObjects;
      subjectObjectsForDomain = mapKeyToYago(subjectObjectsForDomain, wikidataToYagoUrisMapping);
      subjectObjects = propertyShape.getParentShapes().stream()
              .flatMap(Collection::stream)
              .flatMap(ShaclSchema.NodeShape::getClasses)
              .distinct()
              .flatMap(cls -> Stream.ofNullable(yagoShapeInstances.get(cls)))
              .map(subjectObjectsForDomain::intersection)
              .reduce(PairPlanNode::union)
              .orElseGet(PairPlanNode::empty);

      return subjectObjects.map((s, o) -> VALUE_FACTORY.createStatement(s, yagoProperty, o));
    }).reduce(PlanNode::union).orElseGet(PlanNode::empty);
  }

  private static PairPlanNode<Resource, Value> getPropertyValues(PartitionedStatements partitionedStatements, ShaclSchema.PropertyShape propertyShape) {
    return propertyShape.getFromProperties()
            .map(wikidataProperty -> partitionedStatements.getForKey(keyForIri(wikidataProperty)))
            .reduce(PlanNode::union).orElseGet(PlanNode::empty)
            .mapToPair(t -> Map.entry(t.getSubject(), t.getObject()));
  }

  private static PairPlanNode<Resource, Resource> getBestMainSnakComplexValues(PartitionedStatements partitionedStatements, ShaclSchema.PropertyShape propertyShape, PlanNode<Resource> bestRanks) {
    return propertyShape.getFromProperties().map(wikidataProperty ->
            partitionedStatements.getForKey(keyForIri(VALUE_FACTORY.createIRI(P_PREFIX, wikidataProperty.getLocalName())))
                    .mapToPair(t -> Map.entry((Resource) t.getObject(), t.getSubject()))
                    .intersection(bestRanks)
                    .join(partitionedStatements.getForKey(keyForIri(VALUE_FACTORY.createIRI(PSV_PREFIX, wikidataProperty.getLocalName())))
                            .mapToPair(t -> Map.entry(t.getSubject(), (Resource) t.getObject()))
                    ).values().mapToPair(Function.identity())
    ).reduce(PairPlanNode::union).orElseGet(PairPlanNode::empty);
  }

  private static PlanNode<Statement> buildSameAs(PartitionedStatements partitionedStatements, PlanNode<Resource> yagoThings, PairPlanNode<Resource, Resource> wikidataToYagoUrisMapping) {
    // Wikidata
    PlanNode<Statement> wikidata = wikidataToYagoUrisMapping
            .intersection(yagoThings)
            .map((wd, yago) -> VALUE_FACTORY.createStatement(yago, OWL.SAMEAS, wd));

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

    return wikidata.union(dbPedia).union(freebase);
  }

  private static PlanNode<Statement> buildYagoSchema() {
    Model yagoStatements = new LinkedHashModel();
    ShaclSchema schema = ShaclSchema.getSchema();

    // Classes
    schema.getNodeShapes()
            .flatMap(ShaclSchema.NodeShape::getClasses)
            .flatMap(c -> schema.getClass(c).stream())
            .forEach(c -> {
              yagoStatements.add(c.getTerm(), RDF.TYPE, RDFS.CLASS);
              yagoStatements.add(c.getTerm(), RDF.TYPE, OWL.CLASS);
              c.getLabels().forEach(l -> yagoStatements.add(c.getTerm(), RDFS.LABEL, VALUE_FACTORY.createLiteral(camlCaseToRegular(l.stringValue()), "en")));
              c.getComments().forEach(l -> yagoStatements.add(c.getTerm(), RDFS.COMMENT, VALUE_FACTORY.createLiteral(l.stringValue(), "en")));
              c.getSuperClasses().forEach(cp -> {
                if (cp.equals(SCHEMA_INTANGIBLE)) {
                  //We ignore schema:Intangible
                  yagoStatements.add(c.getTerm(), RDFS.SUBCLASSOF, SCHEMA_THING);
                } else if (cp.equals(SCHEMA_STRUCTURED_VALUE)) {
                  //schema:StructuredValue are not schema:Thing
                } else { //We ignore schema:Intangible
                  yagoStatements.add(c.getTerm(), RDFS.SUBCLASSOF, cp);
                }
              });
            });

    // Properties
    schema.getPropertyShapes()
            .forEach(shape -> schema.getProperty(shape.getProperty()).ifPresent(p -> {
              yagoStatements.add(p.getTerm(), RDF.TYPE, RDF.PROPERTY);
              if (shape.getNodeShape().isPresent() && shape.getDatatypes().isEmpty()) {
                yagoStatements.add(p.getTerm(), RDF.TYPE, OWL.OBJECTPROPERTY);
              } else if (shape.getNodeShape().isEmpty() && shape.getDatatypes().isPresent()) {
                yagoStatements.add(p.getTerm(), RDF.TYPE, OWL.DATATYPEPROPERTY);
              } else {
                System.err.println("Not sure if " + p.getTerm() + " is an object or a datatype property.");
              }
              p.getLabels().forEach(l -> yagoStatements.add(p.getTerm(), RDFS.LABEL, VALUE_FACTORY.createLiteral(camlCaseToRegular(l.stringValue()), "en")));
              p.getComments().forEach(l -> yagoStatements.add(p.getTerm(), RDFS.COMMENT, VALUE_FACTORY.createLiteral(l.stringValue(), "en")));
              p.getSuperProperties().forEach(cp -> {
                if (cp.equals(VALUE_FACTORY.createIRI("rdfs:label"))) {
                  // dirty fix for https://github.com/schemaorg/schemaorg/pull/2312
                } else {
                  yagoStatements.add(p.getTerm(), RDFS.SUBPROPERTYOF, cp);
                }
              });
              p.getInverseProperties().forEach(cp -> yagoStatements.add(p.getTerm(), OWL.INVERSEOF, cp));
            }));

    // Some hardcoded triples
    yagoStatements.add(SCHEMA_NAME, RDFS.SUBPROPERTYOF, RDFS.LABEL);
    yagoStatements.add(SCHEMA_DESCRIPTION, RDFS.SUBPROPERTYOF, RDFS.COMMENT);

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
    return StringUtils.join(StringUtils.splitByCharacterTypeCamelCase(txt), " ").toLowerCase();
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

  private static Stream<Value> cleanupTime(Value value, Value precision) {
    if (!(value instanceof Literal) || !(precision instanceof Literal)) {
      return Stream.empty();
    }
    try {
      String[] parts = value.stringValue().split("(?<!\\A)[\\-:TZ]");
      if (parts.length != 6) {
        return Stream.empty();
      }
      int p = ((Literal) precision).intValue();
      switch (p) {
        case 9:
          return Stream.of(VALUE_FACTORY.createLiteral(parts[0], XMLSchema.GYEAR));
        case 10:
          return Stream.of(VALUE_FACTORY.createLiteral(parts[0] + "-" + parts[1], XMLSchema.GYEARMONTH));
        case 11:
          return Stream.of(VALUE_FACTORY.createLiteral(parts[0] + "-" + parts[1] + "-" + parts[2], XMLSchema.DATE));
        case 14:
          return Stream.of(VALUE_FACTORY.createLiteral(value.stringValue(), XMLSchema.DATETIME));
        default:
          return Stream.empty();
      }
    } catch (IllegalArgumentException e) {
      return Stream.empty();
    }
  }
}
