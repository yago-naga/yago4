package org.yago.yago4;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.yago.yago4.converter.JavaStreamEvaluator;
import org.yago.yago4.converter.plan.PlanNode;
import org.yago.yago4.converter.utils.NTriplesReader;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Main {
  private static final ValueFactory VALUE_FACTORY = SimpleValueFactory.getInstance();

  private static final String WD_PREFIX = "http://www.wikidata.org/entity/";
  private static final String WDT_PREFIX = "http://www.wikidata.org/prop/direct/";
  private static final String SCHEMA_PREFIX = "http://schema.org/";

  private static final IRI WIKIBASE_ITEM = VALUE_FACTORY.createIRI("http://wikiba.se/ontology#Item");
  private static final IRI WDT_P31 = VALUE_FACTORY.createIRI(WDT_PREFIX, "P31");
  private static final IRI WDT_P279 = VALUE_FACTORY.createIRI(WDT_PREFIX, "P279");
  private static final IRI SCHEMA_THING = VALUE_FACTORY.createIRI(SCHEMA_PREFIX, "Thing");
  private static final IRI SCHEMA_GEO_COORDINATES = VALUE_FACTORY.createIRI(SCHEMA_PREFIX, "GeoCoordinates");
  private static final IRI SCHEMA_LATITUDE = VALUE_FACTORY.createIRI(SCHEMA_PREFIX, "latitude");
  private static final IRI SCHEMA_LONGITUDE = VALUE_FACTORY.createIRI(SCHEMA_PREFIX, "longitude");

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

  public static void main(String[] args) throws ParseException {
    Options options = new Options();
    options.addRequiredOption("dir", "workingDirectory", true, "Working directory where the partition should be stored");

    // Partitioning
    options.addOption("partition", "partition", false, "Partition the dumps");
    options.addOption("wdDump", "wdDump", true, "Wikidata NTriples Truthy dump in bz2");

    // Processing
    options.addOption("yago", "buildYago", false, "Build Yago");
    options.addOption("yagoNt", "yagoNt", true, "Path to the full Yago N-Triples file to build");

    CommandLine params = (new DefaultParser()).parse(options, args);

    Path workingDir = Path.of(params.getOptionValue("dir"));
    PartitionedStatements partitionedStatements = new PartitionedStatements(workingDir);

    if (params.hasOption("partition")) {
      Path wdDump = Path.of(params.getOptionValue("wdDump"));

      System.out.println("Partitioning Wikidata dump " + wdDump + " to " + workingDir);
      doPartition(partitionedStatements, wdDump);
    }

    if (params.hasOption("yago")) {
      Path yagoNt = Path.of(params.getOptionValue("yagoNt"));

      System.out.println("Generating Yago N-Triples dump to " + yagoNt);
      buildYago(partitionedStatements, yagoNt);
    }
  }

  private static void doPartition(PartitionedStatements partitionedStatements, Path wdDump) {
    NTriplesReader reader = new NTriplesReader(VALUE_FACTORY);
    try (PartitionedStatements.Writer writer = partitionedStatements.getWriter(t -> keyForIri(t.getPredicate()))) {
      reader.read(wdDump).parallel().forEach(writer::write);
    }
  }

  private static String keyForIri(IRI iri) {
    return IRIShortener.shortened(iri).replace('/', '-').replace(':', '/');
  }

  private static void buildYago(PartitionedStatements partitionedStatements, Path outputFile) {
    var classInstances = classesFromSchema(partitionedStatements);
    var yagoClasses = classInstances.entrySet().stream().map(e -> {
      var yagoClass = e.getKey();
      return e.getValue().map(i -> VALUE_FACTORY.createStatement(i, RDF.TYPE, yagoClass));
    }).reduce(PlanNode::union).get();

    var yagoFacts = yagoClasses.union(propertiesFromSchema(partitionedStatements, classInstances));

    (new JavaStreamEvaluator(VALUE_FACTORY)).evaluateToNTriples(yagoFacts, outputFile);
  }

  private static Map<Resource, PlanNode<Resource>> classesFromSchema(PartitionedStatements partitionedStatements) {
    var wikidataInstanceOf = partitionedStatements.getForKey(keyForIri(WDT_P31));
    var wikidataSubClassOf = partitionedStatements.getForKey(keyForIri(WDT_P279));

    var wikidataItems = partitionedStatements.getForKey(keyForIri(RDF.TYPE))
            .filter(t -> WIKIBASE_ITEM.equals(t.getObject()))
            .map(Statement::getSubject);

    var badWikidataClasses = PlanNode.fromCollection(WD_BAD_TYPES).map(t -> (Resource) VALUE_FACTORY.createIRI(WD_PREFIX + t))
            .transitiveClosure(wikidataSubClassOf, Function.identity(), t -> (Resource) t.getObject(), (e, t) -> t.getSubject());

    var badWikidataItems = wikidataInstanceOf.join(badWikidataClasses, t -> (Resource) t.getObject(), Function.identity(), (t1, t2) -> t1.getSubject());

    var schemaThings = wikidataItems.antiJoin(badWikidataItems, Function.identity()).cache();

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
      var sourceClasses = PlanNode.fromCollection(entry.getValue())
              .transitiveClosure(wikidataSubClassOf, Function.identity(), s -> (Resource) s.getObject(), (e, t) -> t.getSubject());
      var mappedInstance = wikidataInstanceOf
              .join(sourceClasses, s -> (Resource) s.getObject(), Function.identity(), (t1, t2) -> t1.getSubject())
              .join(schemaThings, Function.identity(), Function.identity(), (t1, t2) -> t1)
              .cache();
      instancesSet.put(yagoClass, mappedInstance);
    }

    return instancesSet;
  }

  private static PlanNode<Statement> propertiesFromSchema(PartitionedStatements partitionedStatements, Map<Resource, PlanNode<Resource>> classInstances) {
    return ShaclSchema.getSchema().getPropertyShapes().map(propertyShape -> {
      IRI yagoProperty = propertyShape.getProperty();

      var triples = propertyShape.getFromProperties()
              .map(wikidataProperty -> partitionedStatements.getForKey(keyForIri(wikidataProperty)))
              .reduce(PlanNode::union).orElseGet(PlanNode::empty)
              .map(triple -> VALUE_FACTORY.createStatement(triple.getSubject(), yagoProperty, triple.getObject()));

      // Datatype filter
      if (propertyShape.getDatatypes().isPresent()) {
        Set<IRI> dts = propertyShape.getDatatypes().get();

        //We map IRIs to xsd:anyUri
        if (dts.contains(XMLSchema.ANYURI)) {
          triples = triples.flatMap(t -> {
            Value object = t.getObject();
            if (object instanceof IRI || (object instanceof Literal && XMLSchema.ANYURI.equals(((Literal) object).getDatatype()))) {
              return normalizeUri(object.stringValue())
                      .map(o -> VALUE_FACTORY.createStatement(t.getSubject(), t.getPredicate(), VALUE_FACTORY.createLiteral(o, XMLSchema.ANYURI)));
            } else {
              return Stream.of(t);
            }
          });
        }
        //TODO: time and quantity values
        triples = triples.filter(t -> t.getObject() instanceof Literal && dts.contains(((Literal) t.getObject()).getDatatype()));
      }

      // Range type filter
      if (propertyShape.getNodeShape().isPresent()) {
        ShaclSchema.NodeShape nodeShape = propertyShape.getNodeShape().get();
        Set<Resource> expectedClasses = nodeShape.getClasses().collect(Collectors.toSet());
        if (Collections.singleton(SCHEMA_GEO_COORDINATES).equals(expectedClasses)) {
          triples = triples.flatMap(t -> {
            //TODO: precision
            Matcher matcher = WKT_COORDINATES_PATTERN.matcher(t.getObject().stringValue());
            if (!matcher.matches()) {
              return Stream.of(t);
            }
            double longitude = Float.parseFloat(matcher.group(1));
            double latitude = Float.parseFloat(matcher.group(2));
            IRI geo = VALUE_FACTORY.createIRI("geo:" + latitude + "," + longitude);
            return Stream.of(
                    VALUE_FACTORY.createStatement(t.getSubject(), t.getPredicate(), geo),
                    VALUE_FACTORY.createStatement(geo, RDF.TYPE, SCHEMA_GEO_COORDINATES),
                    VALUE_FACTORY.createStatement(geo, SCHEMA_LATITUDE, VALUE_FACTORY.createLiteral(latitude)),
                    VALUE_FACTORY.createStatement(geo, SCHEMA_LONGITUDE, VALUE_FACTORY.createLiteral(longitude))
            );
          });
        } else {
          var rangeExtension = getInstancesOfShape(nodeShape, classInstances);
          triples = triples
                  .filter(t -> t.getObject() instanceof Resource)
                  .join(rangeExtension, t -> (Resource) t.getObject(), Function.identity(), (t1, t2) -> t1);
        }
      }

      //Regex
      if (propertyShape.getPattern().isPresent()) {
        Pattern pattern = propertyShape.getPattern().get();
        triples = triples.filter(t -> pattern.matcher(t.getObject().stringValue()).matches());
      }

      // Domain type filter
      var domainExtension = propertyShape.getParentShapes().stream()
              .flatMap(Collection::stream)
              .map(shape -> getInstancesOfShape(shape, classInstances))
              .reduce(PlanNode::union).get();
      triples = triples
              .join(domainExtension, Statement::getSubject, Function.identity(), (t1, t2) -> t1);

      return triples;
    }).reduce(PlanNode::union).get();
  }

  private static PlanNode<Resource> getInstancesOfShape(ShaclSchema.NodeShape nodeShape, Map<Resource, PlanNode<Resource>> classInstances) {
    return nodeShape.getClasses()
            .map(cls -> {
              if (!classInstances.containsKey(cls)) {
                System.err.println("No instances found for class " + cls);
              }
              return classInstances.getOrDefault(cls, PlanNode.empty());
            })
            .reduce(PlanNode::union).get();
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
