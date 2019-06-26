package org.yago.yago4;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.yago.yago4.converter.JavaStreamEvaluator;
import org.yago.yago4.converter.plan.PlanNode;
import org.yago.yago4.converter.utils.NTriplesReader;
import org.yago.yago4.converter.utils.Pair;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class Main {
  private static final ValueFactory valueFactory = SimpleValueFactory.getInstance();

  private static final String WD_PREFIX = "http://www.wikidata.org/entity/";
  private static final String WDT_PREFIX = "http://www.wikidata.org/prop/direct/";
  private static final String SCHEMA_PREFIX = "http://schema.org/";

  private static final IRI RDF_TYPE = valueFactory.createIRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
  private static final IRI WIKIBASE_ITEM = valueFactory.createIRI("http://wikiba.se/ontology#Item");
  private static final IRI WDT_P31 = valueFactory.createIRI(WDT_PREFIX + "P31");
  private static final IRI WDT_P279 = valueFactory.createIRI(WDT_PREFIX + "P279");

  private static final List<String> WD_BAD_TYPES = Arrays.asList(
          "Q17379835", //Wikimedia page outside the main knowledge tree
          "Q17442446", //Wikimedia internal stuff
          "Q4167410", //disambiguation page
          "Q13406463", //list article
          "Q17524420", //aspect of history
          "Q18340514" //article about events in a specific year or time period
  );

  private static final HashMap<String, List<String>> WD_TO_SCHEMA_TYPES = new HashMap<>();

  static {
    WD_TO_SCHEMA_TYPES.put("Q5", Collections.singletonList("Person")); //human
    WD_TO_SCHEMA_TYPES.put("Q165", Arrays.asList("Place", "Landform", "BodyOfWater", "SeaBodyOfWater"));
    WD_TO_SCHEMA_TYPES.put("Q515", Arrays.asList("Place", "AdministrativeArea", "City"));
    WD_TO_SCHEMA_TYPES.put("Q532", Collections.singletonList("Place")); //village
    WD_TO_SCHEMA_TYPES.put("Q571", Arrays.asList("CreativeWork", "Book"));
    WD_TO_SCHEMA_TYPES.put("Q1004", Arrays.asList("CreativeWork", "ComicStory")); //in bib: extension
    WD_TO_SCHEMA_TYPES.put("Q1420", Arrays.asList("Product", "Vehicle", "Car"));
    WD_TO_SCHEMA_TYPES.put("Q3914", Arrays.asList("Organization", "EducationalOrganization", "School"));
    WD_TO_SCHEMA_TYPES.put("Q3918", Arrays.asList("Organization", "EducationalOrganization", "CollegeOrUniversity"));
    WD_TO_SCHEMA_TYPES.put("Q3947", Arrays.asList("Place", "Accommodation", "House"));
    WD_TO_SCHEMA_TYPES.put("Q4006", Arrays.asList("CreativeWork", "Map"));
    WD_TO_SCHEMA_TYPES.put("Q4022", Arrays.asList("Place", "Landform", "BodyOfWater", "RiverBodyOfWater")); //river
    WD_TO_SCHEMA_TYPES.put("Q5107", Arrays.asList("Place", "Landform", "Continent"));
    WD_TO_SCHEMA_TYPES.put("Q5638", Arrays.asList("Product", "Vehicle", "BusOrCoach")); //Bus, in auto:
    WD_TO_SCHEMA_TYPES.put("Q6256", Arrays.asList("Place", "AdministrativeArea", "Country"));
    WD_TO_SCHEMA_TYPES.put("Q7889", Arrays.asList("CreativeWork", "Game", "SoftwareApplication", "VideoGame"));
    WD_TO_SCHEMA_TYPES.put("Q8502", Arrays.asList("Place", "Landform", "Mountain")); //mountain
    WD_TO_SCHEMA_TYPES.put("Q9826", Arrays.asList("Organization", "EducationalOrganization", "HighSchool")); //TODO: Us only
    WD_TO_SCHEMA_TYPES.put("Q9842", Arrays.asList("Organization", "EducationalOrganization", "ElementarySchool"));
    WD_TO_SCHEMA_TYPES.put("Q11032", Arrays.asList("CreativeWork", "Periodical", "Newspaper"));
    WD_TO_SCHEMA_TYPES.put("Q11410", Arrays.asList("CreativeWork", "Game"));
    WD_TO_SCHEMA_TYPES.put("Q11424", Arrays.asList("CreativeWork", "Movie")); //film
    WD_TO_SCHEMA_TYPES.put("Q11707", Arrays.asList("Place", "Organization", "LocalBusiness", "FoodEstablishment", "Restaurant"));
    WD_TO_SCHEMA_TYPES.put("Q12280", Arrays.asList("Place", "CivicStructure", "Bridge"));
    WD_TO_SCHEMA_TYPES.put("Q16917", Arrays.asList("Place", "CivicStructure", "Hospital"));
    WD_TO_SCHEMA_TYPES.put("Q16970", Arrays.asList("Place", "CivicStructure", "PlaceOfWorship", "Church"));
    WD_TO_SCHEMA_TYPES.put("Q22698", Arrays.asList("Place", "CivicStructure", "Park"));
    WD_TO_SCHEMA_TYPES.put("Q23397", Arrays.asList("Place", "LakeBodyOfWater")); //lake
    WD_TO_SCHEMA_TYPES.put("Q24354", Arrays.asList("Place", "CivicStructure", "PerformingArtsTheater"));
    WD_TO_SCHEMA_TYPES.put("Q30022", Arrays.asList("Place", "Organization", "LocalBusiness", "FoodEstablishment", "CafeOrCoffeeShop"));
    WD_TO_SCHEMA_TYPES.put("Q30849", Arrays.asList("CreativeWork", "Blog"));
    WD_TO_SCHEMA_TYPES.put("Q33506", Arrays.asList("Place", "CivicStructure", "Museum"));
    WD_TO_SCHEMA_TYPES.put("Q34770", Collections.singletonList("Language"));
    WD_TO_SCHEMA_TYPES.put("Q35127", Arrays.asList("CreativeWork", "WebSite"));
    WD_TO_SCHEMA_TYPES.put("Q39614", Arrays.asList("Place", "CivicStructure", "Cemetery"));
    WD_TO_SCHEMA_TYPES.put("Q40080", Arrays.asList("Place", "CivicStructure", "Beach"));
    WD_TO_SCHEMA_TYPES.put("Q41253", Arrays.asList("Place", "CivicStructure", "Organization", "LocalBusiness", "EntertainmentBusiness", "MovieTheater"));
    WD_TO_SCHEMA_TYPES.put("Q41298", Arrays.asList("CreativeWork", "CreativeWorkSeries", "Periodical"));
    WD_TO_SCHEMA_TYPES.put("Q43229", Collections.singletonList("Organization"));
    WD_TO_SCHEMA_TYPES.put("Q43501", Arrays.asList("Place", "CivicStructure", "Zoo"));
    WD_TO_SCHEMA_TYPES.put("Q46970", Arrays.asList("Organization", "Airline"));
    WD_TO_SCHEMA_TYPES.put("Q55488", Arrays.asList("Place", "CivicStructure", "TrainStation"));
    WD_TO_SCHEMA_TYPES.put("Q56061", Arrays.asList("Place", "AdministrativeArea"));
    WD_TO_SCHEMA_TYPES.put("Q79007", Collections.singletonList("Place")); //street
    WD_TO_SCHEMA_TYPES.put("Q79913", Arrays.asList("Organization", "NGO"));
    WD_TO_SCHEMA_TYPES.put("Q95074", Collections.singletonList("Person")); //fictional character
    WD_TO_SCHEMA_TYPES.put("Q107390", Arrays.asList("Place", "AdministrativeArea", "State"));
    WD_TO_SCHEMA_TYPES.put("Q125191", Arrays.asList("CreativeWork", "VisualArtwork", "Photograph"));
    WD_TO_SCHEMA_TYPES.put("Q132241", Arrays.asList("Event", "Festival"));
    WD_TO_SCHEMA_TYPES.put("Q149566", Arrays.asList("Organization", "EducationalOrganization", "MiddleSchool")); //TODO: US only
    WD_TO_SCHEMA_TYPES.put("Q157570", Arrays.asList("Place", "CivicStructure", "Crematorium"));
    WD_TO_SCHEMA_TYPES.put("Q166142", Arrays.asList("CreativeWork", "SoftwareApplication"));
    WD_TO_SCHEMA_TYPES.put("Q191067", Arrays.asList("CreativeWork", "Article"));
    WD_TO_SCHEMA_TYPES.put("Q207628", Arrays.asList("CreativeWork", "MusicComposition"));
    WD_TO_SCHEMA_TYPES.put("Q215380", Arrays.asList("Organization", "PerformingGroup", "MusicGroup"));
    WD_TO_SCHEMA_TYPES.put("Q215627", Collections.singletonList("Person")); //person
    WD_TO_SCHEMA_TYPES.put("Q219239", Arrays.asList("CreativeWork", "Recipe"));
    WD_TO_SCHEMA_TYPES.put("Q277759", Arrays.asList("CreativeWork", "CreativeWorkSeries", "BookSeries"));
    WD_TO_SCHEMA_TYPES.put("Q431289", Collections.singletonList("Brand"));
    WD_TO_SCHEMA_TYPES.put("Q482994", Arrays.asList("CreativeWork", "MusicPlaylist", "MusicAlbum")); //album
    WD_TO_SCHEMA_TYPES.put("Q483110", Arrays.asList("Place", "CivicStructure", "StadiumOrArena"));
    WD_TO_SCHEMA_TYPES.put("Q486972", Collections.singletonList("Place")); //human settlement
    WD_TO_SCHEMA_TYPES.put("Q494829", Arrays.asList("Place", "CivicStructure", "BusStation"));
    WD_TO_SCHEMA_TYPES.put("Q543654", Arrays.asList("Place", "CivicStructure", "GovernmentBuilding", "CityHall"));
    WD_TO_SCHEMA_TYPES.put("Q629206", Collections.singletonList("ComputerLanguage"));
    WD_TO_SCHEMA_TYPES.put("Q860861", Arrays.asList("CreativeWork", "VisualArtwork", "Sculpture"));
    WD_TO_SCHEMA_TYPES.put("Q861951", Arrays.asList("Place", "CivicStructure", "PoliceStation"));
    WD_TO_SCHEMA_TYPES.put("Q928830", Arrays.asList("Place", "CivicStructure", "SubwayStation"));
    WD_TO_SCHEMA_TYPES.put("Q953806", Arrays.asList("Place", "CivicStructure", "BusStop"));
    WD_TO_SCHEMA_TYPES.put("Q2659904", Arrays.asList("Organization", "GovernmentOrganization"));
    WD_TO_SCHEMA_TYPES.put("Q1137809", Arrays.asList("Place", "CivicStructure", "GovernmentBuilding", "Courthouse"));
    WD_TO_SCHEMA_TYPES.put("Q1195942", Arrays.asList("Place", "CivicStructure", "FireStation"));
    WD_TO_SCHEMA_TYPES.put("Q1248784", Arrays.asList("Place", "CivicStructure", "Airport"));
    WD_TO_SCHEMA_TYPES.put("Q1370598", Arrays.asList("Place", "CivicStructure", "PlaceOfWorship"));
    WD_TO_SCHEMA_TYPES.put("Q1656682", Collections.singletonList("Event"));
    WD_TO_SCHEMA_TYPES.put("Q1980247", Arrays.asList("CreativeWork", "Chapter"));
    WD_TO_SCHEMA_TYPES.put("Q1983062", Arrays.asList("CreativeWork", "Episode"));
    WD_TO_SCHEMA_TYPES.put("Q2281788", Arrays.asList("Place", "CivicStructure", "Aquarium"));
    WD_TO_SCHEMA_TYPES.put("Q2393314", Arrays.asList("Organization", "PerformingGroup", "DanceGroup"));
    WD_TO_SCHEMA_TYPES.put("Q2416217", Arrays.asList("Organization", "PerformingGroup", "TheaterGroup"));
    WD_TO_SCHEMA_TYPES.put("Q3305213", Arrays.asList("CreativeWork", "VisualArtwork", "Painting"));
    WD_TO_SCHEMA_TYPES.put("Q3331189", Collections.singletonList("CreativeWork")); //Edition
    WD_TO_SCHEMA_TYPES.put("Q3464665", Arrays.asList("CreativeWork", "CreativeWorkSeason", "TVSeason")); //TV season
    WD_TO_SCHEMA_TYPES.put("Q3917681", Arrays.asList("Place", "CivicStructure", "GovernmentBuilding", "Embassy"));
    WD_TO_SCHEMA_TYPES.put("Q4438121", Arrays.asList("Organization", "SportsOrganization"));
    WD_TO_SCHEMA_TYPES.put("Q4502142", Arrays.asList("CreativeWork", "VisualArtwork"));
    WD_TO_SCHEMA_TYPES.put("Q4830453", Arrays.asList("Organization", "Corporation"));
    WD_TO_SCHEMA_TYPES.put("Q5398426", Arrays.asList("CreativeWork", "CreativeWorkSeries", "TVSeries"));
    WD_TO_SCHEMA_TYPES.put("Q5707594", Arrays.asList("CreativeWork", "Article", "NewsArticle"));
    WD_TO_SCHEMA_TYPES.put("Q7058673", Arrays.asList("CreativeWork", "CreativeWorkSeries", "VideoGameSeries"));
    WD_TO_SCHEMA_TYPES.put("Q7138926", Arrays.asList("Place", "CivicStructure", "GovernmentBuilding", "LegislativeBuilding"));
    WD_TO_SCHEMA_TYPES.put("Q8719053", Arrays.asList("Place", "CivicStructure", "MusicVenue"));
    WD_TO_SCHEMA_TYPES.put("Q12973014", Arrays.asList("Organization", "SportsOrganization", "SportsTeam"));
    WD_TO_SCHEMA_TYPES.put("Q13100073", Collections.singletonList("Place")); //Chinese village TODO
    WD_TO_SCHEMA_TYPES.put("Q13442814", Arrays.asList("CreativeWork", "Article", "ScholarlyArticle")); //scientific article
    WD_TO_SCHEMA_TYPES.put("Q14406742", Arrays.asList("CreativeWork", "CreativeWorkSeries", " Periodical", "ComicSeries"));
    WD_TO_SCHEMA_TYPES.put("Q14623351", Arrays.asList("CreativeWork", "CreativeWorkSeries", "RadioSeries"));
    WD_TO_SCHEMA_TYPES.put("Q16831714", Arrays.asList("Place", "CivicStructure", "GovernmentBuilding"));
    WD_TO_SCHEMA_TYPES.put("Q17537576", Collections.singletonList("CreativeWork")); //creative work
    WD_TO_SCHEMA_TYPES.put("Q18674739", Arrays.asList("Place", "CivicStructure", "EventVenue"));
    //WD_TO_SCHEMA_TYPES.put("Q1137809", Arrays.asList("Place", "CivicStructure", "GovernmentBuilding", "DefenceEstablishment"));
    WD_TO_SCHEMA_TYPES.put("Q19816504", Arrays.asList("CreativeWork", "PublicationVolume"));
    WD_TO_SCHEMA_TYPES.put("Q20950067", Arrays.asList("Organization", "EducationalOrganization", "ElementarySchool")); //TODO: US only
    WD_TO_SCHEMA_TYPES.put("Q27108230", Arrays.asList("Place", "CivicStructure", "Organization", "LocalBusiness", "LodgingBusiness", "Campground"));
  }

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
    NTriplesReader reader = new NTriplesReader(valueFactory);
    try (PartitionedStatements.Writer writer = partitionedStatements.getWriter(t -> keyForIri(t.getPredicate()))) {
      reader.read(wdDump).parallel().forEach(writer::write);
    }
  }

  private static String keyForIri(IRI iri) {
    return IRIShortener.shortened(iri).replace('/', '-');
  }

  private static void buildYago(PartitionedStatements partitionedStatements, Path outputFile) {
    var wikidataInstanceOf = partitionedStatements.getForKey(keyForIri(WDT_P31));
    var wikidataSubClassOf = partitionedStatements.getForKey(keyForIri(WDT_P279));

    var wikidataItems = partitionedStatements.getForKey(keyForIri(RDF_TYPE))
            .filter(t -> WIKIBASE_ITEM.equals(t.getObject()))
            .map(Statement::getSubject);

    var badWikidataClasses = PlanNode.fromCollection(WD_BAD_TYPES).map(t -> (Resource) valueFactory.createIRI(WD_PREFIX + t))
            .transitiveClosure(wikidataSubClassOf, t -> t, Statement::getObject, (e, t) -> t.getSubject());

    var badWikidataItems = wikidataInstanceOf.join(badWikidataClasses, Statement::getObject, t -> t, (t1, t2) -> t1.getSubject());

    var schemaThings = wikidataItems.antiJoin(badWikidataItems, t -> t);

    var classMapping = PlanNode.fromCollection(WD_TO_SCHEMA_TYPES.entrySet()).flatMap(e -> e.getValue().stream().map(v -> new Pair<>(
            (Resource) valueFactory.createIRI(WD_PREFIX + e.getKey()),
            valueFactory.createIRI(SCHEMA_PREFIX + v)
    ))).transitiveClosure(wikidataSubClassOf, Pair::getKey, Statement::getObject, (e, t) -> new Pair<>(t.getSubject(), e.getValue()));

    var typeMapping = wikidataInstanceOf
            .join(schemaThings, Statement::getSubject, t -> t, (t1, t2) -> t1)
            .join(classMapping, Statement::getObject, Pair::getKey, (t1, t2) -> valueFactory.createStatement(t1.getSubject(), RDF_TYPE, t2.getValue()));

    (new JavaStreamEvaluator(valueFactory)).evaluateToNTriples(typeMapping, outputFile);
  }
}
