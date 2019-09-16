package org.yago.yago4;

import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.model.util.Models;
import org.eclipse.rdf4j.model.util.RDFCollections;
import org.eclipse.rdf4j.model.vocabulary.OWL;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.model.vocabulary.SHACL;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.StatementCollector;
import org.yago.yago4.converter.utils.YagoValueFactory;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ShaclSchema {

  private static final ValueFactory VALUE_FACTORY = YagoValueFactory.getInstance();
  private static final IRI YS_FROM_CLASS = VALUE_FACTORY.createIRI("http://yago-knowledge.org/schema#fromClass");
  private static final IRI YS_FROM_PROPERTY = VALUE_FACTORY.createIRI("http://yago-knowledge.org/schema#fromProperty");
  private static final IRI YS_ANNOTATION_PROPERTY_SHAPE = VALUE_FACTORY.createIRI("http://yago-knowledge.org/schema#AnnotationPropertyShape");
  private static final IRI SCHEMA_INVERSE_OF = VALUE_FACTORY.createIRI("http://schema.org/inverseOf");
  private static final ShaclSchema SINGLETON = new ShaclSchema(readSchemaModel());
  private Model model;

  private ShaclSchema(Model model) {
    this.model = model;
  }

  public static ShaclSchema getSchema() {
    return SINGLETON;
  }

  private static Model readSchemaModel() {
    Model model = new LinkedHashModel();
    RDFParser parser = Rio.createParser(RDFFormat.TURTLE, VALUE_FACTORY);
    parser.setRDFHandler(new StatementCollector(model));
    try {
      parser.parse(new URL("https://schema.org/version/latest/all-layers.ttl").openStream(), "http://schema.org/");
      parser.parse(ShaclSchema.class.getResourceAsStream("/bioschemas.ttl"), "");
      parser.parse(ShaclSchema.class.getResourceAsStream("/shapes.ttl"), "");
      parser.parse(ShaclSchema.class.getResourceAsStream("/shapes-bio.ttl"), "");

    } catch (IOException e) {
      throw new IllegalArgumentException("The provided schema is not valid JSON", e);
    }
    return model;
  }

  public Stream<NodeShape> getNodeShapes() {
    return model.filter(null, RDF.TYPE, SHACL.NODE_SHAPE).subjects().stream()
            .map(SingleNodeShape::new);
  }

  public Stream<PropertyShape> getPropertyShapes() {
    return Stream.concat(
            model.filter(null, RDF.TYPE, SHACL.PROPERTY_SHAPE).subjects().stream(),
            model.filter(null, SHACL.PROPERTY, null).objects().stream().map(t -> (Resource) t)
    ).distinct().map(PropertyShape::new);
  }

  public Stream<PropertyShape> getAnnotationPropertyShapes() {
    return model.filter(null, RDF.TYPE, YS_ANNOTATION_PROPERTY_SHAPE).subjects().stream().map(PropertyShape::new);
  }

  public Optional<Class> getClass(Resource term) {
    if (model.contains(term, RDF.TYPE, RDFS.CLASS) || model.contains(term, RDF.TYPE, OWL.CLASS)) {
      return Optional.of(new Class(term));
    } else {
      return Optional.empty();
    }
  }

  public Stream<Class> getClasses() {
    return Stream.concat(
            model.filter(null, RDF.TYPE, RDFS.CLASS).subjects().stream(),
            model.filter(null, RDF.TYPE, OWL.CLASS).subjects().stream()
    ).distinct().map(Class::new);
  }

  public Optional<Property> getProperty(Resource term) {
    if (model.contains(term, RDF.TYPE, RDF.PROPERTY) || model.contains(term, RDF.TYPE, OWL.OBJECTPROPERTY) || model.contains(term, RDF.TYPE, OWL.DATATYPEPROPERTY)) {
      return Optional.of(new Property(term));
    } else {
      return Optional.empty();
    }
  }

  public interface NodeShape {
    String getName();

    Stream<Resource> getClasses();

    Stream<PropertyShape> getProperties();

    Stream<IRI> getFromClasses();
  }

  private class SingleNodeShape implements NodeShape {
    private Resource id;

    private SingleNodeShape(Resource id) {
      this.id = id;
    }

    @Override
    public String getName() {
      return Models.getPropertyResource(model, id, SHACL.TARGET_CLASS)
              .flatMap(target -> Models.getPropertyString(model, target, RDFS.LABEL))
              .orElseGet(() ->
                      Models.getPropertyString(model, id, RDFS.LABEL).orElseGet(() ->
                              (id instanceof IRI) ? ((IRI) id).getLocalName() : id.toString()
                      )
              );
    }

    @Override
    public Stream<Resource> getClasses() {
      var classes = Models.getPropertyResources(model, id, SHACL.TARGET_CLASS);
      return classes.isEmpty() ? Stream.of(id) : classes.stream();
    }

    @Override
    public Stream<PropertyShape> getProperties() {
      return Models.getPropertyResources(model, id, SHACL.PROPERTY).stream().map(PropertyShape::new);
    }

    @Override
    public Stream<IRI> getFromClasses() {
      return Models.getPropertyIRIs(model, id, YS_FROM_CLASS).stream();
    }

    @Override
    public boolean equals(Object obj) {
      return (obj instanceof SingleNodeShape) && id.equals(((SingleNodeShape) obj).id);
    }

    @Override
    public int hashCode() {
      return id.hashCode();
    }
  }

  private class UnionNodeShape implements NodeShape {
    private Set<NodeShape> shapes;

    private UnionNodeShape(Stream<NodeShape> shapes) {
      this.shapes = shapes.collect(Collectors.toSet());
    }

    @Override
    public String getName() {
      return shapes.stream().map(NodeShape::getName).sorted().collect(Collectors.joining("Or"));
    }

    @Override
    public Stream<Resource> getClasses() {
      return shapes.stream().flatMap(NodeShape::getClasses);
    }

    @Override
    public Stream<PropertyShape> getProperties() {
      return shapes.stream().flatMap(NodeShape::getProperties);
    }

    @Override
    public Stream<IRI> getFromClasses() {
      return shapes.stream().flatMap(NodeShape::getFromClasses).distinct();
    }
  }

  public class PropertyShape {
    private Resource id;

    private PropertyShape(Resource id) {
      this.id = id;
    }

    public IRI getProperty() {
      return Models.getPropertyIRI(model, id, SHACL.PATH)
              .orElseThrow(() -> new IllegalArgumentException("The sh:PropertyShape " + id + " should have a single property sh:path pointing to the IRI of a property"));
    }

    public Optional<NodeShape> getParentShape() {
      return nodeShapeUnion(model.filter(null, SHACL.PROPERTY, id).subjects());
    }

    private Optional<NodeShape> nodeShapeUnion(Set<Resource> shapes) {
      switch (shapes.size()) {
        case 0:
          return Optional.empty();
        case 1:
          return Optional.of(new SingleNodeShape(shapes.iterator().next()));
        default:
          return Optional.of(new UnionNodeShape(shapes.stream().map(SingleNodeShape::new)));
      }
    }

    public Optional<Set<IRI>> getDatatypes() {
      Set<IRI> datatypes = Stream.concat(
              Stream.of(id),
              Models.getPropertyResources(model, id, SHACL.OR).stream()
                      .flatMap(head -> RDFCollections.asValues(model, head, new ArrayList<>()).stream().map(v -> (Resource) v))
      ).flatMap(node -> Models.getPropertyIRIs(model, node, SHACL.DATATYPE).stream())
              .collect(Collectors.toSet());
      return datatypes.isEmpty() ? Optional.empty() : Optional.of(datatypes);
    }

    public Optional<NodeShape> getNodeShape() {
      return nodeShapeUnion(Stream.concat(
              Stream.of(id),
              Models.getPropertyResources(model, id, SHACL.OR).stream()
                      .flatMap(head -> RDFCollections.asValues(model, head, new ArrayList<>()).stream().map(v -> (Resource) v))
      ).flatMap(node -> Models.getPropertyResources(model, node, SHACL.NODE).stream()).collect(Collectors.toSet()));
    }

    public OptionalInt getMinCount() {
      return Models.getPropertyLiterals(model, id, SHACL.MIN_COUNT).stream().mapToInt(Literal::intValue).findFirst();
    }

    public OptionalInt getMaxCount() {
      return Models.getPropertyLiterals(model, id, SHACL.MAX_COUNT).stream().mapToInt(Literal::intValue).findFirst();
    }

    public boolean isUniqueLang() {
      return model.contains(id, SHACL.UNIQUE_LANG, VALUE_FACTORY.createLiteral(true));
    }

    public Optional<Pattern> getPattern() {
      return Models.getPropertyLiteral(model, id, SHACL.PATTERN).map(t -> Pattern.compile(t.stringValue()));
    }

    public Stream<IRI> getFromProperties() {
      return Models.getPropertyIRIs(model, id, YS_FROM_PROPERTY).stream();
    }
  }

  private abstract class OntologyElement {
    protected Resource term;

    private OntologyElement(Resource term) {
      this.term = term;
    }

    public Resource getTerm() {
      return term;
    }

    public Stream<Literal> getLabels() {
      return Models.getPropertyLiterals(model, term, RDFS.LABEL).stream();
    }

    public Stream<Literal> getComments() {
      return Models.getPropertyLiterals(model, term, RDFS.COMMENT).stream();
    }
  }

  public class Class extends OntologyElement {
    private Class(Resource term) {
      super(term);
    }

    public Stream<Resource> getSuperClasses() {
      return Models.getPropertyResources(model, term, RDFS.SUBCLASSOF).stream();
    }

    public Stream<Resource> getDisjointedClasses() {
      return Stream.concat(
              Models.getPropertyResources(model, term, OWL.DISJOINTWITH).stream(),
              model.filter(null, OWL.DISJOINTWITH, term).stream().map(Statement::getSubject)
      ).distinct();
    }
  }

  public class Property extends OntologyElement {
    private Property(Resource term) {
      super(term);
    }

    public Stream<Resource> getSuperProperties() {
      return Models.getPropertyResources(model, term, RDFS.SUBPROPERTYOF).stream();
    }

    public Stream<Resource> getInverseProperties() {
      return Stream.concat(
              Stream.concat(
                      Models.getPropertyResources(model, term, OWL.INVERSEOF).stream(),
                      Models.getPropertyResources(model, term, SCHEMA_INVERSE_OF).stream()
              ),
              Stream.concat(
                      model.filter(null, OWL.INVERSEOF, term).stream().map(Statement::getSubject),
                      model.filter(null, SCHEMA_INVERSE_OF, term).stream().map(Statement::getSubject)
              )
      ).distinct();
    }
  }
}
