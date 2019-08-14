package org.yago.yago4;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;

import java.util.Objects;

public class AnnotatedStatement {
  private final Statement subject;
  private final IRI predicate;
  private final Value object;

  public AnnotatedStatement(Statement subject, IRI predicate, Value object) {
    this.subject = subject;
    this.predicate = predicate;
    this.object = object;
  }

  public Statement getSubject() {
    return subject;
  }

  public IRI getPredicate() {
    return predicate;
  }

  public Value getObject() {
    return object;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }

    if (!(other instanceof AnnotatedStatement)) {
      return false;
    }
    AnnotatedStatement that = (AnnotatedStatement) other;
    return object.equals(that.getObject()) && subject.equals(that.getSubject()) && predicate.equals(that.getPredicate());
  }

  @Override
  public int hashCode() {
    return Objects.hash(subject, predicate, object);
  }

  @Override
  public String toString() {
    return "(" + subject + ", " + predicate + ", " + object + ")";
  }
}
