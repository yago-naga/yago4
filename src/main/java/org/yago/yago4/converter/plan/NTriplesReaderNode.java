package org.yago.yago4.converter.plan;

import org.eclipse.rdf4j.model.Statement;

import java.nio.file.Path;

public class NTriplesReaderNode extends PlanNode<Statement> {
  private final Path filePath;

  NTriplesReaderNode(Path filePath) {
    this.filePath = filePath;
  }

  public Path getFilePath() {
    return filePath;
  }

  @Override
  public boolean equals(Object obj) {
    return (obj instanceof NTriplesReaderNode) && filePath.equals(((NTriplesReaderNode) obj).filePath);
  }

  @Override
  public int hashCode() {
    return filePath.hashCode();
  }
}
