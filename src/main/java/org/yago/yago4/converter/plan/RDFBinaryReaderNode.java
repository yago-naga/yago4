package org.yago.yago4.converter.plan;

import org.eclipse.rdf4j.model.Statement;

import java.nio.file.Path;

public class RDFBinaryReaderNode extends PlanNode<Statement> {
  private final Path filePath;

  RDFBinaryReaderNode(Path filePath) {
    this.filePath = filePath;
  }

  public Path getFilePath() {
    return filePath;
  }

  @Override
  public boolean equals(Object obj) {
    return (obj instanceof RDFBinaryReaderNode) && filePath.equals(((RDFBinaryReaderNode) obj).filePath);
  }

  @Override
  public int hashCode() {
    return filePath.hashCode();
  }
}
