package org.yago.yago4.converter.plan;

import java.nio.file.Path;

public class TSVReaderNode extends PlanNode<String[]> {
  private final Path filePath;

  TSVReaderNode(Path filePath) {
    this.filePath = filePath;
  }

  public Path getFilePath() {
    return filePath;
  }

  @Override
  public boolean equals(Object obj) {
    return (obj instanceof TSVReaderNode) && filePath.equals(((TSVReaderNode) obj).filePath);
  }

  @Override
  public int hashCode() {
    return filePath.hashCode();
  }
}
