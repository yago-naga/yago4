package org.yago.yago4.converter.utils;

import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.rio.ntriples.NTriplesUtil;
import org.yago.yago4.converter.EvaluationException;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

/**
 * Code from org.eclipse.rdf4j.rio.ntriples.
 * <p>
 * Copyright (c) 2015 Eclipse RDF4J contributors, Aduna, and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/org/documents/edl-v10.php.
 * <p>
 * TODO: allow n-Quads
 */
public class NTriplesReader implements Serializable {

  private final ValueFactory valueFactory;

  public NTriplesReader(ValueFactory valueFactory) {
    this.valueFactory = valueFactory;
  }

  public Stream<Statement> read(Path filePath) {
    try {
      return Files.lines(filePath).parallel().flatMap(this::parseNTriplesLine);
    } catch (IOException e) {
      throw new EvaluationException(e);
    }
  }

  public Stream<Statement> parseNTriplesLine(String line) {
    int i = skipBlanks(line, 0);
    if (i >= line.length() || line.charAt(i) == '#') {
      return Stream.empty();
    }

    int subjectStart = i;
    i = skipNotBlank(line, i);
    Resource subject = NTriplesUtil.parseResource(line.substring(subjectStart, i), valueFactory);

    i = skipBlanks(line, i);
    int predicateStart = i;
    i = skipNotBlank(line, i);
    IRI predicate = NTriplesUtil.parseURI(line.substring(predicateStart, i), valueFactory);

    i = skipBlanks(line, i);
    Value object = NTriplesUtil.parseValue(line.substring(i, skipBlanksAndDotInReverse(line, line.length() - 1) + 1), valueFactory);

    return Stream.ofNullable(valueFactory.createStatement(subject, predicate, object));
  }

  private static int skipBlanks(String str, int i) {
    while (i < str.length() && (str.charAt(i) == ' ' || str.charAt(i) == '\t')) {
      i++;
    }
    return i;
  }

  private static int skipBlanksAndDotInReverse(String str, int i) {
    while (i >= 0 && str.charAt(i) == ' ' || str.charAt(i) == '\t' || str.charAt(i) == '.') {
      i--;
    }
    return i;
  }

  private static int skipNotBlank(String str, int i) {
    while (i < str.length() && str.charAt(i) != ' ' && str.charAt(i) != '\t') {
      i++;
    }
    return i;
  }
}
