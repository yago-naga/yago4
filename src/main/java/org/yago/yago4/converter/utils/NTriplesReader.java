package org.yago.yago4.converter.utils;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.io.FilenameUtils;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.rio.ntriples.NTriplesUtil;
import org.yago.yago4.converter.EvaluationException;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

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
      return openReader(filePath).lines().parallel().flatMap(this::parseNTriplesLineSafe);
    } catch (IOException e) {
      throw new EvaluationException(e);
    }
  }

  private BufferedReader openReader(Path filePath) throws IOException {
    String extension = FilenameUtils.getExtension(filePath.toString());
    InputStream inputStream = new BufferedInputStream(Files.newInputStream(filePath), 16777216);
    if (extension.equals("gz")) {
      inputStream = new GZIPInputStream(inputStream);
    } else if (extension.equals("bz2")) {
      inputStream = new BZip2CompressorInputStream(inputStream);
    }
    return new BufferedReader(new InputStreamReader(inputStream));
  }

  private Stream<Statement> parseNTriplesLineSafe(String line) {
    try {
      return parseNTriplesLine(line);
    } catch (Exception e) {
      System.err.println(e.getMessage() + ": " + line);
      return Stream.empty();
    }
  }

  private Stream<Statement> parseNTriplesLine(String line) {
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

    return Stream.of(valueFactory.createStatement(subject, predicate, object));
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
