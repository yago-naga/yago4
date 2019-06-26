package org.yago.yago4;

import org.eclipse.rdf4j.model.IRI;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class IRIShortener {

  private static final Map<String, String> IRI_FOR_PREFIXES = new HashMap<>();

  static {
    IRI_FOR_PREFIXES.put("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#");
    IRI_FOR_PREFIXES.put("rdfs", "http://www.w3.org/2000/01/rdf-schema#");
    IRI_FOR_PREFIXES.put("xsd", "http://www.w3.org/2001/XMLSchema#");
    IRI_FOR_PREFIXES.put("owl", "http://www.w3.org/2002/07/owl#");
    IRI_FOR_PREFIXES.put("skos", "http://www.w3.org/2004/02/skos/core#");
    IRI_FOR_PREFIXES.put("schema", "http://schema.org/");
    IRI_FOR_PREFIXES.put("prov", "http://www.w3.org/ns/prov#");
    IRI_FOR_PREFIXES.put("geo", "http://www.opengis.net/ont/geosparql#");
    IRI_FOR_PREFIXES.put("wikibase", "http://wikiba.se/ontology#");
    IRI_FOR_PREFIXES.put("wdata", "http://www.wikidata.org/Special:EntityData/");
    IRI_FOR_PREFIXES.put("wd", "http://www.wikidata.org/entity/");
    IRI_FOR_PREFIXES.put("wds", "http://www.wikidata.org/entity/statement/");
    IRI_FOR_PREFIXES.put("wdv", "http://www.wikidata.org/value/");
    IRI_FOR_PREFIXES.put("wdref", "http://www.wikidata.org/reference/");
    IRI_FOR_PREFIXES.put("wdt", "http://www.wikidata.org/prop/direct/");
    IRI_FOR_PREFIXES.put("wdtn", "http://www.wikidata.org/prop/direct-normalized/");
    IRI_FOR_PREFIXES.put("p", "http://www.wikidata.org/prop/");
    IRI_FOR_PREFIXES.put("wdno", "http://www.wikidata.org/prop/novalue/");
    IRI_FOR_PREFIXES.put("ps", "http://www.wikidata.org/prop/statement/");
    IRI_FOR_PREFIXES.put("psv", "http://www.wikidata.org/prop/statement/value/");
    IRI_FOR_PREFIXES.put("psn", "http://www.wikidata.org/prop/statement/value-normalized/");
    IRI_FOR_PREFIXES.put("pq", "http://www.wikidata.org/prop/qualifier/");
    IRI_FOR_PREFIXES.put("pqv", "http://www.wikidata.org/prop/qualifier/value/");
    IRI_FOR_PREFIXES.put("pqn", "http://www.wikidata.org/prop/qualifier/value-normalized/");
    IRI_FOR_PREFIXES.put("pr", "http://www.wikidata.org/prop/reference/");
    IRI_FOR_PREFIXES.put("prv", "http://www.wikidata.org/prop/reference/value/");
    IRI_FOR_PREFIXES.put("prn", "http://www.wikidata.org/prop/reference/value-normalized/");
  }

  private static final Map<String, String> PREFIX_FOR_IRIS;

  static {
    PREFIX_FOR_IRIS = IRI_FOR_PREFIXES.entrySet().stream().collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
  }

  public static String shortened(IRI IRI) {
    String iri = IRI.stringValue();

    int localNameIndex = getLocalNameIndex(iri);
    if (localNameIndex > 0) {
      String prefix = PREFIX_FOR_IRIS.get(iri.substring(0, localNameIndex));
      if (prefix != null) {
        return prefix + ':' + iri.substring(localNameIndex);
      }
    }
    return iri;
  }

  /**
   * Code from org.eclipse.rdf4j.model.util.URIUtil
   * <p>
   * Copyright (c) 2015 Eclipse RDF4J contributors, Aduna, and others.
   * All rights reserved. This program and the accompanying materials
   * are made available under the terms of the Eclipse Distribution License v1.0
   * which accompanies this distribution, and is available at
   * http://www.eclipse.org/org/documents/edl-v10.php.
   */
  private static int getLocalNameIndex(String iri) {
    int separatorIdx = iri.indexOf('#');
    if (separatorIdx < 0) {
      separatorIdx = iri.lastIndexOf('/');
    }
    if (separatorIdx < 0) {
      separatorIdx = iri.lastIndexOf(':');
    }
    if (separatorIdx < 0) {
      return -1;
    }
    return separatorIdx + 1;
  }
}
