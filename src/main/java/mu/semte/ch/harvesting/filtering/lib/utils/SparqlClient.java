package mu.semte.ch.harvesting.filtering.lib.utils;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdfconnection.RDFConnection;
import org.apache.jena.rdfconnection.RDFConnectionRemote;
import org.apache.jena.riot.RDFLanguages;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Slf4j
public class SparqlClient {
  private String url;
  private Map<String, String> httpHeaders;

  public void insertModel(String graphUri, Model model) {
    var triples = ModelUtils.toString(model, RDFLanguages.NTRIPLES);
    String updateQuery = String.format("INSERT DATA { GRAPH <%s> { %s } }", graphUri, triples);
    executeUpdateQuery(updateQuery);
  }

  @SneakyThrows
  public void executeUpdateQuery(String updateQuery) {
    try (RDFConnection conn = RDFConnectionRemote.create()
                                                 .destination(url)
                                                 .httpClient(buildHttpClient())
                                                 .build()) {
      conn.update(updateQuery);
    }

  }

  public <R> R executeSelectQuery(String query, Function<ResultSet, R> resultHandler) {
    log.debug(query);
    try (QueryExecution queryExecution = QueryExecutionFactory.sparqlService(url, query, buildHttpClient())) {
      return resultHandler.apply(queryExecution.execSelect());
    }
  }

  public Model executeSelectQuery(String query) {
    return executeSelectQuery(query, resultSet -> {
      Model model = ModelFactory.createDefaultModel();
      resultSet.forEachRemaining(querySolution -> {
        RDFNode subject = querySolution.get("s");
        RDFNode predicate = querySolution.get("p");
        RDFNode object = querySolution.get("o");
        var triple = Triple.create(subject.asNode(), predicate.asNode(), object.asNode());
        model.getGraph().add(triple);
      });
      return model;
    });
  }

  public boolean executeAskQuery(String askQuery) {
    log.debug(askQuery);
    try (QueryExecution queryExecution = QueryExecutionFactory.sparqlService(url, askQuery, buildHttpClient())) {
      return queryExecution.execAsk();
    }
  }

  public void dropGraph(String graphUri) {
    executeUpdateQuery("clear graph <" + graphUri + ">");
  }

  private HttpClient buildHttpClient() {
    return HttpClients.custom()
                      .setDefaultHeaders(httpHeaders.entrySet()
                                                  .stream()
                                                  .map(h -> new BasicHeader(h.getKey(), h.getValue()))
                                                  .collect(Collectors.toList()))
                      .build();

  }
}
