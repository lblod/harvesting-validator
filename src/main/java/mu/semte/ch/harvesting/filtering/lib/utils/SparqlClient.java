package mu.semte.ch.harvesting.filtering.lib.utils;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicNameValuePair;
import org.apache.jena.atlas.web.HttpException;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.web.HttpOp;
import org.apache.jena.web.HttpSC;

import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
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
  private Map<String, String> muHeaders;

  public void insertModel(String graphUri, Model model) {
    var triples = ModelUtils.toString(model, RDFLanguages.NTRIPLES);
    String updateQuery = String.format("INSERT DATA { GRAPH <%s> { %s } }", graphUri, triples);
    executeUpdateQuery(updateQuery);
  }

  @SneakyThrows
  public void executeUpdateQuery(String updateQuery) {
    log.debug(updateQuery);

    HttpClient httpclient = buildHttpClient();

    HttpOp.setDefaultHttpClient(httpclient);

    HttpPost httpPost = new HttpPost(url);
    httpPost.setEntity(new UrlEncodedFormEntity(Collections.singletonList(new BasicNameValuePair("query", updateQuery)), StandardCharsets.UTF_8));
    HttpResponse response = httpclient.execute(httpPost);

    StatusLine statusLine = response.getStatusLine();
    int statusCode = statusLine.getStatusCode();
    if (HttpSC.isClientError(statusCode) || HttpSC.isServerError(statusCode)) {
      final String contentPayload = HttpOp.readPayload(response.getEntity());
      throw new HttpException(statusCode, statusLine.getReasonPhrase(), contentPayload);
    }
    else {
      response.getEntity().getContent().close();
    }

  }

  public <R> R executeSelectQuery(String query, Function<ResultSet, R> resultHandler) {
    log.debug(query);
    try (QueryExecution queryExecution = QueryExecutionFactory.sparqlService(url, query, buildHttpClient())) {
      return resultHandler.apply(queryExecution.execSelect());
    }
  }

  public Model executeConstructQuery(String query) {
    log.debug(query);
    try (QueryExecution queryExecution = QueryExecutionFactory.sparqlService(url, query, buildHttpClient())) {
      return queryExecution.execConstruct();
    }
  }

  // todo workaround to bypass mu-auth response not returning rdf data
  // https://github.com/mu-semtech/mu-authorization/issues/4
  public Model executeConstructQueryResultAsSparqlJson(String query) {
    log.debug(query);
    try (QueryExecution queryExecution = QueryExecutionFactory.sparqlService(url, query, buildHttpClient())) {
      var result = queryExecution.execSelect();
      Model model = ModelFactory.createDefaultModel();
      result.forEachRemaining(querySolution -> {
        RDFNode subject = querySolution.get("s");
        RDFNode predicate = querySolution.get("p");
        RDFNode object = querySolution.get("o");
        var triple = Triple.create(subject.asNode(), predicate.asNode(), object.asNode());
        model.getGraph().add(triple);
      });
      return model;
    }
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
                      .setDefaultHeaders(muHeaders.entrySet()
                                                  .stream()
                                                  .map(h -> new BasicHeader(h.getKey(), h.getValue()))
                                                  .collect(Collectors.toList()))
                      .build();

  }
}
