package mu.semte.ch.harvesting.filtering.lib.service;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicNameValuePair;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.Syntax;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.rdf.model.impl.StatementImpl;
import org.apache.jena.riot.web.HttpOp;
import org.apache.jena.sparql.resultset.RDFOutput;
import org.apache.jena.system.Txn;
import org.apache.jena.system.TxnCounter;
import org.apache.jena.update.UpdateExecutionFactory;
import org.apache.jena.update.UpdateFactory;
import org.apache.jena.update.UpdateProcessor;
import org.apache.jena.update.UpdateRequest;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.InputStream;
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
public class SparqlService {
    private String url;
    private Map<String,String> muHeaders;

    public String getServerUrl() {
        return url + "/sparql";
    }

    public void executeUpdateInTransaction(String graphUri, Model model) {
        Txn.executeWrite(new TxnCounter(0), () -> {
            UpdateRequest updateRequest = UpdateFactory.create();
            var writer = new StringWriter();
            model.write(writer, "ttl");

            String updateQuery = String.format("INSERT DATA { GRAPH <%s> { %s } }", graphUri, writer.toString());

            updateRequest.add(updateQuery);

            UpdateProcessor remoteForm = UpdateExecutionFactory.createRemoteForm(updateRequest, getServerUrl(), buildHttpClient());
            remoteForm.execute();

        });
    }

    @SneakyThrows
    public void executeUpdateQuery(String updateQuery) {

        HttpClient httpclient = buildHttpClient();

        HttpOp.setDefaultHttpClient(httpclient);

        HttpPost httpPost = new HttpPost(getServerUrl());
        httpPost.setEntity(new UrlEncodedFormEntity(Collections.singletonList(new BasicNameValuePair("query", updateQuery)), StandardCharsets.UTF_8));
        HttpResponse execute = httpclient.execute(httpPost);

        StatusLine statusLine = execute.getStatusLine();
        if (statusLine.getStatusCode() != 200) {
            try (InputStream content = execute.getEntity().getContent()) {
                String response = IOUtils.toString(content, StandardCharsets.UTF_8);
                log.error("Update didn't answer 200 code: {}", IOUtils.toString(content, StandardCharsets.UTF_8));
            }
        }
        else {
            execute.getEntity().getContent().close();
        }

    }

    public <R> R executeSelectQuery(String query, Function<ResultSet, R> resultHandler) {
        try (QueryExecution queryExecution = QueryExecutionFactory.sparqlService(getServerUrl(), query, buildHttpClient())) {
            return resultHandler.apply(queryExecution.execSelect());
        }
    }

    public Model executeConstructQuery(String query) {
        try (QueryExecution queryExecution = QueryExecutionFactory.sparqlService(getServerUrl(), query, buildHttpClient())) {
            return queryExecution.execConstruct();
        }
    }

    // todo workaround to bypass mu-auth response not returning rdf data
    // https://github.com/mu-semtech/mu-authorization/issues/4
    public Model executeConstructQueryResultAsSparqlJson(String query) {
        try (QueryExecution queryExecution = QueryExecutionFactory.sparqlService(getServerUrl(), query, buildHttpClient())) {
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

    public Model executeSelectQuery(String query) {
        return executeSelectQuery(query, RDFOutput::encodeAsModel);
    }

    public boolean executeAskQuery(String askQuery) {
        try (QueryExecution queryExecution = QueryExecutionFactory.sparqlService(getServerUrl(), askQuery, buildHttpClient())) {
            return queryExecution.execAsk();
        }
    }

    public void dropGraph(String graphUri) {
        executeUpdateQuery("clear graph <" + graphUri + ">");
    }

    private HttpClient buildHttpClient() {
      return HttpClients.custom()
                        .setDefaultHeaders(muHeaders.entrySet().stream().map(h -> new BasicHeader(h.getKey(), h.getValue())).collect(Collectors.toList()))
                        .build();

    }
}
