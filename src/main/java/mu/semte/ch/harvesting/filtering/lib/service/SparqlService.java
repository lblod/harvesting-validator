package mu.semte.ch.harvesting.filtering.lib.service;

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
import org.apache.http.message.BasicNameValuePair;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.web.HttpOp;
import org.apache.jena.sparql.resultset.RDFOutput;
import org.apache.jena.system.Txn;
import org.apache.jena.system.TxnCounter;
import org.apache.jena.update.UpdateExecutionFactory;
import org.apache.jena.update.UpdateFactory;
import org.apache.jena.update.UpdateProcessor;
import org.apache.jena.update.UpdateRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.Authenticator;
import java.net.HttpURLConnection;
import java.net.PasswordAuthentication;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.function.Function;

@Service
@Slf4j
public class SparqlService {
    @Value("${sparql.endpoint}")
    private String url;
    @Value("${sparql.username}")
    private String user;
    @Value("${sparql.password}")
    private String password;


    private static void loadIntoGraph_exception(byte[] data, String updateUrl) throws Exception {
        URL url = new URL(updateUrl);

        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setDoOutput(true);
        conn.setInstanceFollowRedirects(true);
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "application/x-turtle");
        conn.setRequestProperty("charset", "utf-8");
        conn.setRequestProperty("Content-Length", Integer.toString(data.length));
        conn.setUseCaches(false);
        conn.getOutputStream().write(data);

        if ((conn.getResponseCode() / 100) != 2)
            throw new RuntimeException("Not 2xx as answer: " + conn.getResponseCode() + " " + conn.getResponseMessage());
    }

    public String getServerUrl() {
        return url + "/sparql";
    }

    public void executeUpdateInTransaction(String graphUri, Model model) {
        Txn.executeWrite(new TxnCounter(0), () -> {
            UpdateRequest updateRequest = UpdateFactory.create();
            var writer = new StringWriter();
            model.write(writer, "ttl");

            String updateQuery = String.format("INSERT DATA { GRAPH <%s> { %s } }", graphUri, writer.toString());

            updateRequest.add(String.format("CLEAR GRAPH <%s>", graphUri))
                    .add(updateQuery);
            ;
            UpdateProcessor remoteForm = UpdateExecutionFactory.createRemoteForm(updateRequest, getServerUrl(), buildHttpClient());
            remoteForm.execute();

        });
    }

    private void setAuth(){
        if (StringUtils.isNotBlank(user)) {
            Authenticator.setDefault(new Authenticator() {
                protected PasswordAuthentication getPasswordAuthentication() {
                    return new PasswordAuthentication(user, password.toCharArray());
                }
            });
        }
    }

    @SneakyThrows
    public void uploadTtlFile(File file) {
        setAuth();
        String sparqlUrl = getServerUrl() + "-graph-crud-auth?graph-uri=" + StringUtils.removeEnd(file.getName(), ".ttl");
        loadIntoGraph_exception(FileUtils.readFileToByteArray(file), sparqlUrl);

    }

    @SneakyThrows
    public Model queryForModel(String query) {
        try (QueryExecution queryExecution = QueryExecutionFactory.sparqlService(getServerUrl(), query, buildHttpClient())) {
            return queryExecution.execConstruct();
        }
    }


    @SneakyThrows
    public void executeUpdateQuery(String updateQuery) {

            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(user, password));
            HttpClient httpclient = HttpClients.custom()
                    .setDefaultCredentialsProvider(credentialsProvider)
                    .build();
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
            } else {
               execute.getEntity().getContent().close();
            }

    }

    @SneakyThrows
    public void upload(Model model, String graphUri) {
       setAuth();
        StringWriter writer = new StringWriter();
        model.write(writer, "ttl");
            String sparqlUrl = getServerUrl() + "-graph-crud-auth?graph-uri=" + graphUri;
            loadIntoGraph_exception(writer.toString().getBytes(), sparqlUrl);
    }

    public <R> R executeSelectQuery(String query, Function<ResultSet, R> resultHandler) {
        try (QueryExecution queryExecution = QueryExecutionFactory.sparqlService(getServerUrl(), query, buildHttpClient())) {
            return resultHandler.apply(queryExecution.execSelect());
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
        if (StringUtils.isBlank(getServerUrl())) return null;
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(user, password));
        return HttpClients.custom()
                .setDefaultCredentialsProvider(credentialsProvider)
                .build();

    }
}
