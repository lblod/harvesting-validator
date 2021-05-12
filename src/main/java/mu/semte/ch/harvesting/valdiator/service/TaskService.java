package mu.semte.ch.harvesting.valdiator.service;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import mu.semte.ch.lib.dto.DataContainer;
import mu.semte.ch.lib.dto.Task;
import mu.semte.ch.lib.utils.ModelUtils;
import mu.semte.ch.lib.utils.SparqlClient;
import mu.semte.ch.lib.utils.SparqlQueryStore;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static java.util.Optional.ofNullable;
import static mu.semte.ch.harvesting.valdiator.Constants.ERROR_URI_PREFIX;
import static mu.semte.ch.harvesting.valdiator.Constants.LOGICAL_FILE_PREFIX;
import static mu.semte.ch.lib.utils.ModelUtils.filenameToLang;
import static mu.semte.ch.lib.utils.ModelUtils.formattedDate;
import static mu.semte.ch.lib.utils.ModelUtils.getContentType;
import static mu.semte.ch.lib.utils.ModelUtils.getExtension;
import static mu.semte.ch.lib.utils.ModelUtils.uuid;

@Service
@Slf4j
public class TaskService {

  private final SparqlQueryStore queryStore;
  private final SparqlClient sparqlClient;
  @Value("${share-folder.path}")
  private String shareFolderPath;
  @Value("${sparql.defaultBatchSize}")
  private int defaultBatchSize;
  @Value("${sparql.defaultLimitSize}")
  private int defaultLimitSize;

  public TaskService(SparqlQueryStore queryStore, SparqlClient sparqlClient) {
    this.queryStore = queryStore;
    this.sparqlClient = sparqlClient;
  }


  public boolean isTask(String subject) {
    String queryStr = queryStore.getQuery("isTask").formatted(subject);

    return sparqlClient.executeAskQuery(queryStr);
  }

  public Task loadTask(String deltaEntry) {
    String queryTask = queryStore.getQuery("loadTask").formatted(deltaEntry);

    return sparqlClient.executeSelectQuery(queryTask, resultSet -> {
      if (!resultSet.hasNext()) {
        return null;
      }
      var t = resultSet.next();
      Task task = Task.builder().task(t.getResource("task").getURI())
                      .job(t.getResource("job").getURI())
                      .error(ofNullable(t.getResource("error")).map(Resource::getURI).orElse(null))
                      .id(t.getLiteral("id").getString())
                      .created(t.getLiteral("created").getString())
                      .modified(t.getLiteral("modified").getString())
                      .operation(t.getResource("operation").getURI())
                      .index(t.getLiteral("index").getString())
                      .graph(t.getResource("graph").getURI())
                      .status(t.getResource("status").getURI())
                      .build();
      log.debug("task: {}", task);
      return task;
    });

  }

  public Model loadImportedTriples(String graphImportedTriples) {
    var countTriplesQuery = queryStore.getQuery("countImportedTriples").formatted(graphImportedTriples);
    var countTriples = sparqlClient.executeSelectQuery(countTriplesQuery,resultSet -> {
      if (!resultSet.hasNext()){
        return 0;
      }
      return resultSet.next().getLiteral("count").getInt();
    });
    var pagesCount = countTriples > defaultLimitSize ? countTriples / defaultLimitSize : defaultLimitSize;

    return IntStream.rangeClosed(0, pagesCount)
             .mapToObj(page -> {
               var query = queryStore.getQueryWithParameters("loadImportedTriplesStream",
                                                             Map.of("graphUri",graphImportedTriples,
                                                                    "limitSize", defaultLimitSize,
                                                                    "offsetNumber",page * defaultLimitSize
                                                             )
               );
               return sparqlClient.executeSelectQuery(query);
             }).reduce(ModelFactory.createDefaultModel(), Model::add);
  }

  public void updateTaskStatus(Task task, String status) {
    log.debug("set task status to {}...", status);

    String queryUpdate = queryStore.getQuery("updateTaskStatus")
                                   .formatted(status, formattedDate(LocalDateTime.now()), task.getTask());
    sparqlClient.executeUpdateQuery(queryUpdate);
  }

  public void importTriples(String graph,
                            Model model) {
    log.debug("running import triples with batch size {}, model size: {}, graph: <{}>", defaultBatchSize, model.size(), graph);
    List<Triple> triples = model.getGraph().find().toList(); //duplicate so we can splice
    Lists.partition(triples, defaultBatchSize)
         .stream()
         .parallel()
         .map(batch -> {
           Model batchModel = ModelFactory.createDefaultModel();
           Graph batchGraph = batchModel.getGraph();
           batch.forEach(batchGraph::add);
           return batchModel;
         }).forEach(batchModel -> sparqlClient.insertModel(graph, batchModel));
  }

  @SneakyThrows
  public String writeTtlFile(String graph,
                             Model content,
                             String logicalFileName) {
    var rdfLang = filenameToLang(logicalFileName);
    var fileExtension = getExtension(rdfLang);
    var contentType = getContentType(rdfLang);
    var phyId = uuid();
    var phyFilename = "%s.%s".formatted(phyId, fileExtension);
    var path = "%s/%s".formatted(shareFolderPath, phyFilename);
    var physicalFile = "share://%s".formatted(phyFilename);
    var loId = uuid();
    var logicalFile = "%s/%s".formatted(LOGICAL_FILE_PREFIX, loId);
    var now = formattedDate(LocalDateTime.now());
    var file = ModelUtils.toFile(content, rdfLang, path);
    var fileSize = file.length();
    var queryParameters = ImmutableMap.<String, Object>builder()
            .put("graph", graph)
            .put("physicalFile", physicalFile)
            .put("logicalFile", logicalFile)
            .put("phyId", phyId)
            .put("phyFilename", phyFilename)
            .put("now", now)
            .put("fileSize", fileSize)
            .put("loId", loId)
            .put("logicalFileName", logicalFileName)
            .put("fileExtension", "nt")
            .put("contentType", contentType).build();

    var queryStr = queryStore.getQueryWithParameters("writeTtlFile", queryParameters);
    sparqlClient.executeUpdateQuery(queryStr);
    return logicalFile;
  }

  public void appendTaskResultFile(Task task,
                                   DataContainer dataContainer) {
    var containerUri = dataContainer.getUri();
    var containerId = dataContainer.getId();
    var fileUri = dataContainer.getGraphUri();
    var queryParameters = Map.of(
            "containerUri", containerUri,
            "containerId", containerId,
            "fileUri", fileUri, "task", task
    );
    var queryStr = queryStore.getQueryWithParameters("appendTaskResultFile", queryParameters);

    sparqlClient.executeUpdateQuery(queryStr);
  }

  public void appendTaskResultGraph(Task task,
                                    DataContainer dataContainer) {
    var queryParameters = Map.of(
            "task", task,
            "dataContainer", dataContainer
    );
    var queryStr = queryStore.getQueryWithParameters("appendTaskResultGraph", queryParameters);
    log.debug(queryStr);
    sparqlClient.executeUpdateQuery(queryStr);

  }

  public List<DataContainer> selectInputContainer(Task task) {
    String queryTask = queryStore.getQuery("selectInputContainerGraph").formatted(task.getTask());

    return sparqlClient.executeSelectQuery(queryTask, resultSet -> {
      if (!resultSet.hasNext()) {
        throw new RuntimeException("Input container graph not found");
      }
      List<DataContainer> graphUris = new ArrayList<>();
      resultSet.forEachRemaining(r -> graphUris.add(DataContainer.builder()
                                                                 .graphUri(r.getResource("graph").getURI())
                                                                 .validationGraphUri(ofNullable(r.getResource("validationGraph"))
                                                                                             .map(Resource::getURI)
                                                                                             .orElse(null))
                                                                 .build()));
      return graphUris;
    });
  }

  public void appendTaskError(Task task, String message) {
    var id = uuid();
    var uri = ERROR_URI_PREFIX + id;

    Map<String, Object> parameters = Map.of("task", task, "id", id, "uri", uri, "message", message);
    var queryStr = queryStore.getQueryWithParameters("appendTaskError", parameters);

    sparqlClient.executeUpdateQuery(queryStr);
  }

}
