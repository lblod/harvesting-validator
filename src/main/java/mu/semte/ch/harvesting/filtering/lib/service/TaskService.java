package mu.semte.ch.harvesting.filtering.lib.service;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import mu.semte.ch.harvesting.filtering.lib.dto.Task;
import org.apache.commons.io.FileUtils;
import org.apache.jena.atlas.lib.DateTimeUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.Date;

import static java.util.Optional.ofNullable;
import static mu.semte.ch.harvesting.filtering.utils.ModelUtils.escapeDateTime;
import static mu.semte.ch.harvesting.filtering.utils.ModelUtils.escapeNumber;
import static mu.semte.ch.harvesting.filtering.utils.ModelUtils.uuid;

@Service
@Slf4j
public class TaskService {


  private final SparqlQueryStore queryStore;

    public TaskService(SparqlQueryStore queryStore) {
        this.queryStore = queryStore;
    }

    public boolean isTask(String subject, SparqlService sparqlService) {
        String queryStr = queryStore.getQuery("isTask").formatted(subject);

        return sparqlService.executeAskQuery(queryStr);
    }

    public Task loadTask(String deltaEntry, SparqlService sparqlService) {
        String queryTask = queryStore.getQuery("loadTask").formatted(deltaEntry);

        return sparqlService.executeSelectQuery(queryTask, resultSet -> {
            if (!resultSet.hasNext()) {
                return null;
            }
            var t = resultSet.next();
            return Task.builder().task(t.getResource("task").getURI())
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
        });

    }

    public Model loadImportedTriples(String graphImportedTriples,
                                     SparqlService sparqlService) {
        String queryTask = queryStore.getQuery("loadImportedTriples").formatted(graphImportedTriples);
        return sparqlService.executeConstructQueryResultAsSparqlJson(queryTask);
    }

    public void updateTaskStatus(Task task,
                                 String status,
                                 SparqlService sparqlService) {
        String queryUpdate = queryStore.getQuery("updateTaskStatus")
                                       .formatted(status, escapeDateTime(new Date()), task.getTask());
        sparqlService.executeUpdateQuery(queryUpdate);
    }

    public void importTriples(String filteredGraph,
                              Model filteredTriples,
                              SparqlService sparqlService) {
        sparqlService.executeUpdateInTransaction(filteredGraph,filteredTriples);
    }

    @SneakyThrows
    public String writeTtlFile(String graph,
                               Model content,
                               String logicalFileName,
                               SparqlService sparqlService) {
        var phyId = uuid();
        var phyFilename = "%s.ttl".formatted(phyId);
        var path = "/share/%s".formatted(phyFilename);
        var physicalFile = path.replace("/share/", "share://");
        var loId = uuid();
        var logicalFile = "http://data.lblod.info/id/files/%s".formatted(loId);
        var now = DateTimeUtils.nowAsString();
        var file = new File(path);
        var writer = new StringWriter();
        content.write(writer, "TTL");
        FileUtils.writeStringToFile(file, writer.toString(), StandardCharsets.UTF_8);
        var fileSize = escapeNumber(file.length());

        var queryStr = queryStore.getQuery("writeTtlFile")
                                 .formatted(graph, physicalFile, logicalFile, phyId, phyFilename, now, now, fileSize, logicalFile, loId, logicalFileName, now, now, fileSize);
        log.info(queryStr);
        sparqlService.executeUpdateQuery(queryStr);
        return logicalFile;
    }

    public void appendTaskResultFile(Task task,
                                     String containerUri,
                                     String containerId,
                                     String fileUri,
                                     SparqlService sparqlService) {
        var queryStr = queryStore.getQuery("appendTaskResultFile")
                                 .formatted(task.getGraph(), containerUri, containerUri, containerId, containerUri, fileUri, task
                                         .getTask(), containerUri);
         log.info(queryStr);
        sparqlService.executeUpdateQuery(queryStr);

    }

    public void appendTaskResultGraph(Task task,
                                      String graphContainerUri,
                                      String graphContainerId,
                                      String filteredGraph,
                                      SparqlService sparqlService) {
        var queryStr = queryStore.getQuery("appendTaskResultGraph")
                                 .formatted(task.getGraph(), graphContainerUri, graphContainerUri, graphContainerId, graphContainerUri, filteredGraph, task
                                         .getTask(), graphContainerUri);
      log.info(queryStr);
      sparqlService.executeUpdateQuery(queryStr);

    }
}
