package mu.semte.ch.harvesting.filtering.lib.service;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import mu.semte.ch.harvesting.filtering.lib.dto.Task;
import mu.semte.ch.harvesting.filtering.utils.ModelUtils;
import org.apache.jena.rdf.model.Model;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.FileWriter;
import java.util.Date;

import static mu.semte.ch.harvesting.filtering.lib.Constants.TASK_TYPE;
import static mu.semte.ch.harvesting.filtering.utils.ModelUtils.escapeDateTime;
import static mu.semte.ch.harvesting.filtering.utils.ModelUtils.escapeNumber;
import static mu.semte.ch.harvesting.filtering.utils.ModelUtils.uuid;

@Service
@Slf4j
public class TaskService {
    private final SparqlService sparqlService;
    private final SparqlQueryStore queryStore;

    public TaskService(SparqlService sparqlService, SparqlQueryStore queryStore) {
        this.sparqlService = sparqlService;
        this.queryStore = queryStore;
    }

    public boolean isTask(String subject) {
        String queryStr = queryStore.getQuery("isTask").formatted(subject, TASK_TYPE);

        return sparqlService.executeAskQuery(queryStr);
    }

    public Task loadTask(String deltaEntry) {
        String queryTask = queryStore.getQuery("loadTask").formatted(deltaEntry, TASK_TYPE);

        return sparqlService.executeSelectQuery(queryTask, resultSet -> {
            if (!resultSet.hasNext()) {
                return null;
            }
            var t = resultSet.next();
            return Task.builder().task(t.getLiteral("task").getString())
                       .job(t.getLiteral("job").getString())
                       .error(t.getLiteral("error").getString())
                       .id(t.getLiteral("id").getString())
                       .created(t.getLiteral("created").getString())
                       .modified(t.getLiteral("modified").getString())
                       .operation(t.getLiteral("operation").getString())
                       .index(t.getLiteral("index").getString())
                       .graph(t.getLiteral("graph").getString())
                       .status(t.getLiteral("status").getString())
                       .build();
        });

    }

    public Model loadImportedTriples(String graphImportedTriples) {
        String queryTask = queryStore.getQuery("loadImportedTriples").formatted(graphImportedTriples);
        return sparqlService.executeSelectQuery(queryTask);
    }

    public void updateTaskStatus(Task task, String status) {
        String queryUpdate = queryStore.getQuery("updateTaskStatus")
                                       .formatted(status, escapeDateTime(new Date()), task.getTask());
        sparqlService.executeUpdateQuery(queryUpdate);
    }

    public void importTriples(String filteredGraph, Model filteredTriples) {
        sparqlService.upload(filteredTriples, filteredGraph);
    }

    @SneakyThrows
    public String writeTtlFile(String graph, Model content, String logicalFileName) {
        var phyId = uuid();
        var phyFilename = "%s.ttl".formatted(phyId);
        var path = "/share/%s".formatted(phyFilename);
        var physicalFile = path.replace("/share/", "share://");
        var loId = uuid();
        var logicalFile = "http://data.lblod.info/id/files/%s".formatted(loId);
        var now = ModelUtils.escapeDateTime(new Date());
        var file = new File(path);
        content.write(new FileWriter(file), "TTL");
        var fileSize = escapeNumber(file.length());

        var queryStr = queryStore.getQuery("writeTtlFile")
                                 .formatted(graph, physicalFile, logicalFile, phyId, phyFilename, now, now, fileSize, logicalFile, loId, logicalFileName, now, now, fileSize);

        sparqlService.executeUpdateQuery(queryStr);
        return logicalFile;
    }

    public void appendTaskResultFile(Task task, String containerUri, String containerId, String fileUri) {
        var queryStr = queryStore.getQuery("appendTaskResultFile")
                                 .formatted(task.getGraph(), containerUri, containerUri, containerId, containerUri, fileUri, task
                                         .getTask(), containerUri);

        sparqlService.executeUpdateQuery(queryStr);

    }

    public void appendTaskResultGraph(Task task, String graphContainerUri, String graphContainerId, String filteredGraph) {
        var queryStr = queryStore.getQuery("appendTaskResultGraph")
                                 .formatted(task.getGraph(), graphContainerUri, graphContainerUri, graphContainerId, graphContainerUri, filteredGraph, task
                                         .getTask(), graphContainerUri);
        sparqlService.executeUpdateQuery(queryStr);

    }
}
