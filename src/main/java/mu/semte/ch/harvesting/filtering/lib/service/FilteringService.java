package mu.semte.ch.harvesting.filtering.lib.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.jena.shacl.ValidationReport;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import static mu.semte.ch.harvesting.filtering.lib.Constants.STATUS_BUSY;
import static mu.semte.ch.harvesting.filtering.lib.Constants.STATUS_SUCCESS;
import static mu.semte.ch.harvesting.filtering.lib.Constants.TASK_HARVESTING_FILTERING;
import static mu.semte.ch.harvesting.filtering.utils.ModelUtils.uuid;

@Service
@Slf4j
public class FilteringService {
  private final TaskService taskService;
  private final ShaclService shaclService;

  public FilteringService(TaskService taskService, ShaclService shaclService) {
    this.taskService = taskService;
    this.shaclService = shaclService;
  }

  @Async
  public void runFilterPipeline(String deltaEntry) {
    if (!taskService.isTask(deltaEntry)) {
      log.debug("delta entry {} is not a task", deltaEntry);
      return;
    }
    var task = taskService.loadTask(deltaEntry);

    if (task == null || !task.getOperation().contains(TASK_HARVESTING_FILTERING)) {
      log.debug("task for delta entry {} not found", deltaEntry);
      return;
    }
    log.info("set task status to busy...");
    taskService.updateTaskStatus(task, STATUS_BUSY);

    var graphImportedTriples = "http://mu.semte.ch/graphs/harvesting/tasks/import/%s".formatted(task.getId());

    log.info("Graph to import from: '{}'", graphImportedTriples);

    var importedTriples = taskService.loadImportedTriples(graphImportedTriples);
    var fileUri = taskService.writeTtlFile(task.getGraph(), importedTriples, "original.ttl");

    var fileContainerId = uuid();
    var fileContainerUri = "http://redpencil.data.gift/id/dataContainers/%s".formatted(fileContainerId);

    log.info("fileContainerUri: '{}'", graphImportedTriples);

    taskService.appendTaskResultFile(task, fileContainerUri, fileContainerId, fileUri);

    ValidationReport report = shaclService.validate(importedTriples.getGraph());

    var reportFile = taskService.writeTtlFile(task.getGraph(), report.getModel(), "validation-report.ttl");
    taskService.appendTaskResultFile(task, fileContainerUri, fileContainerId, reportFile);

    if (report.conforms()) {
      taskService.updateTaskStatus(task, STATUS_SUCCESS);
      return;
    }

    var filteredTriples = shaclService.filter(importedTriples, report);

    var filteredFile = taskService.writeTtlFile(task.getGraph(), filteredTriples, "filtered-triples.ttl");
    taskService.appendTaskResultFile(task, fileContainerUri, fileContainerId, filteredFile);

    var errorTriples = importedTriples.difference(filteredTriples);

    var errorFile = taskService.writeTtlFile(task.getGraph(), errorTriples, "error-triples.ttl");
    taskService.appendTaskResultFile(task, fileContainerUri, fileContainerId, errorFile);

    var filteredGraph = "http://mu.semte.ch/graphs/harvesting/tasks/filter/%s".formatted(task.getId());

    log.info("filteredGraph: '{}'", filteredGraph);

    taskService.importTriples(filteredGraph, filteredTriples);

    var graphContainerId = uuid();
    var graphContainerUri = "http://redpencil.data.gift/id/dataContainers/%s".formatted(graphContainerId);

    log.info("graphContainerUri: '{}'", graphContainerUri);

    taskService.appendTaskResultGraph(task, graphContainerUri, graphContainerId, filteredGraph);

    taskService.updateTaskStatus(task, STATUS_SUCCESS);

    log.info("Done with success for task {}", task.getId());

  }


}
