package mu.semte.ch.harvesting.filtering.lib.service;

import lombok.extern.slf4j.Slf4j;
import mu.semte.ch.harvesting.filtering.lib.dto.DataContainer;
import mu.semte.ch.harvesting.filtering.lib.dto.Task;
import mu.semte.ch.harvesting.filtering.lib.utils.ModelUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.shacl.ValidationReport;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import static mu.semte.ch.harvesting.filtering.lib.Constants.FILTER_GRAPH_PREFIX;
import static mu.semte.ch.harvesting.filtering.lib.Constants.BLANK_NODE_SUBSTITUTE;
import static mu.semte.ch.harvesting.filtering.lib.Constants.STATUS_BUSY;
import static mu.semte.ch.harvesting.filtering.lib.Constants.STATUS_FAILED;
import static mu.semte.ch.harvesting.filtering.lib.Constants.STATUS_SUCCESS;
import static mu.semte.ch.harvesting.filtering.lib.Constants.TASK_HARVESTING_FILTERING;

@Service
@Slf4j
public class FilteringService {

  private final ShaclService shaclService;
  private final TaskService taskService;

  @Value("${sparql.defaultBatchSize}")
  private int defaultBatchSize;


  public FilteringService(ShaclService shaclService,TaskService taskService) {
    this.shaclService = shaclService;
    this.taskService = taskService;
  }

  @Async
  public void runFilterPipeline(String deltaEntry) {
    // validate task
    if (!taskService.isTask(deltaEntry)) return;
    var task = taskService.loadTask(deltaEntry);
    if (task == null || !task.getOperation().contains(TASK_HARVESTING_FILTERING)) return;

    try {
      taskService.updateTaskStatus(task, STATUS_BUSY);

      var importedTriples = fetchTriplesFromInputContainer(task);
      var fileContainer = DataContainer.builder().build();

      var report = writeValidationReport(task, fileContainer, importedTriples);

      var validTriples = writeValidTriples(task, fileContainer, report, importedTriples);

      writeErrorTriples(task, fileContainer, importedTriples, validTriples);

      // import filtered triples
      var filteredGraph = "%s/%s".formatted(FILTER_GRAPH_PREFIX, task.getId());

      taskService.importTriples(filteredGraph, validTriples, defaultBatchSize);

      // append result graph
      var graphContainer = DataContainer.builder()
                                        .graphUri(filteredGraph)
                                        .build();
      taskService.appendTaskResultGraph(task, graphContainer);

      // success
      taskService.updateTaskStatus(task, STATUS_SUCCESS);
      log.debug("Done with success for task {}", task.getId());
    }
    catch (Exception e) {
      log.error("Error while running filtering:", e);
      taskService.appendTaskError(task, e.getMessage());
      taskService.updateTaskStatus(task, STATUS_FAILED);
    }

  }

  private void writeErrorTriples(Task task, DataContainer fileContainer, Model importedTriples, Model validTriples) {
    var errorTriples = importedTriples.difference(validTriples);
    log.debug("Number of errored triples: {}", errorTriples.size());
    var dataContainer = fileContainer
            .toBuilder()
            .graphUri(taskService.writeTtlFile(task.getGraph(), errorTriples, "error-triples.ttl"))
            .build();
    taskService.appendTaskResultFile(task, dataContainer);
  }

  private Model writeValidTriples(Task task, DataContainer fileContainer, ValidationReport report, Model importedTriples) {
    log.debug("filter non conform triples...");
    var validTriples = shaclService.filter(importedTriples, report);
    var dataContainer = fileContainer.toBuilder()
                         .graphUri(taskService.writeTtlFile(task.getGraph(), validTriples, "valid-triples.ttl"))
                          .build();
    taskService.appendTaskResultFile(task, dataContainer);
    return validTriples;
  }

  private ValidationReport writeValidationReport(Task task, DataContainer fileContainer, Model importedTriples) {
    log.debug("generate validation reports...");
    var report = shaclService.validate(importedTriples.getGraph());
    log.debug("triples conforms: {}", report.conforms());
    var reportModel = ModelUtils.replaceAnonNodes(report.getModel(), BLANK_NODE_SUBSTITUTE);
    var dataContainer = fileContainer.toBuilder()
                                     .graphUri(taskService.writeTtlFile(task.getGraph(), reportModel, "validation-report.ttl"))
                                     .build();
    taskService.appendTaskResultFile(task, dataContainer);
    return report;
  }

  private Model fetchTriplesFromInputContainer(Task task) {

    var graphImportedTriples = taskService.selectInputContainerGraph(task);

    return  taskService.loadImportedTriples(graphImportedTriples);
  }


}
