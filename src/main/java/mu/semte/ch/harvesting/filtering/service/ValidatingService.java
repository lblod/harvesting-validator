package mu.semte.ch.harvesting.filtering.service;

import lombok.extern.slf4j.Slf4j;
import mu.semte.ch.harvesting.filtering.lib.dto.DataContainer;
import mu.semte.ch.harvesting.filtering.lib.dto.Task;
import mu.semte.ch.harvesting.filtering.lib.utils.ModelUtils;
import org.apache.jena.rdf.model.Model;
import org.springframework.stereotype.Service;

import static mu.semte.ch.harvesting.filtering.Constants.VALIDATING_GRAPH_PREFIX;

@Service
@Slf4j
public class ValidatingService {
  private final ShaclService shaclService;
  private final TaskService taskService;

  public ValidatingService(ShaclService shaclService, TaskService taskService) {
    this.shaclService = shaclService;
    this.taskService = taskService;
  }

  public void runValidatePipeline(Task task) {
    var inputContainer = taskService.selectInputContainer(task).get(0);
    var importedTriples = taskService.loadImportedTriples(inputContainer.getGraphUri());

    var fileContainer = DataContainer.builder().build();

    var report = writeValidationReport(task, fileContainer, importedTriples);

    // import validation report
    var reportGraph = "%s/%s".formatted(VALIDATING_GRAPH_PREFIX, task.getId());

    taskService.importTriples(reportGraph, report);

    // append result graph
    var resultContainer = DataContainer.builder()
                                       .graphUri(inputContainer.getGraphUri())
                                       .validationGraphUri(reportGraph)
                                       .build();

    taskService.appendTaskResultGraph(task, resultContainer);
  }


  private Model writeValidationReport(Task task, DataContainer fileContainer, Model importedTriples) {
    log.debug("generate validation reports...");
    var report = shaclService.validate(importedTriples.getGraph());
    log.debug("triples conforms: {}", report.conforms());
    var reportModel = ModelUtils.replaceAnonNodes(report.getModel(), "report");
    var dataContainer = fileContainer.toBuilder()
                                     .graphUri(taskService.writeTtlFile(task.getGraph(), reportModel, "validation-report.ttl"))
                                     .build();
    taskService.appendTaskResultFile(task, dataContainer);
    return reportModel;
  }
}
