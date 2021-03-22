package mu.semte.ch.harvesting.filtering.lib.service;

import lombok.extern.slf4j.Slf4j;
import mu.semte.ch.harvesting.filtering.lib.dto.DataContainer;
import mu.semte.ch.harvesting.filtering.lib.utils.ModelUtils;
import mu.semte.ch.harvesting.filtering.lib.utils.SparqlClient;
import mu.semte.ch.harvesting.filtering.lib.utils.SparqlQueryStore;
import mu.semte.ch.harvesting.filtering.lib.utils.TaskHelper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.Map;

import static mu.semte.ch.harvesting.filtering.lib.Constants.FILTER_GRAPH_PREFIX;
import static mu.semte.ch.harvesting.filtering.lib.Constants.REPORT_GRAPH_PREFIX;
import static mu.semte.ch.harvesting.filtering.lib.Constants.BLANK_NODE_SUBSTITUTE;
import static mu.semte.ch.harvesting.filtering.lib.Constants.STATUS_BUSY;
import static mu.semte.ch.harvesting.filtering.lib.Constants.STATUS_FAILED;
import static mu.semte.ch.harvesting.filtering.lib.Constants.STATUS_SUCCESS;
import static mu.semte.ch.harvesting.filtering.lib.Constants.TASK_HARVESTING_FILTERING;

@Service
@Slf4j
public class FilteringService {

  private final ShaclService shaclService;
  private final SparqlQueryStore queryStore;

  @Value("${sparql.endpoint}")
  private String sparqlUrl;

  @Value("${sparql.defaultBatchSize}")
  private int defaultBatchSize;

  @Value("${share-folder.path}")
  private String shareFolderPath;

  public FilteringService(ShaclService shaclService, SparqlQueryStore queryStore) {
    this.shaclService = shaclService;
    this.queryStore = queryStore;
  }

  @Async
  public void runFilterPipeline(String deltaEntry, Map<String, String> muHeaders) {
    SparqlClient sparqlClient = SparqlClient.builder()
                                            .url(sparqlUrl)
                                            .muHeaders(muHeaders)
                                            .build();
    TaskHelper helper = TaskHelper.builder()
                                  .sparqlClient(sparqlClient)
                                  .shareFolderPath(shareFolderPath)
                                  .queryStore(queryStore).build();
    // validate task ***********************************************************************************************************
    if (!helper.isTask(deltaEntry)) return;
    var task = helper.loadTask(deltaEntry);
    if (task == null || !task.getOperation().contains(TASK_HARVESTING_FILTERING)) return;

    try {
      // fetch triples from input container ************************************************************************************
      log.info("set task status to busy...");
      helper.updateTaskStatus(task, STATUS_BUSY);

      var graphImportedTriples = helper.selectInputContainerGraph(task);

      var importedTriples = helper.loadImportedTriples(graphImportedTriples);


      // write shacl report*****************************************************************************************************
      log.info("generate validation reports...");
      var report = shaclService.validate(importedTriples.getGraph());
      log.info("triples conforms: {}", report.conforms());
      var reportModel = ModelUtils.replaceAnonNodes(report.getModel(), BLANK_NODE_SUBSTITUTE);
      var dataContainer = DataContainer.builder()
                                       .graphUri(helper.writeTtlFile(task.getGraph(), reportModel, "validation-report"))
                                       .build();
      helper.appendTaskResultFile(task, dataContainer);

      // write filtered triples*************************************************************************************************
      log.info("filter non conform triples...");
      var filteredTriples = shaclService.filter(importedTriples, report);

      dataContainer.setGraphUri(helper.writeTtlFile(task.getGraph(), filteredTriples, "filtered-triples"));
      helper.appendTaskResultFile(task, dataContainer);

      // write errored triples**************************************************************************************************
      var errorTriples = importedTriples.difference(filteredTriples);
      log.info("Number of errored triples: {}", errorTriples.size());

      dataContainer.setGraphUri(helper.writeTtlFile(task.getGraph(), errorTriples, "error-triples"));
      helper.appendTaskResultFile(task, dataContainer);

      // import filtered triples************************************************************************************************
      var filteredGraph = "%s/%s".formatted(FILTER_GRAPH_PREFIX, task.getId());

      helper.importTriples(filteredGraph, filteredTriples, defaultBatchSize);

      // import report triples**************************************************************************************************
      var validationReportGraph = "%s/%s".formatted(REPORT_GRAPH_PREFIX, task.getId());

      helper.importTriples(validationReportGraph, reportModel, defaultBatchSize);

      // append result graph****************************************************************************************************
      var graphContainer = DataContainer.builder()
                                        .graphUri(filteredGraph)
                                        .build();
      helper.appendTaskResultGraph(task, graphContainer);

      graphContainer.setGraphUri(filteredGraph);
      helper.appendTaskResultGraph(task, graphContainer);

      // Status success*********************************************************************************************************
      log.info("set task status to success...");
      helper.updateTaskStatus(task, STATUS_SUCCESS);
      log.info("Done with success for task {}", task.getId());
    }
    catch (Exception e) {
      log.error("Error while running filtering:", e);
      helper.appendTaskError(task, e.getMessage());
      helper.updateTaskStatus(task, STATUS_FAILED);
    }

  }


}
