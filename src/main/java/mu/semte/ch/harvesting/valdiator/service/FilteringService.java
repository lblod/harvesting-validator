package mu.semte.ch.harvesting.valdiator.service;

import java.util.ArrayList;
import java.util.Map;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import mu.semte.ch.lib.dto.DataContainer;
import mu.semte.ch.lib.dto.Task;
import mu.semte.ch.lib.shacl.ShaclService;
import mu.semte.ch.lib.utils.ModelUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.shacl.ValidationReport;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class FilteringService {

        private final ShaclService shaclService;
        private final TaskService taskService;
        @Value("${sparql.defaultLimitSize}")
        private int defaultLimitSize;

        public FilteringService(ShaclService shaclService, TaskService taskService) {
                this.shaclService = shaclService;
                this.taskService = taskService;
        }

        @SneakyThrows
        public void runFilterPipeline(Task task) {
                var inputContainer = taskService.selectInputContainer(task).get(0);
                log.debug("input container: {}", inputContainer);
                var countTriples = taskService.countTriplesFromFileInputContainer(
                                inputContainer.getGraphUri());
                var pagesCount = countTriples > defaultLimitSize ? countTriples / defaultLimitSize : 1;

                var fileContainer = DataContainer.builder().build();
                var graphContainer = DataContainer.builder().build();
                var resultContainer = DataContainer.builder().graphUri(graphContainer.getUri()).build();

                var sleepMs = 50;
                for (var i = 0; i <= pagesCount; i++) {
                        var threads = new ArrayList<Thread>();
                        var offset = i * defaultLimitSize;
                        var importedTriples = taskService.fetchTripleFromFileInputContainer(
                                        inputContainer.getGraphUri(), defaultLimitSize, offset);
                        for (var mdb : importedTriples) {
                                threads.add(Thread.startVirtualThread(() -> {
                                        // var report = taskService.fetchValidationGraphByDerivedFrom(
                                        // inputContainer.getValidationGraphUri(),
                                        // mdb.derivedFrom());

                                        log.info("generate validation reports...");
                                        var report = shaclService.validate(mdb.model().getGraph());
                                        log.info("triples conforms: {}", report.conforms());

                                        var validTriples = writeValidTriples(task, fileContainer, report, mdb);

                                        var filteredGraph = validTriples.getKey().getGraphUri();

                                        taskService.appendTaskResultFile(
                                                        task,
                                                        graphContainer.toBuilder().graphUri(filteredGraph).build());
                                        if (!report.conforms()) {
                                                var reportModel = ModelUtils.replaceAnonNodes(report.getModel());
                                                writeReport(task, fileContainer, reportModel, mdb.derivedFrom());
                                                writeErrorTriples(task, fileContainer, mdb.model(),
                                                                validTriples.getValue(), mdb.derivedFrom());
                                        }
                                }));
                                try {
                                        log.info("sleep for {} ms to let virtuoso breathe", sleepMs);
                                        Thread.sleep(sleepMs);
                                } catch (Throwable e) {
                                        log.error("could not sleep", e);
                                }
                        }
                        for (var thread : threads) {
                                thread.join();
                        }
                }

                taskService.appendTaskResultGraph(task, resultContainer);
        }

        private void writeErrorTriples(Task task, DataContainer fileContainer,
                        Model importedTriples, Model validTriples,
                        String derivedFrom) {
                var errorTriples = importedTriples.difference(validTriples);
                log.debug("Number of errored triples: {}", errorTriples.size());
                var dataContainer = fileContainer.toBuilder()
                                .graphUri(taskService.writeTtlFile(
                                                task.getGraph(), new ModelByDerived(derivedFrom, errorTriples),
                                                "error-triples.ttl"))

                                .build();
                taskService.appendTaskResultFile(task, dataContainer);
        }

        private void writeReport(Task task, DataContainer fileContainer, Model report,
                        String derivedFrom) {
                var dataContainer = fileContainer.toBuilder()
                                .graphUri(taskService.writeTtlFile(
                                                task.getGraph(), new ModelByDerived(derivedFrom, report),
                                                "validation-report.ttl"))

                                .build();
                taskService.appendTaskResultFile(task, dataContainer);
        }

        private Map.Entry<DataContainer, Model> writeValidTriples(Task task, DataContainer fileContainer,
                        ValidationReport report, ModelByDerived importedTriples) {
                log.debug("filter non conform triples...");
                var validTriples = shaclService.filter(importedTriples.model(), report);
                var dataContainer = fileContainer.toBuilder()
                                .graphUri(taskService.writeTtlFile(
                                                task.getGraph(),
                                                new ModelByDerived(importedTriples.derivedFrom(), validTriples),
                                                "valid-triples.ttl"))
                                .build();
                taskService.appendTaskResultFile(task, dataContainer);
                return Map.entry(dataContainer, validTriples);
        }
}
