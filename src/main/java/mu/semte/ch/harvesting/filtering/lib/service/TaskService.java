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

import static mu.semte.ch.harvesting.filtering.lib.Constants.PREFIXES;
import static mu.semte.ch.harvesting.filtering.lib.Constants.TASK_TYPE;
import static mu.semte.ch.harvesting.filtering.utils.ModelUtils.*;

@Service
@Slf4j
public class TaskService {
    private final SparqlService sparqlService;

    public TaskService(SparqlService sparqlService) {
        this.sparqlService = sparqlService;
    }

    public boolean isTask(String subject) {
        String queryStr = """
                  %s
                        ASK {
                            GRAPH ?g {
                              <%s> a <%s>.
                            }
                        }
                """.formatted(PREFIXES, subject, TASK_TYPE);


        return sparqlService.executeAskQuery(queryStr);
    }

    public Task loadTask(String deltaEntry) {
        String queryTask = """
                %s
                SELECT DISTINCT ?graph ?task ?id ?job ?created ?modified ?status ?index ?operation ?error WHERE {
                      GRAPH ?graph {
                            BIND(<%s> as ?task)
                            ?task a %s.
                            ?task dct:isPartOf ?job;
                                      mu:uuid ?id;
                                      dct:created ?created;
                                      dct:modified ?modified;
                                      adms:status ?status;
                                      task:index ?index;
                                      task:operation ?operation.
                                      OPTIONAL { ?task task:error ?error. }
                      }
                }
                """.formatted(PREFIXES, deltaEntry, TASK_TYPE);

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
        String queryTask = """
                %s
                SELECT ?s ?p ?o WHERE {
                      GRAPH <%s> {
                           ?s ?p ?o
                      }
                }
                """.formatted(PREFIXES, graphImportedTriples);
        return sparqlService.executeSelectQuery(queryTask);
    }

    public void updateTaskStatus(Task task, String status) {
        String queryUpdate = """
                    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
                    PREFIX adms: <http://www.w3.org/ns/adms#>
                    PREFIX dct: <http://purl.org/dc/terms/>
                    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
                    DELETE {
                      GRAPH ?g {
                        ?subject adms:status ?status .
                        ?subject dct:modified ?modified.
                      }
                    }
                    INSERT {
                      GRAPH ?g {
                       ?subject adms:status <%s>.
                       ?subject dct:modified %s.
                      }
                    }
                    WHERE {
                      GRAPH ?g {
                        BIND(<%s> as ?subject)
                        ?subject adms:status ?status .
                        OPTIONAL { ?subject dct:modified ?modified. }
                      }
                    }
                """.formatted(status, escapeDateTime(new Date()), task.getTask());

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

        var queryStr = """
                          PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
                          PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
                          PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
                          PREFIX dct: <http://purl.org/dc/terms/>
                          PREFIX dbpedia: <http://dbpedia.org/ontology/>
                          INSERT DATA {
                              GRAPH <%s> {
                                  <%s> a nfo:FileDataObject;
                                  nie:dataSource <%s> ;
                                  mu:uuid %s;
                                  nfo:fileName "%s" ;
                                  dct:creator <http://lblod.data.gift/services/harvesting-filter-service>;
                                  dct:created %s;
                                  dct:modified %s;
                                  dct:format "text/turtle";
                                  nfo:fileSize %s;
                                  dbpedia:fileExtension "ttl".
                                          <%s> a nfo:FileDataObject;
                                  mu:uuid "%s";
                                  nfo:fileName "%s" ;
                                  dct:creator <http://lblod.data.gift/services/harvesting-import-service>;
                                  dct:created %s;
                                  dct:modified %s;
                                  dct:format "text/turtle";
                                  nfo:fileSize %s;
                                  dbpedia:fileExtension "ttl" .
                              }
                          }
                """.formatted(graph, physicalFile, logicalFile, phyId, phyFilename, now, now, fileSize, logicalFile, loId, logicalFileName, now, now, fileSize);

        sparqlService.executeUpdateQuery(queryStr);
        return logicalFile;
    }

    public void appendTaskResultFile(Task task, String containerUri, String containerId, String fileUri) {
        var queryStr = """
                      PREFIX dct: <http://purl.org/dc/terms/>
                      PREFIX task: <http://redpencil.data.gift/vocabularies/tasks/>
                      PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
                      PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
                      INSERT DATA {
                          GRAPH <%s> {
                              <%s> a nfo:DataContainer.
                                      <%s> mu:uuid "%s".
                              <%s> task:hasFile <%s>

                              <%s> task:resultsContainer <%s>.
                          }
                      }
                """.formatted(task.getGraph(), containerUri, containerUri, containerId, containerUri, fileUri, task.getTask(), containerUri);

        sparqlService.executeUpdateQuery(queryStr);


    }

    public void appendTaskResultGraph(Task task, String graphContainerUri, String graphContainerId, String filteredGraph) {
        var queryStr = """
                PREFIX dct: <http://purl.org/dc/terms/>
                PREFIX task: <http://redpencil.data.gift/vocabularies/tasks/>
                PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
                PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
                INSERT DATA {
                    GRAPH <%s> {
                        <%s> a nfo:DataContainer.
                        <%s> mu:uuid "%s".
                        <%s> task:hasGraph <%s>.

                        <%s> task:resultsContainer <%s>
                    }
                }
                          """.formatted(task.getGraph(), graphContainerUri, graphContainerUri, graphContainerId, graphContainerUri, filteredGraph, task.getTask(), graphContainerUri);

        sparqlService.executeUpdateQuery(queryStr);

    }
}
