package mu.semte.ch.harvesting.filtering.lib;

public interface Constants {
    String TASK_HARVESTING_FILTERING = "http://lblod.data.gift/id/jobs/concept/TaskOperation/filtering";

    String SUBJECT_STATUS = "http://www.w3.org/ns/adms#status";
    String STATUS_BUSY = "http://redpencil.data.gift/id/concept/JobStatus/busy";
    String STATUS_SCHEDULED = "http://redpencil.data.gift/id/concept/JobStatus/scheduled";
    String STATUS_SUCCESS = "http://redpencil.data.gift/id/concept/JobStatus/success";
    String STATUS_FAILED = "http://redpencil.data.gift/id/concept/JobStatus/failed";

    String JOB_TYPE = "http://vocab.deri.ie/cogs#Job";
    String TASK_TYPE = "http://redpencil.data.gift/vocabularies/tasks/Task";
    String ERROR_TYPE = "http://open-services.net/ns/core#Error";
    String ERROR_URI_PREFIX = "http://redpencil.data.gift/id/jobs/error/";

    String PREFIXES = """
                  PREFIX harvesting: <http://lblod.data.gift/vocabularies/harvesting/>
                  PREFIX terms: <http://purl.org/dc/terms/>
                  PREFIX prov: <http://www.w3.org/ns/prov#>
                  PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
                  PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
                  PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
                  PREFIX task: <http://redpencil.data.gift/vocabularies/tasks/>
                  PREFIX dct: <http://purl.org/dc/terms/>
                  PREFIX oslc: <http://open-services.net/ns/core#>
                  PREFIX cogs: <http://vocab.deri.ie/cogs#>
                  PREFIX adms: <http://www.w3.org/ns/adms#>
            """;

}
