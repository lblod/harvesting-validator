package mu.semte.ch.harvesting.filtering.rest;

import lombok.extern.slf4j.Slf4j;
import mu.semte.ch.harvesting.filtering.lib.service.FilteringService;
import mu.semte.ch.harvesting.filtering.lib.dto.Delta;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import static mu.semte.ch.harvesting.filtering.lib.Constants.STATUS_SCHEDULED;
import static mu.semte.ch.harvesting.filtering.lib.Constants.SUBJECT_STATUS;

@RestController
@Slf4j
public class AppController {

    private final FilteringService filteringService;

    public AppController(FilteringService filteringService) {
        this.filteringService = filteringService;
    }

    @PostMapping("/delta")
    public ResponseEntity<Void> delta(@RequestBody Delta delta) {

        var entries = delta.getInsertsFor(SUBJECT_STATUS, STATUS_SCHEDULED);

        if (entries.isEmpty()) {
            log.error("Delta dit not contain potential tasks that are ready for filtering, awaiting the next batch!");
            return ResponseEntity.noContent().build();
        }

        // NOTE: we don't wait as we do not want to keep hold off the connection.
        entries.forEach(filteringService::runFilterPipeline);

        return ResponseEntity.ok().build();
    }
}
