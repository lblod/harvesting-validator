package mu.semte.ch.harvesting.valdiator.rest;

import static mu.semte.ch.harvesting.valdiator.Constants.STATUS_SCHEDULED;
import static mu.semte.ch.harvesting.valdiator.Constants.SUBJECT_STATUS;

import java.util.List;
import lombok.extern.slf4j.Slf4j;
import mu.semte.ch.harvesting.valdiator.service.PipelineService;
import mu.semte.ch.lib.dto.Delta;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import jakarta.servlet.http.HttpServletRequest;

@RestController
@Slf4j
public class AppController {

    private final PipelineService pipelineService;

    public AppController(PipelineService pipelineService) {
        this.pipelineService = pipelineService;
    }

    @PostMapping("/delta")
    public ResponseEntity<Void> delta(@RequestBody List<Delta> deltas,
            HttpServletRequest request) {
        var entries = deltas.stream()
                .map(delta -> delta.getInsertsFor(SUBJECT_STATUS, STATUS_SCHEDULED))
                .flatMap(delta-> delta.stream())
                .toList();

        if (entries.isEmpty()) {
            log.warn(
                    "Delta did not contain potential tasks that are ready for filtering, awaiting the next batch!");
            return ResponseEntity.noContent().build();
        }

        // NOTE: we don't wait as we do not want to keep hold off the connection.
        entries.forEach(pipelineService::runPipeline);

        return ResponseEntity.ok().build();
    }

    @PostMapping("/retry")
    public ResponseEntity<Void> retry(@RequestParam("taskUri") String taskUri) {
        pipelineService.runPipeline(taskUri);
        return ResponseEntity.ok().build();
    }
}
