package mu.semte.ch.harvesting.valdiator.service;

import lombok.extern.slf4j.Slf4j;
import mu.semte.ch.lib.dto.Task;
import org.apache.commons.lang3.StringUtils;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.Optional;
import java.util.function.Consumer;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static mu.semte.ch.harvesting.valdiator.Constants.STATUS_BUSY;
import static mu.semte.ch.harvesting.valdiator.Constants.STATUS_FAILED;
import static mu.semte.ch.harvesting.valdiator.Constants.STATUS_SUCCESS;
import static mu.semte.ch.harvesting.valdiator.Constants.TASK_HARVESTING_FILTERING;
import static mu.semte.ch.harvesting.valdiator.Constants.TASK_HARVESTING_VALIDATING;

@Service
@Slf4j
public class PipelineService {
  private final TaskService taskService;
  private final FilteringService filteringService;
  private final ValidatingService validatingService;

  public PipelineService(TaskService taskService,
                         FilteringService filteringService,
                         ValidatingService validatingService) {
    this.taskService = taskService;
    this.filteringService = filteringService;
    this.validatingService = validatingService;
  }


  @Async
  public void runPipeline(String deltaEntry) {

    if (!taskService.isTask(deltaEntry)) return;
    var task = taskService.loadTask(deltaEntry);

    if (task == null || StringUtils.isEmpty(task.getOperation())) {
      log.debug("task or operation is empty for delta entry {}", deltaEntry);
      return;
    }

    Optional<Consumer<Task>> taskConsumer = switch (task.getOperation()) {
      case TASK_HARVESTING_FILTERING -> of(filteringService::runFilterPipeline);
      case TASK_HARVESTING_VALIDATING -> of(validatingService::runValidatePipeline);
      default -> empty();
    };

    taskConsumer.ifPresentOrElse(consumer -> {
      try {
        taskService.updateTaskStatus(task, STATUS_BUSY);
        consumer.accept(task);
        taskService.updateTaskStatus(task, STATUS_SUCCESS);
        log.debug("Done with success for task {}", task.getId());
      }
      catch (Throwable e) {
        log.error("Error:", e);
        taskService.updateTaskStatus(task, STATUS_FAILED);
        taskService.appendTaskError(task, e.getMessage());
      }
    }, () -> log.debug("unknown operation '{}' for delta entry {}", task.getOperation(), deltaEntry));


  }


}
