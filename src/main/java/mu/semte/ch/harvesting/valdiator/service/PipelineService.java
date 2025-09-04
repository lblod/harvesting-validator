package mu.semte.ch.harvesting.valdiator.service;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static mu.semte.ch.harvesting.valdiator.Constants.STATUS_BUSY;
import static mu.semte.ch.harvesting.valdiator.Constants.STATUS_FAILED;
import static mu.semte.ch.harvesting.valdiator.Constants.STATUS_SUCCESS;
import static mu.semte.ch.harvesting.valdiator.Constants.TASK_HARVESTING_FILTERING;

import java.util.Optional;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import mu.semte.ch.harvesting.valdiator.service.TaskService.TaskWithJobId;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class PipelineService {
    private final TaskService taskService;
    private final FilteringService filteringService;

    public PipelineService(TaskService taskService,
            FilteringService filteringService) {
        this.taskService = taskService;
        this.filteringService = filteringService;
    }

    public void runPipeline(String deltaEntry) {

        Thread.startVirtualThread(() -> {
            if (!taskService.isTask(deltaEntry))
                return;
            var taskWithJobId = taskService.loadTask(deltaEntry);
            var task = taskWithJobId.task();
            if (task == null || StringUtils.isEmpty(task.getOperation())) {
                log.debug("task or operation is empty for delta entry {}", deltaEntry);
                return;
            }

            Optional<Consumer<TaskWithJobId>> taskConsumer = switch (task.getOperation()) {
                case TASK_HARVESTING_FILTERING -> of(filteringService::runFilterPipeline);
                default -> empty();
            };

            taskConsumer.ifPresentOrElse(consumer -> {
                try {
                    taskService.updateTaskStatus(task, STATUS_BUSY);
                    consumer.accept(taskWithJobId);
                    taskService.updateTaskStatus(task, STATUS_SUCCESS);
                    log.debug("Done with success for task {}", task.getId());
                } catch (Throwable e) {
                    log.error("Error:", e);
                    taskService.updateTaskStatus(task, STATUS_FAILED);
                    taskService.appendTaskError(task, StringUtils.abbreviate(e.getMessage(), 100));
                }
            }, () -> log.debug("unknown operation '{}' for delta entry {}", task.getOperation(), deltaEntry));

        });
    }

}
