package mu.semte.ch.harvesting.filtering.lib.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
public class Task {
    private String graph;
    private String task;
    private String id;
    private String job;
    private String created;
    private String modified;
    private String status;
    private String index;
    private String operation;
    private String error;
}
