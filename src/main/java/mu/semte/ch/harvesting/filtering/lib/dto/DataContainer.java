package mu.semte.ch.harvesting.filtering.lib.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import static mu.semte.ch.harvesting.filtering.lib.utils.ModelUtils.uuid;

@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
public class DataContainer {
  public static String DATA_CONTAINER_PREFIX = "http://redpencil.data.gift/id/dataContainers";

  @Builder.Default
  private String id = uuid();
  private String graphUri;
  private String validationGraphUri;

  public String getUri() {
    return "%s/%s".formatted(DATA_CONTAINER_PREFIX, id);
  }

}
