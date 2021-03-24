package mu.semte.ch.harvesting.valdiator.lib.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import mu.semte.ch.harvesting.valdiator.lib.utils.ModelUtils;

@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
public class DataContainer {
  public static String DATA_CONTAINER_PREFIX = "http://redpencil.data.gift/id/dataContainers";

  @Builder.Default
  private String id = ModelUtils.uuid();
  private String graphUri;
  private String validationGraphUri;

  public String getUri() {
    return "%s/%s".formatted(DATA_CONTAINER_PREFIX, id);
  }

}
