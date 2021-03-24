package mu.semte.ch.harvesting.valdiator.lib.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.stream.Collectors;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Delta {
  private List<Triple> inserts;
  private List<Triple> deletes;

  public List<String> getInsertsFor(String predicate, String object) {
    return this.inserts
            .stream()
            .filter(t -> predicate.equals(t.getPredicate().getValue()) && object.equals(t.getObject().getValue()))
            .map(t -> t.getSubject().getValue())
            .collect(Collectors.toList());
  }

}
