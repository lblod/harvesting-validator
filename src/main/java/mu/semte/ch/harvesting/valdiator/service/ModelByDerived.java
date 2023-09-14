package mu.semte.ch.harvesting.valdiator.service;

import org.apache.jena.rdf.model.Model;

public record ModelByDerived(String derivedFrom, Model model) {
}
