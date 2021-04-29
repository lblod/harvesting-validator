package mu.semte.ch.harvesting.valdiator.service;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import mu.semte.ch.lib.utils.ModelUtils;
import org.apache.commons.io.IOUtils;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.shacl.ShaclValidator;
import org.apache.jena.shacl.Shapes;
import org.apache.jena.shacl.ValidationReport;
import org.apache.jena.shacl.engine.ShaclPaths;
import org.apache.jena.vocabulary.RDF;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;


@Service
@Slf4j
public class ShaclService {
  private final Shapes applicationProfile;

  public ShaclService(Shapes applicationProfile) {
    this.applicationProfile = applicationProfile;
  }

  public ValidationReport validate(Graph dataGraph) {
    return validate(dataGraph, applicationProfile);
  }

  public ValidationReport validate(Graph dataGraph, Shapes shapes) {
    return ShaclValidator.get().validate(shapes, dataGraph);
  }

  public Graph filter(InputStream dataModel, Lang modelLang, InputStream shapesModel, Lang shapesLang) {
    Graph dataGraph = ModelUtils.toModel(dataModel, modelLang).getGraph();
    Graph shapesGraph = ModelUtils.toModel(shapesModel, shapesLang).getGraph();
    Shapes shapes = Shapes.parse(shapesGraph);
    ValidationReport report = validate(dataGraph, shapes);
    return filter(dataGraph, shapes, report);
  }

  public ValidationReport fromModel(Model report) {
    return ValidationReport.fromModel(report);
  }

  public Model filter(Model model, ValidationReport report) {
    Model copy = ModelFactory.createDefaultModel();
    copy.add(model);
    Graph dataGraph = copy.getGraph();
    return ModelFactory.createModelForGraph(filter(dataGraph, applicationProfile, report));
  }

  public Model filter(Model model) {
    Graph dataGraph = model.getGraph();
    ValidationReport report = validate(dataGraph);
    return filter(model, report);
  }

  public Graph filter(InputStream dataModel, Lang modelLang) {
    Graph dataGraph = ModelUtils.toModel(dataModel, modelLang).getGraph();
    ValidationReport report = validate(dataGraph);
    return filter(dataGraph, applicationProfile, report);
  }

  public Graph filter(Graph dataGraph, Shapes shapes, ValidationReport report) {
    List<String> targetClasses = shapes
            .getTargetShapes()
            .stream()
            .flatMap(s -> s.getTargets().stream().map(t -> t.getObject().getURI()))
            .collect(Collectors.toList());

    report.getEntries().forEach(r -> dataGraph.remove(r.focusNode(), ShaclPaths.pathNode(r.resultPath()), null));

    // filter the classes not defined as target shapes
    List<String> classesNotDefinedAsTargetShapes = dataGraph
            .find(null, RDF.type.asNode(), null)
            .filterDrop(triple -> targetClasses.contains(triple.getObject().getURI()))
            .mapWith(triple -> triple.getSubject().getURI()).toList();

    classesNotDefinedAsTargetShapes.forEach(sub -> dataGraph.remove(NodeFactory.createURI(sub), null, null));

    return dataGraph;
  }

  @SneakyThrows
  public Graph filter(String dataModel, Lang modelLang) {
    return filter(IOUtils.toInputStream(dataModel, StandardCharsets.UTF_8), modelLang);
  }

  @SneakyThrows
  public Graph filter(MultipartFile dataModel) {
    return filter(dataModel.getInputStream(), ModelUtils.filenameToLang(dataModel.getOriginalFilename()));
  }

  @SneakyThrows
  public Graph filter(MultipartFile dataModel, MultipartFile shapesFile) {
    return filter(dataModel.getInputStream(), ModelUtils.filenameToLang(dataModel.getOriginalFilename()),
                  shapesFile.getInputStream(), ModelUtils.filenameToLang(shapesFile.getOriginalFilename()));
  }

}
