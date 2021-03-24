package mu.semte.ch.harvesting.valdiator.config;

import lombok.extern.slf4j.Slf4j;
import mu.semte.ch.harvesting.valdiator.lib.config.SparqlConfig;
import mu.semte.ch.harvesting.valdiator.lib.utils.SparqlClient;
import mu.semte.ch.harvesting.valdiator.lib.utils.SparqlQueryStore;
import org.apache.commons.io.IOUtils;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;
import org.apache.jena.graph.Graph;
import org.apache.jena.riot.Lang;
import org.apache.jena.shacl.Shapes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.io.Resource;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.github.jsonldjava.shaded.com.google.common.collect.Maps.immutableEntry;
import static java.nio.charset.StandardCharsets.UTF_8;
import static mu.semte.ch.harvesting.valdiator.Constants.HEADER_MU_AUTH_SUDO;
import static mu.semte.ch.harvesting.valdiator.lib.utils.ModelUtils.filenameToLang;
import static mu.semte.ch.harvesting.valdiator.lib.utils.ModelUtils.toModel;
import static org.apache.commons.io.FilenameUtils.removeExtension;
import static org.apache.commons.text.CaseUtils.toCamelCase;

@Configuration
// TODO if the lib package is extracted to make some kind of mu-java-template, you may want to uncomment this
//@Import(SparqlConfig.class)
@Slf4j
public class ApplicationConfig {
  @Value("${application-profile.default}")
  private Resource applicationProfile;

  @Bean
  public Shapes defaultApplicationProfile() throws IOException {
    Graph shapesGraph = toModel(applicationProfile.getInputStream(),
                                filenameToLang(applicationProfile.getFilename(), Lang.TURTLE)).getGraph();
    return Shapes.parse(shapesGraph);
  }

}
