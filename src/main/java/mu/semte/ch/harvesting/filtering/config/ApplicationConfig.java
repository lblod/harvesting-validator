package mu.semte.ch.harvesting.filtering.config;

import lombok.extern.slf4j.Slf4j;
import mu.semte.ch.harvesting.filtering.lib.utils.SparqlClient;
import mu.semte.ch.harvesting.filtering.lib.utils.SparqlQueryStore;
import org.apache.commons.io.IOUtils;
import org.apache.jena.graph.Graph;
import org.apache.jena.riot.Lang;
import org.apache.jena.shacl.Shapes;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

import static com.github.jsonldjava.shaded.com.google.common.collect.Maps.immutableEntry;
import static java.nio.charset.StandardCharsets.UTF_8;
import static mu.semte.ch.harvesting.filtering.Constants.HEADER_MU_AUTH_SUDO;
import static mu.semte.ch.harvesting.filtering.lib.utils.ModelUtils.filenameToLang;
import static mu.semte.ch.harvesting.filtering.lib.utils.ModelUtils.toModel;
import static org.apache.commons.io.FilenameUtils.removeExtension;
import static org.apache.commons.text.CaseUtils.toCamelCase;

@Configuration
@Slf4j
public class ApplicationConfig {
  @Value("${application-profile.default}")
  private Resource applicationProfile;

  @Value("classpath:queries/*.sparql")
  private Resource[] queries;

  @Value("${sparql.endpoint}")
  private String sparqlUrl;

  @Bean
  public Shapes defaultApplicationProfile() throws IOException {
    Graph shapesGraph = toModel(applicationProfile.getInputStream(),
                                filenameToLang(applicationProfile.getFilename(), Lang.TURTLE)).getGraph();
    return Shapes.parse(shapesGraph);
  }

  @Bean
  public SparqlQueryStore sparqlQueryLoader() {
    var queriesMap = Arrays.stream(queries)
                           .map(r -> {
                             try {
                               var key = toCamelCase(removeExtension(r.getFilename()), false, '-');
                               return immutableEntry(key, IOUtils.toString(r.getInputStream(), UTF_8));
                             }
                             catch (IOException e) {
                               throw new RuntimeException(e);
                             }
                           })
                           .peek(e -> log.info("query {} added to the store", e.getKey()))
                           .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    return queriesMap::get;
  }

  @Bean
  public SparqlClient defaultSudoSparqlClient(){
    return SparqlClient.builder()
                .url(sparqlUrl)
                .httpHeaders(Map.of(HEADER_MU_AUTH_SUDO, "true"))
                .build();
  }


}
