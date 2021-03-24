package mu.semte.ch.harvesting.valdiator.lib.config;

import lombok.extern.slf4j.Slf4j;
import mu.semte.ch.harvesting.valdiator.lib.utils.SparqlClient;
import mu.semte.ch.harvesting.valdiator.lib.utils.SparqlQueryStore;
import org.apache.commons.io.IOUtils;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.github.jsonldjava.shaded.com.google.common.collect.Maps.immutableEntry;
import static java.nio.charset.StandardCharsets.UTF_8;
import static mu.semte.ch.harvesting.valdiator.Constants.HEADER_MU_AUTH_SUDO;
import static org.apache.commons.io.FilenameUtils.removeExtension;
import static org.apache.commons.text.CaseUtils.toCamelCase;

@Configuration
@Slf4j
public class SparqlConfig {

  @Value("classpath:sparql/*.sparql")
  private Resource[] queries;

  @Value("${sparql.endpoint}")
  private String sparqlUrl;

  @Bean
  public SparqlQueryStore sparqlQueryLoader() {
    log.info("Adding {} queries to the store", queries.length);

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
  @Autowired
  public SparqlClient defaultSudoSparqlClient(CloseableHttpClient closeableHttpClient) {
    return SparqlClient.builder()
                       .url(sparqlUrl)
                       .httpClient(closeableHttpClient)
                       .build();
  }

  @Bean(destroyMethod = "close")
  public CloseableHttpClient buildHttpClient() {
    return HttpClients.custom()
                      .setDefaultHeaders(List.of(new BasicHeader(HEADER_MU_AUTH_SUDO, "true")))
                      .build();

  }

}
