package mu.semte.ch.harvesting.valdiator.config;

import lombok.extern.slf4j.Slf4j;
import mu.semte.ch.lib.config.CoreConfig;
import org.apache.jena.graph.Graph;
import org.apache.jena.riot.Lang;
import org.apache.jena.shacl.Shapes;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.io.Resource;

import java.io.IOException;

import static mu.semte.ch.lib.utils.ModelUtils.filenameToLang;
import static mu.semte.ch.lib.utils.ModelUtils.toModel;

@Configuration
@Import(CoreConfig.class)
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
