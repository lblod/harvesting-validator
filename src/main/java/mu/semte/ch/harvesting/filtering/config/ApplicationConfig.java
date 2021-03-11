package mu.semte.ch.harvesting.filtering.config;

import org.apache.jena.graph.Graph;
import org.apache.jena.riot.Lang;
import org.apache.jena.shacl.Shapes;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;

import java.io.IOException;

import static mu.semte.ch.harvesting.filtering.utils.ModelUtils.filenameToLang;
import static mu.semte.ch.harvesting.filtering.utils.ModelUtils.toModel;


@Configuration
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
