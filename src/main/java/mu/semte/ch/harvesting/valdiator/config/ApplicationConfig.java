package mu.semte.ch.harvesting.valdiator.config;

import mu.semte.ch.lib.handler.DefaultExceptionHandler;
import mu.semte.ch.lib.shacl.ShaclService;
import mu.semte.ch.lib.utils.SparqlClient;

import org.apache.jena.graph.Graph;
import org.apache.jena.riot.Lang;
import org.apache.jena.shacl.Shapes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.io.Resource;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import mu.semte.ch.lib.utils.SparqlQueryStore;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.text.CaseUtils;

import com.github.jsonldjava.shaded.com.google.common.collect.Maps;
import static mu.semte.ch.lib.utils.ModelUtils.filenameToLang;
import static mu.semte.ch.lib.utils.ModelUtils.toModel;

@Configuration
@Slf4j
@Import({ SparqlClient.class, DefaultExceptionHandler.class })
public class ApplicationConfig {
    @Value("${shacl.application-profile.default}")
    private Resource applicationProfile;
    @Value("${shacl.strictModeFiltering}")
    private boolean strictModeFiltering;

    @Value("${sparql.queryStore.path:classpath*:sparql}/*.sparql")
    private Resource[] queries;

    @Bean
    public Shapes defaultApplicationProfile() throws IOException {
        Graph shapesGraph = toModel(applicationProfile.getInputStream(),
                filenameToLang(applicationProfile.getFilename(), Lang.TURTLE)).getGraph();
        return Shapes.parse(shapesGraph);

    }

    @Bean
    public ShaclService shaclService(@Autowired Shapes defaultApplicationProfile) throws IOException {
        return new ShaclService(defaultApplicationProfile, strictModeFiltering);

    }

    @Bean
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public SparqlQueryStore sparqlQueryLoader() {
        log.info("Adding {} queries to the store", this.queries.length);
        Map<String, String> queriesMap = (Map) Arrays.stream(this.queries).map((r) -> {
            try {
                String key = CaseUtils.toCamelCase(FilenameUtils.removeExtension(r.getFilename()), false,
                        new char[] { '-' });
                return Maps.immutableEntry(key, IOUtils.toString(r.getInputStream(), StandardCharsets.UTF_8));
            } catch (IOException var2) {
                throw new RuntimeException(var2);
            }
        }).peek((e) -> {
            log.info("query {} added to the store", e.getKey());
        }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        return () -> {
            return queriesMap;
        };
    }

}
