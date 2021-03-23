package mu.semte.ch.harvesting.filtering.lib.utils;

import freemarker.template.Configuration;
import freemarker.template.Template;
import lombok.SneakyThrows;

import java.io.StringReader;
import java.util.Map;

import static org.springframework.ui.freemarker.FreeMarkerTemplateUtils.processTemplateIntoString;

public interface SparqlQueryStore {
  String getQuery(String queryName);

  @SneakyThrows
  default String getQueryWithParameters(String queryName, Map<String, Object> parameters) {
    String query = getQuery(queryName);
    Template template = new Template("name", new StringReader(query),
                                     new Configuration(Configuration.VERSION_2_3_30));
    return processTemplateIntoString(template, parameters);
  }
}
