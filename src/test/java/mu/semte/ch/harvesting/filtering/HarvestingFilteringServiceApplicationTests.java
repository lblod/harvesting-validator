package mu.semte.ch.harvesting.filtering;

import mu.semte.ch.harvesting.filtering.lib.utils.SparqlQueryStore;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.platform.commons.util.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class HarvestingFilteringServiceApplicationTests {

  @Autowired
  private SparqlQueryStore queryStore;

  @Test
  void contextLoads() {
    Assertions.assertTrue(StringUtils.isNotBlank(queryStore.getQuery("isTask")));
    Assertions.assertTrue(StringUtils.isNotBlank(queryStore.getQuery("loadTask")));
    Assertions.assertTrue(StringUtils.isNotBlank(queryStore.getQuery("loadImportedTriples")));
    Assertions.assertTrue(StringUtils.isNotBlank(queryStore.getQuery("updateTaskStatus")));
    Assertions.assertTrue(StringUtils.isNotBlank(queryStore.getQuery("writeTtlFile")));
    Assertions.assertTrue(StringUtils.isNotBlank(queryStore.getQuery("appendTaskResultFile")));
    Assertions.assertTrue(StringUtils.isNotBlank(queryStore.getQuery("appendTaskResultGraph")));
  }

}
