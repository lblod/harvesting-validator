package mu.semte.ch.harvesting.filtering;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@EnableAsync
public class HarvestingFilteringServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(HarvestingFilteringServiceApplication.class, args);
    }

}
