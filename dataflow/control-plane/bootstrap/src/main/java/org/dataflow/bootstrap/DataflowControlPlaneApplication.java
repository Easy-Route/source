package org.dataflow.bootstrap;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@ComponentScan(basePackages = {
        "org.dataflow.api",
        "org.dataflow.application",
        "org.dataflow.bootstrap",
        "org.dataflow.infrastructure",
        "org.dataflow.observer",
        "org.dataflow.reconciler",
        "org.dataflow.validator"
})
@EntityScan(basePackages = "org.dataflow.domain")
public class DataflowControlPlaneApplication {
    public static void main(String[] args) {
        SpringApplication.run(DataflowControlPlaneApplication.class, args);
    }
}
