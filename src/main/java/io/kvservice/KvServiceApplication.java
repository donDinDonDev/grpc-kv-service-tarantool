package io.kvservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class KvServiceApplication {

  public static void main(String[] args) {
    SpringApplication.run(KvServiceApplication.class, args);
  }
}
