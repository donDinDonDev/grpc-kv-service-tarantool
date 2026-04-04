package io.kvservice;

import static org.assertj.core.api.Assertions.assertThat;

import io.kvservice.config.KvServiceProperties;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.util.unit.DataSize;

@SpringBootTest(properties = "spring.grpc.server.port=0")
class KvServiceApplicationTests {

    @Autowired
    private KvServiceProperties properties;

    @Test
    void contextLoadsWithNormalizedBootstrapDefaults() {
        assertThat(this.properties.getLimits().getMaxKeyBytes()).isEqualTo(DataSize.ofBytes(256));
        assertThat(this.properties.getLimits().getMaxValueBytes()).isEqualTo(DataSize.ofMegabytes(1));
        assertThat(this.properties.getDeadlines().getUnary()).hasSeconds(3);
        assertThat(this.properties.getDeadlines().getCount()).hasSeconds(15);
        assertThat(this.properties.getRange().getBatchSize()).isEqualTo(512);
        assertThat(this.properties.getRange().getMaxActiveStreams()).isEqualTo(16);
    }
}
