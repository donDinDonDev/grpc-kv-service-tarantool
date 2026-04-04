package io.kvservice;

import static org.assertj.core.api.Assertions.assertThat;

import io.kvservice.config.KvServiceProperties;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.util.unit.DataSize;

@SpringBootTest(properties = {
        "spring.grpc.server.port=0",
        "kvservice.tarantool.enabled=false",
        "management.endpoint.health.group.readiness.include=readinessState"
})
class KvServiceApplicationTests {

    @Autowired
    private KvServiceProperties properties;

    @Test
    void contextLoadsWithNormalizedBootstrapDefaults() {
        assertThat(this.properties.getGrpc().getMaxRequestBytes()).isEqualTo(DataSize.ofMegabytes(4));
        assertThat(this.properties.getGrpc().getMaxResponseBytes()).isEqualTo(DataSize.ofMegabytes(4));
        assertThat(this.properties.getLimits().getMaxKeyBytes()).isEqualTo(DataSize.ofBytes(256));
        assertThat(this.properties.getLimits().getMaxValueBytes()).isEqualTo(DataSize.ofMegabytes(1));
        assertThat(this.properties.getDeadlines().getUnary()).hasSeconds(3);
        assertThat(this.properties.getDeadlines().getCount()).hasSeconds(15);
        assertThat(this.properties.getRange().getBatchSize()).isEqualTo(512);
        assertThat(this.properties.getRange().getMaxActiveStreams()).isEqualTo(16);
        assertThat(this.properties.getTarantool().isEnabled()).isFalse();
        assertThat(this.properties.getTarantool().getHost()).isEqualTo("127.0.0.1");
        assertThat(this.properties.getTarantool().getPort()).isEqualTo(3301);
        assertThat(this.properties.getTarantool().getUsername()).isEqualTo("kvservice");
        assertThat(this.properties.getTarantool().getConnectTimeout()).hasSeconds(3);
        assertThat(this.properties.getTarantool().getReconnectAfter()).hasSeconds(1);
        assertThat(this.properties.getTarantool().getRequestTimeout()).hasSeconds(5);
        assertThat(this.properties.getTarantool().getInit().isEnabled()).isTrue();
        assertThat(this.properties.getTarantool().getInit().getEngine()).isEqualTo("vinyl");
    }
}
