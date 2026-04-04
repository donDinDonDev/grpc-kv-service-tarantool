package io.kvservice.config;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.util.unit.DataSize;

@ConfigurationProperties(prefix = "kvservice")
public class KvServiceProperties {

    private final Grpc grpc = new Grpc();
    private final Limits limits = new Limits();
    private final Deadlines deadlines = new Deadlines();
    private final Range range = new Range();
    private final Tarantool tarantool = new Tarantool();

    public Grpc getGrpc() {
        return this.grpc;
    }

    public Limits getLimits() {
        return this.limits;
    }

    public Deadlines getDeadlines() {
        return this.deadlines;
    }

    public Range getRange() {
        return this.range;
    }

    public Tarantool getTarantool() {
        return this.tarantool;
    }

    public static final class Grpc {

        private DataSize maxRequestBytes = DataSize.ofMegabytes(4);

        private DataSize maxResponseBytes = DataSize.ofMegabytes(4);

        public DataSize getMaxRequestBytes() {
            return this.maxRequestBytes;
        }

        public void setMaxRequestBytes(DataSize maxRequestBytes) {
            this.maxRequestBytes = maxRequestBytes;
        }

        public DataSize getMaxResponseBytes() {
            return this.maxResponseBytes;
        }

        public void setMaxResponseBytes(DataSize maxResponseBytes) {
            this.maxResponseBytes = maxResponseBytes;
        }
    }

    public static final class Limits {

        private DataSize maxKeyBytes = DataSize.ofBytes(256);

        private DataSize maxValueBytes = DataSize.ofMegabytes(1);

        public DataSize getMaxKeyBytes() {
            return this.maxKeyBytes;
        }

        public void setMaxKeyBytes(DataSize maxKeyBytes) {
            this.maxKeyBytes = maxKeyBytes;
        }

        public DataSize getMaxValueBytes() {
            return this.maxValueBytes;
        }

        public void setMaxValueBytes(DataSize maxValueBytes) {
            this.maxValueBytes = maxValueBytes;
        }
    }

    public static final class Deadlines {

        private Duration unary = Duration.ofSeconds(3);

        private Duration count = Duration.ofSeconds(15);

        public Duration getUnary() {
            return this.unary;
        }

        public void setUnary(Duration unary) {
            this.unary = unary;
        }

        public Duration getCount() {
            return this.count;
        }

        public void setCount(Duration count) {
            this.count = count;
        }
    }

    public static final class Range {

        private int batchSize = 512;

        private int maxActiveStreams = 16;

        public int getBatchSize() {
            return this.batchSize;
        }

        public void setBatchSize(int batchSize) {
            this.batchSize = batchSize;
        }

        public int getMaxActiveStreams() {
            return this.maxActiveStreams;
        }

        public void setMaxActiveStreams(int maxActiveStreams) {
            this.maxActiveStreams = maxActiveStreams;
        }
    }

    public static final class Tarantool {

        private boolean enabled = true;

        private String host = "127.0.0.1";

        private int port = 3301;

        private String username = "kvservice";

        private String password = "kvservice";

        private Duration connectTimeout = Duration.ofSeconds(3);

        private Duration reconnectAfter = Duration.ofSeconds(1);

        private Duration requestTimeout = Duration.ofSeconds(5);

        private final Init init = new Init();

        public boolean isEnabled() {
            return this.enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public String getHost() {
            return this.host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public int getPort() {
            return this.port;
        }

        public void setPort(int port) {
            this.port = port;
        }

        public String getUsername() {
            return this.username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public String getPassword() {
            return this.password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public Duration getConnectTimeout() {
            return this.connectTimeout;
        }

        public void setConnectTimeout(Duration connectTimeout) {
            this.connectTimeout = connectTimeout;
        }

        public Duration getReconnectAfter() {
            return this.reconnectAfter;
        }

        public void setReconnectAfter(Duration reconnectAfter) {
            this.reconnectAfter = reconnectAfter;
        }

        public Duration getRequestTimeout() {
            return this.requestTimeout;
        }

        public void setRequestTimeout(Duration requestTimeout) {
            this.requestTimeout = requestTimeout;
        }

        public Init getInit() {
            return this.init;
        }

        public static final class Init {

            private boolean enabled = true;

            private String engine = "vinyl";

            private Resource scriptLocation = new ClassPathResource("tarantool/kv-init.lua");

            public boolean isEnabled() {
                return this.enabled;
            }

            public void setEnabled(boolean enabled) {
                this.enabled = enabled;
            }

            public String getEngine() {
                return this.engine;
            }

            public void setEngine(String engine) {
                this.engine = engine;
            }

            public Resource getScriptLocation() {
                return this.scriptLocation;
            }

            public void setScriptLocation(Resource scriptLocation) {
                this.scriptLocation = scriptLocation;
            }
        }
    }
}
