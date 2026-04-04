package io.kvservice.config;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.util.unit.DataSize;

@ConfigurationProperties(prefix = "kvservice")
public class KvServiceProperties {

    private final Limits limits = new Limits();
    private final Deadlines deadlines = new Deadlines();
    private final Range range = new Range();

    public Limits getLimits() {
        return this.limits;
    }

    public Deadlines getDeadlines() {
        return this.deadlines;
    }

    public Range getRange() {
        return this.range;
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
}
