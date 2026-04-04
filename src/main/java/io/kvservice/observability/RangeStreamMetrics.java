package io.kvservice.observability;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import io.grpc.Status;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

public final class RangeStreamMetrics {

    private final MeterRegistry meterRegistry;
    private final AtomicInteger activeStreams = new AtomicInteger();

    public RangeStreamMetrics(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        Gauge.builder("kvservice.grpc.range.stream.active", this.activeStreams, AtomicInteger::get)
                .register(meterRegistry);
    }

    public ActiveStream startStream() {
        this.activeStreams.incrementAndGet();
        return new ActiveStream(this.meterRegistry, this.activeStreams);
    }

    public static final class ActiveStream implements AutoCloseable {

        private final MeterRegistry meterRegistry;
        private final AtomicInteger activeStreams;
        private final AtomicLong itemCount = new AtomicLong();
        private final AtomicBoolean closed = new AtomicBoolean();
        private final long startedAtNanos = System.nanoTime();
        private volatile Status.Code statusCode = Status.Code.OK;

        private ActiveStream(MeterRegistry meterRegistry, AtomicInteger activeStreams) {
            this.meterRegistry = meterRegistry;
            this.activeStreams = activeStreams;
        }

        public void recordItem() {
            this.itemCount.incrementAndGet();
        }

        public void complete(Status.Code statusCode) {
            this.statusCode = statusCode;
            close();
        }

        @Override
        public void close() {
            if (!this.closed.compareAndSet(false, true)) {
                return;
            }
            this.activeStreams.decrementAndGet();
            long durationNanos = System.nanoTime() - this.startedAtNanos;
            String status = this.statusCode.name();
            Timer.builder("kvservice.grpc.range.stream.duration")
                    .tag("status", status)
                    .register(this.meterRegistry)
                    .record(durationNanos, TimeUnit.NANOSECONDS);
            DistributionSummary.builder("kvservice.grpc.range.stream.items")
                    .baseUnit("items")
                    .tag("status", status)
                    .register(this.meterRegistry)
                    .record(this.itemCount.get());
        }
    }
}
