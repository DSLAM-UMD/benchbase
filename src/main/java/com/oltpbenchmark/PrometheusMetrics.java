package com.oltpbenchmark;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;
import io.prometheus.client.exporter.PushGateway;

public class PrometheusMetrics {
    private static final Logger LOG = LoggerFactory.getLogger(PrometheusMetrics.class);

    private static final CollectorRegistry registry = new CollectorRegistry();

    public static final Counter TXNS = Counter.build()
            .name("benchbase_txn_total")
            .help("Total number of transactions")
            .labelNames("benchmark", "type", "status")
            .register(registry);

    public static final Histogram TXN_DURATION = Histogram.build()
            .name("benchbase_txn_duration_seconds")
            .help("Transaction duration in seconds")
            .labelNames("benchmark", "type", "status")
            .buckets(0.000_001, 0.000_010, 0.000_100, // 1 us, 10 us, 100 us
                    0.001_000, 0.010_000, 0.100_000, // 1 ms, 10 ms, 100 ms
                    1.0, 10.0, 100.0) // 1 s, 10 s, 100 s
            .register(registry);

    public static final Histogram STATEMENT_DURATION = Histogram.build()
            .name("benchbase_statement_duration_seconds")
            .help("Statement duration in seconds")
            .labelNames("benchmark", "type", "name")
            .buckets(0.000_001, 0.000_010, 0.000_100, // 1 us, 10 us, 100 us
                    0.001_000, 0.010_000, 0.100_000, // 1 ms, 10 ms, 100 ms
                    1.0, 10.0, 100.0) // 1 s, 10 s, 100 s
            .register(registry);

    public static final Histogram DEBUG = Histogram.build()
            .name("benchbase_debug_seconds")
            .help("Debugging")
            .labelNames("name")
            .buckets(0.000_001, 0.000_010, 0.000_100, // 1 us, 10 us, 100 us
                    0.001_000, 0.010_000, 0.100_000, // 1 ms, 10 ms, 100 ms
                    1.0, 10.0, 100.0) // 1 s, 10 s, 100 s
            .register(registry);

    public static void run(String pushGatewayAddress) {
        new Thread() {
            @Override
            public void run() {
                PushGateway pushGateway = new PushGateway(pushGatewayAddress);
                while (true) {
                    try {
                        pushGateway.pushAdd(registry, "benchbase");
                        Thread.sleep(500);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }.start();

        LOG.info("Started Prometheus metrics pusher to {}", pushGatewayAddress);
    }
}
