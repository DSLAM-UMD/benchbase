package com.oltpbenchmark.benchmarks.hot;

import com.oltpbenchmark.api.AbstractTestWorker;
import com.oltpbenchmark.api.Procedure;

import java.util.List;

public class TestHOTWorker extends AbstractTestWorker<HOTBenchmark> {

    @Override
    public List<Class<? extends Procedure>> procedures() {
        return TestHOTBenchmark.PROCEDURE_CLASSES;
    }

    @Override
    public Class<HOTBenchmark> benchmarkClass() {
        return HOTBenchmark.class;
    }
}
