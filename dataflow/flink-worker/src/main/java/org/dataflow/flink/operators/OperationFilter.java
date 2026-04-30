package org.dataflow.flink.operators;

import org.apache.flink.api.common.functions.FilterFunction;
import org.dataflow.flink.event.CdcEvent;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class OperationFilter implements FilterFunction<CdcEvent> {

    private static final long serialVersionUID = 1L;

    private final Set<String> excludedOps;

    public OperationFilter(List<String> excludedOperations) {
        Set<String> set = new HashSet<>();
        for (String op : excludedOperations == null ? List.<String>of() : excludedOperations) {
            switch (op) {
                case "INSERT" -> set.add("c");
                case "UPDATE" -> set.add("u");
                case "DELETE" -> set.add("d");
                case "TRUNCATE" -> set.add("t");
                case "DDL" -> set.add("ddl");
                default -> {}
            }
        }
        this.excludedOps = set;
    }

    @Override
    public boolean filter(CdcEvent event) {
        return !excludedOps.contains(event.op());
    }
}
