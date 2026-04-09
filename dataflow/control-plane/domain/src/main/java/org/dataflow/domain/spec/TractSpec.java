package org.dataflow.domain.spec;

public record TractSpec(String apiVersion, String name, String rawDocument) {
    public static TractSpec placeholder(String name) {
        return new TractSpec("dataflow/v1", name, "");
    }
}
