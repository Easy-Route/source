package org.dataflow.reconciler.plan;

public interface ReconcileStep {

    String name();

    void execute(StepContext ctx);

    boolean verify(StepContext ctx);

    default boolean idempotent() {
        return true;
    }
}
