package org.dataflow.reconciler.plan;

import java.util.List;

public record ReconcilePlan(List<ReconcileStep> steps, String reason) {

    public ReconcilePlan {
        steps = steps == null ? List.of() : List.copyOf(steps);
    }

    public static ReconcilePlan noop() {
        return new ReconcilePlan(List.of(), "no drift");
    }

    public boolean isEmpty() {
        return steps.isEmpty();
    }
}
