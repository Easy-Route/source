package org.dataflow.domain.tract;

public class InvalidTransitionException extends RuntimeException {
    private final DesiredState from;
    private final DesiredState to;

    public InvalidTransitionException(DesiredState from, DesiredState to) {
        super("Transition not allowed: " + from + " -> " + to);
        this.from = from;
        this.to = to;
    }

    public DesiredState from() {
        return from;
    }

    public DesiredState to() {
        return to;
    }
}
