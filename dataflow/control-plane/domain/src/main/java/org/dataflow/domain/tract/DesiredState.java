package org.dataflow.domain.tract;

import java.util.EnumSet;
import java.util.Set;

public enum DesiredState {
    DRAFTED,
    DEPLOYED,
    SUSPENDED,
    DELETED,
    FAILED;

    private static final Set<Transition> ALLOWED = EnumSet.of(
            Transition.of(DRAFTED, DEPLOYED),
            Transition.of(DRAFTED, DELETED),
            Transition.of(DEPLOYED, SUSPENDED),
            Transition.of(DEPLOYED, DELETED),
            Transition.of(DEPLOYED, FAILED),
            Transition.of(SUSPENDED, DEPLOYED),
            Transition.of(SUSPENDED, DELETED),
            Transition.of(SUSPENDED, FAILED),
            Transition.of(FAILED, SUSPENDED),
            Transition.of(FAILED, DELETED),
            Transition.of(FAILED, DEPLOYED)
    );

    public boolean canTransitionTo(DesiredState target) {
        if (this == target) {
            return true;
        }
        return ALLOWED.contains(Transition.of(this, target));
    }

    public boolean isTerminal() {
        return this == DELETED;
    }

    record Transition(DesiredState from, DesiredState to) {
        static Transition of(DesiredState from, DesiredState to) {
            return new Transition(from, to);
        }
    }
}
