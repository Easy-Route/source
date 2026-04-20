package org.dataflow.validator.diff;

import org.dataflow.domain.tract.ChangeImpact;

import java.util.LinkedHashMap;
import java.util.Map;

public final class DiffClassificationRules {

    private DiffClassificationRules() {
    }

    public static Map<String, ChangeImpact> defaults() {
        Map<String, ChangeImpact> rules = new LinkedHashMap<>();

        // Hot — applied without pausing the tract.
        rules.put("spec.sinks[*]/added", ChangeImpact.REQUIRES_RESTART);
        rules.put("spec.sinks[*]/removed", ChangeImpact.REQUIRES_RESTART);
        rules.put("spec.sinks[*].connection", ChangeImpact.HOT);
        rules.put("spec.sinks[*].connection.bufferFlush", ChangeImpact.HOT);
        rules.put("spec.transformation.filter.excludeOperations", ChangeImpact.HOT);
        rules.put("spec.dlq.retentionDays", ChangeImpact.HOT);

        // Restart with savepoint.
        rules.put("spec.source.publication.tables", ChangeImpact.REQUIRES_RESTART);
        rules.put("spec.transformation.deduplication.key", ChangeImpact.REQUIRES_RESTART);
        rules.put("spec.transformation.deduplication.ttlHours", ChangeImpact.REQUIRES_RESTART);
        rules.put("spec.transformation.timezone.from", ChangeImpact.REQUIRES_RESTART);
        rules.put("spec.transformation.timezone.to", ChangeImpact.REQUIRES_RESTART);

        // Recreate — must be a brand-new tract.
        rules.put("apiVersion", ChangeImpact.REQUIRES_RECREATE);
        rules.put("kind", ChangeImpact.REQUIRES_RECREATE);
        rules.put("metadata.name", ChangeImpact.REQUIRES_RECREATE);
        rules.put("spec.source.type", ChangeImpact.REQUIRES_RECREATE);
        rules.put("spec.sinks[*].type", ChangeImpact.REQUIRES_RECREATE);

        return rules;
    }
}
