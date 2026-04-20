package org.dataflow.validator.diff;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.dataflow.domain.spec.TractSpec;
import org.dataflow.domain.tract.ChangeImpact;
import org.dataflow.validator.YamlSpecParser;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@Component
public class DiffClassifier {

    private final ObjectMapper json;
    private final Map<String, ChangeImpact> rules;

    public DiffClassifier(YamlSpecParser parser) {
        this.json = parser.jsonMapper();
        this.rules = DiffClassificationRules.defaults();
    }

    public Classification classify(TractSpec oldSpec, TractSpec newSpec) {
        JsonNode oldTree = json.valueToTree(oldSpec);
        JsonNode newTree = json.valueToTree(newSpec);
        List<DiffEntry> entries = new ArrayList<>();
        diff("", oldTree, newTree, entries);

        ChangeImpact aggregate = ChangeImpact.HOT;
        for (DiffEntry entry : entries) {
            ChangeImpact rule = matchRule(entry.path());
            aggregate = aggregate.merge(rule);
        }
        return new Classification(aggregate, entries);
    }

    private void diff(String path, JsonNode left, JsonNode right, List<DiffEntry> out) {
        if (left == null || left.isMissingNode() || left.isNull()) {
            if (right != null && !right.isMissingNode() && !right.isNull()) {
                out.add(new DiffEntry(path + "/added", null, right));
            }
            return;
        }
        if (right == null || right.isMissingNode() || right.isNull()) {
            out.add(new DiffEntry(path + "/removed", left, null));
            return;
        }
        if (left.getNodeType() != right.getNodeType()) {
            out.add(new DiffEntry(path, left, right));
            return;
        }
        if (left.isObject()) {
            Iterator<String> fields = left.fieldNames();
            while (fields.hasNext()) {
                String field = fields.next();
                diff(append(path, field), left.get(field), right.get(field), out);
            }
            Iterator<String> rightFields = right.fieldNames();
            while (rightFields.hasNext()) {
                String field = rightFields.next();
                if (left.get(field) == null) {
                    diff(append(path, field), null, right.get(field), out);
                }
            }
            return;
        }
        if (left.isArray()) {
            int max = Math.max(left.size(), right.size());
            for (int i = 0; i < max; i++) {
                diff(append(path, "[" + i + "]"),
                        i < left.size() ? left.get(i) : null,
                        i < right.size() ? right.get(i) : null,
                        out);
            }
            return;
        }
        if (!left.equals(right)) {
            out.add(new DiffEntry(path, left, right));
        }
    }

    private ChangeImpact matchRule(String path) {
        ChangeImpact best = null;
        for (Map.Entry<String, ChangeImpact> rule : rules.entrySet()) {
            if (matches(rule.getKey(), path)) {
                if (best == null || rule.getValue().merge(best) != best) {
                    best = rule.getValue().merge(best == null ? ChangeImpact.HOT : best);
                }
            }
        }
        return best == null ? ChangeImpact.REQUIRES_RESTART : best;
    }

    private boolean matches(String rulePattern, String actualPath) {
        String regex = rulePattern
                .replace("[*]", "\\[\\d+\\]")
                .replace(".", "\\.")
                .replace("[", "\\[")
                .replace("]", "\\]")
                .replace("\\\\[", "\\[")
                .replace("\\\\]", "\\]");
        return actualPath.matches(regex + "(\\..*|/.*|)");
    }

    private static String append(String prefix, String segment) {
        if (prefix.isEmpty()) {
            return segment;
        }
        if (segment.startsWith("[")) {
            return prefix + segment;
        }
        return prefix + "." + segment;
    }

    public record Classification(ChangeImpact impact, List<DiffEntry> changes) {
    }

    public record DiffEntry(String path, JsonNode before, JsonNode after) {
    }
}
