package org.dataflow.domain.tract;

import java.util.regex.Pattern;

public record TractName(String value) {
    private static final Pattern PATTERN = Pattern.compile("^[a-z][a-z0-9-]{2,63}$");

    public TractName {
        if (value == null || !PATTERN.matcher(value).matches()) {
            throw new IllegalArgumentException(
                    "Tract name must match " + PATTERN.pattern() + ", got: " + value);
        }
    }

    @Override
    public String toString() {
        return value;
    }
}
