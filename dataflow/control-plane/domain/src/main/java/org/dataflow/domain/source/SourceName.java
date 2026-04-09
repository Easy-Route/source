package org.dataflow.domain.source;

import java.util.regex.Pattern;

public record SourceName(String value) {
    private static final Pattern PATTERN = Pattern.compile("^[a-z][a-z0-9-]{2,63}$");

    public SourceName {
        if (value == null || !PATTERN.matcher(value).matches()) {
            throw new IllegalArgumentException(
                    "Source name must match " + PATTERN.pattern() + ", got: " + value);
        }
    }

    @Override
    public String toString() {
        return value;
    }
}
