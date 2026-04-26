package org.dataflow.api.dto;

import org.dataflow.domain.tract.Tract;

import java.time.Instant;

public record TractDto(
        String name,
        String source,
        String desiredState,
        long specVersion,
        Instant createdAt,
        Instant updatedAt
) {
    public static TractDto from(Tract tract) {
        return new TractDto(
                tract.name().value(),
                tract.source().value(),
                tract.desiredState().name(),
                tract.specVersion().value(),
                tract.createdAt(),
                tract.updatedAt());
    }
}
