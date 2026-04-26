package org.dataflow.api.dto;

import java.time.Instant;
import java.util.List;

public record ApiError(
        int status,
        String code,
        String message,
        List<FieldError> errors,
        Instant timestamp
) {
    public static ApiError of(int status, String code, String message) {
        return new ApiError(status, code, message, List.of(), Instant.now());
    }

    public static ApiError of(int status, String code, String message, List<FieldError> errors) {
        return new ApiError(status, code, message, errors, Instant.now());
    }

    public record FieldError(String path, String message, Integer line, Integer column) {
    }
}
