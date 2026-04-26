package org.dataflow.api.dto;

import jakarta.validation.constraints.NotBlank;

public record SpecRequest(@NotBlank String document) {
}
