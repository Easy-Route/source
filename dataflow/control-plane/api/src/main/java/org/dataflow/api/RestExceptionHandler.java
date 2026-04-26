package org.dataflow.api;

import org.dataflow.api.dto.ApiError;
import org.dataflow.application.usecase.TractRegistrationService;
import org.dataflow.domain.tract.InvalidTransitionException;
import org.dataflow.validator.SpecParseException;
import org.dataflow.validator.ValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.List;

@RestControllerAdvice
public class RestExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(RestExceptionHandler.class);

    @ExceptionHandler(SpecParseException.class)
    public ResponseEntity<ApiError> onParse(SpecParseException ex) {
        return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY).body(ApiError.of(
                422, "spec_parse_error", ex.getMessage(),
                List.of(new ApiError.FieldError("$", ex.getMessage(), ex.line(), ex.column()))));
    }

    @ExceptionHandler(ValidationException.class)
    public ResponseEntity<ApiError> onValidation(ValidationException ex) {
        List<ApiError.FieldError> fields = ex.result().errors().stream()
                .map(e -> new ApiError.FieldError(e.path(), e.message(), e.line(), e.column()))
                .toList();
        return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY).body(ApiError.of(
                422, "spec_validation_failed", "Spec validation failed", fields));
    }

    @ExceptionHandler(InvalidTransitionException.class)
    public ResponseEntity<ApiError> onTransition(InvalidTransitionException ex) {
        return ResponseEntity.status(HttpStatus.CONFLICT).body(ApiError.of(
                409, "invalid_transition", ex.getMessage()));
    }

    @ExceptionHandler(TractRegistrationService.TractNotFoundException.class)
    public ResponseEntity<ApiError> onNotFound(TractRegistrationService.TractNotFoundException ex) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(ApiError.of(404, "tract_not_found", ex.getMessage()));
    }

    @ExceptionHandler(TractRegistrationService.TractAlreadyExistsException.class)
    public ResponseEntity<ApiError> onConflict(TractRegistrationService.TractAlreadyExistsException ex) {
        return ResponseEntity.status(HttpStatus.CONFLICT).body(ApiError.of(409, "tract_exists", ex.getMessage()));
    }

    @ExceptionHandler(TractRegistrationService.SourceNotRegisteredException.class)
    public ResponseEntity<ApiError> onSourceMissing(TractRegistrationService.SourceNotRegisteredException ex) {
        return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY).body(ApiError.of(
                422, "source_not_registered", ex.getMessage()));
    }

    @ExceptionHandler(TractController.RestartRequiredException.class)
    public ResponseEntity<ApiError> onRestartNeeded(TractController.RestartRequiredException ex) {
        return ResponseEntity.status(HttpStatus.CONFLICT).body(ApiError.of(
                409, "restart_required", ex.getMessage()));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<ApiError> onIllegalArg(IllegalArgumentException ex) {
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(ApiError.of(400, "bad_request", ex.getMessage()));
    }

    @ExceptionHandler(IllegalStateException.class)
    public ResponseEntity<ApiError> onIllegalState(IllegalStateException ex) {
        return ResponseEntity.status(HttpStatus.CONFLICT).body(ApiError.of(409, "illegal_state", ex.getMessage()));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ApiError> onAny(Exception ex) {
        log.error("Unhandled error", ex);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(ApiError.of(500, "internal_error", "Internal server error"));
    }
}
