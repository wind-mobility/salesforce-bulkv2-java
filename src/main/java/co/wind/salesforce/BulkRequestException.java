package co.wind.salesforce;

import co.wind.salesforce.response.ErrorResponse;

import java.util.List;

import static java.util.stream.Collectors.joining;

public class BulkRequestException extends RuntimeException {

    public BulkRequestException(String message, Throwable cause) {
        super(message, cause);
    }

    public BulkRequestException(String message) {
        super(message);
    }

    public BulkRequestException(Exception cause) {
        super(cause);
    }

    public BulkRequestException(List<ErrorResponse> errors) {
        super(errors.stream().map(ErrorResponse::toString).collect(joining(", ", "[ ", " ]")));
    }
}
