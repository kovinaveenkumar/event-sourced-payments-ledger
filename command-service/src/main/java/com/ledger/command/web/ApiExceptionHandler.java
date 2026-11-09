package com.ledger.command.web;

import com.ledger.command.domain.DomainException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

import java.util.stream.Collectors;

/** Turns errors into standard error JSON with a "code" field clients can check. */
@RestControllerAdvice
public class ApiExceptionHandler {

    @ExceptionHandler(DomainException.class)
    ProblemDetail domain(DomainException e) {
        ProblemDetail p = ProblemDetail.forStatusAndDetail(HttpStatus.valueOf(e.code().httpStatus()), e.getMessage());
        p.setTitle(e.code().name());
        p.setProperty("code", e.code().name());
        return p;
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    ProblemDetail validation(MethodArgumentNotValidException e) {
        String detail = e.getBindingResult().getFieldErrors().stream()
                .map(f -> f.getField() + " " + f.getDefaultMessage())
                .sorted()
                .collect(Collectors.joining("; "));
        return badRequest(detail);
    }

    @ExceptionHandler({HttpMessageNotReadableException.class, MethodArgumentTypeMismatchException.class})
    ProblemDetail unreadable(Exception e) {
        return badRequest("Malformed request");
    }

    private static ProblemDetail badRequest(String detail) {
        ProblemDetail p = ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, detail);
        p.setTitle("INVALID_REQUEST");
        p.setProperty("code", "INVALID_REQUEST");
        return p;
    }
}
