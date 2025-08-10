package com.itc.learnkafka.exception;

import java.util.stream.Collectors;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.messaging.handler.annotation.support.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

import lombok.extern.slf4j.Slf4j;

@ControllerAdvice
@Slf4j
public class LibraryEventControllerAdvice {

	@ExceptionHandler(MethodArgumentNotValidException.class)
	public ResponseEntity<?> exceptionHandle(MethodArgumentNotValidException ex) {

		var error = ex.getBindingResult().getFieldErrors().stream()
				.map(fieldError -> fieldError.getField() + "- **-" + fieldError.getDefaultMessage()).sorted()
				.collect(Collectors.joining(", "));

		log.info("error message ** {}", error);

		return new ResponseEntity<>(error, HttpStatus.BAD_REQUEST);

	}

}
