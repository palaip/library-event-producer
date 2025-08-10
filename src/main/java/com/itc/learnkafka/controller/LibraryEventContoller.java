package com.itc.learnkafka.controller;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.itc.learnkafka.domain.LibraryEvent;
import com.itc.learnkafka.producer.LibraryEventProducer;

import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;

@RestController
@Slf4j
public class LibraryEventContoller {

	private LibraryEventProducer libraryEventProducer;
	
	public LibraryEventContoller(LibraryEventProducer libraryEventProducer) {
		this.libraryEventProducer = libraryEventProducer;
	}

	@PostMapping("/v1/libraryEvent")
	public ResponseEntity<LibraryEvent> postLibraryEvent(@RequestBody @Valid LibraryEvent libraryEvent) {
		// TODO: process POST request

		log.info("library event sent to topic from producer{}", libraryEvent);
		try {
		  
			//libraryEventProducer.sendLibraryEvent(libraryEvent);
			
			libraryEventProducer.sendLibraryEvent2(libraryEvent);
			//libraryEventProducer.sendLibraryEventWithProduceRecord(libraryEvent);
		   
		   log.info("library event sent to topic from producer end");
		   
		   return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
		}
		catch (Exception e) {
			// TODO: handle exception
			return ResponseEntity.status(HttpStatus.EXPECTATION_FAILED).build();
		}

	}

	@GetMapping("/hello")
	public String helloTest(@RequestParam("id") String param) {
		return "helloWolrd" + param;
	}

}
