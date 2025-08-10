package com.itc.learnkafka.producer;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.itc.learnkafka.domain.LibraryEvent;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class LibraryEventProducer {

	@Value("${spring.kafka.template.default-topic}")
	private String topicName;

	private final KafkaTemplate<Integer, String> kafkaTemplate;

	private ObjectMapper objectMapper;

	public LibraryEventProducer(KafkaTemplate<Integer, String> kafkaTemplate, ObjectMapper objectMapper) {
		this.kafkaTemplate = kafkaTemplate;
		this.objectMapper = objectMapper;
	}

	public CompletableFuture<SendResult<Integer, String>> sendLibraryEvent(LibraryEvent libraryEvent)
			throws JsonProcessingException {

		var key = libraryEvent.libraryEventId();

		var value = objectMapper.writeValueAsString(libraryEvent);

		// blocking call get the metadata about the kafka cluster
		// call the send method for the 1st time asynch call

		var result = kafkaTemplate.send(topicName, key, value);

		return result.whenComplete((sendResult, throwable) -> {

			if (throwable != null) {
				handleFailure(key, value, throwable);
			} else {
				handleSucess(key, value, sendResult);
			}

		});

	}

	private void handleFailure(Integer key, String value, Throwable throwable) {
		// TODO Auto-generated method stub

		log.info("Unable to send message to topic {}", throwable);

	}

	private void handleSucess(Integer key, String value, SendResult<Integer, String> sendResult) {
		// TODO Auto-generated method stub

		log.info("message sent to topic successfully for key{} , and the value {} ,partition {}", key, value,
				sendResult.getRecordMetadata().partition());

	}

	// 2nd approach
	public SendResult<Integer, String> sendLibraryEvent2(LibraryEvent libraryEvent)
			throws JsonProcessingException, InterruptedException, ExecutionException, TimeoutException {

		var key = libraryEvent.libraryEventId();

		var value = objectMapper.writeValueAsString(libraryEvent);

		// blocking call get the metadata about the kafka cluster
		// call the send method synch call

		var result = kafkaTemplate.send(topicName, key, value).get(3, TimeUnit.SECONDS);
		handleSucess(key, value, result);

		return result;

	}

	// 3rd approach
	public CompletableFuture<SendResult<Integer, String>> sendLibraryEventWithProduceRecord(LibraryEvent libraryEvent)
			throws JsonProcessingException, InterruptedException, ExecutionException, TimeoutException {

		var key = libraryEvent.libraryEventId();

		var value = objectMapper.writeValueAsString(libraryEvent);

		// blocking call get the metadata about the kafka cluster
		// call the send method asynch call

		var producerRecord = buildProducerRecord(key, value);

		var result = kafkaTemplate.send(producerRecord);

		return result.whenComplete((sendResult, throwable) -> {

			if (throwable != null) {
				handleFailure(key, value, throwable);
			} else {
				handleSucess(key, value, sendResult);
			}

		});

	}
	
	/*
	 * below code is for only buildProducerRecord1 
	 */

	private ProducerRecord<Integer, String> buildProducerRecord1(Integer key, String value) {

		return new ProducerRecord<Integer, String>(topicName,  key, value);
	}

	/*
	 * below code is for only buildProducerRecord  with header value 
	 * 
	 */
	private ProducerRecord<Integer, String> buildProducerRecord(Integer key, String value) {

		List<Header> recordHeader = List.of(new RecordHeader("event-souce", "scanner".getBytes()));

		return new ProducerRecord<Integer, String>(topicName, null, key, value, recordHeader);
	}
}
