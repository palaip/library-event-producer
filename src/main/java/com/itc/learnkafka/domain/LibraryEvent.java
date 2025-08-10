package com.itc.learnkafka.domain;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;

@Builder
public record LibraryEvent(

		Integer libraryEventId, LibraryEventType libraryEventType, @NotNull @Valid Book book

) {

}
