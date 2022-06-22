package com.transport.data.Processor;

import com.transport.data.model.Person;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;

@Slf4j
public class PersonItemProcessor implements ItemProcessor<Person, Person> {


    @Override
    public Person process(final Person person) throws Exception {
    	final int person_id = person.getPerson_id();
        final String firstName = person.getFirstName();
        final String lastName = person.getLastName();

        final Person transformedPerson = new Person(person_id,firstName, lastName);

        log.info("Converting (" + person + ") into (" + transformedPerson + ")");

        return transformedPerson;
    }

}