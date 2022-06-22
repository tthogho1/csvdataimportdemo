package com.transport.data.model;

import lombok.Data;

@Data
public class Person {

	private int person_id;
	private String lastName;
	private String firstName;
	
    public Person() {
    }

    public Person(int person_id,String firstName, String lastName) {
    	this.person_id = person_id;
        this.firstName = firstName;
        this.lastName = lastName;
    }
}
