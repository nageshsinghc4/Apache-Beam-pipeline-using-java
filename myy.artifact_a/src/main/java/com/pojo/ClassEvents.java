package com.pojo;

import java.io.Serializable;

public class ClassEvents implements Serializable{

	private static final long serialVersionUID = 1L;
	public String EventList;
	public String EventDescription;
	public String Vehicle;
	public String getEventList() {
		return EventList;
	}
	public void setEventList(String eventList) {
		EventList = eventList;
	}
	public String getEventDescription() {
		return EventDescription;
	}
	public void setEventDescription(String eventDescription) {
		EventDescription = eventDescription;
	}
	public String getVehicle() {
		return Vehicle;
	}
	public void setVehicle(String vehicle) {
		Vehicle = vehicle;
	}
	
	

}
