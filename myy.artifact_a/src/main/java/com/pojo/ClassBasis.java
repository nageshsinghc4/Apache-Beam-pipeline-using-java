package com.pojo;

import java.io.Serializable;

public class ClassBasis implements Serializable {

	private static final long serialVersionUID = 1L;
	public String Vehicle;
	public double Basis;
	public double BasisDuration;
	public String getVehicle() {
		return Vehicle;
	}
	public void setVehicle(String vehicle) {
		Vehicle = vehicle;
	}
	public double getBasis() {
		return Basis;
	}
	public void setBasis(double basis) {
		Basis = basis;
	}
	public double getBasisDuration() {
		return BasisDuration;
	}
	public void setBasisDuration(double basisDuration) {
		BasisDuration = basisDuration;
	}
	
}
