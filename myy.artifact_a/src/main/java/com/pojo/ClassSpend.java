package com.pojo;

import java.io.Serializable;

public class ClassSpend implements Serializable {

	private static final long serialVersionUID = 1L;
	public String Vehicle;
	public String Copy;
	public String PrincipalBrand;
	public String FiscalYear;
	public String FiscalQuarter;
	public String CalendarYear;
	public String CalendarQuarter;
	public double Spend;
	public String getVehicle() {
		return Vehicle;
	}
	public void setVehicle(String vehicle) {
		Vehicle = vehicle;
	}
	public String getCopy() {
		return Copy;
	}
	public void setCopy(String copy) {
		Copy = copy;
	}
	public String getPrincipalBrand() {
		return PrincipalBrand;
	}
	public void setPrincipalBrand(String principalBrand) {
		PrincipalBrand = principalBrand;
	}
	public String getFiscalYear() {
		return FiscalYear;
	}
	public void setFiscalYear(String fiscalYear) {
		FiscalYear = fiscalYear;
	}
	public String getFiscalQuarter() {
		return FiscalQuarter;
	}
	public void setFiscalQuarter(String fiscalQuarter) {
		FiscalQuarter = fiscalQuarter;
	}
	public String getCalendarYear() {
		return CalendarYear;
	}
	public void setCalendarYear(String calendarYear) {
		CalendarYear = calendarYear;
	}
	public String getCalendarQuarter() {
		return CalendarQuarter;
	}
	public void setCalendarQuarter(String calendarQuarter) {
		CalendarQuarter = calendarQuarter;
	}
	public double getSpend() {
		return Spend;
	}
	public void setSpend(double spend) {
		Spend = spend;
	}

}
