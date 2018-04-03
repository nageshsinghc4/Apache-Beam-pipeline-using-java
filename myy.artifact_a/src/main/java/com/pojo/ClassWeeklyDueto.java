package com.pojo;

import java.io.Serializable;
import java.util.Date;

public class ClassWeeklyDueto implements Serializable {

	private static final long serialVersionUID = 1L;
	public String Outlet;
	public String CatLib;
	public String ProdKey;
	public Date Week;
	public String SalesComponent;
	public double DuetoValue;
	public String PrimaryCausalKey;
	public double CausalValue;
	public double ModelIteration;
	public double Published;
	public String getOutlet() {
		return Outlet;
	}
	public void setOutlet(String outlet) {
		Outlet = outlet;
	}
	public String getCatLib() {
		return CatLib;
	}
	public void setCatLib(String catLib) {
		CatLib = catLib;
	}
	public String getProdKey() {
		return ProdKey;
	}
	public void setProdKey(String prodKey) {
		ProdKey = prodKey;
	}
	public Date getWeek() {
		return Week;
	}
	public void setWeek(Date date) {
		Week = date;
	}
	public String getSalesComponent() {
		return SalesComponent;
	}
	public void setSalesComponent(String salesComponent) {
		SalesComponent = salesComponent;
	}
	public double getDuetoValue() {
		return DuetoValue;
	}
	public void setDuetoValue(double duetoValue) {
		DuetoValue = duetoValue;
	}
	public String getPrimaryCausalKey() {
		return PrimaryCausalKey;
	}
	public void setPrimaryCausalKey(String primaryCausalKey) {
		PrimaryCausalKey = primaryCausalKey;
	}
	public double getCausalValue() {
		return CausalValue;
	}
	public void setCausalValue(double causalValue) {
		CausalValue = causalValue;
	}
	public double getModelIteration() {
		return ModelIteration;
	}
	public void setModelIteration(double modelIteration) {
		ModelIteration = modelIteration;
	}
	public double getPublished() {
		return Published;
	}
	public void setPublished(double published) {
		Published = published;
	}
	
		
}