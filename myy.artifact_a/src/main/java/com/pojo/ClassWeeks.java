package com.pojo;

import java.io.Serializable;
import java.util.Date;
public class ClassWeeks implements Serializable {

	private static final long serialVersionUID = 1L;
	public String Quarter;
	public Date Week;
	public String FinancialYear;
	public String CurrentYear;
	public String getQuarter() {
		return Quarter;
	}
	public void setQuarter(String quarter) {
		Quarter = quarter;
	}
	public Date getWeek() {
		return Week;
	}
	public void setWeek(Date date) {
		Week = date;
	}
	public String getFinancialYear() {
		return FinancialYear;
	}
	public void setFinancialYear(String financialYear) {
		FinancialYear = financialYear;
	}
	public String getCurrentYear() {
		return CurrentYear;
	}
	public void setCurrentYear(String currentYear) {
		CurrentYear = currentYear;
	}

	
}
