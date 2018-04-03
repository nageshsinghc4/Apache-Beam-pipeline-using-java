package com.pojo;

import java.io.Serializable;
import java.util.Date;

public class ClassxNorms implements Serializable {

	private static final long serialVersionUID = 1L;
	
	public String Vehicle;
	public String PrincipalBrand;
	public String Period;
	public Date PeriodStartWeek;
	public Date PeriodEndWeek;
	public double GRPs;
	public double Duration;
	public double Continuity;
	public String BasisPeriod;
	public double Basis;
	public double BasisDuration;
	public double Alpha;
	public double Beta;
	public double Volume;
	public double Spend;
	public double Efficiency;
	public double Xnorm;
	public String Key;
	public String getVehicle() {
		return Vehicle;
	}
	public void setVehicle(String vehicle) {
		Vehicle = vehicle;
	}
	public String getPrincipalBrand() {
		return PrincipalBrand;
	}
	public void setPrincipalBrand(String principalBrand) {
		PrincipalBrand = principalBrand;
	}
	public String getPeriod() {
		return Period;
	}
	public void setPeriod(String period) {
		Period = period;
	}
	public Date getPeriodStartWeek() {
		return PeriodStartWeek;
	}
	public void setPeriodStartWeek(Date periodStartWeek) {
		PeriodStartWeek = periodStartWeek;
	}
	public Date getPeriodEndWeek() {
		return PeriodEndWeek;
	}
	public void setPeriodEndWeek(Date periodEndWeek) {
		PeriodEndWeek = periodEndWeek;
	}
	public double getGRPs() {
		return GRPs;
	}
	public void setGRPs(double gRPs) {
		GRPs = gRPs;
	}
	public double getDuration() {
		return Duration;
	}
	public void setDuration(double duration) {
		Duration = duration;
	}
	public double getContinuity() {
		return Continuity;
	}
	public void setContinuity(double continuity) {
		Continuity = continuity;
	}
	public String getBasisPeriod() {
		return BasisPeriod;
	}
	public void setBasisPeriod(String basisPeriod) {
		BasisPeriod = basisPeriod;
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
	public double getAlpha() {
		return Alpha;
	}
	public void setAlpha(double alpha) {
		Alpha = alpha;
	}
	public double getBeta() {
		return Beta;
	}
	public void setBeta(double beta) {
		Beta = beta;
	}
	public double getVolume() {
		return Volume;
	}
	public void setVolume(double volume) {
		Volume = volume;
	}
	public double getSpend() {
		return Spend;
	}
	public void setSpend(double spend) {
		Spend = spend;
	}
	public double getEfficiency() {
		return Efficiency;
	}
	public void setEfficiency(double efficiency) {
		Efficiency = efficiency;
	}
	public double getXnorm() {
		return Xnorm;
	}
	public void setXnorm(double xnorm) {
		Xnorm = xnorm;
	}
	public String getKey() {
		return Key;
	}
	public void setKey(String key) {
		Key = key;
	}
	
	
}
