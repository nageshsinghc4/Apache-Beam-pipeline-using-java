package com.pojo;

import java.io.Serializable;
import java.util.Date;

public class ClassxNormCamp implements Serializable {

	private static final long serialVersionUID = 1L;
	public String Vehicle;
	public String Campaign;
	public String Copy;
	public String PrincipalBrand;
	public String Period;
	public Date PeriodStartDate;
	public Date PeriodEndDate;
	public double GRPs;
	public double Duration;
	public double Continuity;
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
	public String getCampaign() {
		return Campaign;
	}
	public void setCampaign(String campaign) {
		Campaign = campaign;
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
	public String getPeriod() {
		return Period;
	}
	public void setPeriod(String period) {
		Period = period;
	}
	public Date getPeriodStartDate() {
		return PeriodStartDate;
	}
	public void setPeriodStartDate(Date periodStartDate) {
		PeriodStartDate = periodStartDate;
	}
	public Date getPeriodEndDate() {
		return PeriodEndDate;
	}
	public void setPeriodEndDate(Date periodEndDate) {
		PeriodEndDate = periodEndDate;
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
