package com.pojo;

import java.io.Serializable;


public class ClassFinance implements Serializable {

	private static final long serialVersionUID = 1L;
	public String SubbrandName;
	public String BrandName;
	public String Period3;
	public double PplScVolume;
	public double TlPerSc;
	public double NetRealPerSc;
	public double CpfPerSc;
	public double NcsPerSc;
	public double ContribPerSc;
	public double AdjContribPerSc;
	public double TL;
	public double NetReal;
	public double CPF;
	public double NCS;
	public double Contrib;
	public double AdjContrib;
	public String getSubbrandName() {
		return SubbrandName;
	}
	public void setSubbrandName(String subbrandName) {
		SubbrandName = subbrandName;
	}
	public String getBrandName() {
		return BrandName;
	}
	public void setBrandName(String brandName) {
		BrandName = brandName;
	}
	public String getPeriod3() {
		return Period3;
	}
	public void setPeriod3(String period) {
		Period3 = period;
	}
	public double getPplScVolume() {
		return PplScVolume;
	}
	public void setPplScVolume(double pplScVolume) {
		PplScVolume = pplScVolume;
	}
	public double getTlPerSc() {
		return TlPerSc;
	}
	public void setTlPerSc(double tlPerSc) {
		TlPerSc = tlPerSc;
	}
	public double getNetRealPerSc() {
		return NetRealPerSc;
	}
	public void setNetRealPerSc(double netRealPerSc) {
		NetRealPerSc = netRealPerSc;
	}
	public double getCpfPerSc() {
		return CpfPerSc;
	}
	public void setCpfPerSc(double cpfPerSc) {
		CpfPerSc = cpfPerSc;
	}
	public double getNcsPerSc() {
		return NcsPerSc;
	}
	public void setNcsPerSc(double ncsPerSc) {
		NcsPerSc = ncsPerSc;
	}
	public double getContribPerSc() {
		return ContribPerSc;
	}
	public void setContribPerSc(double contribPerSc) {
		ContribPerSc = contribPerSc;
	}
	public double getAdjContribPerSc() {
		return AdjContribPerSc;
	}
	public void setAdjContribPerSc(double adjContribPerSc) {
		AdjContribPerSc = adjContribPerSc;
	}
	public double getTL() {
		return TL;
	}
	public void setTL(double tL) {
		TL = tL;
	}
	public double getNetReal() {
		return NetReal;
	}
	public void setNetReal(double netReal) {
		NetReal = netReal;
	}
	public double getCPF() {
		return CPF;
	}
	public void setCPF(double cPF) {
		CPF = cPF;
	}
	public double getNCS() {
		return NCS;
	}
	public void setNCS(double nCS) {
		NCS = nCS;
	}
	public double getContrib() {
		return Contrib;
	}
	public void setContrib(double contrib) {
		Contrib = contrib;
	}
	public double getAdjContrib() {
		return AdjContrib;
	}
	public void setAdjContrib(double adjContrib) {
		AdjContrib = adjContrib;
	}
	
}
