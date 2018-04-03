package com.pojo;

import java.io.Serializable;

public class ClassShip implements Serializable {

	private static final long serialVersionUID = 1L;
	public String Brand;
	public String SubBrand;
	public String CatLib;
	public String Channel;
	public String Period2;
	public String ChannelVolume;
	public String AllOutletVolume;
	public double ProjectionFactor;
	public String getBrand() {
		return Brand;
	}
	public void setBrand(String brand) {
		Brand = brand;
	}
	public String getSubBrand() {
		return SubBrand;
	}
	public void setSubBrand(String subBrand) {
		SubBrand = subBrand;
	}
	public String getCatLib() {
		return CatLib;
	}
	public void setCatLib(String catLib) {
		CatLib = catLib;
	}
	public String getChannel() {
		return Channel;
	}
	public void setChannel(String channel) {
		Channel = channel;
	}
	public String getPeriod2() {
		return Period2;
	}
	public void setPeriod2(String period2) {
		Period2 = period2;
	}
	public String getChannelVolume() {
		return ChannelVolume;
	}
	public void setChannelVolume(String channelVolume) {
		ChannelVolume = channelVolume;
	}
	public String getAllOutletVolume() {
		return AllOutletVolume;
	}
	public void setAllOutletVolume(String allOutletVolume) {
		AllOutletVolume = allOutletVolume;
	}
	public double getProjectionFactor() {
		return ProjectionFactor;
	}
	public void setProjectionFactor(double projectionFactor) {
		ProjectionFactor = projectionFactor;
	}
	
	
}
