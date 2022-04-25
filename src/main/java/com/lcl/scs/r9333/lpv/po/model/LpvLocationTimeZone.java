package com.lcl.scs.r9333.lpv.po.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Document(collection = "LocationTimeZone")
public class LpvLocationTimeZone {

	@Id
	private String id;
	private String location;
	private double compareToEasternTime;
	private String city;
	private String province;
	private String country;
	private String timeZone;
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getLocation() {
		return location;
	}
	public void setLocation(String location) {
		this.location = location;
	}
	public double getCompareToEasternTime() {
		return compareToEasternTime;
	}
	public void setCompareToEasternTime(double compareToEasternTime) {
		this.compareToEasternTime = compareToEasternTime;
	}
	public String getCity() {
		return city;
	}
	public void setCity(String city) {
		this.city = city;
	}
	public String getProvince() {
		return province;
	}
	public void setProvince(String province) {
		this.province = province;
	}
	public String getCountry() {
		return country;
	}
	public void setCountry(String country) {
		this.country = country;
	}
	public String getTimeZone() {
		return timeZone;
	}
	public void setTimeZone(String timeZone) {
		this.timeZone = timeZone;
	}
}
