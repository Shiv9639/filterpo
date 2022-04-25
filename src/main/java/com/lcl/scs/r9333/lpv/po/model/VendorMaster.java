package com.lcl.scs.r9333.lpv.po.model;

import java.util.Date;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Document(collection="VendorMaster")
public class VendorMaster {
	@Id
	private String id;
	private String site;
	private String participant;
	private String siteOwnerName;
	private String siteName;
	private String siteType;
	private String siteOwnerType;
	private String locationType;
	private String addressDescriptor;
	private String addressLine1;
	private String city;
	private String state;
	private String country;
	private String regionHrCity;
	private String regionHrState;
	private String regionHrCountry;
	private String region;
	private String latitude;
	private String longitude;
	private String zipCode;
	private String continent; 
	private String status;
	private String piComputeRequired;
	private String earlyDelToleranceDays;
	private String lateDelToleranceDays;
	private String dwellTime;
	private String accountGroup;
	private String vendorUpdatedIndicator;
	private String fileName;
	private Date vendorUpdatedTime;
	private Date vendorProcessedTime;
	private String vendorProcessedIndicator;
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getSite() {
		return site;
	}
	public void setSite(String site) {
		this.site = site;
	}
	public String getParticipant() {
		return participant;
	}
	public void setParticipant(String participant) {
		this.participant = participant;
	}
	public String getSiteOwnerName() {
		return siteOwnerName;
	}
	public void setSiteOwnerName(String siteOwnerName) {
		this.siteOwnerName = siteOwnerName;
	}
	public String getSiteName() {
		return siteName;
	}
	public void setSiteName(String siteName) {
		this.siteName = siteName;
	}
	public String getSiteType() {
		return siteType;
	}
	public void setSiteType(String siteType) {
		this.siteType = siteType;
	}
	public String getSiteOwnerType() {
		return siteOwnerType;
	}
	public void setSiteOwnerType(String siteOwnerType) {
		this.siteOwnerType = siteOwnerType;
	}
	public String getLocationType() {
		return locationType;
	}
	public void setLocationType(String locationType) {
		this.locationType = locationType;
	}
	public String getAddressDescriptor() {
		return addressDescriptor;
	}
	public void setAddressDescriptor(String addressDescriptor) {
		this.addressDescriptor = addressDescriptor;
	}
	public String getAddressLine1() {
		return addressLine1;
	}
	public void setAddressLine1(String addressLine1) {
		this.addressLine1 = addressLine1;
	}
	public String getCity() {
		return city;
	}
	public void setCity(String city) {
		this.city = city;
	}
	public String getState() {
		return state;
	}
	public void setState(String state) {
		this.state = state;
	}
	public String getCountry() {
		return country;
	}
	public void setCountry(String country) {
		this.country = country;
	}
	public String getRegionHrCity() {
		return regionHrCity;
	}
	public void setRegionHrCity(String regionHrCity) {
		this.regionHrCity = regionHrCity;
	}
	public String getRegionHrState() {
		return regionHrState;
	}
	public void setRegionHrState(String regionHrState) {
		this.regionHrState = regionHrState;
	}
	public String getRegionHrCountry() {
		return regionHrCountry;
	}
	public void setRegionHrCountry(String regionHrCountry) {
		this.regionHrCountry = regionHrCountry;
	}
	public String getRegion() {
		return region;
	}
	public void setRegion(String region) {
		this.region = region;
	}
	public String getLatitude() {
		return latitude;
	}
	public void setLatitude(String latitude) {
		this.latitude = latitude;
	}
	public String getLongitude() {
		return longitude;
	}
	public void setLongitude(String longitude) {
		this.longitude = longitude;
	}
	public String getZipCode() {
		return zipCode;
	}
	public void setZipCode(String zipCode) {
		this.zipCode = zipCode;
	}
	public String getContinent() {
		return continent;
	}
	public void setContinent(String continent) {
		this.continent = continent;
	}
	public String getStatus() {
		return status;
	}
	public void setStatus(String status) {
		this.status = status;
	}
	public String getPiComputeRequired() {
		return piComputeRequired;
	}
	public void setPiComputeRequired(String piComputeRequired) {
		this.piComputeRequired = piComputeRequired;
	}
	public String getEarlyDelToleranceDays() {
		return earlyDelToleranceDays;
	}
	public void setEarlyDelToleranceDays(String earlyDelToleranceDays) {
		this.earlyDelToleranceDays = earlyDelToleranceDays;
	}
	public String getLateDelToleranceDays() {
		return lateDelToleranceDays;
	}
	public void setLateDelToleranceDays(String lateDelToleranceDays) {
		this.lateDelToleranceDays = lateDelToleranceDays;
	}
	public String getDwellTime() {
		return dwellTime;
	}
	public void setDwellTime(String dwellTime) {
		this.dwellTime = dwellTime;
	}
	public String getAccountGroup() {
		return accountGroup;
	}
	public void setAccountGroup(String accountGroup) {
		this.accountGroup = accountGroup;
	}
	public Date getVendorUpdatedTime() {
		return vendorUpdatedTime;
	}
	public void setVendorUpdatedTime(Date vendorUpdatedTime) {
		this.vendorUpdatedTime = vendorUpdatedTime;
	}
	public Date getVendorProcessedTime() {
		return vendorProcessedTime;
	}
	public void setVendorProcessedTime(Date vendorProcessedTime) {
		this.vendorProcessedTime = vendorProcessedTime;
	}
	public String getVendorUpdatedIndicator() {
		return vendorUpdatedIndicator;
	}
	public void setVendorUpdatedIndicator(String vendorUpdatedIndicator) {
		this.vendorUpdatedIndicator = vendorUpdatedIndicator;
	}
	public String getFileName() {
		return fileName;
	}
	public void setFileName(String fileName) {
		this.fileName = fileName;
	}
	public String getVendorProcessedIndicator() {
		return vendorProcessedIndicator;
	}
	public void setVendorProcessedIndicator(String vendorProcessedIndicator) {
		this.vendorProcessedIndicator = vendorProcessedIndicator;
	}
	
	
}

