package com.lcl.scs.r9333.lpv.po.model;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Document(collection = "LpvReasonCodeCollection")
public class LpvReasonCode {
	@Id
	private String userId;
	private String reasonCode;
	private String reasonCodeDescription;
	private Date modifyDate;

	public void setReasonCode(String reasonCode) {
		this.reasonCode = reasonCode;
	}
	public String getReasonCodeDescription() {
		return reasonCodeDescription;
	}
	public void setReasonCodeDescription(String reasonCodeDescription) {
		this.reasonCodeDescription = reasonCodeDescription;
	}
	public String getUserId() {
		return userId;
	}
	public void setUserId(String userId) {
		this.userId = userId;
	}
	public Date getModifyDate() {
		return modifyDate;
	}
	public void setModifyDate(Date modifyDate) {
		this.modifyDate = modifyDate;
	}
	public String getReasonCode() {
		return reasonCode;
	}
	
}
