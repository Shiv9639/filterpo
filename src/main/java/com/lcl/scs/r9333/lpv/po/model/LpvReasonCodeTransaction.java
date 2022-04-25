package com.lcl.scs.r9333.lpv.po.model;

import java.util.Date;

import org.springframework.data.mongodb.core.mapping.Document;

@Document(collection = "LpvReasonCodeTransaction")
public class LpvReasonCodeTransaction {
	private String reasonCode;
	private String idocNumber;
	private LpvPoInterface purchaseOrder;
	private Date time;
	private String exceptionMessage;
	private String reasonCodeDescription;
	private String poType;
	public String getPoType() {
		return poType;
	}

	public void setPoType(String poType) {
		this.poType = poType;
	}

	public String getReasonCodeDescription() {
		return reasonCodeDescription;
	}

	public void setReasonCodeDescription(String reasonCodeDescription) {
		this.reasonCodeDescription = reasonCodeDescription;
	}

	public String getReasonCode() {
		return reasonCode;
	}

	public void setReasonCode(String reasonCode) {
		this.reasonCode = reasonCode;
	}

	public String getIdocNumber() {
		return idocNumber;
	}

	public void setIdocNumber(String idocNumber) {
		this.idocNumber = idocNumber;
	}

	public Date getTime() {
		return time;
	}

	public void setTime(Date time) {
		this.time = time;
	}

	public String getExceptionMessage() {
		return exceptionMessage;
	}

	public void setExceptionMessage(String exceptionMessage) {
		this.exceptionMessage = exceptionMessage;
	}

	public LpvPoInterface getPurchaseOrder() {
		return purchaseOrder;
	}

	public void setPurchaseOrder(LpvPoInterface purchaseOrder) {
		this.purchaseOrder = purchaseOrder;
	}

}
