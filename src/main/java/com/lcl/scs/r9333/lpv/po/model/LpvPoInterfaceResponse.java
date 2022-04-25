package com.lcl.scs.r9333.lpv.po.model;

import java.time.ZonedDateTime;

public class LpvPoInterfaceResponse {
	private ZonedDateTime timeStamp;
	private String ingestionId;
	private String message;
	private String status;

	public ZonedDateTime getTimeStamp() {
		return timeStamp;
	}

	public void setTimeStamp(ZonedDateTime timeStamp) {
		this.timeStamp = timeStamp;
	}

	public String getIngestionId() {
		return ingestionId;
	}

	public void setIngestionId(String ingestionId) {
		this.ingestionId = ingestionId;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}
}
