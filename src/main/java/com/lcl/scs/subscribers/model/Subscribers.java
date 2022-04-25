package com.lcl.scs.subscribers.model;

import java.util.Date;

import org.bson.BsonTimestamp;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Document(collection = "subscribers")

public class Subscribers {
	private String createdby;	
	private Date createddate;
	@Id
	private String id;
	private String inboundcollection;
	private String interfaceid;
	private Date lastrun;
	private String modifiedby;
	private Date modifieddate;
	private String name;
	private String outboundcollection;
	private String subscribername;
	private String subscriberprovider;
	public String getCreatedby() {
		return createdby;
	}
	public Date getCreateddate() {
		return createddate;
	}
	public String getId() {
		return id;
	}
	public String getInboundcollection() {
		return inboundcollection;
	}
	public String getInterfaceid() {
		return interfaceid;
	}
	public Date getLastrun() {
		return lastrun;
	}
	public String getModifiedby() {
		return modifiedby;
	}
	public Date getModifieddate() {
		return modifieddate;
	}
	public String getName() {
		return name;
	}
	public String getOutboundcollection() {
		return outboundcollection;
	}
	public String getSubscribername() {
		return subscribername;
	}
	public String getSubscriberprovider() {
		return subscriberprovider;
	}
	public void setCreatedby(String createdby) {
		this.createdby = createdby;
	}
	public void setCreateddate(Date createddate) {
		this.createddate = createddate;
	}
	public void setId(String id) {
		this.id = id;
	}
	public void setInboundcollection(String inboundcollection) {
		this.inboundcollection = inboundcollection;
	}
	public void setInterfaceid(String interfaceid) {
		this.interfaceid = interfaceid;
	}
	public void setLastrun(Date lastrun) {
		this.lastrun = lastrun;
	}
	public void setModifiedby(String modifiedby) {
		this.modifiedby = modifiedby;
	}
	public void setModifieddate(Date modifieddate) {
		this.modifieddate = modifieddate;
	}
	public void setName(String name) {
		this.name = name;
	}
	public void setOutboundcollection(String outboundcollection) {
		this.outboundcollection = outboundcollection;
	}
	public void setSubscribername(String subscribername) {
		this.subscribername = subscribername;
	}
	public void setSubscriberprovider(String subscriberprovider) {
		this.subscriberprovider = subscriberprovider;
	}
}
