package com.lcl.scs.controller;

import java.util.Date;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.lcl.scs.subscribers.model.Subscribers;
import com.lcl.scs.util.logging.LoggingUtilities;

@RestController
@RequestMapping("/subscribers")
public class SubscribersController {
@Autowired
private com.lcl.scs.subscribers.service.SubscribersService subscribersService;

@PostMapping(value="/byName", produces = { MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE })
public Subscribers getSubscriberByName(@RequestHeader("name") String name) {
	Subscribers s=new Subscribers();
	s.setName("LPV_PO");
	s.setCreatedby("cyang");
	s.setCreateddate(new Date());
	s.setInboundcollection("R9333");
	s.setInterfaceid("LPV_PO");
	s.setLastrun(new Date());
	s.setModifiedby("cyang");
	s.setModifieddate(new Date());
	s.setOutboundcollection("OUT_LPV_PO_Interface");
	s.setSubscribername("LPV");
	s.setSubscriberprovider("BY");
	//subscribersService.saveOrUpdateSubscribers(s);
	//System.out.println("Parameter: "+ricefnum);
	LoggingUtilities.generateInfoLog("Parameter: "+name);
	return subscribersService.findByName(name);
}
}
