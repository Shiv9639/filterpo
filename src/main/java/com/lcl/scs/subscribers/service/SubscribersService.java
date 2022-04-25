package com.lcl.scs.subscribers.service;

import java.util.List;

import org.springframework.stereotype.Service;

import com.lcl.scs.subscribers.model.Subscribers;

@Service
public interface SubscribersService {
	Subscribers findByName(String name);
	List<Subscribers> findBySubscribername(String subscribername);
	void saveOrUpdateSubscribers(Subscribers s);
}
