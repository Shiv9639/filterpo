package com.lcl.scs.subscribers.service.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.lcl.scs.subscribers.model.Subscribers;
import com.lcl.scs.subscribers.repository.SubscribersRepository;
import com.lcl.scs.subscribers.service.SubscribersService;
@Service
public class SubscribersServiceImpl implements SubscribersService {
	@Autowired
	private SubscribersRepository subscribersRepository;

	@Override
	public Subscribers findByName(String name) {
		// TODO Auto-generated method stub
		return subscribersRepository.findByName(name);
	}

	@Override
	public List<Subscribers> findBySubscribername(String subscribername) {
		// TODO Auto-generated method stub
		return subscribersRepository.findBySubscribername(subscribername);
	}

	@Override
	public void saveOrUpdateSubscribers(Subscribers s) {
		// TODO Auto-generated method stub
		subscribersRepository.save(s);
	}

}
