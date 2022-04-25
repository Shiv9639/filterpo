package com.lcl.scs.subscribers.repository;

import java.util.List;

import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import com.lcl.scs.subscribers.model.Subscribers;

@Repository
public interface SubscribersRepository extends MongoRepository<Subscribers, String> {

	Subscribers findByName(String name);
	List<Subscribers> findBySubscribername(String subscribername);
	
}
