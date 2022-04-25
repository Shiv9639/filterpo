package com.lcl.scs.r9333.lpv.po.repository;

import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import com.lcl.scs.r9333.lpv.po.model.LpvLanes;

@Repository
public interface LpvLanesRepository extends MongoRepository<LpvLanes, String> {
	LpvLanes findByBuyerAndVendor(String buyer, String vendor);
//	LpvLanes findByVendor(String vendor);
}
