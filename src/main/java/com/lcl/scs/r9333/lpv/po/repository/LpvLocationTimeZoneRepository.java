package com.lcl.scs.r9333.lpv.po.repository;

import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import com.lcl.scs.r9333.lpv.po.model.LpvLocationTimeZone;
@Repository
public interface LpvLocationTimeZoneRepository extends MongoRepository<LpvLocationTimeZone, String> {

	LpvLocationTimeZone findByLocation(String Location);
}
