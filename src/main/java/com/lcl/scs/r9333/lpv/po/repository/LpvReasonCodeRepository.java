package com.lcl.scs.r9333.lpv.po.repository;

import java.util.List;

import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import com.lcl.scs.r9333.lpv.po.model.LpvReasonCode;

@Repository
public interface LpvReasonCodeRepository extends MongoRepository<LpvReasonCode, String> {
	List<LpvReasonCode> findByReasonCode(String reasonCode);
}
