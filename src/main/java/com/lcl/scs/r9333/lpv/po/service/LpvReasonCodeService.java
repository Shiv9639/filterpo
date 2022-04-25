package com.lcl.scs.r9333.lpv.po.service;

import java.util.List;

import org.springframework.stereotype.Service;

import com.lcl.scs.r9333.lpv.po.model.LpvPoInterface;
import com.lcl.scs.r9333.lpv.po.model.LpvReasonCode;
import com.lcl.scs.r9333.lpv.po.model.LpvReasonCodeTransaction;

@Service
public interface LpvReasonCodeService {

	List<LpvReasonCode> findReasonCodeByCode(String reasonCode);

	void saveLpvReasonCode(LpvReasonCode lpvReasonCode);

	void saveLpvReasonCodeTransaction(LpvReasonCodeTransaction lpvReasonCodeTransaction);

	LpvReasonCodeTransaction publishLpvReasonCodeTransaction(List<LpvReasonCode> lpvReasonCodeList, LpvPoInterface po);
}

