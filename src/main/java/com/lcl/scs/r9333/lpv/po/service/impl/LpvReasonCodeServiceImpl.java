package com.lcl.scs.r9333.lpv.po.service.impl;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.lcl.scs.r9333.lpv.po.model.LpvPoInterface;
import com.lcl.scs.r9333.lpv.po.model.LpvReasonCode;
import com.lcl.scs.r9333.lpv.po.model.LpvReasonCodeTransaction;
import com.lcl.scs.r9333.lpv.po.repository.LpvReasonCodeRepository;
import com.lcl.scs.r9333.lpv.po.repository.LpvReasonCodeTransactionRepository;
import com.lcl.scs.r9333.lpv.po.service.LpvReasonCodeService;

@Service
public class LpvReasonCodeServiceImpl implements LpvReasonCodeService {
	@Autowired
	private LpvReasonCodeRepository lpvReasonCodeRepository;

	@Autowired
	private LpvReasonCodeTransactionRepository lpvReasonCodeTransactionRepository;

	@Override
	public void saveLpvReasonCode(LpvReasonCode lpvReasonCode) {
		lpvReasonCodeRepository.save(lpvReasonCode);
	}

	@Override
	public void saveLpvReasonCodeTransaction(LpvReasonCodeTransaction lpvReasonCodeTransaction) {
		lpvReasonCodeTransactionRepository.save(lpvReasonCodeTransaction);
	}

	@Override
	public List<LpvReasonCode> findReasonCodeByCode(String reasonCode) {
		return lpvReasonCodeRepository.findByReasonCode(reasonCode);
	}

	@Override
	public LpvReasonCodeTransaction publishLpvReasonCodeTransaction(List<LpvReasonCode> lpvReasonCodeList,
			LpvPoInterface po) {
		LpvReasonCodeTransaction lpvReasonCodeTransaction = new LpvReasonCodeTransaction();
		if (lpvReasonCodeList != null && !lpvReasonCodeList.isEmpty()) {
			for (LpvReasonCode lpvReasonCode : lpvReasonCodeList) {
				lpvReasonCodeTransaction.setReasonCode(lpvReasonCode.getReasonCode());
				lpvReasonCodeTransaction.setPurchaseOrder(po);
				lpvReasonCodeTransaction.setPoType(po.getErpOrderType());
				lpvReasonCodeTransaction.setIdocNumber(po.getiDocSerialNumber());
				lpvReasonCodeTransaction.setTime(new Date());
				lpvReasonCodeTransaction.setReasonCodeDescription(lpvReasonCode.getReasonCodeDescription());
				return lpvReasonCodeTransaction;
			}
		}
		return lpvReasonCodeTransaction;
	}

}
