package com.lcl.scs.r9333.lpv.po.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.lcl.scs.r9333.lpv.po.model.LpvLanes;
import com.lcl.scs.r9333.lpv.po.repository.LpvLanesRepository;
import com.lcl.scs.r9333.lpv.po.service.LpvLanesService;
@Service
public class LpvLanesServiceImpl implements LpvLanesService {
	@Autowired
	private LpvLanesRepository lpvLanesRepository;

	@Override
	public LpvLanes findByBuyerAndVendor(String buyer, String vendor) {
		// TODO Auto-generated method stub
		return lpvLanesRepository.findByBuyerAndVendor(buyer, vendor);
	}
	
	
	@Override
	public LpvLanes createNewLane(LpvLanes lpv) {
		return lpvLanesRepository.insert(lpv);
	}



}
