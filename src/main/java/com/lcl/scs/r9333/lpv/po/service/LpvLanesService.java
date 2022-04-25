package com.lcl.scs.r9333.lpv.po.service;

import org.springframework.stereotype.Service;

import com.lcl.scs.r9333.lpv.po.model.LpvLanes;

@Service
public interface LpvLanesService {
	LpvLanes findByBuyerAndVendor(String buyer, String vendor);
//	LpvLanes findByVendor (String vendor);
	public LpvLanes createNewLane(LpvLanes lpv);
}
