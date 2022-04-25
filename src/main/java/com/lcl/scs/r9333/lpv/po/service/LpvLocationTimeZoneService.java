package com.lcl.scs.r9333.lpv.po.service;

import org.springframework.stereotype.Service;

import com.lcl.scs.r9333.lpv.po.model.LpvLocationTimeZone;

@Service
public interface LpvLocationTimeZoneService {
	LpvLocationTimeZone findByLocation(String Location);
}
