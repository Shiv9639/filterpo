package com.lcl.scs.r9333.lpv.po.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.lcl.scs.r9333.lpv.po.model.LpvLocationTimeZone;
import com.lcl.scs.r9333.lpv.po.repository.LpvLocationTimeZoneRepository;
import com.lcl.scs.r9333.lpv.po.service.LpvLocationTimeZoneService;
@Service
public class LpvLocationTimeZoneServiceImpl implements LpvLocationTimeZoneService {

	@Autowired
	private LpvLocationTimeZoneRepository lpvLocationTimeZoneRepository;
	@Override
	public LpvLocationTimeZone findByLocation(String Location) {
		// TODO Auto-generated method stub
		return lpvLocationTimeZoneRepository.findByLocation(Location);
	}

}
