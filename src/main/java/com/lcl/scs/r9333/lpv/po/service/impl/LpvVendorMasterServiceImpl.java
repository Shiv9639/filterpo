package com.lcl.scs.r9333.lpv.po.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.lcl.scs.r9333.lpv.po.model.VendorMaster;
import com.lcl.scs.r9333.lpv.po.repository.LpvVendorMasterRepository;
import com.lcl.scs.r9333.lpv.po.service.LpvVendorMasterService;

@Service
public class LpvVendorMasterServiceImpl implements LpvVendorMasterService{
	@Autowired
	LpvVendorMasterRepository lpvVendorMasterRepository;
	@Override
	public VendorMaster findBySite(String site) {
		return lpvVendorMasterRepository.findBySite(site);
	}

}
