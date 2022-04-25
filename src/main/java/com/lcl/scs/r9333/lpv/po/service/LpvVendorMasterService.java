package com.lcl.scs.r9333.lpv.po.service;

import org.springframework.stereotype.Service;

import com.lcl.scs.r9333.lpv.po.model.VendorMaster;

@Service
public interface LpvVendorMasterService {
	VendorMaster findBySite (String site);
}
