package com.lcl.scs.r9333.lpv.po.service.impl;

import java.util.Date;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.lcl.scs.r9333.lpv.po.model.LpvPoInterface;
import com.lcl.scs.r9333.lpv.po.repository.LpvPoInterfaceRepository;
import com.lcl.scs.r9333.lpv.po.service.LpvPoInterfaceService;
@Service
public class LpvPoInterfaceServiceImpl implements LpvPoInterfaceService {
	@Autowired
	private LpvPoInterfaceRepository lpvPoInterfaceRepository;

	@Override
	public List<LpvPoInterface> findByPurchaseOrderIdOrderByLoadingDateDesc(String purchaseOrderId) {
		// TODO Auto-generated method stub
		return lpvPoInterfaceRepository.findByPurchaseOrderIdOrderByLoadingDateDesc(purchaseOrderId);
	}

	@Override
	public LpvPoInterface findByOriginalFileName(String originalFileName) {
		// TODO Auto-generated method stub
		return lpvPoInterfaceRepository.findByOriginalFileName(originalFileName);
	}

	@Override
	public List<LpvPoInterface> findByProcessIndicatorOrderByLoadingDateAsc(String processIndicator) {
		// TODO Auto-generated method stub
		return lpvPoInterfaceRepository.findByProcessIndicatorOrderByLoadingDateAsc(processIndicator);
	}

	@Override
	public void saveOrUpdateLpvPoInterface(LpvPoInterface l) {
		// TODO Auto-generated method stub
		lpvPoInterfaceRepository.save(l);
	}

	@Override
	public LpvPoInterface findFirstByPurchaseOrderIdOrderByLoadingDateDesc(String purchaseOrderId) {
		// TODO Auto-generated method stub
		return lpvPoInterfaceRepository.findFirstByPurchaseOrderIdOrderByLoadingDateDesc(purchaseOrderId);
	}

	@Override
	public void deleteById(String id) {
		// TODO Auto-generated method stub
		lpvPoInterfaceRepository.deleteById(id);
	}

	@Override
	public void deleteByByReadyToArchiveAndLoadingDateLessThanEqual(String readyToArchive, Date loadingDate) {
		// TODO Auto-generated method stub
		lpvPoInterfaceRepository.deleteByByReadyToArchiveAndLoadingDateLessThanEqual(readyToArchive, loadingDate);
	}

	@Override
	public List<LpvPoInterface> findByProcessIndicatorOrderById(String processIndicator) {
		// TODO Auto-generated method stub
		 List<LpvPoInterface> result=lpvPoInterfaceRepository.findByProcessIndicatorOrderById(processIndicator);
		 
		 for(LpvPoInterface po:result) {
			 po.setProcessIndicator("P");
			 saveOrUpdateLpvPoInterface(po);
		 }
		 
		 return result;
	}

	@Override
	public List<LpvPoInterface> findByProcessIndicatorOrderByPurchaseOrderIdAscIdAsc(String processIndicator) {
		// TODO Auto-generated method stub
		return lpvPoInterfaceRepository.findByProcessIndicatorOrderByPurchaseOrderIdAscIdAsc(processIndicator);
	}

}
