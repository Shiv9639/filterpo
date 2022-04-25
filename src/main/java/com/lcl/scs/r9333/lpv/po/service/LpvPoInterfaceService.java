package com.lcl.scs.r9333.lpv.po.service;

import java.util.Date;
import java.util.List;

import org.springframework.stereotype.Service;

import com.lcl.scs.r9333.lpv.po.model.LpvPoInterface;

@Service
public interface LpvPoInterfaceService {
	List<LpvPoInterface> findByPurchaseOrderIdOrderByLoadingDateDesc(String purchaseOrderId);

	LpvPoInterface findByOriginalFileName(String originalFileName);

	List<LpvPoInterface> findByProcessIndicatorOrderByLoadingDateAsc(String processIndicator);
	
	void saveOrUpdateLpvPoInterface(LpvPoInterface l);
	
	LpvPoInterface findFirstByPurchaseOrderIdOrderByLoadingDateDesc(String purchaseOrderId);
	
	void deleteById(String id);
	
	void deleteByByReadyToArchiveAndLoadingDateLessThanEqual(String readyToArchive, Date loadingDate);
	
	List<LpvPoInterface> findByProcessIndicatorOrderById(String processIndicator);
	
	List<LpvPoInterface> findByProcessIndicatorOrderByPurchaseOrderIdAscIdAsc(String processIndicator);
}
