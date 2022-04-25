package com.lcl.scs.r9333.lpv.po.repository;

import java.util.Date;
import java.util.List;

import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.stereotype.Repository;

import com.lcl.scs.r9333.lpv.po.model.LpvPoInterface;

@Repository
public interface LpvPoInterfaceRepository extends MongoRepository<LpvPoInterface, String> {

	List<LpvPoInterface> findByPurchaseOrderIdOrderByLoadingDateDesc(String purchaseOrderId);

	//List<LpvPoInterface> findByReadyToArchiveAndLoadingDateLessThanEqualAsc(String readyToArchive, Date loadingDate);

	LpvPoInterface findByOriginalFileName(String originalFileName);

	List<LpvPoInterface> findByProcessIndicatorOrderByLoadingDateAsc(String processIndicator);

	LpvPoInterface findFirstByPurchaseOrderIdOrderByLoadingDateDesc(String purchaseOrderId);

	@Query(value = "{'id' : $0}", delete = true)
	void deleteById(String id);

	@Query(value = "{'readyToArchive':{$eq:$0}, loadingDate:{$lte:$1}}", delete = true)
	void deleteByByReadyToArchiveAndLoadingDateLessThanEqual(String readyToArchive, Date loadingDate);
	
	List<LpvPoInterface> findByProcessIndicatorOrderById(String processIndicator);
	
	List<LpvPoInterface> findByProcessIndicatorOrderByPurchaseOrderIdAscIdAsc(String processIndicator);
}
