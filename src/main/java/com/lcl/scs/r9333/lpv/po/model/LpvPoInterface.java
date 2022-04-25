package com.lcl.scs.r9333.lpv.po.model;

import java.util.Date;
import java.util.List;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Document(collection = "R9333LpvPoInterface")
public class LpvPoInterface {
	@Id
	private String id;
	private Date loadingDate;
	private String processIndicator;
	private Date idocProcessedTime;
	private String readyToArchive;
	private String originalFileName;
	private String targetPOCSVFileName;
	private String targetDelvCSVFileName;
	private String purchaseOrderId;
	private String iDocSerialNumber;
	private Date iDocCreateDate;
	private String purchaseOrderType;
	private String OrderState;
	private Date erpCreationDate;
	private String purchasingGroup;
	private String purchasingOrg;
	private String asnId;
	private String billTo;
	private String buyerName;
	private String supplierName;
	private String udfshipToSiteName;
	private Date c3Appointment;
	private String customerName;
	private String erpOrderType;
	private String erpTransactionId;
	private String hazardClass;
	private String incoTerms1;
	private String incoTerms2;
	private String LOB;
	private Date needByDate;
	private String operationName;
	private String poCloseInd;
	private String poDeliveryScheduleId;
	private String poLineId;
	private String processType;
	private String requisitionCreateDate;
	private String requisitionNo;
	private String salesOrderLineNo;
	private String salesOrderNo;
	private String salesOrderType;
	private int dcPallet;
	private int vanPallet;
	private String soldTo;
	private String sourceErpSystem;
	private String supplierItemDescription;
	private String supplierItemName;
	private String supplierItemOwnerName;
	private String udfcurrency;
	private String udfFromSite;
	private Date udfneedByDate;
	private Date udfrequestedShipDate;
	private int udftotalConfCount;
	private int udftotalReqCount;
	private int udfDCPallet;
	private int udfVendorPallet;
	private Date poClosedDate;
	private Date udfErpCrtnDate;
	private Date udfErpChgDate;
	private Date udfAsnCrtnDate;
	private String messageType;
	private int processedCount;
    private Boolean isItemCategoryValid;
    private String idocOutputType;
    private int totalGrQty;
	public Boolean getIsItemCategoryValid() {
		return isItemCategoryValid;
	}

	public void setIsItemCategoryValid(Boolean isItemCategoryValid) {
		this.isItemCategoryValid = isItemCategoryValid;
	}

	List<LpvPoDetailInterface> poDetails;

	public int getProcessedCount() {
		return processedCount;
	}

	public void setProcessedCount(int processedCount) {
		this.processedCount = processedCount;
	}

	public Date getIdocProcessedTime() {
		return idocProcessedTime;
	}

	public String getAsnId() {
		return asnId;
	}

	public String getBillTo() {
		return billTo;
	}

	public String getBuyerName() {
		return buyerName;
	}

	public Date getC3Appointment() {
		return c3Appointment;
	}
	
	public String getCustomerName() {
		return customerName;
	}

	public Date getErpCreationDate() {
		return erpCreationDate;
	}

	public String getErpOrderType() {
		return erpOrderType;
	}

	public String getErpTransactionId() {
		return erpTransactionId;
	}

	public String getHazardClass() {
		return hazardClass;
	}

	public String getIncoTerms1() {
		return incoTerms1;
	}

	public String getIncoTerms2() {
		return incoTerms2;
	}

	public String getLOB() {
		return LOB;
	}

	public String getOperationName() {
		return operationName;
	}

	public String getOrderState() {
		return OrderState;
	}

	public String getPoCloseInd() {
		return poCloseInd;
	}

	public String getPoDeliveryScheduleId() {
		return poDeliveryScheduleId;
	}

	public String getPoLineId() {
		return poLineId;
	}

	public String getProcessType() {
		return processType;
	}

	public String getPurchaseOrderId() {
		return purchaseOrderId;
	}

	public String getPurchaseOrderType() {
		return purchaseOrderType;
	}

	public String getPurchasingGroup() {
		return purchasingGroup;
	}

	public String getPurchasingOrg() {
		return purchasingOrg;
	}

	public String getRequisitionCreateDate() {
		return requisitionCreateDate;
	}

	public String getRequisitionNo() {
		return requisitionNo;
	}

	public String getSalesOrderLineNo() {
		return salesOrderLineNo;
	}

	public String getSalesOrderNo() {
		return salesOrderNo;
	}

	public String getSalesOrderType() {
		return salesOrderType;
	}

	public String getSoldTo() {
		return soldTo;
	}

	public String getSourceErpSystem() {
		return sourceErpSystem;
	}

	public String getSupplierItemDescription() {
		return supplierItemDescription;
	}

	public String getSupplierItemName() {
		return supplierItemName;
	}

	public String getSupplierItemOwnerName() {
		return supplierItemOwnerName;
	}

	public String getSupplierName() {
		return supplierName;
	}

	public String getUdfcurrency() {
		return udfcurrency;
	}

	public String getUdfFromSite() {
		return udfFromSite;
	}

	public Date getUdfneedByDate() {
		return udfneedByDate;
	}

	public Date getUdfrequestedShipDate() {
		return udfrequestedShipDate;
	}

	public String getUdfshipToSiteName() {
		return udfshipToSiteName;
	}

	public int getUdftotalConfCount() {
		return udftotalConfCount;
	}

	public int getUdftotalReqCount() {
		return udftotalReqCount;
	}

	public void setAsnId(String asnId) {
		this.asnId = asnId;
	}

	public void setBillTo(String billTo) {
		this.billTo = billTo;
	}

	public void setBuyerName(String buyerName) {
		this.buyerName = buyerName;
	}

	public void setC3Appointment(Date c3Appointment) {
		this.c3Appointment = c3Appointment;
	}

	public void setCustomerName(String customerName) {
		this.customerName = customerName;
	}

	public void setErpCreationDate(Date erpCreationDate) {
		this.erpCreationDate = erpCreationDate;
	}

	public void setErpOrderType(String erpOrderType) {
		this.erpOrderType = erpOrderType;
	}

	public void setErpTransactionId(String erpTransactionId) {
		this.erpTransactionId = erpTransactionId;
	}

	public void setHazardClass(String hazardClass) {
		this.hazardClass = hazardClass;
	}

	public void setIncoTerms1(String incoTerms1) {
		this.incoTerms1 = incoTerms1;
	}

	public void setIncoTerms2(String incoTerms2) {
		this.incoTerms2 = incoTerms2;
	}

	public void setLOB(String lOB) {
		LOB = lOB;
	}

	public void setOperationName(String operationName) {
		this.operationName = operationName;
	}

	public void setOrderState(String orderState) {
		OrderState = orderState;
	}

	public void setPoCloseInd(String poCloseInd) {
		this.poCloseInd = poCloseInd;
	}

	public void setPoDeliveryScheduleId(String poDeliveryScheduleId) {
		this.poDeliveryScheduleId = poDeliveryScheduleId;
	}

	public void setPoLineId(String poLineId) {
		this.poLineId = poLineId;
	}

	public void setProcessType(String processType) {
		this.processType = processType;
	}

	public void setPurchaseOrderId(String purchaseOrderId) {
		this.purchaseOrderId = purchaseOrderId;
	}

	public void setPurchaseOrderType(String purchaseOrderType) {
		this.purchaseOrderType = purchaseOrderType;
	}

	public void setPurchasingGroup(String purchasingGroup) {
		this.purchasingGroup = purchasingGroup;
	}

	public void setPurchasingOrg(String purchasingOrg) {
		this.purchasingOrg = purchasingOrg;
	}

	public void setRequisitionCreateDate(String requisitionCreateDate) {
		this.requisitionCreateDate = requisitionCreateDate;
	}

	public void setRequisitionNo(String requisitionNo) {
		this.requisitionNo = requisitionNo;
	}

	public void setSalesOrderLineNo(String salesOrderLineNo) {
		this.salesOrderLineNo = salesOrderLineNo;
	}

	public void setSalesOrderNo(String salesOrderNo) {
		this.salesOrderNo = salesOrderNo;
	}

	public void setSalesOrderType(String salesOrderType) {
		this.salesOrderType = salesOrderType;
	}

	public void setSoldTo(String soldTo) {
		this.soldTo = soldTo;
	}

	public void setSourceErpSystem(String sourceErpSystem) {
		this.sourceErpSystem = sourceErpSystem;
	}

	public void setSupplierItemDescription(String supplierItemDescription) {
		this.supplierItemDescription = supplierItemDescription;
	}

	public void setSupplierItemName(String supplierItemName) {
		this.supplierItemName = supplierItemName;
	}

	public void setSupplierItemOwnerName(String supplierItemOwnerName) {
		this.supplierItemOwnerName = supplierItemOwnerName;
	}

	public void setSupplierName(String supplierName) {
		this.supplierName = supplierName;
	}

	public void setUdfcurrency(String udfcurrency) {
		this.udfcurrency = udfcurrency;
	}

	public void setUdfFromSite(String udfFromSite) {
		this.udfFromSite = udfFromSite;
	}

	public void setUdfneedByDate(Date udfneedByDate) {
		this.udfneedByDate = udfneedByDate;
	}

	public void setUdfrequestedShipDate(Date udfrequestedShipDate) {
		this.udfrequestedShipDate = udfrequestedShipDate;
	}

	public void setUdfshipToSiteName(String udfshipToSiteName) {
		this.udfshipToSiteName = udfshipToSiteName;
	}

	public void setUdftotalConfCount(int udftotalConfCount) {
		this.udftotalConfCount = udftotalConfCount;
	}

	public void setUdftotalReqCount(int udftotalReqCount) {
		this.udftotalReqCount = udftotalReqCount;
	}

	public String getiDocSerialNumber() {
		return iDocSerialNumber;
	}

	public void setiDocSerialNumber(String iDocSerialNumber) {
		this.iDocSerialNumber = iDocSerialNumber;
	}

	public Date getiDocCreateDate() {
		return iDocCreateDate;
	}

	public void setiDocCreateDate(Date iDocCreateDate) {
		this.iDocCreateDate = iDocCreateDate;
	}

	public List<LpvPoDetailInterface> getPoDetails() {
		return poDetails;
	}

	public void setPoDetails(List<LpvPoDetailInterface> poDetails) {
		this.poDetails = poDetails;
	}

	public Date getNeedByDate() {
		return needByDate;
	}

	public void setNeedByDate(Date needByDate) {
		this.needByDate = needByDate;
	}

	public int getDcPallet() {
		return dcPallet;
	}

	public void setDcPallet(int dcPallet) {
		this.dcPallet = dcPallet;
	}

	public int getVanPallet() {
		return vanPallet;
	}

	public void setVanPallet(int vanPallet) {
		this.vanPallet = vanPallet;
	}

	public Date getLoadingDate() {
		return loadingDate;
	}

	public void setLoadingDate(Date loadingDate) {
		this.loadingDate = loadingDate;
	}

	public String getOriginalFileName() {
		return originalFileName;
	}

	public void setOriginalFileName(String originalFileName) {
		this.originalFileName = originalFileName;
	}

	public String getProcessIndicator() {
		return processIndicator;
	}

	public void setProcessIndicator(String processIndicator) {
		this.processIndicator = processIndicator;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getTargetPOCSVFileName() {
		return targetPOCSVFileName;
	}

	public void setTargetPOCSVFileName(String targetPOCSVFileName) {
		this.targetPOCSVFileName = targetPOCSVFileName;
	}

	public String getTargetDelvCSVFileName() {
		return targetDelvCSVFileName;
	}

	public void setTargetDelvCSVFileName(String targetDelvCSVFileName) {
		this.targetDelvCSVFileName = targetDelvCSVFileName;
	}

	public int getUdfDCPallet() {
		return udfDCPallet;
	}

	public void setUdfDCPallet(int udfDCPallet) {
		this.udfDCPallet = udfDCPallet;
	}

	public String getReadyToArchive() {
		return readyToArchive;
	}

	public void setReadyToArchive(String readyToArchive) {
		this.readyToArchive = readyToArchive;
	}

	public int getUdfVendorPallet() {
		return udfVendorPallet;
	}

	public void setUdfVendorPallet(int udfVendorPallet) {
		this.udfVendorPallet = udfVendorPallet;
	}

	public Date getPoClosedDate() {
		return poClosedDate;
	}

	public void setPoClosedDate(Date poClosedDate) {
		this.poClosedDate = poClosedDate;
	}

	public Date getUdfErpCrtnDate() {
		return udfErpCrtnDate;
	}

	public void setUdfErpCrtnDate(Date udfErpCrtnDate) {
		this.udfErpCrtnDate = udfErpCrtnDate;
	}

	public Date getUdfErpChgDate() {
		return udfErpChgDate;
	}

	public void setUdfErpChgDate(Date udfErpChgDate) {
		this.udfErpChgDate = udfErpChgDate;
	}

	public Date getUdfAsnCrtnDate() {
		return udfAsnCrtnDate;
	}

	public void setUdfAsnCrtnDate(Date udfAsnCrtnDate) {
		this.udfAsnCrtnDate = udfAsnCrtnDate;
	}

	public String getMessageType() {
		return messageType;
	}

	public void setMessageType(String messageType) {
		this.messageType = messageType;
	}

	public void setIdocProcessedTime(Date idocProcessedTime) {
		this.idocProcessedTime=idocProcessedTime;
		
	}

	public String getIdocOutputType() {
		return idocOutputType;
	}

	public void setIdocOutputType(String idocOutputType) {
		this.idocOutputType = idocOutputType;
	}

	public int getTotalGrQty() {
		return totalGrQty;
	}

	public void setTotalGrQty(int totalGrQty) {
		this.totalGrQty = totalGrQty;
	}

}
