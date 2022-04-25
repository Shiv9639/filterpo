package com.lcl.scs.r9333.lpv.po.model;

import java.time.ZonedDateTime;
import java.util.Date;

public class LpvPoDetailInterface {
	private String poLineId;
	private String customerItemName;
	private String materialGroup;
	private double orderQuantity;
	private double unitPrice;
	private double poDollarValue;
	private String orderUom;
	private String lineState;
	private String shipTo;
	private String udfshipFromSiteName;
	private Date needByDate;
	private String cancelInd;
	private String customerItemOwnerName;
	private String shipFromSiteName;
	private String lineStatus;
	private double confirmedQuantity;
	private Date confirmedDeliveryDate;
	private String hlItem;
	private String shipToSiteName;
	private Date confirmedShipDate;
	private String pstyp;
	private double grQuantity;
	private String deliveryCompletionIndicator;
	private double lineVendorPallet;
	public double getPoDollarValue(LpvPoInterface po) {
			poDollarValue = 0.0;
			for (LpvPoDetailInterface poDetail : po.getPoDetails()) {
				if (poDetail.getCancelInd().isEmpty() || poDetail.getCancelInd()==null || poDetail.getCancelInd().equals("")) {
					poDollarValue = ((poDetail.getOrderQuantity()*poDetail.getUnitPrice())+poDollarValue);
				}
				else {
					poDetail.setPoDollarValue(poDollarValue);
				}
			}
		return poDollarValue;
	}

	public void setPoDollarValue(double poDollarValue) {
		this.poDollarValue = poDollarValue;
	}

	public String getPoLineId() {
		return poLineId;
	}

	public void setPoLineId(String poLineId) {
		this.poLineId = poLineId;
	}

	public String getCustomerItemName() {
		return customerItemName;
	}

	public void setCustomerItemName(String customerItemName) {
		this.customerItemName = customerItemName;
	}

	public String getMaterialGroup() {
		return materialGroup;
	}

	public void setMaterialGroup(String materialGroup) {
		this.materialGroup = materialGroup;
	}

	public double getOrderQuantity() {
		return orderQuantity;
	}

	public void setOrderQuantity(double orderQuantity) {
		this.orderQuantity = orderQuantity;
	}

	public double getUnitPrice() {
		return unitPrice;
	}

	public void setUnitPrice(double unitPrice) {
		this.unitPrice = unitPrice;
	}

	public String getOrderUom() {
		return orderUom;
	}

	public void setOrderUom(String orderUom) {
		this.orderUom = orderUom;
	}

	public String getLineState() {
		return lineState;
	}

	public void setLineState(String lineState) {
		this.lineState = lineState;
	}

	public String getShipTo() {
		return shipTo;
	}

	public void setShipTo(String shipTo) {
		this.shipTo = shipTo;
	}

	public String getUdfshipFromSiteName() {
		return udfshipFromSiteName;
	}

	public void setUdfshipFromSiteName(String udfshipFromSiteName) {
		this.udfshipFromSiteName = udfshipFromSiteName;
	}

	public Date getNeedByDate() {
		return needByDate;
	}

	public void setNeedByDate(Date needByDate) {
		this.needByDate = needByDate;
	}

	public String getCancelInd() {
		return cancelInd;
	}

	public void setCancelInd(String cancelInd) {
		this.cancelInd = cancelInd;
	}

	public String getCustomerItemOwnerName() {
		return customerItemOwnerName;
	}

	public void setCustomerItemOwnerName(String customerItemOwnerName) {
		this.customerItemOwnerName = customerItemOwnerName;
	}

	public String getShipFromSiteName() {
		return shipFromSiteName;
	}

	public void setShipFromSiteName(String shipFromSiteName) {
		this.shipFromSiteName = shipFromSiteName;
	}

	public String getLineStatus() {
		return lineStatus;
	}

	public void setLineStatus(String lineStatus) {
		this.lineStatus = lineStatus;
	}

	public double getConfirmedQuantity() {
		return confirmedQuantity;
	}

	public void setConfirmedQuantity(double confirmedQuantity) {
		this.confirmedQuantity = confirmedQuantity;
	}

	public Date getConfirmedDeliveryDate() {
		return confirmedDeliveryDate;
	}

	public void setConfirmedDeliveryDate(Date confirmedDeliveryDate) {
		this.confirmedDeliveryDate = confirmedDeliveryDate;
	}

	public String getHlItem() {
		return hlItem;
	}

	public void setHlItem(String hlItem) {
		this.hlItem = hlItem;
	}

	public String getShipToSiteName() {
		return shipToSiteName;
	}

	public void setShipToSiteName(String shipToSiteName) {
		this.shipToSiteName = shipToSiteName;
	}

	public Date getConfirmedShipDate() {
		return confirmedShipDate;
	}

	public void setConfirmedShipDate(Date confirmedShipDate) {
		this.confirmedShipDate = confirmedShipDate;
	}

	public String getPstyp() {
		return pstyp;
	}

	public void setPstyp(String pstyp) {
		this.pstyp = pstyp;
	}

	public double getGrQuantity() {
		return grQuantity;
	}

	public void setGrQuantity(double grQuantity) {
		this.grQuantity = grQuantity;
	}

	public String getDeliveryCompletionIndicator() {
		return deliveryCompletionIndicator;
	}

	public void setDeliveryCompletionIndicator(String deliveryCompletionIndicator) {
		this.deliveryCompletionIndicator = deliveryCompletionIndicator;
	}

	public double getLineVendorPallet() {
		return lineVendorPallet;
	}

	public void setLineVendorPallet(double lineVendorPallet) {
		this.lineVendorPallet = lineVendorPallet;
	}

}
