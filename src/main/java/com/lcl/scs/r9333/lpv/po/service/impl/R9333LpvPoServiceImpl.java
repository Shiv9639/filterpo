package com.lcl.scs.r9333.lpv.po.service.impl;

import java.io.File;
import java.io.FileWriter;
import java.io.StringReader;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.Map.Entry;

import javax.json.Json;
import javax.json.JsonMergePatch;
import javax.json.JsonPatch;
import javax.json.JsonStructure;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;

import com.fasterxml.jackson.databind.ObjectMapper;
//import com.lcl.scs.constants.LpvConstants.EntityName;
//import com.lcl.scs.constants.LpvConstants.LpvEndpointUrl;
import com.lcl.scs.r9333.lpv.po.model.ArticleDCNodes;
import com.lcl.scs.r9333.lpv.po.model.LpvLanes;
import com.lcl.scs.r9333.lpv.po.model.LpvLocationTimeZone;
import com.lcl.scs.r9333.lpv.po.model.LpvPoDetailInterface;
import com.lcl.scs.r9333.lpv.po.model.LpvPoInterface;
import com.lcl.scs.r9333.lpv.po.model.LpvReasonCode;
import com.lcl.scs.r9333.lpv.po.model.LpvReasonCodeTransaction;
import com.lcl.scs.r9333.lpv.po.model.VendorMaster;
import com.lcl.scs.r9333.lpv.po.repository.LpvArticleNodesRepository;
import com.lcl.scs.r9333.lpv.po.service.LPVArticleNodeService;
import com.lcl.scs.r9333.lpv.po.service.LpvLanesService;
import com.lcl.scs.r9333.lpv.po.service.LpvLocationTimeZoneService;
import com.lcl.scs.r9333.lpv.po.service.LpvPoInterfaceService;
import com.lcl.scs.r9333.lpv.po.service.LpvReasonCodeService;
import com.lcl.scs.r9333.lpv.po.service.LpvVendorMasterService;
import com.lcl.scs.r9333.lpv.po.service.R9333LpvPoService;
//import com.lcl.scs.email.service.impl.AutomatedLanesEmail;
import com.lcl.scs.subscribers.model.Subscribers;
import com.lcl.scs.util.DateFormater;
import com.lcl.scs.util.JSonStringFormatter;
import com.lcl.scs.util.logging.LoggingUtilities;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.result.DeleteResult;

public class R9333LpvPoServiceImpl implements R9333LpvPoService {
	private static final SimpleDateFormat DATEFORMAT = new SimpleDateFormat("EEE yyyy-MM-dd HH:mm:ss z");
	private static final SimpleDateFormat LPVDATEFORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
	private static final SimpleDateFormat DATETIMEFORMATTER = new SimpleDateFormat("yyyyMMddHHmmss");
	private static final SimpleDateFormat DATEFORMATTER = new SimpleDateFormat("yyyyMMdd");
	private LpvPoInterfaceService lpvPoInterfaceService;
	private LpvLocationTimeZoneService lpvLocationTimeZoneService;
	private LpvLanesService lpvLanesService;
	private LpvVendorMasterService lpvVendorMasterService;
	private LPVArticleNodeService lpvArticleService;
	private LpvReasonCodeService lpvReasonCodeService;;
	private String mongodbDatabase;
	private String mongodbURI;
	private com.lcl.scs.subscribers.service.SubscribersService subscribersService;
	// private com.lcl.scs.email.service.impl.AutomatedLanesEmail
	// automatedLanesEmail;

	private List<LpvReasonCode> reasonCodeList = new ArrayList<LpvReasonCode>();

	HashMap<String, String> poTypes = new HashMap<String, String>();
	HashMap<String, LpvReasonCode> reasonCodeMap = new HashMap<String, LpvReasonCode>();

	private int LAST_RUN_TIME_BUFFER = 0;

	private String[] PO_CHANGE_POINTER_FIELD_LIST = { "purchaseOrderId", "poLineId", "supplierName", "udfFromSite",
			"shipFromSiteName", "incoTerms1", "incoTerms2", "udfcurrency", "shipTo", "udfshipToSiteName",
			"erpOrderType", "customerItemName", "materialGroup", "purchasingOrg", "buyerName", "orderQuantity",
			"unitPrice", "orderUom", "shipTo", "poCloseInd", "confirmedQuantity", "udftotalConfCount",
			"udftotalReqCount", "erpOrderType", "processType", "customerItemOwnerName", "customerName", "poCloseInd",
			"erpCreationDate", "udfrequestedShipDate", "needByDate", "confirmedDeliveryDate", "confirmedQuantity",
			"confirmedShipDate", "supplierName", "udfshipFromSiteName", "udfneedByDate", "c3Appointment",
			"operationName", "cancelInd", "udfDCPallet", "vanPallet", "sourceErpSystem" };

	private String[] PO_DETAIL_CHANGE_POINTER_FIELD_LIST = { "poLineId", "customerItemName", "materialGroup",
			"orderQuantity", "unitPrice", "orderUom", "lineState", "shipTo", "udfshipFromSiteName", "needByDate",
			"cancelInd", "customerItemOwnerName", "shipFromSiteName", "lineStatus", "confirmedQuantity",
			"confirmedDeliveryDate", "hlItem", "shipToSiteName", "confirmedShipDate", "pstyp", "grQuantity",
			"unitPrice", "poDollarValue" };

	private String[] PO_CHANGE_POINTER_EXCLUDING_FIELD_LIST = { "id", "loadingDate", "processIndicator",
			"originalFileName", "targetPOCSVFileName", "targetDelvCSVFileName", "erpTransactionId",
			// To ignore Pallet fields from Change Pointer for now
			"udfDCPallet", "vanPallet" };

	private final String estTimeZone = "Canada/Eastern";

	private LpvPoInterface getLastR9333toLpvPOExtract(String poNumber) throws Exception {
		LpvPoInterface po = new LpvPoInterface();

		try {
			po = lpvPoInterfaceService.findFirstByPurchaseOrderIdOrderByLoadingDateDesc(poNumber);
		} catch (Exception ex) {
			throw ex;
		}

		return po;
	}

	public String getMongodbDatabase() {
		return mongodbDatabase;
	}

	public String getMongodbURI() {
		return mongodbURI;
	}

	public com.lcl.scs.subscribers.service.SubscribersService getSubscribersService() {
		return subscribersService;
	}

	private LpvPoInterface parseR9333toLpvPO(Document doc) throws Exception {
		LpvPoInterface po = new LpvPoInterface();

		String dcTimeZone = estTimeZone;

		try {
			Document SapOrders05Zorders0501 = (Document) doc.get("NS1:SapOrders05Zorders0501");
			Document SapOrders05Zorders0501IDocBO = (Document) SapOrders05Zorders0501
					.get("SapOrders05Zorders0501IDocBO");
			Document SapOrders05Zorders0501DataRecord = (Document) SapOrders05Zorders0501IDocBO
					.get("SapOrders05Zorders0501DataRecord");
			Document E1EDK01 = (Document) SapOrders05Zorders0501DataRecord.get("SapOrders05Zorders0501E2edk01005");

			po.setPurchaseOrderId(E1EDK01.get("BELNR").toString());
			po.setErpOrderType(E1EDK01.getString("BSART"));

			po.setPurchaseOrderType("standardPO");
			po.setProcessType("supply");
			po.setCustomerName("Loblaw");
			po.setOperationName("CreateOrder");
			po.setProcessIndicator("N");
			po.setReadyToArchive("N");
			po.setIsItemCategoryValid(true);
			po.setUdfcurrency(E1EDK01.getString("CURCY"));
			po.setUdfDCPallet(
					E1EDK01.get("ABRVW_BEZ") != null ? Integer.parseInt(E1EDK01.get("ABRVW_BEZ").toString()) : 0);

			List<Document> E1EDKA1Docs = (List<Document>) SapOrders05Zorders0501DataRecord
					.get("SapOrders05Zorders0501E2edka1003");
			for (Document E1EDKA1 : E1EDKA1Docs) {
				if (E1EDKA1.getString("PARVW") != null && E1EDKA1.getString("PARVW").equals("LF")) {
					po.setSupplierName(E1EDKA1.getString("PARTN"));
				}
			}

			List<Document> E1EDK03Docs = (List<Document>) SapOrders05Zorders0501DataRecord
					.get("SapOrders05Zorders0501E2edk03");
			for (Document E1EDK03 : E1EDK03Docs) {
				if (E1EDK03.getInteger("DATUM") != null && E1EDK03.get("IDDAT").toString().equals("012"))
					po.setErpCreationDate(java.util.Date.from(ZonedDateTime
							.ofInstant(DATEFORMATTER.parse(E1EDK03.getInteger("DATUM").toString()).toInstant(),
									ZoneId.of(estTimeZone))
							.toInstant()));
			}

			List<Document> E1EDK14Docs = (List<Document>) SapOrders05Zorders0501DataRecord
					.get("SapOrders05Zorders0501E2edk14");
			for (Document E1EDK14 : E1EDK14Docs) {
				if (E1EDK14.get("QUALF") != null && E1EDK14.get("QUALF").toString().equals("009")) {
					po.setBuyerName(E1EDK14.get("ORGID").toString());
					po.setPurchasingGroup(po.getBuyerName());
				} else if (E1EDK14.get("QUALF") != null && E1EDK14.get("QUALF").toString().equals("014")) {
					po.setPurchasingOrg(E1EDK14.get("ORGID").toString());
				}
			}

			try {
				Document E1EDK17 = (Document) SapOrders05Zorders0501DataRecord.get("SapOrders05Zorders0501E2edk17");
				if (E1EDK17.get("QUALF") != null && E1EDK17.get("QUALF").toString().equals("001")) {
					po.setIncoTerms1(E1EDK17.get("LKOND").toString());
					po.setIncoTerms2("");
				} else if (E1EDK17.get("QUALF") != null && E1EDK17.get("QUALF").toString().equals("002")) {
					po.setIncoTerms2(E1EDK17.get("LKOND").toString());
				}
			} catch (Exception ex) {
				List<Document> E1EDK17Docs = new ArrayList();
				E1EDK17Docs = (List<Document>) SapOrders05Zorders0501DataRecord.get("SapOrders05Zorders0501E2edk17");
				if (E1EDK17Docs != null && E1EDK17Docs.size() > 0)
					for (Document E1EDK17 : E1EDK17Docs) {
						if (E1EDK17.get("QUALF") != null && E1EDK17.get("QUALF").toString().equals("001")) {
							po.setIncoTerms1(E1EDK17.get("LKOND").toString());
							po.setIncoTerms2("");
						} else if (E1EDK17.get("QUALF") != null && E1EDK17.get("QUALF").toString().equals("002")
								&& E1EDK17.get("LKTEXT") != null) {
							po.setIncoTerms2(E1EDK17.get("LKTEXT").toString());
						}
					}
			}

			if (SapOrders05Zorders0501IDocBO != null) {
				Document SapIDocControlRecord = (Document) SapOrders05Zorders0501IDocBO.get("SapIDocControlRecord");
				if (SapIDocControlRecord != null) {
					po.setSourceErpSystem(SapIDocControlRecord.getString("SNDPOR"));
					String createDate = SapIDocControlRecord.get("CREDAT").toString();
					String createTime = SapIDocControlRecord.get("CRETIM").toString();
					po.setiDocCreateDate(DATETIMEFORMATTER.parse(createDate + createTime));
					po.setiDocSerialNumber(SapIDocControlRecord.getString("DOCNUM"));
					// LTSCIH-276
					// -----------------------------------------------------------------------------
					po.setMessageType(SapIDocControlRecord.getString("MESTYP"));
					if (po.getMessageType() != null && po.getMessageType().equalsIgnoreCase("ORDERS")) {
						po.setErpCreationDate(po.getiDocCreateDate());
						po.setUdfErpCrtnDate(po.getiDocCreateDate());
					} else if (po.getMessageType() != null && po.getMessageType().equalsIgnoreCase("ORDCHG")) {
						po.setUdfErpChgDate(po.getiDocCreateDate());
					}
					// -----------------------------------------------------------------------------
				}
				po.setOriginalFileName(doc.getString("Name"));

//				po.setOriginalFileName(doc.getString("Name"));
//				po.setId("R9333-LPV-" + po.getOriginalFileName() + "-" + po.getiDocSerialNumber() + "-"
//						+ po.getPurchaseOrderId() + "-" + datetimeFormatter.format(new Date()));
				po.setErpTransactionId(po.getiDocSerialNumber());
			}

			// Document E1EDKA1 = (Document)
			// SapOrders05Zorders0501DataRecord.get("SapOrders05Zorders0501E2edka1003");

			Document Z1EDK35 = (Document) E1EDK01.get("SapOrders05Zorders0501Z2edk35000122376872");
			po.setPoCloseInd(Z1EDK35 != null ? Z1EDK35.getString("ZZPOSTATUS") : "");
			po.setIdocOutputType(Z1EDK35 != null ? Z1EDK35.getString("ZZKSCHL") : "");
			if (po.getPoCloseInd() != null
					&& (po.getPoCloseInd().equalsIgnoreCase("D") || po.getPoCloseInd().equalsIgnoreCase("F")))
				po.setReadyToArchive("Y");
			
			if (po.getPoCloseInd() == null || !po.getPoCloseInd().equalsIgnoreCase("F"))
				po.setPoCloseInd(null);
			else if (po.getPoCloseInd().equalsIgnoreCase("F"))
				po.setPoCloseInd("X");
//			LoggingUtilities.generateInfoLog("PO closed indicator: " + po.getPoCloseInd());	
			// LTSCIH-277
			// --------------------------------------------------------------------------------------
			if (po.getPoCloseInd() != null && po.getPoCloseInd().trim().equals("X")) {
				LpvPoInterface previousPO = getLastR9333toLpvPOExtract(po.getPurchaseOrderId());
				if (previousPO == null || previousPO.getPoCloseInd() == null
						|| !previousPO.getPoCloseInd().trim().equals("X"))
					po.setPoClosedDate(po.getiDocCreateDate());
			}
			// --------------------------------------------------------------------------------------

			if (Z1EDK35.get("ZZPICKDATE") != null && !Z1EDK35.get("ZZPICKDATE").toString().trim().equalsIgnoreCase(""))
				po.setUdfrequestedShipDate(DATETIMEFORMATTER.parse(Z1EDK35.get("ZZPICKDATE").toString() + "235959"));

			String apptDate = Z1EDK35.get("ZZAPPDTTM").toString();
			if (apptDate != null && !apptDate.trim().equals("")) {
				po.setC3Appointment(java.util.Date.from(ZonedDateTime
						.ofInstant(DATETIMEFORMATTER.parse(apptDate).toInstant(), ZoneId.of(dcTimeZone)).toInstant()));
			}

			List<Document> EDP01Docs = new ArrayList();
			try {
				EDP01Docs = (List<Document>) SapOrders05Zorders0501DataRecord.get("SapOrders05Zorders0501E2edp01008");
			} catch (Exception ex) {
				EDP01Docs.add((Document) SapOrders05Zorders0501DataRecord.get("SapOrders05Zorders0501E2edp01008"));
			}

			List<LpvPoDetailInterface> poDetails = new ArrayList<LpvPoDetailInterface>();

			int vanPallet = 0;
			int totalConfirmedQty = 0;
			int totalQty = 0;
			int totalGrQty = 0; 
			if (EDP01Docs != null)
				for (Document EDP01 : EDP01Docs) {
					LpvPoDetailInterface poDetail = new LpvPoDetailInterface();
					po.setUdfshipToSiteName(EDP01.get("WERKS").toString());
//					if ((EDP01.get("WERKS").toString().trim().equals("")) || EDP01.get("WERKS").equals(null)) {
//						// ReasonCode 04 DC not assigned for PO items
//						List<LpvReasonCode> lpvReasonCodeList = findReasonCode("04");
//						LpvReasonCodeTransaction lpvReasonCodeTransaction = lpvReasonCodeService
//								.publishLpvReasonCodeTransaction(lpvReasonCodeList, po);
//						lpvReasonCodeService.saveLpvReasonCodeTransaction(lpvReasonCodeTransaction);
//					} else {
//						po.setUdfshipToSiteName(EDP01.get("WERKS").toString());
//					}

					LpvLocationTimeZone locTimezone = lpvLocationTimeZoneService
							.findByLocation(po.getUdfshipToSiteName());

					dcTimeZone = po.getUdfshipToSiteName() != null && locTimezone != null ? locTimezone.getTimeZone()
							: estTimeZone;

					poDetail.setPstyp(EDP01.get("PSTYP").toString());
					if (poDetail.getPstyp() == null || !poDetail.getPstyp().equals("0"))
					{
						po.setIsItemCategoryValid(false);
						List<LpvReasonCode> lpvReasonCodeList = findReasonCode("22");
						LpvReasonCode lpvReasonCode=lpvReasonCodeList.get(0);
						String reasonCodeDescription=lpvReasonCode.getReasonCodeDescription();
						lpvReasonCode.setReasonCodeDescription(reasonCodeDescription+" (PSTYP)="+poDetail.getPstyp());
					LpvReasonCodeTransaction lpvReasonCodeTransaction = lpvReasonCodeService
							.publishLpvReasonCodeTransaction(lpvReasonCodeList, po);
					lpvReasonCodeService.saveLpvReasonCodeTransaction(lpvReasonCodeTransaction);
					lpvReasonCode.setReasonCodeDescription(reasonCodeDescription);
						break;
					}			
					poDetail.setPoLineId(EDP01.get("POSEX").toString());

					List<Document> E1EDP19Docs = (List<Document>) EDP01.get("SapOrders05Zorders0501E2edp19002");
					for (Document E1EDP19 : E1EDP19Docs) {
						if (E1EDP19.get("QUALF") != null && E1EDP19.get("QUALF").toString().equalsIgnoreCase("001")) {
							poDetail.setCustomerItemName(E1EDP19.get("IDTNR").toString());
						} else if (E1EDP19.get("QUALF") != null
								&& E1EDP19.get("QUALF").toString().equalsIgnoreCase("007")) {
							poDetail.setMaterialGroup(E1EDP19.get("IDTNR").toString());
						}
					}

//				Filter Condition is : Exclude Material group 'M99490101' from SAP while sending purchase order to JDA
					if (poDetail.getMaterialGroup().equals("M99490101"))
						continue;

					int poLineId = Integer.parseInt(poDetail.getPoLineId());
					if (poLineId % 10 != 0)
						continue;

					poDetail.setOrderQuantity(Double.parseDouble(EDP01.get("MENGE").toString()));
					

					poDetail.setUnitPrice(Double.parseDouble(EDP01.get("VPREI").toString()));
					poDetail.setOrderUom(EDP01.get("MENEE").toString());

					List<Document> EDP35Docs = new ArrayList();
					try {
						EDP35Docs = (List<Document>) EDP01.get("SapOrders05Zorders0501Z2edp35000");
					} catch (Exception ex) {
						EDP35Docs.add((Document) EDP01.get("SapOrders05Zorders0501Z2edp35000"));
					}
					Document EDP35 = EDP35Docs.get(0);// (Document) EDP01.get("SapOrders05Zorders0501Z2edp35000");
					poDetail.setLineState(EDP35.getString("ZZLOEKZ"));

					/*
					 * LLCT-248 Implementation :Delivery completion indicator implementation in SC
					 * HUB - LT (with OrderType "ZMAN" and "ZIPR" Only)
					 */

					if (EDP35.getString("ZZELIKZ") != null && !EDP35.getString("ZZELIKZ").isEmpty()) {
						poDetail.setDeliveryCompletionIndicator(EDP35.getString("ZZELIKZ"));
					} else {
						poDetail.setDeliveryCompletionIndicator("");
					}

					poDetail.setShipTo(po.getUdfshipToSiteName());
					poDetail.setUdfshipFromSiteName(po.getSupplierName());
					// poDetail.setNeedByDate(po.getC3Appointment());
					poDetail.setCancelInd(poDetail.getLineState());
				
					poDetail.setCustomerItemOwnerName("Loblaw");
					poDetail.setShipFromSiteName(po.getSupplierName());

					po.setUdfFromSite(poDetail.getShipFromSiteName());

					if (EDP35.get("ZZWEMNG").toString().trim().equals(""))
						poDetail.setConfirmedQuantity(0);
					else
						poDetail.setConfirmedQuantity(Double.parseDouble(EDP35.get("ZZWEMNG").toString()));

					if (EDP35.get("ZZMENGEGR") == null || EDP35.get("ZZMENGEGR").toString().trim().equals(""))
						poDetail.setGrQuantity(0);
					else if (EDP35.get("ZZMENGEGR") != null && !EDP35.get("ZZMENGEGR").toString().trim().equals(""))
						poDetail.setGrQuantity(Double.parseDouble(EDP35.get("ZZMENGEGR").toString()));

					totalConfirmedQty += poDetail.getConfirmedQuantity();

					if (po.getUdfrequestedShipDate() != null)
						poDetail.setConfirmedShipDate(
								DateFormater.convertLocalTimeToTimeZone(
										java.util.Date.from(ZonedDateTime.ofInstant(DATETIMEFORMATTER
												.parse(DATEFORMATTER.format(po.getUdfrequestedShipDate()) + "235900")
												.toInstant(), ZoneId.of(dcTimeZone)).toInstant()),
										dcTimeZone, estTimeZone));

//				if (EDP35.get("ZZPICKDATE") == null || EDP35.get("ZZPICKDATE").toString().trim().equals(""))
//					poDetail.setConfirmedShipDate(null);
//				else
//					poDetail.setConfirmedShipDate(java.util.Date.from(ZonedDateTime
//							.ofInstant(dateFormatter.parse(EDP35.get("ZZPICKDATE").toString() + "235900").toInstant(),
//									ZoneId.of(dcTimeZone))
//							.toInstant()));

					try {
						Document EDP20 = (Document) EDP01.get("SapOrders05Zorders0501E2edp20001");
						poDetail.setConfirmedDeliveryDate(
								DateFormater.convertLocalTimeToTimeZone(java.util.Date.from(ZonedDateTime.ofInstant(
										DATETIMEFORMATTER.parse(EDP20.get("EDATU").toString() + "235900").toInstant(),
										ZoneId.of(dcTimeZone)).toInstant()), dcTimeZone, estTimeZone));
						po.setUdfneedByDate(poDetail.getConfirmedDeliveryDate());
					} catch (Exception ex) {
						List<Document> EDP20s = new ArrayList();
						EDP20s = (List<Document>) EDP01.get("SapOrders05Zorders0501E2edp20001");
						for (Document EDP20 : EDP20s) {
							poDetail.setConfirmedDeliveryDate(DateFormater.convertLocalTimeToTimeZone(
									java.util.Date.from(ZonedDateTime
											.ofInstant(DATETIMEFORMATTER.parse(EDP20.get("EDATU").toString() + "235900")
													.toInstant(), ZoneId.of(dcTimeZone))
											.toInstant()),
									dcTimeZone, estTimeZone));
							break;
						}
						po.setUdfneedByDate(poDetail.getConfirmedDeliveryDate());
					}
					po.setNeedByDate(po.getUdfneedByDate());

					if (EDP35.getString("ZZLOEKZ").equalsIgnoreCase("L")
							|| EDP35.getString("ZZLOEKZ").equalsIgnoreCase("X")
							|| EDP35.getString("ZZLOEKZ").equalsIgnoreCase("S")) {
						poDetail.setLineStatus("Cancelled");
					} else if (poDetail.getConfirmedQuantity() <= 0) {
						poDetail.setLineStatus("Open");
					} else if (poDetail.getOrderQuantity() == poDetail.getConfirmedQuantity()
							&& (apptDate != null && !apptDate.trim().equals("")
									&& poDetail.getConfirmedDeliveryDate().compareTo(po.getC3Appointment()) == 0)) {
						poDetail.setLineStatus("Confirmed");
					} else if (poDetail.getOrderQuantity() != poDetail.getConfirmedQuantity()
							&& poDetail.getConfirmedQuantity() > 0
							|| (apptDate != null && !apptDate.trim().equals("")
									&& poDetail.getConfirmedDeliveryDate().compareTo(po.getC3Appointment()) != 0)) {
						poDetail.setLineStatus("Confirmed with Changes");
					} else
						poDetail.setLineStatus("Open");
					
					if(!poDetail.getLineStatus().equalsIgnoreCase("Cancelled")) {
						int lineVendorPallet = (int) Math.ceil(Double.parseDouble(
								EDP35.get("ZZVENDPALLET") == null ? "0" : EDP35.get("ZZVENDPALLET").toString())); 
						poDetail.setLineVendorPallet(lineVendorPallet);
						vanPallet += lineVendorPallet;
						totalQty += poDetail.getOrderQuantity();
						totalGrQty += poDetail.getGrQuantity();
						}
					poDetail.setHlItem(EDP35.getString("ZZUEBPO"));
					poDetail.setShipToSiteName(po.getUdfshipToSiteName());	
					poDetails.add(poDetail);
					
				}
			
			po.setPoDetails(poDetails);

			po.setDcPallet(E1EDK01.getInteger("ABRVW_BEZ").intValue());
						po.setUdftotalConfCount(totalConfirmedQty);
			po.setUdftotalReqCount(totalQty);;
			po.setVanPallet(vanPallet);
			po.setUdfVendorPallet(vanPallet);
			po.setTotalGrQty(totalGrQty);
			if (po.getUdfrequestedShipDate() != null)
				po.setUdfrequestedShipDate(
						DateFormater.convertLocalTimeToTimeZone(po.getUdfrequestedShipDate(), dcTimeZone, estTimeZone));

			if (apptDate != null && !apptDate.trim().equals(""))
				po.setC3Appointment(DateFormater.convertLocalTimeToTimeZone(java.util.Date.from(ZonedDateTime
						.ofInstant(DATETIMEFORMATTER.parse(apptDate).toInstant(), ZoneId.of(dcTimeZone)).toInstant()),
						dcTimeZone, estTimeZone));
		} catch (Exception ex) {
			ex.printStackTrace();
			if (po == null || po.getiDocSerialNumber() == null)
				try {
					Document SapOrders05Zorders0501 = (Document) doc.get("NS1:SapOrders05Zorders0501");
					Document SapOrders05Zorders0501IDocBO = (Document) SapOrders05Zorders0501
							.get("SapOrders05Zorders0501IDocBO");
					Document SapIDocControlRecord = (Document) SapOrders05Zorders0501IDocBO.get("SapIDocControlRecord");
					LoggingUtilities
							.generateErrorLog("Failed to parse iDoc: " + SapIDocControlRecord.getString("DOCNUM"));
				} catch (Exception ex1) {
					LoggingUtilities.generateErrorLog(ex1.getMessage());
				}
			LoggingUtilities.generateErrorLog(ex.getMessage());
			LoggingUtilities.generateErrorLog("Failed to parse iDoc: " + po.getErpTransactionId());
			throw ex;
		}

		return po;
	}

	public void processR9333Interface() throws Exception {

		MongoClient mongo = null;
		try {
			mongo = MongoClients.create(mongodbURI);
			Subscribers subscriber = subscribersService.findByName("LPV-POFilter");

			LoggingUtilities.generateInfoLog("Last Run Time: " + DATEFORMAT.format(subscriber.getLastrun()));
			LoggingUtilities.generateInfoLog("Inbound Collection: " + subscriber.getInboundcollection());
			MongoCollection<Document> inbound_collection = mongo.getDatabase(mongodbDatabase)
					.getCollection(subscriber.getInboundcollection());
			MongoCollection<Document> outbound_collection = mongo.getDatabase(mongodbDatabase)
					.getCollection(subscriber.getOutboundcollection());
			MongoCollection<Document> reasonCodeCollection = mongo.getDatabase(mongodbDatabase)
					.getCollection("LpvReasonCodeCollection");

			BasicDBObject query = new BasicDBObject();

			Calendar c = Calendar.getInstance();
			c.setTime(subscriber.getLastrun());
			c.add(Calendar.SECOND, LAST_RUN_TIME_BUFFER);
			Date previousRunTime = c.getTime();
			//Get current time and set as last run time for next run
			Date currentTime = new Date();// current time
			Calendar currentTimeCalendar = Calendar.getInstance();
			currentTimeCalendar.setTime(currentTime);
			currentTimeCalendar.set(Calendar.MILLISECOND, 0);
		    Date lastRunTime = currentTimeCalendar.getTime();

			// Modified query
//			query.put("Loading_Time",
//					BasicDBObjectBuilder.start("$gte", previousRunTime).get());
			query.put("Loading_Time",
					BasicDBObjectBuilder.start("$gte", previousRunTime).add("$lt", lastRunTime).get());
//			query.put("Loading_Time", new BasicDBObject("$gte", previousRunTime));
//			LoggingUtilities.generateInfoLog("Query : " + query);
			String json = "{_id:0," + "Loading_Time:1," + "\"NS1:SapOrders05Zorders0501\":1,Name:1}";
			Bson bson = BasicDBObject.parse(json);

			MongoTemplate mongoTemplate = new MongoTemplate(mongo, mongodbDatabase);
			// mongoTemplate.find(query, entityClass);

			FindIterable<Document> documents = inbound_collection.find(query).sort(new BasicDBObject("_id", 1))
					.projection(bson);

			Date processDate = new Date();

			FindIterable<Document> reasonCodes = reasonCodeCollection.find();

			for (Document code : reasonCodes) {
				LpvReasonCode lpvReasonCode = new LpvReasonCode();
				lpvReasonCode.setReasonCode((String) code.get("reasonCode"));
				lpvReasonCode.setReasonCodeDescription((String) code.get("reasonCodeDescription"));
				lpvReasonCode.setUserId("System");
				lpvReasonCode.setModifyDate(new Date());
				reasonCodeMap.put((String) code.get("reasonCode"), lpvReasonCode);
			}
			// LLCT-351 13 Additional PO Types
			poTypes.put("1", "ZMAN");
			poTypes.put("2", "ZITR");
			poTypes.put("3", "ZIBM");
			poTypes.put("4", "ZIPR");
			poTypes.put("5", "ZAWR");
			poTypes.put("6", "ZRX");	
			poTypes.put("7", "ZAWP");
			poTypes.put("8", "ZORM");
			poTypes.put("9", "ZCTO");
			poTypes.put("10", "ZNAR");
			poTypes.put("11", "ZIBI");
			poTypes.put("12", "ZSNL");
			poTypes.put("13", "ZIBR");
			poTypes.put("14", "ZIBA");
			poTypes.put("15", "ZCOV");
			poTypes.put("16", "ZIBN");
			poTypes.put("17", "ZPRO");
			poTypes.put("18", "ZSDM");
			poTypes.put("19", "NB");

			for (Document doc : documents) {
				LpvPoInterface po = new LpvPoInterface();

				try {
					po = parseR9333toLpvPO(doc);
					po.setLoadingDate(processDate);

//				To be put back once the code is stable				
					if (!r9333toLpvPOFilter(po))
						continue;				
					LpvPoInterface previousIDoc = getLastR9333toLpvPOExtract(po.getPurchaseOrderId());
					// Reason 08 - Reprocessing same idoc to LCT
//					if (previousIDoc != null && previousIDoc.getiDocSerialNumber().equals(po.getiDocSerialNumber())) {
//						List<LpvReasonCode> lpvReasonCodeList = findReasonCode("08");
//						LpvReasonCodeTransaction lpvReasonCodeTransaction = lpvReasonCodeService
//								.publishLpvReasonCodeTransaction(lpvReasonCodeList, po);
//						lpvReasonCodeService.saveLpvReasonCodeTransaction(lpvReasonCodeTransaction);
//
//						continue;
//					}
					// Reason Code 08 Checking for change pointer fields
					if (previousIDoc != null && !previousIDoc.getiDocSerialNumber().equals(po.getiDocSerialNumber())
							&& validatePoChangePointer(previousIDoc, po)) {
						List<LpvReasonCode> lpvReasonCodeList = findReasonCode("08");
						LpvReasonCodeTransaction lpvReasonCodeTransaction = lpvReasonCodeService
								.publishLpvReasonCodeTransaction(lpvReasonCodeList, po);
						lpvReasonCodeService.saveLpvReasonCodeTransaction(lpvReasonCodeTransaction);
						continue;
					}
					// Reason Code 01 when the current idoc is same as previous idoc
					if (previousIDoc != null && previousIDoc.getiDocSerialNumber().equals(po.getiDocSerialNumber())
							&& previousIDoc.getIdocProcessedTime().equals(po.getIdocProcessedTime())) {
						List<LpvReasonCode> lpvReasonCodeList = findReasonCode("01");
						LpvReasonCodeTransaction lpvReasonCodeTransaction = lpvReasonCodeService
								.publishLpvReasonCodeTransaction(lpvReasonCodeList, po);
						lpvReasonCodeService.saveLpvReasonCodeTransaction(lpvReasonCodeTransaction);
						continue;
					}
					for (LpvPoDetailInterface poDetail : po.getPoDetails()) {
						ArticleDCNodes node = lpvArticleService.findByarticleIdAndDC(poDetail.getCustomerItemName(),
								poDetail.getShipTo());
						if (node == null) {
							// adding article dc node
							ArticleDCNodes articleNode;
							if (poDetail.getCustomerItemName() != null && poDetail.getShipTo() != null) {
								articleNode = new ArticleDCNodes();
								articleNode.setArticleId(poDetail.getCustomerItemName());
								articleNode.setDC(poDetail.getShipTo());
								articleNode.setCreation_date(new Date());
								articleNode.setCreated_by("System");
								lpvArticleService.createNode(articleNode);
							}
						}
					}
					lpvPoInterfaceService.saveOrUpdateLpvPoInterface(po);

//				LpvPoInterface poFromDB = getLastR9333toLpvPOExtract(po.getPurchaseOrderId());
				} catch (Exception ex) {
					LoggingUtilities.generateErrorLog(ex.getMessage());
				}

			}
			mongo.close();
//			To be put back once the code is stable	
			subscriber.setLastrun(lastRunTime);
			subscribersService.saveOrUpdateSubscribers(subscriber);
		} catch (Exception ex) {
			LoggingUtilities.generateErrorLog(ex.getMessage());
			throw ex;
		} finally {
			if (mongo != null)
				mongo.close();
		}
	}

	private List<LpvReasonCode> findReasonCode(String reasonCode) {
		List<LpvReasonCode> reasonCodelist = new ArrayList<LpvReasonCode>();
		reasonCodelist.add(reasonCodeMap.get(reasonCode));
		return reasonCodelist;

	}

	public void purgeLpvPODocuments(String readyToArchive, String date) throws Exception {
		MongoClient mongo = null;
		try {
			LoggingUtilities.generateInfoLog("Purge from R9333LpvPoInterface Collection starts");
			mongo = MongoClients.create(mongodbURI);
			Subscribers subscriber = subscribersService.findByName("LPV-POFilter");
			MongoCollection<Document> outbound_collection = mongo.getDatabase(mongodbDatabase)
					.getCollection(subscriber.getOutboundcollection());

			Date now = new Date();
			String json = "{readyToArchive:{$eq:\"Y\"},loadingDate:{$lte:new ISODate(\"" + LPVDATEFORMAT.format(now)
					+ "\")}}}";
			Bson bson = BasicDBObject.parse(json);

			DeleteResult result = outbound_collection.deleteMany(bson);
			LoggingUtilities.generateInfoLog("Purge from " + subscriber.getOutboundcollection() + ": "
					+ result.getDeletedCount() + " documents have been purged. ");

			mongo.close();
			mongo = null;
		} catch (Exception ex) {
			LoggingUtilities.generateErrorLog(ex.getMessage());
			throw ex;
		} finally {
			if (mongo != null)
				mongo.close();
		}
	}

	private boolean r9333toLpvPOFilter(LpvPoInterface po) throws Exception {
		boolean result = true;

		try {
			if (po.getErpOrderType() == null || (!poTypes.containsValue(po.getErpOrderType()))) {
				List<LpvReasonCode> lpvReasonCodeList = findReasonCode("05");
				LpvReasonCodeTransaction lpvReasonCodeTransaction = lpvReasonCodeService
						.publishLpvReasonCodeTransaction(lpvReasonCodeList, po);
				lpvReasonCodeService.saveLpvReasonCodeTransaction(lpvReasonCodeTransaction);
				return false;
			}
			if(!po.getIdocOutputType().equalsIgnoreCase("ZLEG")) {
				// ReasonCode 25 IDOC Type
				List<LpvReasonCode> lpvReasonCodeList = findReasonCode("25");
				LpvReasonCode lpvReasonCode=lpvReasonCodeList.get(0);
				String reasonCodeDescription=lpvReasonCode.getReasonCodeDescription();
				lpvReasonCode.setReasonCodeDescription(reasonCodeDescription+" (ZZKSCHL)="+po.getIdocOutputType());
				LpvReasonCodeTransaction lpvReasonCodeTransaction = lpvReasonCodeService
						.publishLpvReasonCodeTransaction(lpvReasonCodeList, po);
				lpvReasonCodeService.saveLpvReasonCodeTransaction(lpvReasonCodeTransaction);
				lpvReasonCode.setReasonCodeDescription(reasonCodeDescription);
				return false;
			}
			if(!po.getIsItemCategoryValid()) {
				return false;
			}	 
			if (po.getPoDetails() == null || po.getPoDetails().isEmpty()) {
				// ReasonCode 02 PO Details null/empty
				List<LpvReasonCode> lpvReasonCodeList = findReasonCode("02");
				LpvReasonCodeTransaction lpvReasonCodeTransaction = lpvReasonCodeService
						.publishLpvReasonCodeTransaction(lpvReasonCodeList, po);
				lpvReasonCodeService.saveLpvReasonCodeTransaction(lpvReasonCodeTransaction);
				return false;
			}
			if (!(po.getIncoTerms2() == null) && !po.getIncoTerms2().trim().equals("")
					&& !po.getIncoTerms2().equalsIgnoreCase("TMS2")
					// LTSCIH-285
					&& !po.getIncoTerms2().equalsIgnoreCase("SDM")) {

				// ReasonCode 06 - INCO TERM2 is not SDM or TMS2 or null
				List<LpvReasonCode> lpvReasonCodeList = findReasonCode("06");
				LpvReasonCodeTransaction lpvReasonCodeTransaction = lpvReasonCodeService
						.publishLpvReasonCodeTransaction(lpvReasonCodeList, po);
				lpvReasonCodeService.saveLpvReasonCodeTransaction(lpvReasonCodeTransaction);
				return false;
			}
			if (po.getUdfshipToSiteName().trim().equals("") || po.getUdfshipToSiteName().equals(null)
					|| po.getUdfshipToSiteName().isEmpty()) {
				// ReasonCode 04 DC not assigned for PO items
				List<LpvReasonCode> lpvReasonCodeList = findReasonCode("04");
				LpvReasonCodeTransaction lpvReasonCodeTransaction = lpvReasonCodeService
						.publishLpvReasonCodeTransaction(lpvReasonCodeList, po);
				lpvReasonCodeService.saveLpvReasonCodeTransaction(lpvReasonCodeTransaction);
				return false;
			}
			if (po.getBuyerName().trim().equals("") || po.getSupplierName().trim().equals("")
					|| po.getBuyerName() == null || po.getSupplierName() == null) {
				// ReasonCode 07 - Buyer or Supplier Name is NULL
				List<LpvReasonCode> lpvReasonCodeList = findReasonCode("07");
				LpvReasonCodeTransaction lpvReasonCodeTransaction = lpvReasonCodeService
						.publishLpvReasonCodeTransaction(lpvReasonCodeList, po);
				lpvReasonCodeService.saveLpvReasonCodeTransaction(lpvReasonCodeTransaction);
				return false;

			}
			boolean supplierFlag = false;
			boolean buyerVendorFilterFlag = false;
			LpvLanes lanes = lpvLanesService.findByBuyerAndVendor(po.getBuyerName(), po.getSupplierName());

			if (lanes != null) {
				buyerVendorFilterFlag = true;
			} else {
				try {

					LpvLanes lpv = new LpvLanes();
					lpv.setBuyer(po.getBuyerName());
					lpv.setVendor(po.getSupplierName());
					lpv.setPoNumber(po.getPurchaseOrderId());
					lpv.setCreation_date(new Date());
					lpv.setCreated_by("System");

					lpvLanesService.createNewLane(lpv);
					LoggingUtilities.generateInfoLog("The lane with buyer " + po.getBuyerName() + " and vendor "
							+ po.getSupplierName() + " has been successfully added");
					buyerVendorFilterFlag = true;
//					VendorMaster vendors = lpvVendorMasterService.findBySite(lpv.getVendor());
//					LoggingUtilities.generateInfoLog(" Vendors is : " + vendors);
//					if (vendors!=null) {
//						vendors.setStatus("Active");
//						supplierFlag=true;
//					}

					// automatedLanesEmail.sendMessage(po.getBuyerName(),po.getSupplierName());
				} catch (Exception e) {
					LoggingUtilities.generateInfoLog("Failed to add lane with Buyer Name: " + po.getBuyerName()
							+ " and Vendor Name: " + po.getSupplierName());
					e.printStackTrace();
				}

			}
//			for (String[] buyerVendorFilter : BUYER_VENDOR_FILTER_LIST) {
//				if (po.getBuyerName().equals(buyerVendorFilter[0])
//						&& po.getSupplierName().equals(buyerVendorFilter[1])) {
//					buyerVendorFilterFlag = true; 
//					break;
//				}
//			}
			if (!buyerVendorFilterFlag) {
				// ReasonCode 03 - Missing Lane
				List<LpvReasonCode> lpvReasonCodeList = findReasonCode("03");
				LpvReasonCodeTransaction lpvReasonCodeTransaction = lpvReasonCodeService
						.publishLpvReasonCodeTransaction(lpvReasonCodeList, po);
				lpvReasonCodeService.saveLpvReasonCodeTransaction(lpvReasonCodeTransaction);
//				LoggingUtilities.generateInfoLog("Going inside reason code"+ buyerVendorFilterFlag);
				result = false;
			}
			// reason code 11: missing supplier
//			if (!supplierFlag) {
//				List<LpvReasonCode> lpvReasonCodeList = findReasonCode("11");
//			LpvReasonCodeTransaction lpvReasonCodeTransaction = lpvReasonCodeService
//					.publishLpvReasonCodeTransaction(lpvReasonCodeList, po);
//			lpvReasonCodeService.saveLpvReasonCodeTransaction(lpvReasonCodeTransaction);
//			}
		} catch (Exception ex) {
			LoggingUtilities.generateErrorLog(ex.getMessage());
		}

		return result;
	}

	public void setLpvLocationTimeZoneService(LpvLocationTimeZoneService lpvLocationTimeZoneService) {
		this.lpvLocationTimeZoneService = lpvLocationTimeZoneService;
	}

	public void setLpvReasonCodeService(LpvReasonCodeService lpvReasonCodeService) {
		this.lpvReasonCodeService = lpvReasonCodeService;
	}

	public void setLpvPoInterfaceService(LpvPoInterfaceService lpvPoInterfaceService) {
		this.lpvPoInterfaceService = lpvPoInterfaceService;
	}

	public void setMongodbDatabase(String mongodbDatabase) {
		this.mongodbDatabase = mongodbDatabase;
	}

	public void setMongodbURI(String mongodbURI) {
		this.mongodbURI = mongodbURI;
	}

	public void setSubscribersService(com.lcl.scs.subscribers.service.SubscribersService subscribersService) {
		this.subscribersService = subscribersService;
	}

	private boolean validatePoChangePointer(LpvPoInterface previousPO, LpvPoInterface po) throws Exception {
		boolean identical = true;

		try {
			LoggingUtilities.generateInfoLog("Comparing iDocs for Change Pointer");
			LoggingUtilities.generateInfoLog("Previous iDocs: " + previousPO.getiDocSerialNumber());
			LoggingUtilities.generateInfoLog("Current iDocs: " + po.getiDocSerialNumber());

			ObjectMapper mapper = new ObjectMapper();
			String poJSONStr = mapper.writeValueAsString(po);
			String previousPOJSONStr = mapper.writeValueAsString(previousPO);

			JsonStructure poJSON = Json.createReader(new StringReader(poJSONStr)).read();
			JsonStructure previousPOJSON = Json.createReader(new StringReader(previousPOJSONStr)).read();

			JsonPatch diff = Json.createDiff(poJSON, previousPOJSON);
			JsonMergePatch merge = Json.createMergeDiff(poJSON, previousPOJSON);

			Document mergeDoc = Document.parse(JSonStringFormatter.format(merge.toJsonValue()));

			Set<String> diffKeys = mergeDoc.keySet();
			if (!diffKeys.isEmpty()) {
				for (String fieldsExcluded : PO_CHANGE_POINTER_EXCLUDING_FIELD_LIST) {
					diffKeys.remove(fieldsExcluded);
				}

				if (diffKeys.contains("poDetails")) {
					if (po.getPoDetails().size() != previousPO.getPoDetails().size())
						identical = false;
					else
						for (int i = 0; i < po.getPoDetails().size(); i++) {
							LpvPoDetailInterface poDetail = po.getPoDetails().get(i);
							LpvPoDetailInterface previousPoDetail = previousPO.getPoDetails().get(i);
							String poDetailJSONstr = mapper.writeValueAsString(poDetail);
							String previousPoDetailJSONStr = mapper.writeValueAsString(previousPoDetail);

							JsonStructure poDetailJSON = Json.createReader(new StringReader(poDetailJSONstr)).read();
							JsonStructure previousPODetailJSON = Json
									.createReader(new StringReader(previousPoDetailJSONStr)).read();

							JsonPatch diffDetail = Json.createDiff(poDetailJSON, previousPODetailJSON);
							JsonMergePatch mergeDetail = Json.createMergeDiff(poDetailJSON, previousPODetailJSON);

							Document mergeDetailDoc = Document
									.parse(JSonStringFormatter.format(mergeDetail.toJsonValue()));

							Set<String> diffDetailKeys = mergeDetailDoc.keySet();
							if (!diffDetailKeys.isEmpty()) {

								for (String detailKey : PO_DETAIL_CHANGE_POINTER_FIELD_LIST) {
									if (diffDetailKeys.contains(detailKey)) {
										identical = false;
										break;
									}
								}
							}
							if (identical == false)
								break;
						}
				}

				if (identical == false)
					return identical;
//				diffKeys.remove("id");
//				diffKeys.remove("loadingDate");
//				diffKeys.remove("processIndicator");
//				diffKeys.remove("originalFileName");
//				diffKeys.remove("targetPOCSVFileName");
//				diffKeys.remove("targetDelvCSVFileName");
				for (String key : PO_CHANGE_POINTER_FIELD_LIST) {
					if (diffKeys.contains(key)) {
						identical = false;
						break;
					}
				}
			}

//			LoggingUtilities.generateInfoLog("Compare Result: " + JSonStringFormatter.format(diff.toJsonArray()));
//			LoggingUtilities
//					.generateInfoLog("Compare Merge Result: " + JSonStringFormatter.format(merge.toJsonValue()));

		} catch (Exception ex) {
			throw ex;
		}

		return identical;
	}

	public void setLpvLanesService(LpvLanesService lpvLanesService) {
		this.lpvLanesService = lpvLanesService;
	}

	public void setLpvArticleNodeService(LPVArticleNodeService lpvArticleService) {
		this.lpvArticleService = lpvArticleService;

	}

	public void setLpvVendorMasterService(LpvVendorMasterService lpvVendorMasterService) {
		this.lpvVendorMasterService = lpvVendorMasterService;
	}

}
