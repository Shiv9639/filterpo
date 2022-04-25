package com.lcl.scs.r9333.lpv.po.controller;

import java.text.SimpleDateFormat;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.lcl.scs.r9333.lpv.po.model.LpvPoInterface;
import com.lcl.scs.r9333.lpv.po.service.LpvPoInterfaceService;
import com.lcl.scs.r9333.lpv.po.service.impl.R9333LpvPoServiceImpl;
import com.lcl.scs.util.logging.LoggingUtilities;

@RestController
@RequestMapping("/lpvpo")
@ComponentScan(basePackages = "com.lcl.scs")
public class R9333LPVPOController {
	@Autowired
	private  LpvPoInterfaceService lpvPoInterfaceService;	
	
	@Autowired
	private com.lcl.scs.subscribers.service.SubscribersService subscribersService;
	
	@PostMapping(value="/findLatestIDocByPONumber", produces = { MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE })
	public LpvPoInterface findLatestIDocByPONumber(@RequestHeader("ponumber") String ponumber) {
		return lpvPoInterfaceService.findFirstByPurchaseOrderIdOrderByLoadingDateDesc(ponumber);
	}
	
	@PostMapping(value="/deleteByByReadyToArchiveAndLoadingDateLessThanEqual", produces = { MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE })
	public ResponseEntity<Object> deleteByByReadyToArchiveAndLoadingDateLessThanEqual(@RequestHeader("readyToArchive") String readyToArchive,@RequestHeader("loadingDate") String loadingDate) {

		String mongodbURI = System.getenv("MongoDBURI");
		String mongodbDatabase = System.getenv("MongoDBDatabaseName");
		try {
			R9333LpvPoServiceImpl r9333ServiceImpl = new R9333LpvPoServiceImpl();
			r9333ServiceImpl.setSubscribersService(subscribersService);
			r9333ServiceImpl.setMongodbDatabase(mongodbDatabase);
			r9333ServiceImpl.setMongodbURI(mongodbURI);
			r9333ServiceImpl.purgeLpvPODocuments(readyToArchive, loadingDate);
//		lpvPoInterfaceService.deleteByByReadyToArchiveAndLoadingDateLessThanEqual(readyToArchive, df.parse(loadingDate));
		return new ResponseEntity<Object>(HttpStatus.OK);}
		catch(Exception ex) {
			LoggingUtilities.generateErrorLog(ex.getMessage());
			return new ResponseEntity<Object>(HttpStatus.BAD_REQUEST);
		}
	}
}
