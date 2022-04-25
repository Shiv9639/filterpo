package com.lcl.scs.r9333.lpv.po.scheduler;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.lcl.scs.r9333.lpv.po.service.LPVArticleNodeService;
import com.lcl.scs.r9333.lpv.po.service.LpvLanesService;
import com.lcl.scs.r9333.lpv.po.service.LpvLocationTimeZoneService;
import com.lcl.scs.r9333.lpv.po.service.LpvPoInterfaceService;
import com.lcl.scs.r9333.lpv.po.service.LpvReasonCodeService;
import com.lcl.scs.r9333.lpv.po.service.LpvVendorMasterService;
import com.lcl.scs.r9333.lpv.po.service.impl.R9333LpvPoServiceImpl;
import com.lcl.scs.util.logging.LoggingUtilities;

@Component
public class LPVR9333Scheduler {
	private static final SimpleDateFormat DATETIMEFORMATTER = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	// @Value("${spring.data.mongodb.uri}")
	private String mongodbURI = System.getenv("MONGO_DB_URI");
	// @Value("${spring.data.mongodb.database}")
	private String mongodbDatabase = System.getenv("MONGO_DB_NAME");

	// @Value("${spring.data.by.token.url}")
	private final String BY_TOKEN_URL = System.getenv("BY_TOKEN_URL");
	// @Value("${spring.data.by.token.client.id}")
	private final String BY_TOKEN_CLIENT_ID = System.getenv("BY_TOKEN_CLIENT_ID");
	// @Value("${spring.data.by.token.client.secret}")
	private final String BY_TOKEN_CLIENT_SECRET = System.getenv("BY_TOKEN_CLIENT_SECRET");
	// @Value("${spring.data.by.token.grant.type}")
	private final String BY_TOKEN_GRANT_TYPE = System.getenv("BY_TOKEN_GRANT_TYPE");
	// @Value("${spring.data.by.token.token.scope}")
	private final String BY_TOKEN_SCOPE = System.getenv("BY_TOKEN_SCOPE");

	@Autowired
	private com.lcl.scs.subscribers.service.SubscribersService subscribersService;

	@Autowired
	private LpvPoInterfaceService lpvPoInterfaceService;
	
	@Autowired
	private LpvLocationTimeZoneService lpvLocationTimeZoneService;
	
	@Autowired
	private LPVArticleNodeService lpvArticleService;
	@Autowired
	private LpvReasonCodeService lpvReasonCodeService;
	
	@Autowired
	private LpvLanesService lpvLanesService;
	
	@Autowired
	private LpvVendorMasterService lpvVendorMasterService;

	@Scheduled(fixedDelay = 1000 * 60 * 1)
	private void processLPVR9333Scheduler() {
		LoggingUtilities.generateInfoLog(DATETIMEFORMATTER.format(new Date()) + ": Start loading all new files");
		try {
			LoggingUtilities.generateInfoLog("MongoDB URI: " + mongodbURI);
			LoggingUtilities.generateInfoLog("MongoDB Database Name: " + mongodbDatabase);

			R9333LpvPoServiceImpl r9333ServiceImpl = new R9333LpvPoServiceImpl();
			r9333ServiceImpl.setSubscribersService(subscribersService);
			r9333ServiceImpl.setLpvPoInterfaceService(lpvPoInterfaceService);
			r9333ServiceImpl.setLpvReasonCodeService(lpvReasonCodeService);
			r9333ServiceImpl.setLpvLocationTimeZoneService(lpvLocationTimeZoneService);
			r9333ServiceImpl.setLpvLanesService(lpvLanesService);
			r9333ServiceImpl.setLpvArticleNodeService(lpvArticleService);
			r9333ServiceImpl.setLpvVendorMasterService(lpvVendorMasterService);
			r9333ServiceImpl.setMongodbDatabase(mongodbDatabase);
			r9333ServiceImpl.setMongodbURI(mongodbURI);
			
			r9333ServiceImpl.processR9333Interface();
		} catch (Exception ex) {
			LoggingUtilities.generateErrorLog("Failed to load all new files! " + ex.getMessage());
			ex.printStackTrace();
		}
		LoggingUtilities
				.generateInfoLog(DATETIMEFORMATTER.format(new Date()) + ": Finish loading all new R9333 files for LPV");

	}
}
