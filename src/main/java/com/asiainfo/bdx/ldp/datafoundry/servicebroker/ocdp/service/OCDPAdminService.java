package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.service;

import com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.exception.OCDPServiceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Utility class for manipulating a OCDP Hadoop services.
 * 
 * @author whitebai1986@gmail.com
 *
 */
@Service
public class OCDPAdminService {

	public static final String ADMIN_DB = "admin";
	
	private Logger logger = LoggerFactory.getLogger(OCDPAdminService.class);

	private OCDPServiceException handleException(Exception e) {
		logger.warn(e.getLocalizedMessage(), e);
		return new OCDPServiceException(e.getLocalizedMessage());
	}

}
