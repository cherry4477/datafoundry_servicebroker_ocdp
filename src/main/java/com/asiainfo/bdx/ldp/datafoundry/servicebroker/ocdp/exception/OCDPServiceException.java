package com.asiainfo.bdx.ldp.datafoundry.servicebroker.ocdp.exception;

import org.springframework.cloud.servicebroker.exception.ServiceBrokerException;


/**
 * Exception thrown when issues with the underlying OCDP Hadoop service occur.
 * 
 * @author whitebai1986@gmail.com
 *
 */
public class OCDPServiceException extends ServiceBrokerException {

	private static final long serialVersionUID = 8667141725171626000L;

	public OCDPServiceException(String message) {
		super(message);
	}
	
}
