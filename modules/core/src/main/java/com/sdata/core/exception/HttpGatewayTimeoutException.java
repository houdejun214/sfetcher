package com.sdata.core.exception;

import org.apache.commons.httpclient.HttpStatus;


public class HttpGatewayTimeoutException extends Exception {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public HttpGatewayTimeoutException(int httpStatuCode){
		super(HttpStatus.getStatusText(httpStatuCode));
	}
}
