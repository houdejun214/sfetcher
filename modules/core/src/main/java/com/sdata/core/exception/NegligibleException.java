package com.sdata.core.exception;

/**
 * 
 * the exception is a negligible exception(such as Network Exception, Timeout exception), 
 * this exception will intercept our main crawl thread.
 * 
 * @author qiumm
 *
 */
public class NegligibleException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public NegligibleException() {
	}

	public NegligibleException(String message) {
		super(message);
	}

	public NegligibleException(Throwable cause) {
		super(cause);
	}

	public NegligibleException(String message, Throwable cause) {
		super(message, cause);
	}

}
