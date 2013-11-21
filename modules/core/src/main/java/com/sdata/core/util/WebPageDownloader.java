package com.sdata.core.util;

import java.io.IOException;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URL;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HeaderElement;
import org.apache.commons.httpclient.HostConfiguration;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.NameValuePair;
import org.apache.commons.httpclient.ProxyHost;
import org.apache.commons.httpclient.SimpleHttpConnectionManager;
import org.apache.commons.httpclient.URI;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.protocol.Protocol;
import org.apache.commons.httpclient.util.EncodingUtil;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lakeside.core.utils.PatternUtils;
import com.lakeside.core.utils.StringUtils;

/**
 * web page content downloader object
 * 
 * @author houdj
 * 
 */
@Deprecated
public class WebPageDownloader {
	
	public static long webPageCounter=0;
	private static boolean addTrustAll = false;
	private static Object synObj = new Object();
	private static Logger log = LoggerFactory.getLogger("SdataCrawler.WebPageDownloader");
	private static Map<String,String> empty = new HashMap<String, String>();
	private String pageUrl;
	
	public String getPageUrl() {
		return pageUrl;
	}

	public void setPageUrl(String pageUrl) {
		this.pageUrl = pageUrl;
	}

	public WebPageDownloader(String url) {
		this.pageUrl = url;
	}
	
	/**
	 * download the web page's html content
	 * 
	 * @return
	 */
	public String download() {
		return this.download(empty);
	}
	
	/**
	 * download the web page's html content
	 * 
	 * @return
	 */
	public String download(Map<String,String> header) {
		int i=0;
		while(true){
			try {
				return downloadWithClient(header,0);
			} catch (HttpGatewayTimeoutException se){ 
				//Http server error exception handling
				if(i>2){
					// repeat download 3 times , still connect with time out exception. 
					log.error("download web content ["+this.pageUrl+"] failed ",se);
					return null;
				}else{
					// log HttpServerErrorException and download again  
					log.info("download web content time out ,download again for "+i);
				}
				sleep(5*60*1000);
			} catch(ConnectException ce){
				// connection exception handling
				String message = ce.getMessage();
				if(message!=null && message.contains("Connection refused")){
					// server refused the connection, need wait more times
					log.info("download web content connection refuesd wait for a space time ["+this.pageUrl+"]");
					sleep(10*60*1000); // wait some time, then go on.
				}else{
					log.error("download web content ["+this.pageUrl+"] failed,{} ",ExceptionUtils.getMessage(ce));
					return null;
				}
			} catch(OutOfMemoryError om){
				log.info("download web content memory is not enough ,download again for "+i);
				sleep(5000);
			}catch (Exception e) {
				e.printStackTrace();
				log.error("download web content ["+this.pageUrl+"] failed,{} ",ExceptionUtils.getMessage(e));
				return null;
			} finally {
				i++;
			}
		}
	}
	
	public String fastDownload() {
		return this.fastDownload(empty);
	}
	
	/**
	 * download the web page's html content
	 * 
	 * @return
	 */
	public String fastDownload(Map<String,String> header) {
		int i=0;
		while(true){
			try {
				return downloadWithClient(header,30000);
			} catch (HttpGatewayTimeoutException se){ 
				//Http server error exception handling
				if(i>2){
					// repeat download 3 times , still connect with time out exception. 
					log.error("download web content ["+this.pageUrl+"] failed ",se);
					return null;
				}else{
					// log HttpServerErrorException and download again  
					log.info("download web content time out ,download again for "+i);
				}
			} catch(ConnectException ce){
				// connection exception handling
				String message = ce.getMessage();
				if(message!=null && message.contains("Connection refused")){
					// server refused the connection, need wait more times
					log.info("download web content connection refuesd wait for a space time ["+this.pageUrl+"]");
				}else{
					log.error("download web content ["+this.pageUrl+"] failed,{} ",ExceptionUtils.getMessage(ce));
					return null;
				}
			} catch(OutOfMemoryError om){
				log.info("download web content memory is not enough ,download again for "+i);
				sleep(5000);
			}catch (Exception e) {
				log.error("download web content ["+this.pageUrl+"] failed,{} ",ExceptionUtils.getMessage(e));
				return null;
			} finally {
				i++;
			}
		}
	}
	
	private static final ThreadLocal<HttpClient> userThreadLocal = new ThreadLocal<HttpClient>();
	public HttpClient getClient(){
		HttpClient httpClient = userThreadLocal.get();
		if(httpClient==null){
			httpClient = new HttpClient(new SimpleHttpConnectionManager());
			userThreadLocal.set(httpClient);
		}
		return httpClient;
	}
	
	private String downloadWithClient(Map<String,String> header,int timeout)throws Exception {
		HttpClient client = getClient();
		GetMethod get = new GetMethod();
		get.addRequestHeader("http.useragent","crawler4j");
		get.addRequestHeader("User-Agent","Mozilla/5.0 (Windows; U; Windows NT 6.1; pl; rv:1.9.1) Gecko/20090624 Firefox/3.5 (.NET CLR 3.5.30729)");
    	get.addRequestHeader("Content-Type","text/html; charset=utf-8");
    	get.addRequestHeader("Connection", "close");
    	if(header != null){
			Iterator<String> iterator = header.keySet().iterator();
			while(iterator.hasNext()){
				String k = iterator.next();
				String v = header.get(k);
				get.addRequestHeader(k,v);
			}
    	}
		while (true) {
			try {
		    	get.setURI(new URI(this.pageUrl,true));
		    	if (this.pageUrl.startsWith("https://") && !addTrustAll) {
					trustAllHost();
				}
				//log.info(this.pageUrl);
				HostConfiguration config = client.getHostConfiguration();
				config.setProxyHost(this.getProxyHost());
				if(timeout>0){
					client.getHttpConnectionManager().getParams().setSoTimeout(timeout);
				}
			    client.executeMethod(get);
				String result =getResponseContent(get);
				this.pageUrl = get.getURI().toString();
				return result;
			} catch (HttpException e) {
				int statusCode = get.getStatusCode();
				if (HttpStatus.SC_GATEWAY_TIMEOUT == statusCode) {
					throw new HttpGatewayTimeoutException(
							HttpStatus.SC_GATEWAY_TIMEOUT);
				} else if ((statusCode % 300) >= 0
						&& (statusCode % 300) < 100) {
					// redirection operation, it only arise when HttpURLConnection can't redirect
					String reLocation = get.getResponseHeader("Location")
							.getValue();
					if(pageUrl.equals(reLocation)){
						throw e;
					}
					if (StringUtils.isNotEmpty(reLocation)) {
						this.pageUrl = reLocation;
						continue;
					}
				}
				continue;
			} catch (IOException e) {
				String message = e.getMessage();
				if (message != null) {
					if (message
							.contains("Server returned HTTP response code: 502")
							|| message
									.contains("Server returned HTTP response code: 504")
							|| message
									.contains("Server returned HTTP response code: 500")
							|| message.contains("Premature EOF")
							|| message.contains("Connection reset")
							|| message.contains("Too many open files")) {
						throw new HttpGatewayTimeoutException(
								HttpStatus.SC_GATEWAY_TIMEOUT);
					}
				}
				throw e;
			} catch (RedirectException e) {
				continue;
			}
		}
	}

	private String getResponseContent(GetMethod get) throws RedirectException {
		String conent = null;
		try {
			byte[] responseBody = get.getResponseBody();
			String body = new String(responseBody);
			String redictUrl = PatternUtils.getMatchPattern("HTTP-EQUIV=\"Refresh\".*URL=(.*)\"\\>",
					body, 1);
			if(!StringUtils.isEmpty(redictUrl)){
				this.pageUrl = redictUrl;
				throw new RedirectException();
			}
			
			Header responseHeader = get.getResponseHeader("Content-Type");
			String charset = null;
			if (responseHeader != null) {
				HeaderElement values[] = responseHeader.getElements();
				// I expect only one header element to be there
				// No more. no less
				if (values.length == 1) {
					NameValuePair param = values[0]
							.getParameterByName("charset");
					if (param != null) {
						// If I get anything "funny"
						// UnsupportedEncondingException will result
						charset = param.getValue();
					}
				}
			}
			
			if (charset == null) {
				charset = PatternUtils.getMatchPattern("(?=<meta).*?(?<=charset=[\\'|\\\"]?)([[a-z]|[A-Z]|[0-9]|-]*)",
						body, 1);
			}
			if(StringUtils.isEmpty(charset)){
				charset = "UTF-8";
			}
			conent = EncodingUtil.getString(responseBody, charset);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return conent;
	}
	
	public String getRedirectUrl(){
		HttpURLConnection connect = null;
		try {
				URL url = new URL(this.pageUrl);
				Proxy proxy = getProxy();
				if(proxy!=null){
					connect = (HttpURLConnection) url.openConnection(proxy);
				}else{
					connect = (HttpURLConnection) url.openConnection();
				}
				connect.setRequestProperty("User-Agent","Mozilla/5.0 (Windows; U; Windows NT 6.1; pl; rv:1.9.1) Gecko/20090624 Firefox/3.5 (.NET CLR 3.5.30729)");
				connect.setInstanceFollowRedirects(false);
				connect.connect();
				int statusCode = connect.getResponseCode();
				if((statusCode % 300)>=0 && (statusCode % 300)<100){
					String reLocation = connect.getHeaderField("Location");
					if(StringUtils.isNotEmpty(reLocation)){
						return reLocation;
					}
				}
				return "";
		}catch (IOException ioe) {
			return "";
		} finally{
			if(connect!=null){
				connect.disconnect();
			}
		}
	}

	private void trustAllHost() throws NoSuchAlgorithmException,
			KeyManagementException {
		synchronized(synObj){
			if(!addTrustAll){
				TrustManager[] trustAllCerts = new TrustManager[] {
					       new X509TrustManager() {
					          public java.security.cert.X509Certificate[] getAcceptedIssuers() { return null; }
					          public void checkClientTrusted(X509Certificate[] certs, String authType)  throws CertificateException{  }
					          public void checkServerTrusted(X509Certificate[] certs, String authType)  throws CertificateException{  }
					       }
					    };
		
				SSLContext sc = SSLContext.getInstance("SSL");
				sc.init(null, trustAllCerts, new java.security.SecureRandom());
				HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
		
				// Create all-trusting host name verifier
				HostnameVerifier allHostsValid = new HostnameVerifier() {
				    public boolean verify(String hostname, SSLSession session) {
				      return true;
				    }
				};
				// Install the all-trusting host verifier
				HttpsURLConnection.setDefaultHostnameVerifier(allHostsValid);
				// Register custom proxy for https 
				Protocol.registerProtocol("https", 
						new Protocol("https", new MySSLSocketFactory(), 443));
                //register https protocol in httpclient's scheme registry  
				addTrustAll= true;
			}
		}
	}

	
	/**
	 * download the web page's html content
	 * @param url
	 * @return
	 */
	public static String download(String url){
		WebPageDownloader downloader = new WebPageDownloader(url);
		webPageCounter++;
		String download = downloader.download(empty);
		return download;
	}
	
	/**
	 * download the web page's html content
	 * @param url
	 * @return
	 */
	public static String download(String url,Map<String,String> header){
		WebPageDownloader downloader = new WebPageDownloader(url);
		webPageCounter++;
		return downloader.download(header);
	}

	private void sleep(long time){
		try {
			Thread.sleep(time);
		} catch (InterruptedException e) {
			;
		}
	}
	
	public static String getRedirectUrl(String url){
		WebPageDownloader downloader = new WebPageDownloader(url);
		return downloader.getRedirectUrl();
	}
	
	private static Proxy getProxy(){
		if(proxies==null || proxies.size()<=0){
			return null;
		}
		Proxy proxy = proxies.get(index);
		index = (index+1)% proxies.size();
		return proxy;
	}
	
	private static ProxyHost getProxyHost(){
		Proxy proxy = getProxy();
		if(proxy==null){
			return null;
		}
		InetSocketAddress addr = (InetSocketAddress)proxy.address();
		return new ProxyHost(addr.getHostName(),addr.getPort());
	}
	
	private class RedirectException extends Exception{
		public RedirectException(){
			super();
		}
		public RedirectException(String message){
			super(message);
		}
	}
	
	private static List<Proxy> proxies = null;
	
	private static int index = 0;
	
	public static List<Proxy> getProxies() {
		return proxies;
	}

	public static void setProxyList(List<Proxy> proxies){
		WebPageDownloader.proxies = proxies;
	}
	
	
	
}
