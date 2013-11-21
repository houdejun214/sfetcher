package com.sdata.component.site;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fi.foyt.foursquare.api.io.DefaultIOHandler;
import fi.foyt.foursquare.api.io.IOHandler;
import fi.foyt.foursquare.api.io.Method;
import fi.foyt.foursquare.api.io.Response;

public class FoursquareFetchApi {

	private static final String apiUrl = "https://api.foursquare.com/v2/";
	private static final String DEFAULT_VERSION = "20110615";
	private static final Logger log = LoggerFactory.getLogger("SdataCrawler.FoursquareFetchApi");
	private String clientId;
	private String clientSecret;
	private IOHandler ioHandler;
	private String version = DEFAULT_VERSION;

	public FoursquareFetchApi() {
		ioHandler = new DefaultIOHandler();
	}

	public FoursquareFetchApi(String clientId, String clientSecret,	String redirectUrl) {
		ioHandler = new DefaultIOHandler();
	}

	private String authToken = "";

	public String getAuthToken() {
		return authToken;
	}

	public void setAuthToken(String authToken) {
		this.authToken = authToken;
	}

	public String getCheckIn(String checkinId, String signature) {
		Response response = doApiRequest(Method.GET, "checkins/" + checkinId,
				true, "signature", signature);
		String responseContent = response.getResponseContent();
		return responseContent;
	}

	private Response doApiRequest(Method method, String path, boolean auth,
			Object... params) {
		String url = getApiRequestUrl(path, auth, params);
		int retryCount = 0;
		while (true) {
			Response response = ioHandler.fetchData(url, method);
			if (response.getResponseCode() == 200) {
				return response;
			}
			if (retryCount >= 5) {
				break;
			}
			retryCount++;
			log.info("fetch data error [{}] try again",response.getMessage());
		}
		return null;
	}

	private String getApiRequestUrl(String path, boolean auth, Object... params) {
		StringBuilder urlBuilder = new StringBuilder(apiUrl);
		urlBuilder.append(path);
		urlBuilder.append('?');

		if (params.length > 0) {
			int paramIndex = 0;
			try {
				while (paramIndex < params.length) {
					Object value = params[paramIndex + 1];
					if (value != null) {
						urlBuilder.append(params[paramIndex]);
						urlBuilder.append('=');
						urlBuilder.append(URLEncoder.encode(value.toString(),
								"UTF-8"));
						urlBuilder.append('&');
					}

					paramIndex += 2;
				}
			} catch (UnsupportedEncodingException e) {

			}
		}

		if (auth) {
			urlBuilder.append("oauth_token=");
			urlBuilder.append(getAuthToken());
		} else {
			urlBuilder.append("client_id=");
			urlBuilder.append(clientId);
			urlBuilder.append("&client_secret=");
			urlBuilder.append(clientSecret);
		}
		urlBuilder.append("&v=" + version);
		return urlBuilder.toString();
	}
}
