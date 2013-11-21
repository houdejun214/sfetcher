package com.sdata.core.site;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseDataApi {

	protected Logger log = LoggerFactory.getLogger("SdataCrawler."+this.getClass().getSimpleName());

	protected String apiKey;

	public String getApiKey() {
		return apiKey;
	}

	public void setApiKey(String apiKey) {
		this.apiKey = apiKey;
	}

	protected String repairLink(String link) {
		if (link == null) {
			return "";
		}
		link = link.replaceAll(" ", "%20");
		link = link.replaceAll("#", "%23");
		link = link.replaceAll("\"", "%22");
		return link;
	}
}
