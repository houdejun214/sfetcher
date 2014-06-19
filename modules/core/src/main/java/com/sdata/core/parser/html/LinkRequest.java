package com.sdata.core.parser.html;

import com.google.common.collect.Maps;
import com.lakeside.core.utils.StringUtils;

import java.util.Map;

/**
 * Created by dejun on 19/06/14.
 */
public class LinkRequest {

    private String url;

    private String method="get";

    private Map<String, String> header = Maps.newHashMap();

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public Map<String, String> getHeader() {
        return header;
    }

    public void setHeader(Map<String, String> header) {
        this.header = header;
    }

    public void addHeader(String key, Object val) {
        this.header.put(key, StringUtils.valueOf(val));
    }

    public boolean isEmpty() {
        return StringUtils.isEmpty(url);
    }
}
