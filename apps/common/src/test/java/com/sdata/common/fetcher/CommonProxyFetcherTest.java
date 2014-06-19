package com.sdata.common.fetcher;

import com.google.common.collect.Maps;
import com.lakeside.download.http.HttpPage;
import com.lakeside.download.http.HttpPageLoader;

import java.util.HashMap;

public class CommonProxyFetcherTest {
    public static void main(String[] args) {
        HttpPageLoader loader = HttpPageLoader.getAdvancePageLoader();
        HashMap<String, String> header = Maps.newHashMap();
        //header.put("User-Agent","Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.153 Safari/537.36");
        header.put("X-Requested-With","XMLHttpRequest");
        //header.put("X-NewRelic-ID","Ug4PUVRADAsCVFFU");
        //header.put("Accept","text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8");
        //header.put("Referer","http://www.jabong.com/women/clothing/womens-dresses/shift-dress/");
        //header.put("Host","www.jabong.com");
        //header.put("Accept","*/*");
        HttpPage page = loader.get(header,"http://www.jabong.com/women/clothing/womens-dresses/shift-dress/?page=4");
        System.out.println(page.getContentHtml());
    }
}