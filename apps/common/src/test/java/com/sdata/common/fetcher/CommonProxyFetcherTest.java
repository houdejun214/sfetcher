package com.sdata.common.fetcher;

import com.google.common.collect.Maps;
import com.lakeside.download.http.HttpPage;
import com.lakeside.download.http.HttpPageLoader;

import java.util.HashMap;

import static org.junit.Assert.*;

public class CommonProxyFetcherTest {
    public static void main(String[] args) {
        HttpPageLoader loader = HttpPageLoader.getAdvancePageLoader();
//        HttpPage page = loader.get("http://www.jabong.com/women/clothing/womens-dresses/shift-dress/?page=2");
        HttpPage page = loader.get("http://www.jabong.com/women/clothing/womens-dresses/shift-dress/");
        HashMap<Object, Object> header = Maps.newHashMap();
        header.put("User-Agent","Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.153 Safari/537.36");
        header.put("X-Requested-With","XMLHttpRequest");
        header.put("Accept","text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8");
        System.out.println(page.getContentHtml());
    }
}