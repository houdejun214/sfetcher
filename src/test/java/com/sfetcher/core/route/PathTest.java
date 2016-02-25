package com.sfetcher.core.route;

import com.google.common.collect.Maps;
import com.lakeside.core.utils.StringUtils;
import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.*;

public class PathTest {

    @Test
    public void testPath() throws Exception {
        String url = "https://hanxierka.tmall.com/category-1046631575.htm?search=y&catName=%B4%BA%CF%C4%D7%B0%D0%C2%C6%B7%C7%F8";
        Path path = new Path("https://hanxierka.tmall.com/category-{name}-{id}.htm?*");
        String[] paths = StringUtils.split(url, "/");
//        HashMap<String, String> params = Maps.newHashMap();
//        assertTrue(path.match(paths, params));
//        assertTrue(params.size()==1);
    }

    @Test
    public void testTokens() throws Exception {
        Path path = new Path("http://www.amazon.com/s/:*");
//        assertTrue(path.tokens().length==4);
    }

    @Test
       public void testMatch1() throws Exception {
        String url = "http://www.amazon.com/s/ref=lp_7147441011_ex_n_1?rh=n%3A7141123011%2Cn%3A10445813011";
        Path path = new Path("http://www.amazon.com/s/*");
        HashMap<String, String> params = Maps.newHashMap();
        assertTrue(path.match(url, params));
    }

    @Test
    public void testMatch2() throws Exception {
        String url = "https://hanxierka.tmall.com/category-1046631575.htm?search=y&catName=%B4%BA%CF%C4%D7%B0%D0%C2%C6%B7%C7%F8";
        Path path = new Path("https://hanxierka.tmall.com/category-{id}.htm?*");
        HashMap<String, String> params = Maps.newHashMap();
        assertTrue(path.match(url, params));
        assertFalse(path.match("https://hanxierka.tmall.com/category.htm?search=y&catName=%B4%BA%CF%C4%D7%B0%D0%C2%C6%B7%C7%F8", params));
    }

//    @Test
//    public void testMatchWithParams() throws Exception {
//        String url = "https://hanxierka.tmall.com/category-1046631575.htm?search=y&catName=%B4%BA%CF%C4%D7%B0%D0%C2%C6%B7%C7%F8";
//        Path path = new Path("https://hanxierka.tmall.com/category-{id}.htm?*");
//        HashMap<String, String> params = Maps.newHashMap();
//        assertTrue(path.match(url, params));
//        assertEquals("1046631575", params.get("id"));
//    }
}