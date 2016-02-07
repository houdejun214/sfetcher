package io.sdata.core.route;

import com.google.common.collect.Maps;
import com.lakeside.core.utils.StringUtils;
import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.*;

public class PathTest {

    @Test
    public void testPath() throws Exception {
        String url = "http://www.amazon.com/s/ref=lp_7147441011_ex_n_1?rh=n%3A7141123011%2Cn%3A10445813011";
        Path path = new Path("http://www.amazon.com/s/:*");
        String[] paths = StringUtils.split(url, "/");
        HashMap<String, String> params = Maps.newHashMap();
        assertTrue(path.match(paths, params));
        assertTrue(params.size()==1);
    }

    @Test
    public void testTokens() throws Exception {
        Path path = new Path("http://www.amazon.com/s/:*");
        assertTrue(path.tokens().length==4);
    }

    @Test
    public void testMatch() throws Exception {
        String url = "http://www.amazon.com/s/ref=lp_7147441011_ex_n_1?rh=n%3A7141123011%2Cn%3A10445813011";
        Path path = new Path("http://www.amazon.com/s/:*");
        String[] paths = StringUtils.split(url, "/");
        HashMap<String, String> params = Maps.newHashMap();
        assertTrue(path.match(paths, params));

    }
}