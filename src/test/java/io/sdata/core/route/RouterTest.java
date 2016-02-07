package io.sdata.core.route;

import org.junit.Test;

import static org.junit.Assert.*;

public class RouterTest {

    @Test
    public void testAddRoute() throws Exception {
        Router<Object> router = new Router<>();
        router.addRoute("http://www.amazon.com/s/:*", "1");
        router.addRoute("http://www.amazon.com/sp/:*", "2");
        assertEquals(2, router.routes().size());
    }

    @Test
    public void testRemovePath() throws Exception {
        Router<Object> router = new Router<>();
        router.addRoute("http://www.amazon.com/s/:*", "1");
        router.addRoute("http://www.amazon.com/sp/:*", "2");
        router.removePath("http://www.amazon.com/sp/:*");
        router.removePath("http://www.amazon.com/sp/:*");
        assertEquals(1, router.routes().size());
    }

    @Test
    public void testRoute() throws Exception {
        Router<Object> router = new Router<>();
        router.addRoute("http://www.amazon.com/s/:*", "1");
        router.addRoute("http://www.amazon.com/sp/:*", "2");
        RouteResult<Object> result = router.route("http://www.amazon.com/s/ref=lp_7147441011_ex_n_1?rh=n%3A7141123011%2Cn%3A10445813011");
        assertTrue("1".equals(result.target()));
        result = router.route("http://www.amazon.com/sp/ref=lp_7147441011_ex_n_1?rh=n%3A7141123011%2Cn%3A10445813011");
        assertTrue("2".equals(result.target()));
    }
}