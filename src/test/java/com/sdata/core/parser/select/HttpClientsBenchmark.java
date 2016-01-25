package com.sdata.core.parser.select;

import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.util.EntityUtils;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Created by dejun on 25/1/16.
 */
public class HttpClientsBenchmark {

    @Test
    public void testAPC() throws ExecutionException, InterruptedException, IOException {
        CloseableHttpAsyncClient client = HttpAsyncClients.createDefault();
        client.start();
        long nanoTime = System.nanoTime();
        for (int i = 0; i < 10; i++) {
            HttpGet get = new HttpGet("http://www.amazon.com/gp/product/B00186YU4M/ref=s9_simh_gw_g241_i4_r?pf_rd_m=ATVPDKIKX0DER&pf_rd_s=desktop-3&pf_rd_r=0ZV3X5C8H6V3EZR25E81&pf_rd_t=36701&pf_rd_p=2084660942&pf_rd_i=desktop");
            HttpResponse response = client.execute(get, null).get();
            String content = EntityUtils.toString(response.getEntity());
        }
        long l = TimeUnit.MILLISECONDS.convert(System.nanoTime() - nanoTime, TimeUnit.NANOSECONDS);
        System.out.println("download bm: " + l);
        client.close();
    }

    @Test
    public void testNing() throws ExecutionException, InterruptedException, IOException {
        AsyncHttpClient asyncHttpClient = new AsyncHttpClient();
        long nanoTime = System.nanoTime();
        for (int i = 0; i < 10; i++) {
            Future<Response> f = asyncHttpClient
                    .prepareGet("http://www.amazon.com/gp/product/B00186YU4M/ref=s9_simh_gw_g241_i4_r?pf_rd_m=ATVPDKIKX0DER&pf_rd_s=desktop-3&pf_rd_r=0ZV3X5C8H6V3EZR25E81&pf_rd_t=36701&pf_rd_p=2084660942&pf_rd_i=desktop")
                    .execute();
            Response r = f.get();
            String content = r.getResponseBody();
        }
        long l = TimeUnit.MILLISECONDS.convert(System.nanoTime() - nanoTime, TimeUnit.NANOSECONDS);

        System.out.println("download bm: "+ l);
        asyncHttpClient.closeAsynchronously();
    }
}
