package com.sdata.apps.amazon;

import com.google.common.collect.Maps;
import com.lakeside.core.utils.PathUtils;
import com.lakeside.data.redis.RedisDB;
import com.lakeside.data.sqldb.MysqlDataSource;
import com.sdata.conf.ArgConfig;
import com.sdata.conf.sites.CrawlConfig;
import com.sdata.conf.sites.CrawlConfigManager;
import com.sdata.context.config.Configuration;
import com.sdata.context.state.crawldb.CrawlDBRedis;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by dejun on 19/05/14.
 *
 * The Meta data is crawled by our AmazonCrawler.
 *
 */
public class AmazonImageDownloader {

    private static final Logger log = LoggerFactory.getLogger(AmazonImageDownloader.class);

    private final ExecutorService pool;
    private final String dirctory;
    private final MysqlDataSource dataSource;
    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final Configuration conf;
    private final RedisDB dbRedis;
    private final int size = 100;
    private final int downloadRetry = 3;

    public AmazonImageDownloader(Configuration conf){
        this.conf = conf;
        Integer thread = conf.getInt("thread", 1);
        dirctory = conf.get("directory");
        pool = Executors.newFixedThreadPool(thread);
        String host = conf.get("storer.mysql.jdbc.host");
        String database = conf.get("storer.mysql.jdbc.database");
        String username = conf.get("storer.mysql.jdbc.username");
        String password = conf.get("storer.mysql.jdbc.password");
        String port = conf.get("storer.mysql.jdbc.port", "3306");
        dataSource = new MysqlDataSource(host, port, database, username, password);
        jdbcTemplate = dataSource.getJdbcTemplate();
        dbRedis = CrawlDBRedis.getRedisDB(conf, "Downloader:Amazon");
    }

    public void start() throws InterruptedException {
        int start = 0;
        final AmazonImageDownloader downloader = this;
        while (true) {
            List<Map<String, Object>> result = query(start, size);
            if(result==null || result.size()==0){
                break;
            }
            for(Map<String,Object> item: result){
                final String category = (String) item.get("category");
                final String imageUrl = (String) item.get("image_url");
                pool.submit(new Callable<Object>() {
                    @Override
                    public Object call() throws Exception {
                        downloader.download(category,imageUrl);
                        return null;
                    }
                });
            }
            start+=size;
        }
        pool.awaitTermination(5, TimeUnit.MINUTES);
    }

    private List<Map<String, Object>> query(int start, int size){
        String sql = "select * from production where seq >=:start and seq<:end";
        HashMap<String, Object> params = Maps.newHashMap();
        params.put("start",start);
        params.put("end",start+size);
        List<Map<String, Object>> results = jdbcTemplate.queryForList(sql, params);
        return results;
    }

    /**
     * download image files
     * @param category
     * @param url
     * @throws IOException
     */
    public void download(String category, String url) throws IOException {
        int i=0;
        while (i<downloadRetry) {
            try{
                String fileName = PathUtils.getFileName(url);
                String destFile = PathUtils.getPath(dirctory + "/" + category + "/" + fileName);
                com.lakeside.core.utils.FileUtils.insureFileDirectory(destFile);
                FileUtils.copyURLToFile(new URL(url), new File(destFile));
                return;
            }catch (Exception e){
                log.info("download image [{}],[{}] exception, retry it.",category, url);
                i++;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        CrawlConfigManager configs = CrawlConfigManager.load("amazon","amazon",false);
        CrawlConfig crawlSite = configs.getCurCrawlSite();
        ArgConfig options = new ArgConfig(args,args.length);
        crawlSite.putAllConf(options);
        AmazonImageDownloader downloader = new AmazonImageDownloader(crawlSite.getConf());
        downloader.start();
    }
}
