package com.sdata.apps.amazon;

import com.google.common.collect.Maps;
import com.lakeside.core.ArgOptions;
import com.lakeside.core.utils.PathUtils;
import com.lakeside.data.redis.RedisDB;
import com.lakeside.data.sqldb.MysqlDataSource;
import com.sdata.conf.ArgConfig;
import com.sdata.conf.sites.CrawlConfig;
import com.sdata.conf.sites.CrawlConfigManager;
import com.sdata.context.config.Configuration;
import com.sdata.context.state.crawldb.CrawlDBRedis;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by dejun on 19/05/14.
 *
 * The Meta data is crawled by our AmazonCrawler.
 *
 */
public class AmazonImageDownloader {

    private final ExecutorService pool;
    private final String dirctory;
    private final MysqlDataSource dataSource;
    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final Configuration conf;
    private final RedisDB dbRedis;
    private final int size = 100;

    public AmazonImageDownloader(Configuration conf){
        this.conf = conf;
        Integer thread = conf.getInt("thread", 10);
        dirctory = conf.get("dirctory");
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

    public void start(){
        int start = 0;
        while (true) {
            query(start, size);
        }
    }

    private List<Map<String, Object>> query(int start, int size){
        String sql = "select * from production where seq >:start and seq<=end";
        HashMap<String, Object> params = Maps.newHashMap();
        params.put("start",start);
        params.put("end",start+size);
        List<Map<String, Object>> results = jdbcTemplate.queryForList(sql, params);
        return results;
    }

    public void download(String category, String url) throws IOException {
        String fileName = PathUtils.getFileName(url);
        String destFile = PathUtils.getPath(dirctory + "/" + category + "/" + fileName);
        com.lakeside.core.utils.FileUtils.insureFileDirectory(destFile);
        FileUtils.copyURLToFile(new URL(url),new File(destFile));
    }

    public static void main(String[] args) throws Exception {
        CrawlConfigManager configs = CrawlConfigManager.load("amazon","amazon",false);
        Configuration conf = configs.getDefaultConf();
        CrawlConfig crawlSite = configs.getCurCrawlSite();
        ArgConfig options = new ArgConfig(args,args.length);
        crawlSite.putAllConf(options);
        AmazonImageDownloader downloader = new AmazonImageDownloader(crawlSite.getConf());
        downloader.start();
    }
}
