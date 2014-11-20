package com.sdata.common.fetcher;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.lakeside.core.utils.ApplicationResourceUtils;
import com.lakeside.core.utils.FileUtils;
import com.lakeside.core.utils.StringUtils;
import com.sdata.context.config.Configuration;
import com.sdata.context.config.Constants;
import com.sdata.context.config.CrawlAppContext;
import com.sdata.context.state.RunState;
import com.sdata.context.state.crawldb.CrawlDBQueue;
import com.sdata.core.FetchDatum;
import com.sdata.core.FetchDispatch;
import com.sdata.core.RawContent;
import com.sdata.core.fetcher.SdataFetcher;
import com.sdata.core.parser.ParseResult;
import org.apache.commons.lang.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Created by dejun on 17/06/14.
 */
public class CommonProxyFetcher extends SdataFetcher {

    private static final Logger log = LoggerFactory.getLogger("Sdatacrawler.CommonProxyFetcher");
    private final CrawlDBQueue queue;
    private boolean complete = false;
    public CommonProxyFetcher(Configuration conf, RunState state) throws UnknownHostException {
        this.setConf(conf);
        this.setRunState(state);
        queue = CrawlAppContext.db;
        String cateListFile = conf.get("input.list");
        initProductCategory(cateListFile);
    }

    private Map<String, Object> curCategory;

    @Override
    public void fetchDatumList(FetchDispatch dispatch) {
        if(curCategory==null) {
            curCategory = queue.poll();
        }
        if(curCategory ==null) {
            complete = true;
        } else {
            String category = (String) curCategory.get(Constants.QUEUE_CATEGORY);
            RawContent c = fetchContent(curCategory);
            c.setMetadata(Constants.QUEUE_CATEGORY, category);
            c.addAllMeata(curCategory);
            ParseResult parseResult = parser.parseList(c);
            if(parseResult.isBlock()){
                log.info("There are likely to be blocked, wait 5 minutes");
                this.await(3 * 1000 * 60);
                return;
            }
            List<Map<String,Object>> categoryList = parseResult.getCategoryList();
            for (Map<String,Object> cate : categoryList) {
                cate.put(Constants.QUEUE_CATEGORY, category);
            }
            queue.insertTopQueueObjects(categoryList);
            List<FetchDatum> datumList = parseResult.getFetchList();
            int size = 0;
            for (FetchDatum datum : datumList) {
                datum.addMetadata(Constants.QUEUE_CATEGORY, category);
                dispatch.dispatch(datum);
                size++;
            }
            log.info("fetch datum list:[{}] with items [{}]" , curCategory.get(Constants.QUEUE_URL), size);
            // move to next
            curCategory = queue.poll();
        }
    }

    @Override
    public FetchDatum fetchDatum(FetchDatum datum) {
        if(datum == null){
            return null;
        }
        log.debug("fetch datum one:" + datum.getUrl());
        RawContent c = new RawContent(datum.getUrl(), true, this);
        c.addAllMeata(datum.getMetadata());
        ParseResult result = parser.parseSingle(c);
        datum.addAllMetadata(result.getMetadata());
        return datum;
    }

    @Override
    public boolean isComplete(){
        return this.complete;
    }

    private void initProductCategory(String cateListFile) {
        if(queue.peek()==null){
            String filterFile = cateListFile;
            String file = ApplicationResourceUtils.getResourceUrl(filterFile);
            List<String> categoryList = null;
            List<Map<String,Object>> categoires = Lists.newLinkedList();
            try {
                categoryList = FileUtils.readLines(new File(file), "utf-8");
                for(String category:categoryList){
                    String[] arrs = category.split(",");
                    if(arrs!=null && arrs.length>=2) {
                        String cate = arrs[0];
                        String link = StringUtils.trim(arrs[1], "\"");
                        Map<String, Object> data = Maps.newHashMap();
                        data.put(Constants.QUEUE_CATEGORY, cate);
                        data.put(Constants.QUEUE_URL, StringEscapeUtils.unescapeXml(link));
                        categoires.add(data);
                    }else{
                        log.warn("[{}] can't be load",category);
                    }
                }
                Collections.reverse(categoires);
                this.queue.insertQueueObjects(categoires);
                log.info("load {} categories from files {}", categoires.size(), filterFile);
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }

    }
}
