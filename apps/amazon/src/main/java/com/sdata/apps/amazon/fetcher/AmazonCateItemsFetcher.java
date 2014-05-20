package com.sdata.apps.amazon.fetcher;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.lakeside.core.utils.ApplicationResourceUtils;
import com.lakeside.core.utils.FileUtils;
import com.lakeside.core.utils.StringUtils;
import com.sdata.apps.amazon.parser.AmazonParseResult;
import com.sdata.apps.amazon.parser.AmazonParser;
import com.sdata.context.config.Configuration;
import com.sdata.context.config.Constants;
import com.sdata.context.state.RunState;
import com.sdata.core.FetchDatum;
import com.sdata.core.RawContent;
import com.sdata.core.fetcher.SdataFetcher;
import com.sdata.core.parser.ParseResult;
import org.apache.commons.lang.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Queue;

/**
 * fetch a single category data
 */
public class AmazonCateItemsFetcher extends SdataFetcher {
    private static final Logger log = LoggerFactory.getLogger("SdataCrawler.AmazonFetcher");

    private Queue<Map<String,Object>> categoires=null;

    private Map<String, Object> curCategory=null;
    private Boolean curCategoryOver = false;
    private int curPageNo = 1;
    private static int maxPage = 100;
    private List<String> categoryFilters;

    public AmazonCateItemsFetcher(Configuration conf, RunState state) throws UnknownHostException {
        this.setConf(conf);
        this.setRunState(state);
        AmazonParser amazonParser = new AmazonParser(conf,state);
        this.parser = amazonParser;
        initProductCategory(amazonParser);
        resetFetchState();
    }

    private void resetFetchState(){
        curCategory = categoires.poll();
        curPageNo = 1;
        curCategoryOver = false;
        if(curCategory!=null) {
            String currentFetchState = state.getCurrentFetchState();
            if (StringUtils.isNotEmpty(currentFetchState)) {
                String[] arrs = currentFetchState.split(",", 2);
                if (arrs != null && arrs.length == 2) {
                    curCategory.put(Constants.QUEUE_URL, arrs[1]);
                    curPageNo = Integer.valueOf(arrs[0]);
                }
            }
        }
    }

    @Override
    public List<FetchDatum> fetchDatumList() {
        if(curCategory==null && (categoires!=null && categoires.size()>0)){
            moveNext();
        }
        if(curCategory!=null){
            String category = (String)curCategory.get(Constants.QUEUE_NAME);
            String link = (String)curCategory.get(Constants.QUEUE_URL);
            String content = ((AmazonParser)parser).download(link);
            RawContent c = new RawContent(link,content);
            c.setMetadata(Constants.QUEUE_DEPTH, curCategory.get(Constants.QUEUE_DEPTH));
            c.setMetadata("category", category);
            AmazonParseResult parseList = (AmazonParseResult)parser.parseList(c);
            if(!parseList.isListEmpty() && parseList.getNextUrl()!=null&&maxPage>=curPageNo){
                curCategory.put(Constants.QUEUE_URL, parseList.getNextUrl());
            } else {
                curCategoryOver = true;
            }
            List<FetchDatum> fetchList = parseList.getFetchList();
            Object name = curCategory.get(Constants.QUEUE_NAME);
            log.info("fetch category list [{}], page [{}], fetch num [{}] ",new Object[]{name,curPageNo,fetchList.size()});
            if(curPageNo<=1 && (fetchList==null || fetchList.size()==0)){
                log.info("There are likely to be blocked, wait 5 minutes");
                this.await(3*1000*60);
            }else{
                moveNext();
            }
            return fetchList;
        }
        return null;
    }

    @Override
    public FetchDatum fetchDatum(FetchDatum datum) {
        this.await(500);
        if(datum!=null && !StringUtils.isEmpty(datum.getUrl())){
            String url = datum.getUrl();
            //log.info("fetching product: "+url);
            String content = ((AmazonParser)parser).download(url);
            //datum.setContent(content);
            RawContent rawContent = new RawContent(url,content);
            rawContent.addAllMeata(datum.getMetadata());
            ParseResult result = parser.parseSingle(rawContent);
            if(result != null){
                datum.setMetadata(result.getMetadata());
                datum.addMetadata(Constants.FETCH_TIME, new Date());
            }else{
                throw new RuntimeException("fetch content is empty");
            }
        }
        return datum;
    }

    /**
     * move to next crawl instance
     */
    @Override
    protected void moveNext() {
        if(curCategory==null || curCategoryOver){
            curCategory = categoires.poll();
            curPageNo = 1;
            curCategoryOver = false;
        } else {
            curPageNo++;
        }
        state.setCurrentEntry((Integer) curCategory.get(Constants.QUEUE_SEQUENCE_ID));
        state.setCurrentFetchState(StringUtils.format("{0},{1}",curPageNo,curCategory.get(Constants.QUEUE_URL)));
    }

    @Override
    public boolean isComplete(){
        boolean complete = false;
        if(curCategory==null&&categoires.size()==0) {
            //delete queue;
            //update currentFetchState to a empty String for next task
            state.updateCurrentFetchState("");
            complete = true;
        }
        return complete;
    }

    private void initProductCategory(AmazonParser amazonParser) {
        String filterFile = this.getConf("cateListFile");
        String file = ApplicationResourceUtils.getResourceUrl(filterFile);
        List<String> categoryList = null;
        categoires = Lists.newLinkedList();
        int startId = state.getCurrentEntry();
        log.info("start crawler from [{}]",startId);
        try {
            categoryList = FileUtils.readLines(new File(file), "utf-8");
            int i=1;
            for(String category:categoryList){
                if(i>=startId){
                    String[] arrs = category.split(",",2);
                    if(arrs!=null && arrs.length==2) {
                        String cate = arrs[0];
                        String link = StringUtils.trim(arrs[1], "\"");
                        Map<String, Object> data = Maps.newHashMap();
                        data.put(Constants.QUEUE_SEQUENCE_ID, i);
                        data.put(Constants.QUEUE_NAME, cate);
                        data.put(Constants.QUEUE_URL, StringEscapeUtils.unescapeXml(link));
                        categoires.add(data);
                    }else{
                        log.warn("[{}] can't be parse",category);
                    }
                }
                i++;
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

    }
}
