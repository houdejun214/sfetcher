package com.sdata.hot.fetcher;

import java.text.MessageFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lakeside.core.utils.StringUtils;
import com.sdata.context.config.Configuration;
import com.sdata.core.FetchDatum;
import com.sdata.core.FetchDispatch;
import com.sdata.core.fetcher.SdataFetcher;
import com.sdata.hot.Hot;
import com.sdata.hot.util.HotUtils;

/**
 * @author zhufb
 *
 */
public class HotProxyFetcher extends SdataFetcher {

	protected static Logger log = LoggerFactory.getLogger("Hot.HotFetcher");
	 /**
	  * Type 取值从Hot中获取
	  
	    All(0),
	    Event(1),
		Interest(2),
		Venue(3),
		Image(4),
		Social(5),
		Food(6),
		Shops(7),
		Video(8);
	 */
	private Hot type = Hot.All;
	private Date batchTime;
	private String notifyUrl;
	
	public HotProxyFetcher(Configuration conf){
		this.setConf(conf);
		this.notifyUrl = this.getConf("notify.url");
		String st = this.getConf("type", "0");
		// number support 0,1,...,8
		if(StringUtils.isNum(st)){
			this.type = Hot.get(Integer.valueOf(st));
		}
		// string support all,event,...,video
		else{
			this.type = Hot.get(st);
		}
	}
	
	/* 
	 * 实现获取Datum List
	 * 
	 * (non-Javadoc)
	 * @see com.sdata.core.fetcher.SdataFetcher#fetchDatumList(com.sdata.core.FetchDispatch)
	 */
	@Override
	public void fetchDatumList(FetchDispatch dispatch) {
		try {
			//定义此次fetch的批次时间
			this.batchTime = new Date();
			// 获取某一类型下的所有List Fetcher
			List<HotBaseFetcher> list = HotFetcherLookup.getFetcher(getConf(),type);
			CountDownLatch latch = new CountDownLatch(list.size());
			// 初始化分发datum的dispatch对象及多线程管理的CountDownLatch
			HotBaseFetcher.initBeforeStart(batchTime,dispatch, latch);
			// 多线程启动fetcher list
			for(HotBaseFetcher f:list){
				Thread t = new Thread(f);
				t.start();
			}
			latch.await();
			log.info("Type "+ type+ " fetch datum list finish! ");
		} catch (InterruptedException e) {
			throw new RuntimeException("Hot fetcher fetch datum list failed ...",e);
		}
	}
	
	/* 
	 * 实现获取单个Datum
	 * 
	 * (non-Javadoc)
	 * @see com.sdata.core.fetcher.SdataFetcher#fetchDatum(com.sdata.core.FetchDatum)
	 */
	@Override
	public FetchDatum fetchDatum(FetchDatum datum){
		HotBaseFetcher fetcher = HotFetcherLookup.getFetcher(getConf(), datum);
		return fetcher.fetchDatum(datum);
	}
	
	@Override
	public boolean isComplete(){
		return true;
	}
	
	@Override
	public void taskFinish(){
		// pass batch time to deal
 		long batch = Long.MAX_VALUE - this.batchTime.getTime(); 
		String type = this.type.getName();
		String url = MessageFormat.format(this.notifyUrl,type,String.valueOf(batch));
		HotUtils.notify(url);
		log.info("notify "+ url);
		log.info("type "+ type +", batch "+ batch +" finish!");
	}
}
