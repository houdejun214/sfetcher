package com.sdata.hot.fetcher;

import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sdata.core.Configuration;
import com.sdata.core.FetchDatum;
import com.sdata.core.FetchDispatch;
import com.sdata.core.SdataConfigurable;
import com.sdata.hot.Source;
import com.sdata.hot.util.HotUtils;

/**
 * @author zhufb
 *
 */
public abstract class HotBaseFetcher extends SdataConfigurable implements IHotFetcher,Runnable {
	
	protected static Logger log = LoggerFactory.getLogger("Hot.HotBaseFetcher");
	private static FetchDispatch dispatch;
	private static CountDownLatch latch;
	private static Date batchTime;
	
	protected HotBaseFetcher() {
		
	}
	
	public HotBaseFetcher(Configuration conf) {
		super.setConf(conf);
	}
	
	/**
	 * 获取datum list 主要方法 abstract
	 * 
	 * @return
	 */
	public abstract List<FetchDatum> getDatumList();
	
	/**
	 * 获取单个datum 主要方法 【默认实现】
	 * 如有单独逻辑，继承该类后，重写该方法
	 * 
	 * @param datum
	 * @return
	 */
	public abstract FetchDatum fetchDatum(FetchDatum datum);
	
	/* 
	 * 线程运行方法
	 * 
	 * 
	 * (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	public void run() {
		try{
			List<FetchDatum> list = this.getDatumList();
			log.info(this.getClass().getSimpleName()+" got datum list size："+list.size());
			// 分发获取的datum list
			this.dispatch(list);
		}catch(Exception e){
			log.error(e.getMessage());
			throw new RuntimeException(e);
		}finally{
			//结束此次获取 datum list
			this.complete();
		}
	}

	/**
	 * 分发Datum list
	 * 
	 * @param list
	 */
	protected void dispatch(List<FetchDatum> list){
		dispatch.dispatch(list);
	}

	/**某一fetch datum list结束时latch -1
	 * 
	 */
	protected void complete(){
		latch.countDown();
	}
	
	/**某一fetch datum list结束时latch -1
	 * 
	 */
	protected Date getBatchTime(){
		return batchTime;
	}
	
	/**
	 * init dispatch and latch before start task
	 * 
	 * @param dispatch
	 * @param latch
	 */
	public static void initBeforeStart(Date batchTime,FetchDispatch dispatch,CountDownLatch latch){
		HotBaseFetcher.batchTime = batchTime;
		HotBaseFetcher.dispatch = dispatch;
		HotBaseFetcher.latch = latch;
	}
	
	/**
	 * 获取当前Fetcher 的rowkey的Bytes值
	 * @param i
	 * @return
	 */
	protected byte[] getHotRowKeyBytes(int rankValue) {
		byte[] rk = HotUtils.getRowkey(type().getValue(),getBatchTime(),source().getValue().hashCode(), rankValue);
		return rk;
	}
}
