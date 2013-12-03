package com.sdata.live.parser.statistic;

import java.util.Calendar;
import java.util.Date;

import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

import com.framework.db.hbase.ids.RowkeyUtils;
import com.sdata.core.Configuration;
import com.sdata.core.Constants;
import com.sdata.core.FetchDispatch;
import com.sdata.core.parser.html.util.DocumentUtils;
import com.sdata.live.LiveItem;
import com.sdata.sense.SenseFetchDatum;
import com.sdata.sense.item.SenseCrawlItem;

/**
 * @author zhufb
 *
 */
public class DengueDayStatistic implements ISenseStatistic{
	//http://www.dengue.gov.sg/
	public void statistic(FetchDispatch fetchDispatch,Configuration conf,
			SenseCrawlItem item) {
		 ;
		String url = item.parse();
		Document document = DocumentUtils.getDocument(url);
    	Elements els = document.select("#inner-coloumn-right #calendar-panel-body");
    	if(els.size()!=2){
    		return;
    	}
    	String count = els.get(0).text();
    	String sum = els.get(1).text();
		Calendar instance = Calendar.getInstance();
		instance.setTime(new Date());
		instance.set(Calendar.HOUR_OF_DAY,0);
		instance.set(Calendar.MINUTE,0);
		instance.set(Calendar.SECOND,0);
		instance.set(Calendar.MILLISECOND,0);
		Date pubTime = instance.getTime();
		
		SenseFetchDatum sfd = new SenseFetchDatum();
		sfd.setCrawlItem(item);
		byte[] rowkey = RowkeyUtils.getRowkey(((LiveItem)item).getObjectId(),pubTime);
		sfd.addMetadata(Constants.OBJECT_ID, rowkey);
		sfd.setId(rowkey);
		sfd.addMetadata("pub_time", pubTime);
		sfd.addMetadata("count", Integer.valueOf(count));
		sfd.addMetadata("total", Integer.valueOf(sum));
		fetchDispatch.dispatch(sfd);
	}
	
}
