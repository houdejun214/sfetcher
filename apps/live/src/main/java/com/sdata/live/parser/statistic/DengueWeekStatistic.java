package com.sdata.live.parser.statistic;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import net.sf.json.JSONObject;

import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.framework.db.hbase.ids.RowkeyUtils;
import com.lakeside.core.utils.PatternUtils;
import com.sdata.context.config.Configuration;
import com.sdata.context.config.Constants;
import com.sdata.core.FetchDatum;
import com.sdata.core.FetchDispatch;
import com.sdata.core.parser.html.util.DocumentUtils;
import com.sdata.live.LiveItem;
import com.sdata.sense.SenseFetchDatum;
import com.sdata.sense.item.SenseCrawlItem;

/**
 * @author zhufb
 *
 */
public class DengueWeekStatistic implements ISenseStatistic{
//	http://www.x-dengue.com/Home/Summary
	private final static String reg = "series:\\[(.*)]}\\);";
	public void statistic(FetchDispatch fetchDispatch,Configuration conf,
			SenseCrawlItem item) {
		List<FetchDatum> result = new ArrayList<FetchDatum>();
		String url = item.parse();
		Document document = DocumentUtils.getDocument(url);
    	Elements els = document.select("script:not([src])");
    	for(Element e:els){
    		String text = e.html();
    		text = text.replaceAll("\r", "").replaceAll("\n", "").replaceAll("\t", "").replaceAll(" ", "");
    		String strjson = PatternUtils.getMatchPattern(reg,text, 1);
    		if(!(strjson.startsWith("{")&&strjson.endsWith("}"))){
    			continue;
    		}
    		JSONObject json = JSONObject.fromObject(strjson);
    		int year = json.getInt("name");
    		List data = (List)json.get("data");
    		for(int i=0;i<data.size();i++){
    			Object num = data.get(i);
				if(num ==null||"null".equals(num.toString())||"".equals(num.toString())){
					continue;
    			}
				Calendar instance = Calendar.getInstance();
				instance.set(Calendar.YEAR,year);
				instance.set(Calendar.WEEK_OF_YEAR, i+1);
				instance.set(Calendar.HOUR,0);
				instance.set(Calendar.MINUTE,0);
				instance.set(Calendar.SECOND,0);
				instance.set(Calendar.MILLISECOND,0);
				Date pubTime = instance.getTime();
				
				SenseFetchDatum sfd = new SenseFetchDatum();
				sfd.setCrawlItem(item);
				byte[] rowkey = RowkeyUtils.getRowkey(((LiveItem)item).getObjectId(),pubTime);
				sfd.setId(rowkey);
				sfd.addMetadata("pub_time", pubTime);
				sfd.addMetadata("count", Integer.valueOf(num.toString()));
				sfd.addMetadata(Constants.OBJECT_ID, rowkey);
				result.add(sfd);
    		}
    	}
		
		fetchDispatch.dispatch(result);
	}
	
}
