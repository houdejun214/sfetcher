package com.sdata.sense.parser.statistic;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import com.framework.db.hbase.ids.RowkeyUtils;
import com.lakeside.core.utils.JSONUtils;
import com.lakeside.core.utils.time.DateTimeUtils;
import com.sdata.core.Configuration;
import com.sdata.core.Constants;
import com.sdata.core.FetchDatum;
import com.sdata.core.FetchDispatch;
import com.sdata.core.http.HttpPage;
import com.sdata.core.http.HttpPageLoader;
import com.sdata.sense.SenseFetchDatum;
import com.sdata.sense.item.SenseCrawlItem;

/**
 * @author zhufb
 *
 */
public class DengueLocationStatistic implements ISenseStatistic{
	//http://www.x-dengue.com/Jsonv1/DataSet/
	public void statistic(FetchDispatch fetchDispatch,Configuration conf,
			SenseCrawlItem item) {
		String url = item.parse();
		HttpPage hp = HttpPageLoader.getDefaultPageLoader().download(url);
		String content = hp.getContentHtml();
		if(!content.startsWith("{")||!content.endsWith("}")){
			return;
		}
		List<FetchDatum> result = new ArrayList<FetchDatum>();
		JSONObject json = JSONObject.fromObject(content);
		JSONArray array = json.getJSONArray("reports");
		Iterator<JSONObject> iterator = array.iterator();
		while(iterator.hasNext()){
			JSONObject next = iterator.next();
			SenseFetchDatum sfd = new SenseFetchDatum();
			sfd.setCrawlItem(item);
			int id = (Integer)next.remove("reportId");
			Date pubTime = DateTimeUtils.parse(String.valueOf(next.remove("reportDate")),"M/d/yyyy");
			byte[] rk = RowkeyUtils.getRowkey(item.getObjectId(), id);
			sfd.addMetadata(Constants.OBJECT_ID, rk);
			sfd.setId(rk);
			sfd.addMetadata("pub_time", pubTime);
			sfd.addMetadata("count", next.remove("cases"));
			sfd.addAllMetadata(JSONUtils.map2JSONObj(next));
			result.add(sfd);
		}
		fetchDispatch.dispatch(result);
	}
	
}
