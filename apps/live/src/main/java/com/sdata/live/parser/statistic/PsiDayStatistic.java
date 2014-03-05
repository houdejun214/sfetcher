package com.sdata.live.parser.statistic;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.framework.db.hbase.ids.RowkeyUtils;
import com.lakeside.core.utils.StringUtils;
import com.lakeside.core.utils.time.DateTimeUtils;
import com.sdata.context.config.Configuration;
import com.sdata.context.config.Constants;
import com.sdata.core.FetchDatum;
import com.sdata.core.FetchDispatch;
import com.sdata.core.parser.html.util.DocumentUtils;
import com.sdata.live.LiveItem;
import com.sdata.proxy.SenseFetchDatum;
import com.sdata.proxy.item.SenseCrawlItem;

/**
 * @author zhufb
 *
 */
public class PsiDayStatistic implements ISenseStatistic{
	
//	private String url = "http://app2.nea.gov.sg/anti-pollution-radiation-protection/air-pollution/psi/historical-psi-readings/year/%d/month/%d/day/%d";
	public void statistic(FetchDispatch fetchDispatch,Configuration conf,
			SenseCrawlItem item) {
		String url = item.parse();
		String beginstr = conf.get("sense.statistic.psi.beginDate");
		Date begin = DateTimeUtils.parse(beginstr, "yyyyMMdd");
		Date end = new Date();
		Date current = begin;
		while(current.before(end)){
			List<FetchDatum> result = new ArrayList<FetchDatum>();
			Calendar ins = Calendar.getInstance();
			ins.setTime(current);
			int year = ins.get(Calendar.YEAR);
			int month = ins.get(Calendar.MONTH) + 1;
			int date = ins.get(Calendar.DATE);
			String format = String.format(url, year,month,date);
			Document document = DocumentUtils.getDocument(format);
			Elements els = document.select("tbody tr");
			for(Element e:els){
				Elements tds = e.select("td");
				SenseFetchDatum oneDatum = getOneDatum(tds,item,ins);
				if(oneDatum!=null){
					result.add(oneDatum);
				}
			}
			fetchDispatch.dispatch(result);
			current = DateTimeUtils.add(current, Calendar.DATE, 1);
		}
	}

	private SenseFetchDatum getOneDatum(Elements tds,SenseCrawlItem item,Calendar current){
		if(tds.size()!=13||(!StringUtils.isNum(tds.get(1).text())&&!StringUtils.isNum(tds.get(7).text()))){
			return null;
		}
		SenseFetchDatum sfd = new SenseFetchDatum();
		sfd.setCrawlItem(item);
		String time = tds.get(0).text();
		String apm = time.substring(time.length()-2,time.length());
		int hour = Integer.valueOf(time.substring(0,time.length()-2));
		if("AM".equals(apm)){
			if(hour == 12){
				hour = 0;
			}
		}else{
			if(hour != 12){
				hour = hour + 12;
			}
		}
		current.set(Calendar.HOUR_OF_DAY, hour);
		
		byte[] rowkey = RowkeyUtils.getRowkey(((LiveItem)item).getObjectId(),current.getTime());
		sfd.addMetadata(Constants.OBJECT_ID, rowkey);
		sfd.setId(rowkey);
		sfd.addMetadata("pub_time", current.getTime());

		if(StringUtils.isNum(tds.get(1).text())){
			sfd.addMetadata("psi-n", tds.get(1).text());
			sfd.addMetadata("psi-s", tds.get(2).text());
			sfd.addMetadata("psi-e", tds.get(3).text());
			sfd.addMetadata("psi-w", tds.get(4).text());
			sfd.addMetadata("psi-c", tds.get(5).text());
			sfd.addMetadata("psi-sg",tds.get(6).text());
		}

		if(StringUtils.isNum(tds.get(7).text())){
			sfd.addMetadata("pm-n", tds.get(7).text());
			sfd.addMetadata("pm-s", tds.get(8).text());
			sfd.addMetadata("pm-e", tds.get(9).text());
			sfd.addMetadata("pm-w", tds.get(10).text());
			sfd.addMetadata("pm-c", tds.get(11).text());
			sfd.addMetadata("pm-sg",tds.get(12).text());
		}
		return sfd;
	}
	
}
