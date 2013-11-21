package com.sdata.component.site;

import java.text.MessageFormat;

import com.sdata.core.PageRegionArea;
import com.sdata.core.RegionArea;
import com.sdata.core.site.BaseDataApi;

public class PanoramioApi extends BaseDataApi{

	private static final String UrlPattern = "http://www.panoramio.com/map/get_panoramas.php?set=full&mapfilter=false&size=original&minx={0}&miny={1}&maxx={2}&maxy={3}&from={4}&to={5}";
	
	private static int pageSize = 100;

	public String getQueryUrl(RegionArea area, int page) {
		if(page<=0){
			page = 1;
		}
		long pageStart =  (page-1)*pageSize;
		long pageEnd = page*pageSize;
		String query=MessageFormat.format(UrlPattern, 
				area.getMinX().toString(),
				area.getMinY().toString(),
				area.getMaxX().toString(),
				area.getMaxY().toString(),
				String.valueOf(pageStart),
				String.valueOf(pageEnd));
		return repairLink(query);
	}

	public String getQueryUrl(PageRegionArea area) {
		return this.getQueryUrl(area, area.getPage());
	}
}
