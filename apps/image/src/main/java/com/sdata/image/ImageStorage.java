package com.sdata.image;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.framework.io.NextFileStore;
import com.lakeside.core.utils.StringUtils;
import com.sdata.context.config.Configuration;
import com.sdata.context.state.crawldb.CrawlDBImageQueueEnum;
import com.sdata.core.exception.NegligibleException;

/**
 * @author zhufb
 * 
 * image storage
 *
 */
public class ImageStorage  {
	
	private static final Logger log = LoggerFactory.getLogger("ImageStorage");
	private NextFileStore nfs ;
	private ImageStatistic is ;
	private int TIMES = 3;
	public ImageStorage(Configuration config){
		String namespace = config.get("image.namespace");
		if(StringUtils.isEmpty(namespace)){
			throw new RuntimeException("image.namespace is null,please config it!");
		}
		nfs = new NextFileStore(namespace);
		is = ImageStatistic.getInstance(config);
	}
	
	/**
	 * put one product into queue
	 * 
	 * @param item
	 */
	public void save(Map<String,Object> item) {
		String url = String.valueOf(item.get(CrawlDBImageQueueEnum.URL.value()));
		if(StringUtils.isEmpty(url)){
			log.warn("image url is empty!");
			return;
		}
		String source = String.valueOf(item.get(CrawlDBImageQueueEnum.SOURCE.value()));
		if(StringUtils.isEmpty(source)){
			log.warn("image source is empty!");
			return;
		}
		int times = 1;
		while(true){
			try{
				// upload file to hdfs
				String fid = nfs.uploadFile(url, source);
				
				//image statistic
				if(!StringUtils.isEmpty(fid)&&!"0".equals(fid)){
					is.save(source);
				}
				return;
			}catch(Exception e){
				times++;
				log.warn(e.getMessage());
				if(times > TIMES){
					throw new NegligibleException(e);
				}
			}
		}
	}
}
