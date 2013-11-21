package com.sdata.component.data;

import java.util.Date;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lakeside.core.utils.UUIDUtils;
import com.sdata.component.data.dao.VideoMgDao;
import com.sdata.component.util.YouTubeVideoDownloadUtils;
import com.sdata.core.Configuration;
import com.sdata.core.Constants;
import com.sdata.core.FetchDatum;
import com.sdata.core.RunState;
import com.sdata.core.data.FieldProcess;
import com.sdata.core.data.SdataStorer;

public class SdataVideoDbStorer extends SdataStorer {

	private static final Logger log = LoggerFactory.getLogger("SdataCrawler.SdataVideoDbStorer");
	
	private VideoMgDao dao = new VideoMgDao();
	
	private FieldProcess fieldProcess ;
	
	public SdataVideoDbStorer(Configuration conf,RunState state){
		this.setConf(conf);
		this.state = state;
		String host = this.getConf("mongoHost");
		int port = this.getConfInt("mongoPort",27017);
		String dbName = this.getConf("mongoDbName");
		this.dao.initilize(host, port, dbName);
		this.state = state;
		fieldProcess = new FieldProcess(conf);
	}
	
	@Override
	public void save(FetchDatum datum) throws Exception {
		try {
			Map<String, Object> metadata = datum.getMetadata();
			String id = (String)metadata.get("id");
			UUID _id = UUIDUtils.getMd5UUID(id);
			if(!dao.isVideoExists(_id)){
				String title = (String)metadata.get("title");
				String url = datum.getUrl();
				String fileDir = (String)metadata.get("fileDir");
				String videoType = (String)metadata.get("fileTypeNum");
				YouTubeVideoDownloadUtils ytd = new YouTubeVideoDownloadUtils();
				int repeatNum = 0;
				while(true){
					try {
						Map<String ,Object> result = ytd.download(url, fileDir, id, title,videoType);
						boolean hasDownload = (Boolean)result.get("isDownload");
						if(!hasDownload){
							String reason = (String)result.get("errorReason");
							log.error("YouTube video id["+id+"] title["+title+"] download fail because:"+reason);
							if(reason.endsWith("Forbidden")){
								if(repeatNum>10){
									log.info("give up to download video id:["+id+"]  is Forbidden. ");
									return;
								}
								this.await(300000);
								repeatNum++;
								continue;
							}else{
								return;
							}
						}
						break;
					} catch (Exception e) {
						if(e.getMessage()!=null&&e.getMessage().equals("Forbidden")){
							log.info("youtube crawler has be Forbidden when download video, and wait 300s.");
							if(repeatNum>10){
								log.info("give up to download video id:["+id+"]  is Forbidden. ");
								return;
							}
							this.await(300000);
							repeatNum++;
							continue;
						}
					}
				}
				log.info("congratulations! we are success to download video id["+id+"].");
			}
			metadata.remove("fileDir");
			metadata.put(Constants.FETCH_TIME, new Date());
			metadata = fieldProcess.fieldReduce(metadata);
			dao.saveVideo(metadata,fieldProcess);
		} catch (Exception e) {
			logFaileMessage(e);
			throw e;
		}
	}

	
	protected void logFaileMessage(Exception e){
		String msg = "save failed : "+e.getMessage();
		Throwable cause = e.getCause();
		if(cause!=null){
			msg+="\r\n cause by :"+cause.getClass()+"\n"+cause.getMessage();
		}
		log.info(msg);
	}
}
