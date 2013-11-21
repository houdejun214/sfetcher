package com.sdata.component.data;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.Writer;
import java.util.Map;

import net.sf.json.JSONObject;

import com.lakeside.core.utils.FileUtils;
import com.lakeside.core.utils.PathUtils;
import com.lakeside.core.utils.StringUtils;
import com.sdata.core.Configuration;
import com.sdata.core.FetchDatum;
import com.sdata.core.RunState;
import com.sdata.core.data.SdataStorer;
import com.sdata.core.util.ApplicationResourceUtils;

public class SdataJsonFileStorer extends SdataStorer {
	
	private String path;
	private Object synObject = new Object();
	private int maxFileCount = 0;
	private int count = 0;
	public SdataJsonFileStorer(Configuration conf,RunState state){
		setConf(conf);
		this.state = state;
		maxFileCount = getConfInt("MaxFileCount",5000);
		String dataDir = getConf("DataDir");
		this.path = ApplicationResourceUtils.getResourceUrl(dataDir);
		FileUtils.insureFileDirectory(path);
		FileUtils.mkDirectory(path);
		FileUtils.emptyDir(path);
	}
	
	public SdataJsonFileStorer(Configuration conf,String path){
		setConf(conf);
		maxFileCount = getConfInt("MaxFileCount",5000);
		this.path = path;
		FileUtils.insureFileDirectory(path);
		FileUtils.mkDirectory(path);
		FileUtils.emptyDir(path);
	}

	@Override
	public void save(FetchDatum datum) throws Exception {
		synchronized(synObject){
			String saveFile = PathUtils.getPath(path+"/"+ StringUtils.padLeft(String.valueOf(count/maxFileCount+1),8,'0')+".dat");
			if (datum == null) {
				return;
			}
			Map metadata = datum.getMetadata();
			JSONObject json = (JSONObject) metadata;
			Writer output = null;
			try {
				output = new BufferedWriter(new FileWriter(saveFile,true));
				json.write(output);
				output.write("\n");
			} finally{
				if(output!=null){
					output.close();
				}
			}
		}
	}
}
