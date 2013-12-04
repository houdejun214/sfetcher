package com.sdata.image;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sdata.context.config.Configuration;
import com.sdata.core.exception.NegligibleException;

/**
 * @author zhufb
 * 
 * the consumer of typical producer-consumer scenario 
 *
 */
public class ImageConsumer implements Runnable{

	private static final Logger log = LoggerFactory.getLogger("SdataCrawler.ImageConsumer");
	private ImageStorage storage;
	private ImageQueue queue;
	public ImageConsumer(Configuration config){
		storage = new ImageStorage(config);
		queue = new ImageQueue(config);
	}
	
	/**
	 * put one product into queue
	 * 
	 * @param item
	 */
	private void consume() {
		try{
			Map<String, Object> data = queue.get();
			if(data == null){
				sleep(300);
				return;
			}
			storage.save(data);
		}catch(NegligibleException ne){
			throw ne;
		}catch (Exception e) {
			throw new NegligibleException(e);
		}
	}
	
	public void run() {
		while(true){
			try{
				consume();
			}catch(NegligibleException e){
				continue;
			}
		}
	}
	
	private void sleep(long second){
		try {
			Thread.sleep(second*1000);
		} catch (InterruptedException e) {
			log.warn(e.getMessage());
		}
	}
}
