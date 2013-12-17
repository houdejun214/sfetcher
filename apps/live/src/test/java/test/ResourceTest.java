package test;

import org.junit.Test;

import com.sdata.conf.sites.CrawlConfig;
import com.sdata.conf.sites.CrawlConfigManager;
import com.sdata.context.config.Configuration;
import com.sdata.context.config.CrawlAppContext;
import com.sdata.core.resource.Resource;
import com.sdata.core.resource.ResourceFactory;
import com.sdata.live.DBFactory;
import com.sdata.proxy.resource.TencentResource;


public class ResourceTest {
	
	private static ResourceFactory<TencentResource> factory;
	
	@Test
	public void test() throws Exception {
		
//		CrawlConfigManager configs = CrawlConfigManager.load("live",null,false);
//		Configuration conf = configs.getDefaultConf();
//		CrawlConfig crawlSite = configs.getCurCrawlSite();
//		conf = crawlSite.getConf();
//		CrawlAppContext.conf = conf;
//		factory = new ResourceFactory<TencentResource>("tencent",DBFactory.getResourceDB(),TencentResource.class);
//		
//		ResourceThread rt1 = new ResourceThread();
//		ResourceThread rt2 = new ResourceThread();
//		
//		Thread t1 = new Thread(rt1);
//		Thread t2 = new Thread(rt2);
//		
//		t1.start();
//		t2.start();
	}
	
	class ResourceThread implements Runnable{

		private Resource current;
		
		public void run() {
			current = factory.getResource();
			System.out.println(current.getId());

			System.out.println(current.getId());

			current = factory.getResource();

			System.out.println(current.getId());
		}
		
	}

}
