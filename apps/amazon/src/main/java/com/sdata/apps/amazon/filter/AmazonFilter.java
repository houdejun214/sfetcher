package com.sdata.apps.amazon.filter;

import com.sdata.apps.amazon.data.AmazonDataDao;
import com.sdata.apps.amazon.data.AmazonStorer;
import com.sdata.context.config.Configuration;
import com.sdata.core.FetchDatum;
import com.sdata.core.filter.SdataFilter;

/**
 * Created by dejun on 20/05/14.
 */
public class AmazonFilter extends SdataFilter {

    public AmazonFilter(Configuration conf) {
        super(conf);
    }

    @Override
    public boolean filter(FetchDatum data) {
        AmazonDataDao dataDao = AmazonStorer.getDataDao();
        String productId = data.getMeta("productId");
        if(dataDao.checkExists(productId)){
            log.info("duplicate datum [{}], will be filtered", productId);
            return DISCARD;
        }
        return CONTINUE;
    };
}
