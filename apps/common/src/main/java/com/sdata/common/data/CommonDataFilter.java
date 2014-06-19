package com.sdata.common.data;

import com.sdata.context.config.Configuration;
import com.sdata.core.FetchDatum;
import com.sdata.core.filter.SdataFilter;

/**
 * Created by dejun on 20/05/14.
 */
public class CommonDataFilter extends SdataFilter {

    public CommonDataFilter(Configuration conf) {
        super(conf);
    }

    @Override
    public boolean filter(FetchDatum data) {
        CommonDatumDao dataDao = CommonDatumStorer.getDataDao();
        //String productId = data.getMeta("productId");
        if(dataDao.checkExists(data.getMetadata())){
            log.debug("duplicate datum [{}], will be filtered", data.getMetadata());
            return DISCARD;
        }
        return CONTINUE;
    };
}
