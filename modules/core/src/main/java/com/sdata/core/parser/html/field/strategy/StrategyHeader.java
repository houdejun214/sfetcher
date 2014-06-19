package com.sdata.core.parser.html.field.strategy;

import com.sdata.core.parser.html.field.Tags;

/**
 * Created by dejun on 19/06/14.
 */
public class StrategyHeader  extends StrategyField{

    private String key;

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    @Override
    public String getName() {
        return Tags.HEADER.getName();
    }
}
