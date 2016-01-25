package com.sdata.core.datum;

import org.jsoup.nodes.Element;

/**
 * Created by dejun on 18/06/14.
 */
public class ConstDatumField extends DatumField {

    private String value="";

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

}
