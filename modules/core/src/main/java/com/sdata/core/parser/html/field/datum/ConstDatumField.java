package com.sdata.core.parser.html.field.datum;

import com.sdata.context.parser.IParserContext;
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

    @Override
    public Object getData(IParserContext context, Element doc) {
        return this.value;
    }
}
