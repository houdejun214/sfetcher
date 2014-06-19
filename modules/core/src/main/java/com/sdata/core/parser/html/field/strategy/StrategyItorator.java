package com.sdata.core.parser.html.field.strategy;

import com.lakeside.core.utils.QueryUrl;
import com.lakeside.core.utils.StringUtils;
import com.sdata.context.parser.IParserContext;
import com.sdata.core.parser.html.LinkRequest;
import com.sdata.core.parser.html.field.Field;
import com.sdata.core.parser.html.field.Tags;
import org.hsqldb.lib.StringUtil;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


/**
 * @author zhufb
 *
 */
public class StrategyItorator extends StrategyField{

    private int start=0;

    private int end=20;

    private int step = 1;

    private String var;

    private String method;

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public int getStart() {
        return start;
    }

    public void setStart(int start) {
        this.start = start;
    }

    public int getEnd() {
        return end;
    }

    public void setEnd(int end) {
        this.end = end;
    }

    public String getVar() {
        return var;
    }

    public void setVar(String var) {
        this.var = var;
    }

    public int getStep() {
        return step;
    }

    public void setStep(int step) {
        this.step = step;
    }

    public String getName() {
		return Tags.ITORTOR.getName();
	}

    public Object getData(IParserContext context, Element doc) {
        this.context = context;
        LinkRequest request = new LinkRequest();
        if (this.match(doc)) {
            int _val = start;
            String s = StringUtils.valueOf(context.getVariable(var));
            if (StringUtils.isNotEmpty(s)) {
                _val = StringUtils.toInt(s);
            }
            if(_val>this.end){
                return null;
            }
            Object result = super.getSelectValue(context, doc);
            // has child it's map
            if(result!=null){
                QueryUrl url = new QueryUrl(result.toString());
                url.setParameter(this.var,String.valueOf(_val));
                url.removeDuplicated();
                context.addData(this.var, _val+step);
                request.setUrl(url.toString());
            }
            if (this.hasChild()) {
                for (Field f : this.getChilds()) {
                    StrategyHeader h = (StrategyHeader) f;
                    request.addHeader(h.getKey(),h.getData(context,doc));
                }
            }
            if (StringUtils.isNotEmpty(method)) {
                request.setMethod(method);
            }
        }
        if (request.isEmpty()) {
            return null;
        }
        return request;
	}
}
