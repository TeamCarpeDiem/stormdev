package storm.starter.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.log4j.Logger;
import storm.starter.Interfaces.IWindowBolt;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by sachin on 6/11/15. //Test bolt to verify tumbling window
 */
public class MobileCompanyCounter extends BaseRichBolt implements IWindowBolt{
    final static Logger LOG = Logger.getLogger(MobileCompanyCounter.class.getName());
    OutputCollector _collector;

    //static WindowObject wObject;
    long counter;

    String sample;
    Long start;
    HashMap<String,Integer> hmap;
    Iterator itr;
    public MobileCompanyCounter()
    {
        counter=0;
        start = System.currentTimeMillis();
        hmap.put("samsung",0);
        hmap.put("moto",0);
        hmap.put("nokia",0);
        hmap.put("apple",0);
        hmap.put("blackberry",0);
        hmap.put("sony",0);
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        hmap = new HashMap<String,Integer>();
        _collector = collector;

    }

    @Override
    public void execute(Tuple tuple) {
        if (isMockTick(tuple))
        {
            LOG.info("~~~Got Mock Tuple");
            counter = counter%5;
            counter++;
            LOG.info("Evaluation for" + counter + "lakhs");
            LOG.info("------------------------------------------------");
            for (String key : hmap.keySet()) {
                LOG.info("Mention of the company::" + key + " is "+ hmap.get(key) +" times");
                hmap.put(key,0);
            }
            LOG.info("------------------------------------------------");

         //   _collector.emit(tuple, new Values(avg));
           // LOG.info("The tuple data is:: " + tuple.getString(0));
            //LOG.info("Window Avg is::" + avg + "    Window Total::" + cma + "   Count::" + count + "   Length::" + length + "   Sample::" + sample);
            //LOG.info("Time between mock tuples" + (System.currentTimeMillis() - start));
           // start = System.currentTimeMillis();

        }
        else {

            //cma = cma + 1;//tuple.getInteger(0);
            sample = tuple.getString(0);
            //samsung, moto, nokia, apple, blackberry, sony,
            if(sample.contains("nokia"))
            {
                hmap.put("nokia", hmap.get("nokia") + 1);
            }
            if(sample.contains("moto"))
            {
                hmap.put("moto",hmap.get("moto")+1);
            }
            if(sample.contains("samsung"))
            {
                hmap.put("samsung",hmap.get("samsung")+1);
            }
            if(sample.contains("apple"))
            {
                hmap.put("apple",hmap.get("apple")+1);
            }
            if(sample.contains("blackberry"))
            {
                hmap.put("blackberry",hmap.get("blackberry")+1);
            }
            if(sample.contains("sony"))
            {
                hmap.put("sony",hmap.get("sony")+1);
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("Average"));
    }

    public boolean isMockTick(Tuple tuple) {
        return tuple.getSourceStreamId().equals("mockTickTuple");
    }
}
