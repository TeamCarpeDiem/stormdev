package storm.starter;

/**
 * Created by Pradheep on 6/3/15.
 */

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

public class TumblingWindow extends BaseWindowBolt {
    final static Logger LOG = Logger.getLogger(TumblingWindow.class.getName());
    OutputCollector _collector;
    long count;

    public TumblingWindow(long windowlength, boolean istimebased) {
        super(windowlength, istimebased);
        count = getWindowLength();
    }

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
    }

    @Override
    public final void execute(Tuple tuple) {
        if(isTimeBased()){
        if (isTickTuple(tuple)) {
            LOG.info("~~~~~~~~Got tick tuple");
            emitMockTickTuple(_collector,tuple);
        }
        else {
            _collector.emit("dataStream",tuple, new Values(tuple.getInteger(0)));
        }}
        else
        {
            count = count -1;

            if(count == 0){
                _collector.emit("dataStream",tuple, new Values(tuple.getInteger(0)));
                emitMockTickTuple(_collector,tuple);
                count = getWindowLength();
            }
            else{
                _collector.emit("dataStream",tuple, new Values(tuple.getInteger(0)));
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("dataStream", new Fields("RandomInt"));
        declarer.declareStream("mockTickTuple", new Fields("MockTick"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS,getWindowLength());
        return conf;
    }
}
