package storm.starter;

/**
 * Created by Pradheep on 6/3/15.
 * Modified by Sachin on 6/9/15. The parameters are accessed from an instance of WindowObject class.
 */

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.log4j.Logger;
import storm.starter.HelperClasses.WindowObject;

import java.util.HashMap;
import java.util.Map;

public class TumblingWindow extends BaseWindowBolt implements ITumbling {
    final static Logger LOG = Logger.getLogger(TumblingWindow.class.getName());
    OutputCollector _collector;
    long count;
    long temp;
    boolean isTimeBased;
    WindowObject tumblingWindowObject;

    public TumblingWindow(WindowObject wObject) {
        super(wObject);
        tumblingWindowObject = wObject;
        count = wObject.getWindowLength();
        temp = wObject.getWindowLength();//added a temp variable
        isTimeBased = wObject.getIsTimeBased(); //added by sachin
    }

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
    }

    @Override
    public final void execute(Tuple tuple) {
        if(isTimeBased) {
            if (isTickTuple(tuple)) {
                LOG.info("~~~~~~~~Got tick tuple");
                emitMockTickTuple(_collector,tuple);
            }
            else {
                _collector.emit("dataStream",tuple, new Values(tuple.getValue(0)));
            }}
        else {
            count = count -1;

            if(count == 0){
                _collector.emit("dataStream",tuple, new Values(tuple.getValue(0)));
                emitMockTickTuple(_collector,tuple);
                count = temp;//removed function call to get window length again
            }
            else{
                _collector.emit("dataStream",tuple, new Values(tuple.getValue(0)));
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