package storm.starter.bolt;

/**
 * Created by Pradheep on 6/3/15.
 * Modified by Sachin on 6/9/15. The parameters are accessed from an instance of WindowObject class.
 * Modified for Bug Fixes on 6/11/15.
 */

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.log4j.Logger;
import storm.starter.HelperClasses.WindowObject;
import storm.starter.Interfaces.ITumbling;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TumblingWindow extends BaseWindowBolt implements ITumbling {
    final static Logger LOG = Logger.getLogger(TumblingWindow.class.getName());
    protected OutputCollector _collector;
    long count;
    long temp;
    boolean isTimeBased;
    WindowObject tumblingWindowObject;
    boolean isExecutedOnce = false;

  //  MyRunnable myRunnable; //temp
  //  Thread t; //temp
    static BlockingQueue<Tuple> bQueue = new LinkedBlockingQueue<Tuple>();;


    /**
     *
     * @param wObject Window Object
     */
    public TumblingWindow(WindowObject wObject) {
        super(wObject);
        tumblingWindowObject = wObject;
        count = wObject.getWindowLength();
        temp = wObject.getWindowLength();
        isTimeBased = wObject.getIsTimeBased();
    }

    /**
     *
     * @param conf The Storm configuration for this bolt. This is the configuration provided to the topology merged in with cluster configuration on this machine.
     * @param context This object can be used to get information about this task's place within the topology, including the task id and component id of this task, input and output information, etc.
     * @param collector The collector is used to emit tuples from this bolt. Tuples can be emitted at any time, including the prepare and cleanup methods. The collector is thread-safe and should be saved as an instance variable of this bolt object.
     */
    public void prepare(Map conf, TopologyContext context, OutputCollector collector)
    {
        _collector = collector;
        //bQueue = new LinkedBlockingQueue<Tuple>();
       /// myRunnable = new MyRunnable(10);
        //t = new Thread(myRunnable);
       // t.start();
    }

    /**
     *
     * @param tuple The input tuple to be processed.
     */
    @Override
    public final void execute(Tuple tuple) {

        if(isTimeBased) {
            if (isTickTuple(tuple)) {
                LOG.info("~~~~~~~~Got tick tuple");
                bQueue.add(tuple);
                // emitMockTickTuple(_collector,tuple);
            }
            else {
                bQueue.add(tuple);
                // _collector.emit("dataStream",tuple, new Values(tuple.getValue(0)));
            }}
        else {
            count = count -1;

            if(count == 0){

                _collector.emit("dataStream",tuple, new Values(tuple.getValue(0)));
                emitMockTickTuple(_collector,tuple);
                count = temp;
            }
            else{
                _collector.emit("dataStream",tuple, new Values(tuple.getValue(0)));
            }
        }

    }



    /**
     *
     * @param declarer this is used to declare output stream ids, output fields, and whether or not each output stream is a direct stream
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("dataStream", new Fields("RandomInt"));
        declarer.declareStream("mockTickTuple", new Fields("MockTick"));
    }

    /**
     *
     * Declare configuration specific to this component.
     */
    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS,getWindowLength());
        return conf;
    }

}

