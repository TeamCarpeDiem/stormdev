package storm.starter.bolt;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import org.apache.log4j.Logger;
import storm.starter.HelperClasses.WindowObject;
import storm.starter.Interfaces.IWindowBolt;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Harini Rajendran on 6/7/15.
 * Modified by Sachin Jain on 6/13/2015. Moving emittter logic to BaseWindowBolt
 */

public class SlidingWindowBolt extends BaseWindowBolt implements IWindowBolt{
    final static Logger LOG = Logger.getLogger(TumblingWindow.class.getName());
    OutputCollector _collector;
    long windowStart; //Variable which keeps track of the window start
    long windowEnd; //Variable which keeps track of the window end
    long tupleCount; //Variable to keep track of the tuple count for time based window
    boolean isExecutedOnce = false; //Boolean which controls thread spawning
    boolean isTimeBased = false;
    long slideBy;
    /*/**
     * Constructor which takes the WindowObject as the parameter
     * @param wObj Window Object specifying the window parameters and window type
     */
    /*public SlidingWindowBolt(WindowObject wObj)
    {
        super(wObj);
        windowStart = 1;
        windowEnd = wObj.getWindowLength();
        tupleCount = 0;
        slideBy = wObj.getSlideBy();
        if(wObj.getIsTimeBased())
        {
            isTimeBased = wObj.getIsTimeBased();
            LOG.info("Window Start::" + tupleCount);
            addStartAddress(0l);
            windowStart += wObj.getSlideBy();
        }
    }*/

    public SlidingWindowBolt(long wlength, long slideby, boolean istimebased){
        super(wlength, slideby, istimebased);
        windowStart = 1;
        windowEnd = wlength;
        tupleCount = 0;
        slideBy = slideby;
        if(istimebased)
        {
            isTimeBased = istimebased;
            LOG.info("Window Start::" + tupleCount);
            addStartAddress(0l);
            windowStart += slideby;
        }
    }

    @Override
    /**
     * This function will get executed whenever a tuple is being received by this bolt. &This function has the logic to
     * store the tuples to the disk based on the window type, window length and slide by values.
     *
     * @param tuple The input tuple to be processed
     */
    public final void execute(Tuple tuple) {
        if(!isExecutedOnce) //Initiating the emitter thread which will emit the tuples from the disk.
        {
            Thread thread = new Thread() {
                public void run() {
                    while(true)
                    {
                        initiateEmitter(_collector);
                    }
                }
            };
            thread.start();
            isExecutedOnce = true;
        }
        if(isTimeBased)
        {
            if (isTickTuple(tuple)) {
                System.out.println("Count for this second::" + secondCount);//Testing
                secondCount = 0;//Testing
                tupleCount++;
                if(tupleCount == windowStart-1)//If the tuple marks the window beginning
                {
                    LOG.info("Window Start::" + tupleCount);
                    storeTuple(tuple, 0, 1);
                    windowStart += slideBy;
                }
                if (tupleCount == windowEnd) { //If the tuple marks the window end
                    LOG.info("Window End::" + (tupleCount));
                    storeTuple(tuple, 1, 1);
                    windowEnd += slideBy;
                }
            }
            else {
                secondCount++;//Testing
                storeTuple(tuple, -1, 1); //The tuple is in the middle of a window
            }
        }
        else {
            tupleCount++;
            if (tupleCount == windowStart) {//If the tuple marks the window beginning
                LOG.info("Window Start::" + tupleCount);
                storeTuple(tuple, 0, 1);
                windowStart += slideBy;
            }
            if (tupleCount == windowEnd) { //If the tuple marks the window end
                LOG.info("Window End::" + (tupleCount));
                storeTuple(tuple, 1, 1);
                windowEnd += slideBy;
            } else if(tupleCount != windowStart) //The tuple is in the middle of a window
                storeTuple(tuple, -1, 1);
        }
    }

    @Override
    /**
     * @param conf The Storm configuration for this bolt. This is the configuration provided to the topology merged in with cluster configuration on this machine.
     * @param context This object can be used to get information about this task's place within the topology, including the task id and component id of this task, input and output information, etc.
     * @param collector The collector is used to emit tuples from this bolt. Tuples can be emitted at any time, including the prepare and cleanup methods. The collector is thread-safe and should be saved as an instance variable of this bolt object.
     */
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        super.prepare(conf, context, collector);
        _collector = collector;
    }

    @Override
    /**
     * @param declarer this is used to declare output stream ids, output fields, and whether or not each output stream is a direct stream
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("dataStream", new Fields("RandomInt"));
        declarer.declareStream("mockTickTuple", new Fields("MockTick"));
    }

    @Override
    /**
     * Declare configuration specific to this component.
     */
    public Map<String, Object> getComponentConfiguration() {
        if(isTimeBased) {
            Map<String, Object> conf = new HashMap<String, Object>();
            conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 1);
            return conf;
        }
        return null;
    }

    public boolean isMockTick(Tuple tuple){
        return true;
    }
}
