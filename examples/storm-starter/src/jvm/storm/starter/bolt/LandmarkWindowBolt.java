package storm.starter.bolt;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import org.apache.log4j.Logger;
import storm.starter.Interfaces.IWindowBolt;

import java.util.Map;

/**
 * Created by Harini Rajendran on 6/29/15.
 */

public class LandmarkWindowBolt extends BaseWindowBolt implements IWindowBolt{
    final static Logger LOG = Logger.getLogger(LandmarkWindowBolt.class.getName());
    OutputCollector _collector;
    long windowStart; //Variable which keeps track of the window start
    long windowEnd; //Variable which keeps track of the window end
    long tupleCount; //Variable to keep track of the tuple count for time based window
    boolean isExecutedOnce = false; //Boolean which controls thread spawning
    boolean isTimeBased = false;
    int slideBy;//Variable used to indicate when the window start has to be slided
    long wLength;
    /**
     * Constructor which takes the WindowObject as the parameter
     * @param wlength window length
     * @param sBy slideBy value
     * @param isTBased Boolean to indicate whether the window is time based on count based
     */
    public LandmarkWindowBolt(long wlength, long sBy, boolean isTBased)
    {
        super(wlength, sBy, isTBased);
        LOG.info("Created a LandMark Window Bolt");
        windowStart = 1;
        windowEnd = wlength;
        tupleCount = 0;
        slideBy = (int)sBy;
        wLength = wlength;

        if(isTBased)
        {
            isTimeBased = isTBased;
            for(int i = 0 ; i < sBy; i++) {
                addStartAddress(0l);
                LOG.info("Window Start::" + tupleCount);
            }
            windowStart += slideBy*wLength;
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
                    while(true) {
                        LOG.info("Initiated Emitter!!!");
                        try {
                            initiateEmitter(_collector);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            };
            thread.start();
            isExecutedOnce = true;
        }
        if(isTimeBased)
        {
            if (isTickTuple(tuple)) {
                LOG.info("Count for this Second::" + secondCount);//Testing
                secondCount = 0;
                tupleCount++;
                if(tupleCount == windowStart-1)//If the tuple marks the window beginning
                {
                    for(int i = 0; i < slideBy; i++)
                        LOG.info("Window Start::" + tupleCount);
                    storeTuple(tuple, 0, slideBy);//Window start will be same slideBy times
                    windowStart += slideBy*wLength;//WindowStart has to be updated only after slideBy tick tuples
                }
                if (tupleCount == windowEnd) { //If the tuple marks the window end
                    LOG.info("Window End::" + (tupleCount));
                    storeTuple(tuple, 1, 1);
                    windowEnd += wLength;//Window end should be updated for every window length
                }
            }
            else {
                secondCount++;//Testing
                storeTuple(tuple, -1, 1); //The tuple is in the middle of a window
            }
        }
        else {
            tupleCount++;
            if(tupleCount != windowStart && tupleCount != windowEnd) //The tuple is in the middle of a window
                storeTuple(tuple, -1, 1);
            if (tupleCount == windowEnd) { //If the tuple marks the window end
                LOG.info("Window End::" + (tupleCount));
                storeTuple(tuple, 1, 1);
                windowEnd += wLength;
            }
            if (tupleCount == windowStart) {//If the tuple marks the window beginning
                for(int i = 0; i < slideBy; i++)
                    LOG.info("Window Start::" + tupleCount);
                storeTuple(tuple, 0, slideBy);
                windowStart += slideBy*wLength;
            }
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

    public void cleanup()
    {
        System.out.println("Cleaned Up!!!");
    }

    @Override
    /**
     * Declare configuration specific to this component.
     */
    public Map<String, Object> getComponentConfiguration() {
        if(isTimeBased) {
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 1);
        return conf;
        }
        return null;
    }

    public boolean isMockTick(Tuple tuple){
        return true;
    }
}
