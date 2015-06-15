package storm.starter.bolt;

import backtype.storm.Constants;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.starter.HelperClasses.WindowObject;

import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

/**
 * Created by Harini Rajendran on 6/3/15.
 * Modified by Sachin Jain on 6/9/15. Constructor with windowobject as a parameter
 */
public class BaseWindowBolt extends BaseRichBolt{
    private long windowLength;
    private long slideBy;
    private String windowingMechanism; //A String to set the type of windowing mechanism
    private Queue<Long> windowStartAddress; //Data Structure to store the start of each window
    private Queue<Long> windowEndAddress; //Data Structure to store the end of each window
    private boolean isTimeBased; //True if time based, false if count based

    /*   Constructors */

    public BaseWindowBolt(WindowObject wObject)
    {
        if(wObject.getWindowLength() <= 0) {
            throw new IllegalArgumentException("Window length is either null or negative");
        }
        else {
            windowLength = wObject.getWindowLength();
        }
        if(wObject.getSlideBy() <= 0) {
            throw new IllegalArgumentException("Slideby should be a Positive value");
        }
        else {
            slideBy = wObject.getSlideBy();
        }
        isTimeBased = wObject.getIsTimeBased();
        windowStartAddress = new LinkedList<Long>();
        windowEndAddress = new LinkedList<Long>();
        windowingMechanism = wObject.getWindowingMechanism();

    }

    /*    Abstract Functions   */
    public void prepare(Map conf, TopologyContext context, OutputCollector collector)
    {
    }

    public void execute(Tuple tuple)
    {
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
    }

    /*  Getter Functions  */
    protected long getWindowLength()
    {
        return windowLength;
    }

    protected long getSlideByValue()
    {
        return slideBy;
    }

    protected boolean isTimeBased()
    {
        return isTimeBased;
    }

    protected boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
                && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
    }

    protected String getWindowingMechanism()
    {
        return windowingMechanism;
    }

    protected void emitMockTickTuple(OutputCollector collector, Tuple tuple)
    {
        collector.emit("mockTickTuple",tuple, new Values("__MOCKTICKTUPLE__"));
    }

}