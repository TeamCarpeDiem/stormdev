package storm.starter;

import backtype.storm.Constants;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.starter.Interfaces.IBaseWindowBolt;

import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

/**
 * Created by Harini Rajendran on 6/3/15.
 */
public class BaseWindowBolt extends BaseRichBolt implements IBaseWindowBolt{
    private long windowLength;
    private long slideBy;
    private String windowingMechanism; //A String to set the type of windowing mechanism
    private Queue<Long> windowStartAddress; //Data Structure to store the start of each window
    private Queue<Long> windowEndAddress; //Data Structure to store the end of each window
    private boolean isTimeBased; //True if time based, false if count based

    /*   Constructors */

    public BaseWindowBolt()
    {
        this(10, 10, true, -1);
    }

    public BaseWindowBolt(long windowlength, boolean istimebased)
    {
        this(windowlength, windowlength,istimebased, -1);
    }

    public BaseWindowBolt(long windowlength, long slideby, boolean istimebased, int windowType)
    {
        if(windowlength <= 0)
            throw new IllegalArgumentException("Window length should be a Positive value");
        else
            windowLength = windowlength;

        if(slideby <= 0 && windowlength != slideby)
            throw new IllegalArgumentException("Slideby should be a Positive value");
        else
            slideBy = slideby;

        isTimeBased = istimebased;
        windowStartAddress = new LinkedList<Long>();
        windowEndAddress = new LinkedList<Long>();
        windowingMechanism = getWindowingMechanism(windowType);
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

    private String getWindowingMechanism(int wType)
    {
        if(wType==1)
        {
            return "Landmark Window";
        }
        if(windowLength == slideBy)
            return "Tumbling Window";
        else if(windowLength > slideBy)
            return "Sliding Window";
        else if((windowLength < slideBy) && (slideBy % windowLength == 0))
            return "Jumping Window";
        else if(windowLength < slideBy)
            return "Sampling Window";

        return "Unrecognized Windowing Mechanism";
    }
}