package storm.starter.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import org.apache.log4j.Logger;
import storm.starter.HelperClasses.WindowObject;
import storm.starter.Interfaces.IWindowBolt;

/**
 * Created by Harini Rajendran on 6/7/15.
 * Modified by Sachin Jain on 6/13/2015. Moving emittter logic to BaseWindowBolt
 * Modified by Sachin Jain on 7/12/15. Removed all unnecessary overrides. Moved the control to BaseWindowBolt.
 */

public class SlidingWindowBolt extends BaseWindowBolt implements IWindowBolt{
    final static Logger LOG = Logger.getLogger(SlidingWindowBolt.class.getName());
    OutputCollector _collector;
    long windowStart; //Variable which keeps track of the window start
    long windowEnd; //Variable which keeps track of the window end
    long tupleCount; //Variable to keep track of the tuple count for time based window
    boolean isExecutedOnce = false; //Boolean which controls thread spawning
    boolean isTimeBased = false;
    long slideBy;

    public SlidingWindowBolt(WindowObject wObject)
    {
        super(wObject);
        windowStart = 1;
        windowEnd = wObject.getWindowLength();
        tupleCount = 0;
        slideBy = wObject.getSlideBy();
        if(wObject.getIsTimeBased())
        {
            isTimeBased = wObject.getIsTimeBased();
            LOG.info("Window Start::" + tupleCount);
            addStartAddress(0l);
            windowStart += wObject.getSlideBy();
        }
    }


    @Override
    /**
     * This function will get executed whenever a tuple is being received by this bolt. &This function has the logic to
     * store the tuples to the disk based on the window type, window length and slide by values.
     *
     * @param tuple The input tuple to be processed
     */
    public final void delegateExecute(Tuple tuple) {
        if(isTimeBased)
        {
            LOG.info("Count for this second::"+secondCount);
            secondCount = 0;
            if (isTickTuple(tuple)) {
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

            if(tupleCount != windowStart && tupleCount != windowEnd) //The tuple is in the middle of a window
                storeTuple(tuple, -1, 1);
            if (tupleCount == windowEnd) { //If the tuple marks the window end
                storeTuple(tuple, 1, 1);
                windowEnd += slideBy;
            }
            if (tupleCount == windowStart) {//If the tuple marks the window beginning
                storeTuple(tuple, 0, 1);
                windowStart += slideBy;
            }
        }


    }

    @Override
    public boolean isMockTick(Tuple tuple) {
        return false;
    }

}
