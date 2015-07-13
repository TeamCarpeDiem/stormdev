package storm.starter.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import org.apache.log4j.Logger;
import storm.starter.HelperClasses.WindowObject;
import storm.starter.Interfaces.IWindowBolt;

/**
 * Created by Harini Rajendran on 6/29/15.
 * Modified by Sachin Jain on 7/12/15. Removed all unnecessary overrides. Moved the control to BaseWindowBolt.
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

    public LandmarkWindowBolt(WindowObject wObject)
    {
        super(wObject);
        LOG.info("Created a LandMark Window Bolt");
        windowStart = 1;
        windowEnd = wObject.getWindowLength();
        tupleCount = 0;
        slideBy = (int)wObject.getSlideBy();
        wLength = wObject.getWindowLength();
        isTimeBased = wObject.getIsTimeBased();

        if(isTimeBased)
        {
            for(int i = 0 ; i < slideBy; i++) {
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
    public final void delegateExecute(Tuple tuple) {
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


    public boolean isMockTick(Tuple tuple){
        return true;
    }
}
