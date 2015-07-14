package storm.starter.Interfaces;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import java.util.Map;

/**
 * Created by Pradheep on 6/18/15.
 */
public interface IWindowBolt {
    /**
     * This function should be implemented when the bolt has to detect the end of window "Mock Tick Tuple" sent by the
     * Common Window Framework. Implementing this function is critical for functioning of the Windowing mechanism. If
     * a bolt doesn't implement this function, then it won't be able to detect the end of windows.
     * @param tuple The tuple which has to be evaluated
     * @return Returns true if the tuple is a mock tick tuple else returns false
     */
    boolean isMockTick(Tuple tuple);
}
