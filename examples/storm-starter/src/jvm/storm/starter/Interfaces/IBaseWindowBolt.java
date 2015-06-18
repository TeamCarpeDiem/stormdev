package storm.starter.Interfaces;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import java.util.Map;

/**
 * Created by sachin on 6/15/15.
 */
public interface IBaseWindowBolt {

    /**
     * THis function takes in a Tuple object and should check if that tuple is a tick tuple or not
     * @param tuple
     * @return boolean value
     */
    boolean isTickTuple(Tuple tuple);

    /**
     *This method accepts a tuple that needs to be saved on the disk.
     * The flag variable marks if it is a start of a window, or an end of a window or one.
     * The count variable decides how many time the same tuple should act as an start address.
     * @param tuple
     * @param flag
     * @param count
     */
    void storeTuple(Tuple tuple, int flag, int count);

    /**
     * The initiateEmitter class takes a basecollector as a parameter from the class which receives the tuple.
     * If the collector is not from the class that receives the tuple then it will cause null pointer exception.
     * The OutputCollector's object taken as a parameter should be used to emit tuple from the base class
     * @param baseCollector
     */
    void initiateEmitter(OutputCollector baseCollector);




}
