package storm.starter.Interfaces;

import backtype.storm.tuple.Tuple;

/**
 * Created by sachin on 6/8/15.
 */
public interface IBaseWindowBolt{

    /*
     * This method returns the length of the windwow.
     * Window length is a mandatory parameter which is passed during the BaseWindowBolt initialization.
     * This method may be usd by the window developer to create some specific windowing mechanismm
     * <p>This includes the:</p>
     *
     * @param  No param for this
     */
     long getWindowLength();

    /*
     * This method returns the slide by value of the windowing mechanism.
     * This method returns the second parameter of the BaseWindowBolt constructor. This parameters
     * name is slide by value as most common windowing mechanism needs slide by value. But this
     * parameter can be used as required by the windwow developer.
     *
     * <p>This includes the:</p>
     *
     * @param  No param for this
     */
    long getSlideByValue();

    boolean isTimeBased();

    boolean isTickTuple(Tuple tuple);

}
