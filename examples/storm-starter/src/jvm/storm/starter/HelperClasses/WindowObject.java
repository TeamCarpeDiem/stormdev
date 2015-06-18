package storm.starter.HelperClasses;

import storm.starter.bolt.BaseWindowBolt;
import storm.starter.bolt.SlidingWindowBolt;
import storm.starter.bolt.TumblingWindow;

import java.io.Serializable;

/**
 * Created by sachin on 6/9/15. Window object will be used to pass parameters related to windowing mechanism
 */
public class WindowObject implements Serializable{

    /******* Private Variable ********/
    long windowLength;
    long slideBy;
    boolean isTimeBased;
    String windowingMechanism;
    /******* End Private Variable ********/

    /******* Constructor ********/
    public WindowObject()
    {
        windowLength = -1;
        slideBy = -1;
    }

    public WindowObject(String type, long wLength,boolean isTBased)
    {
        windowingMechanism = type;
        windowLength= wLength;
        //slideBy = wLength; //by default it is a tumbling window
        isTimeBased= isTBased;
    }

    public WindowObject(String type, long wLength, long slideB, boolean isTBased)
    {
        windowingMechanism = type;
        windowLength= wLength;
        slideBy = slideB;
        isTimeBased= isTBased;
    }
    /******* End Constructor ********/


    /*******Getter and Setter ********/
    /**
     * retrieve the window length of the window
     * @return
     */
    public long getWindowLength()
    {
        return windowLength;
    }

    /**
     * set the windowlength paramter of the window object
     * @param wLength
     */
    public void setWindowLength(long wLength)
    {
        windowLength = wLength;
    }

    /**
     * Retrive the slideby valueof the window
     * @return
     */
    public long getSlideBy()
    {
        return slideBy;
    }

    /**
     * Set the slideBy value of the window
     * @param sBy
     */
    public void setSlideBy(long sBy)
    {
        slideBy = sBy;
    }


    /**
     * This is a getter method which says if the windowing applied is timebased or not
     * @return
     */
    public boolean getIsTimeBased()
    {
        return isTimeBased;
    }
    /**
     * This method allows user to mark the window  as Timbae or not
     * @return
     */
    public void setIsTimeBased(boolean iTimeBased)
    {
        isTimeBased = iTimeBased;
    }

    /**
     * This method allows user to retrieve name to the windowing mechanism
     * @return
     */
    public String getWindowingMechanism()
    {
        if(windowingMechanism == null || windowingMechanism.equals(""))
        {
            return "unknown";
        }
        return windowingMechanism;
    }

    /**
     * This method allows user to give name to the windowing mechanism
     * @param windowName
     */
    public void setWindowingMechanism(String windowName)
    {
        if(windowName == null || windowName.equals(""))
        {
            windowingMechanism="unknown";
        }
        windowingMechanism = windowName;
    }
    /******* End of Getter and Setter ********/

    public BaseWindowBolt CreateWindow(){

        BaseWindowBolt window = null;

        if(windowingMechanism.equals("tumbling")){
            window = new TumblingWindow(windowLength, isTimeBased);
        }

        if(windowingMechanism.equals("Sliding")) {
            window = new SlidingWindowBolt(windowLength, slideBy, isTimeBased);
        }
        /*if(windowingMechanism.equals(“landmark”)
            window = new LandmarkWindow(windowlength, slideBy, isTimeBased);*/

        return window;
    }
}
