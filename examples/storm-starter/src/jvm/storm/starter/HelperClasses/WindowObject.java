package storm.starter.HelperClasses;

import storm.starter.bolt.BaseWindowBolt;
import storm.starter.bolt.LandmarkWindowBolt;
import storm.starter.bolt.SlidingWindowBolt;

import java.io.Serializable;

/**
 * Created by sachin on 6/9/15. Window object will be used to pass parameters related to windowing mechanism
 * Modified by Sachin createwindows() funtion will accept WindowObject as a parameter.
 * Pradheep: Added CreateWindow function which creates the corresponding window object based on the type of
 * window which the user needs
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
        windowLength = 1000;
        slideBy = 1000;
        isTimeBased = false;
        windowingMechanism = "tumbling";
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

    /**
     * This method creates the corresponding object of the window type class which the user wants to use.
     * @return window object
     * @throws Exception
     */
    public BaseWindowBolt createWindow() throws Exception {//Findbug Fix

        BaseWindowBolt window = null;
        WindowObject wObject = new WindowObject(windowingMechanism,windowLength, slideBy, isTimeBased );

        if(windowingMechanism.equals("tumbling") || windowingMechanism.equals("Tumbling")){
            if (windowLength == slideBy)
                window = new SlidingWindowBolt(wObject);
            else
                throw new Exception("Window Length and Slide By Values are not same for tumbling window");
        }
        else if(windowingMechanism.equals("sliding") || windowingMechanism.equals("Sliding")) {
            window = new SlidingWindowBolt(wObject);
        }
        else if(windowingMechanism.equals("landmark") || windowingMechanism.equals("Landmark")) {
            window = new LandmarkWindowBolt(wObject);
        }
        else {
            throw new Exception("Not a valid window type. Available window types are 1) Sliding \n 2) Tumbling and 3) Landmark");
        }
        return window;
    }
}
