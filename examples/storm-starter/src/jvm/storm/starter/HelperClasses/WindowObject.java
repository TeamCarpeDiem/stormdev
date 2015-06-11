package storm.starter.HelperClasses;

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

    public WindowObject(long wLength,boolean isTBased)
    {
        windowLength= wLength;
        slideBy = wLength; //by default it is a tumbling window
        isTimeBased= isTBased;
    }

    public WindowObject(long wLength, long slideB, boolean isTBased)
    {
        windowLength= wLength;
        slideBy = slideB;
        isTimeBased= isTBased;
    }
    /******* End Constructor ********/


    /*******Getter and Setter ********/
    public long getWindowLength()
    {
        return windowLength;
    }
    public void setWindowLength(long wLength)
    {
        windowLength = wLength;
    }


    public long getSlideBy()
    {
        return slideBy;
    }
    public void setSlideBy(long sBy)
    {
        slideBy = sBy;
    }


    public boolean getIsTimeBased()
    {
        return isTimeBased;
    }
    public void setIsTimeBased(boolean iTimeBased)
    {
        isTimeBased = iTimeBased;
    }

    public String getWindowingMechanism()
    {
        if(windowingMechanism == null || windowingMechanism.equals(""))
        {
            return "unknown";
        }
        return windowingMechanism;
    }
    public void setWindowingMechanism(String windowName)
    {
        if(windowName == null || windowName.equals(""))
        {
            windowingMechanism="unknown";
        }
        windowingMechanism = windowName;
    }
    /******* End of Getter and Setter ********/
}
