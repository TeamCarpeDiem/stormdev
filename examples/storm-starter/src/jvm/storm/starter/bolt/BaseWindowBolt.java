package storm.starter.bolt;

import backtype.storm.Constants;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.starter.HelperClasses.WindowObject;
import storm.starter.Interfaces.IBaseWindowBolt;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;


/**
 * Created by Harini Rajendran on 6/3/15.
 */

public class BaseWindowBolt extends BaseRichBolt implements IBaseWindowBolt{
    OutputCollector _collector;
    private long windowLength;
    private long slideBy;
    private String windowingMechanism; //A String to set the type of windowing mechanism

    protected BlockingQueue<Long> _windowStartAddress;
    protected BlockingQueue<Long> _windowEndAddress;

    private boolean isTimeBased; //True if time based, false if count based

    byte[] writeBuffer; //Buffer which is used to perform bulk write to the disk
    int bufferIndex; //Index of the write buffer
    RandomAccessFile fileWriter; //File to which contents will be written
    RandomAccessFile fileReader;

    byte[] readBuffer;

    int secondCount; //For testing

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
        _windowStartAddress = new LinkedBlockingQueue<Long>();
        _windowEndAddress = new LinkedBlockingQueue<Long>();
        windowingMechanism = wObject.getWindowingMechanism();

        writeBuffer = new byte[100*1024*1024]; //Write Buffer size 100 MB
        readBuffer = new byte[100*1024*1024]; // Read Buffer size 100 MB

        bufferIndex = 0;
        secondCount = 0;

    }




    /*    Abstract Functions   */
    public void prepare(Map conf, TopologyContext context, OutputCollector collector)
    {
        try {
            fileWriter = new RandomAccessFile("//tmp//WindowContents","rw");
            fileReader = new RandomAccessFile("//tmp//WindowContents","r");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void execute(Tuple tuple)
    {
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
    }

    @Override
    public boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
                && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
    }


    protected void sendEndOfWindowSignal(OutputCollector collector)
    {
        collector.emit("mockTickTuple",new Values("__MOCKTICKTUPLE__"));
    }

    protected void addStartAddress(Long address) {
        if(address >= 0) {
            _windowStartAddress.add(address);
        }
    }

    protected void addEndAddress(Long address) {
        if(address >= 0) {
            _windowEndAddress.add(address);
        }
    }

    @Override
    public void storeTuple(Tuple tuple, int flag, int count)
    {
        try {
            if (flag == 0) {
                fileWriter.write(writeBuffer, 0, bufferIndex);
                Arrays.fill(writeBuffer, 0, bufferIndex, (byte) 0);
                bufferIndex = 0;
                for (int i = 0; i < count; i++) {
                    _windowStartAddress.add(fileWriter.getFilePointer());
                }
            }

            if(bufferIndex >= 75*1024*1024) //If the buffer is 75% full
            {
                fileWriter.write(writeBuffer, 0, bufferIndex);
                Arrays.fill(writeBuffer, 0, bufferIndex, (byte) 0);
                bufferIndex = 0;
            }

            if((!isTimeBased && flag != 0) || (isTimeBased && flag == -1)) {
                String obj = tuple.getString(0);
                byte[] bytes = obj.getBytes();
                int len = bytes.length;
                byte[] length = new byte[2];
                length[1] = (byte)(len & 0xFF);
                len = len >> 8;
                length[0] = (byte)(len & 0xFF);
                System.arraycopy(length, 0, writeBuffer, bufferIndex, length.length);
                bufferIndex += length.length;
                System.arraycopy(bytes, 0, writeBuffer, bufferIndex, bytes.length);
                bufferIndex += bytes.length;
            }

            if (flag == 1) {
                fileWriter.write(writeBuffer, 0, bufferIndex);
                Arrays.fill(writeBuffer, 0, bufferIndex, (byte) 0);
                bufferIndex = 0;

                for (int i = 0; i < count; i++) {
                    _windowEndAddress.add(fileWriter.getFilePointer());
                }
            }
        }
        catch(IOException ex)
        {
            ex.printStackTrace();
        }
    }

    @Override
    public void initiateEmitter(OutputCollector baseCollector)
    {
        _collector = baseCollector;
        readTuplesFromDisk();
    }

    private void readTuplesFromDisk()
    {
        long startOffset = 0;
        long endOffset = 0;
        int bufferSize = 100*1024*1024;
        int bufferIndex = -1;
        while(!_windowStartAddress.isEmpty() && !_windowEndAddress.isEmpty()) {
            startOffset = _windowStartAddress.remove();
            endOffset = _windowEndAddress.remove();
            do {
                if (endOffset - startOffset < bufferSize) {

                    try {
                        fileReader.seek(startOffset);
                        bufferIndex = (int) (endOffset - startOffset + 1);
                        fileReader.read(readBuffer, 0 , bufferIndex);
                        startOffset = emitTuples(0, bufferIndex);
                        sendEndOfWindowSignal(_collector);
                        break;
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                } else {
                    try {
                        fileReader.seek(startOffset);
                        bufferIndex = bufferSize - 1;
                        fileReader.read(readBuffer, 0, bufferIndex);
                        startOffset += emitTuples(0, bufferSize-1);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }while(true);
        }
    }

    private int getIntFromTwoBytes(byte tens, byte units)
    {
        return ((tens & 0xFF) << 8) | ((int)units & 0xFF);
    }

    private int emitTuples(int start, int end)//Given start and end buffer position, create and emit tuples. This is incmplete
    {
        int nextStart = 0;
        while(start < end) {
            if(start+1 > end)
                return start;

            int tupleLength = getIntFromTwoBytes(readBuffer[start], readBuffer[start+1]);
            nextStart = start + 2 + tupleLength;

            if(nextStart-1 > end)
                return start;

            byte[] tempArray = Arrays.copyOfRange(readBuffer, start+2, nextStart);
            String tupleData = new String(tempArray);
            _collector.emit("dataStream", new Values(tupleData));
            start = nextStart;
        }
        return start;
    }
}