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

import static java.lang.System.exit;


/**
 * Created by Harini Rajendran on 6/3/15.
 */

public class BaseWindowBolt extends BaseRichBolt implements IBaseWindowBolt{
    OutputCollector _collector;
    private long windowLength;
    private long slideBy;
    private String windowingMechanism; //A String to set the type of windowing mechanism
    private String FILEPATH = System.getProperty("user.home")+"//WindowsContent";
    private long MAXFILESIZE = ((1l * 1024 * 1024 * 1024) - 1)/2;
    private final int bufferSize = 100 * 1024 * 1024;
    private final int WRITEBUFFERSIZE = 100 * 1024 * 1024;
    private final int READBUFFERSIZE = 100 * 1024 * 1024;
    private final double percentage = 0.75;

    protected BlockingQueue<Long> _windowStartAddress;
    protected BlockingQueue<Long> _windowEndAddress;

    private boolean isTimeBased; //True if time based, false if count based

    byte[] writeBuffer; //Buffer which is used to perform bulk write to the disk
    int bufferIndex; //Index of the write buffer
    RandomAccessFile fileWriter; //File to which contents will be written
    RandomAccessFile fileReader;
    int tcount, scount;
    byte[] readBuffer;

    int secondCount; //For testing

    /*   Constructors */

    public BaseWindowBolt(WindowObject wObject)
    {
        tcount = 0;scount = 0;
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

        writeBuffer = new byte[WRITEBUFFERSIZE]; //Write Buffer size 100 MB
        readBuffer = new byte[READBUFFERSIZE]; // Read Buffer size 100 MB

        bufferIndex = 0;
        secondCount = 0;

    }




    /*    Abstract Functions   */
    public void prepare(Map conf, TopologyContext context, OutputCollector collector)
    {
        try {
            fileWriter = new RandomAccessFile(FILEPATH,"rw");
            fileReader = new RandomAccessFile(FILEPATH,"r");
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
                if(!wrapUpRequired(bufferIndex, fileWriter.getFilePointer())) {

                    //System.out.println(" 135 not updated file pointer:" + fileWriter.getFilePointer());
                    fileWriter.write(writeBuffer, 0, bufferIndex);
                    if(fileWriter.getFilePointer() > MAXFILESIZE) {
                        System.out.println("BufferIndex::"+bufferIndex);
                        System.out.println(" 137 updated file pointer:" + fileWriter.getFilePointer());
                    }
                }
                else
                {
                    //System.out.println(" 146 not updated file pointer:" + fileWriter.getFilePointer());
                    writeInParts(writeBuffer,0,bufferIndex);
                    if(fileWriter.getFilePointer() > MAXFILESIZE) {
                        System.out.println("BufferIndex::"+bufferIndex);
                        System.out.println(" 150 updated file pointer:" + fileWriter.getFilePointer());
                    }
                }

            //    Arrays.fill(writeBuffer, 0, bufferIndex, (byte) 0);
                bufferIndex = 0;
                for (int i = 0; i < count; i++) {
                    //System.out.println("Start Address::"+fileWriter.getFilePointer());
                    //System.out.println("Added Start Address::"+fileWriter.getFilePointer());
                    _windowStartAddress.add(fileWriter.getFilePointer());
                }
            }



            if(!isTimeBased || (isTimeBased && flag == -1)) {
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

                if(!wrapUpRequired(bufferIndex, fileWriter.getFilePointer())) {
                   // System.out.println(" 176 not updated file pointer:" + fileWriter.getFilePointer());
                    fileWriter.write(writeBuffer, 0, bufferIndex);
                    if(fileWriter.getFilePointer() > MAXFILESIZE) {
                        System.out.println("BufferIndex::"+bufferIndex);
                        System.out.println(" 178 updated file pointer:" + fileWriter.getFilePointer());
                    }
                }
                else
                {
                    //System.out.println(" 187 not updated file pointer:" + fileWriter.getFilePointer());
                    writeInParts(writeBuffer,0,bufferIndex);
                    if(fileWriter.getFilePointer() > MAXFILESIZE) {
                        System.out.println("BufferIndex::"+bufferIndex);
                        System.out.println(" 190 updated file pointer:" + fileWriter.getFilePointer());
                    }
                }
                //Arrays.fill(writeBuffer, 0, bufferIndex, (byte) 0);
                bufferIndex = 0;

                for (int i = 0; i < count; i++) {
                    //System.out.println("Added End Address::"+fileWriter.getFilePointer());
                    _windowEndAddress.add(fileWriter.getFilePointer()-1);
                }
            }
            if(bufferIndex >= bufferSize*percentage) //If the buffer is 75% full
            {
                if(!wrapUpRequired(bufferIndex, fileWriter.getFilePointer())) {
                    //System.out.println(" 205 not updated file pointer:" + fileWriter.getFilePointer() + "BufferSize:" + bufferIndex);
                    fileWriter.write(writeBuffer, 0, bufferIndex);
                    if(fileWriter.getFilePointer() > MAXFILESIZE)
                        System.out.println(" 205 updated file pointer:" + fileWriter.getFilePointer() + "BufferSize:" + bufferIndex);
                }
                else
                {
                //    System.out.println(" 207 not updated file pointer:" + fileWriter.getFilePointer() + "BufferSize:" + bufferIndex);
                    writeInParts(writeBuffer,0,bufferIndex);
                    if(fileWriter.getFilePointer() > MAXFILESIZE) {
                        System.out.println("BufferIndex::"+bufferIndex);
                        System.out.println(" 207 updated file pointer:" + fileWriter.getFilePointer());
                    }
                }
                Arrays.fill(writeBuffer, 0, bufferIndex, (byte) 0);
                bufferIndex = 0;
            }
        }
        catch(IOException ex)
        {
            ex.printStackTrace();
        }
    }

    private boolean wrapUpRequired(int bufferSize,  final long writerPointer) throws IOException
    {
        //System.out.println(" 204 Before Wrapping up " + (long)(writerPointer));
        return true;
        /*long sum =writerPointer + (long)bufferSize;
        //System.out.println(" 207Before Wrapping up " + ((writerPointer + (long)bufferSize) >= MAXFILESIZE)+ "calculated sum is: " + sum );
        //System.out.println("Check :: File Writer Pointer is::" + writerPointer + "    bufferSize is::" + bufferSize+ "  MAXFILESIZE is::" + MAXFILESIZE + "  Sum is::"+ (long)(fileWriter.getFilePointer() + bufferSize) );

        if(Long.compare(sum,MAXFILESIZE-1) > 0)
        {
          //  System.out.println("Wrapping up" + ((writerPointer + bufferSize) >= MAXFILESIZE));
           // System.out.println("213 :: File Writer Pointer is::" + writerPointer + "    bufferSize is::" + (long)bufferSize + "  MAXFILESIZE is::" + MAXFILESIZE + "  Sum is::"+ sum);
            //System.out.println("Sachin Jain");
            return true;
        }
        //System.out.println("216 :: File Writer Pointer is::" + fileWriter.getFilePointer() + "    bufferSize is::" + (long)bufferSize+ "  MAXFILESIZE is::" + MAXFILESIZE + "  Sum is::"+ (long)(fileWriter.getFilePointer() + bufferSize) );
        return false;*/
    }

    private void writeInParts(byte[] buffer, int startIndex, int length) throws IOException
    {
        int remainingSpace = (int)(MAXFILESIZE - fileWriter.getFilePointer());
        //System.out.println("Current Writer position is::"+ fileWriter.getFilePointer() + "   Start Index is:: " + startIndex + " remaining space::" + remainingSpace);
        if(length <= remainingSpace)
            fileWriter.write(writeBuffer, startIndex, length);
        else {
            fileWriter.write(writeBuffer, startIndex, remainingSpace);
            fileWriter.seek(0);
            //System.out.println(" 2333222 Current Writer position is::" + fileWriter.getFilePointer());
            fileWriter.write(writeBuffer, remainingSpace, length - remainingSpace);
        }
    }

    @Override
    public void initiateEmitter(OutputCollector baseCollector)
    {
        _collector = baseCollector;
        try {
            populateReadBuffer();
            //readTuplesFromDisk();
        }
        catch(IOException ex)
        {
            ex.printStackTrace();
        }
    }

    private int getIntFromTwoBytes(byte tens, byte units)
    {
        return ((tens & 0xFF) << 8) | ((int)units & 0xFF);
    }

    private int emitTuples(int start, int end) throws IOException//Given start and end buffer position, create and emit tuples. This is incmplete
    {
        int nextStart = 0;
        int count =0;
        while(start < end) {
            if(start+1 > end) {
                System.out.println("2000009::: Count is "+ count);
                return start;
            }
            int tupleLength = getIntFromTwoBytes(readBuffer[start], readBuffer[start+1]);
            nextStart = start + 2 + tupleLength;

            if(nextStart-1 > end) {
                System.out.println("29800009::: Count is "+ count);
                return start;
            }
            byte[] tempArray = Arrays.copyOfRange(readBuffer, start+2, nextStart);
            String tupleData = new String(tempArray);
            _collector.emit("dataStream", new Values(tupleData));
            count++;
            if(nextStart == fileWriter.length())
                start = 0;
            else
                start = nextStart;
        }
        System.out.println("30000009::: Count is "+ count);
        return start;
    }

    @Override
    public void cleanup() {
        writeBuffer = null;
        readBuffer = null;
    }

    void populateReadBuffer() throws IOException
    {
        long startOffset = 0;
        long endOffset = 0;
        int end;
        while (_windowStartAddress.isEmpty()) ;
        startOffset = _windowStartAddress.remove();
        System.out.println(" 295:: Start offset removed" + startOffset);
        fileReader.seek(startOffset);
        while(true) {
            if (fileWriter.getFilePointer() > fileReader.getFilePointer()
                    && ((fileWriter.getFilePointer() - fileReader.getFilePointer()  > bufferSize) || !_windowEndAddress.isEmpty())) {
                if(!_windowEndAddress.isEmpty() && _windowEndAddress.peek() - startOffset > 0 && _windowEndAddress.peek() - startOffset + 1 <= bufferSize) {
                    endOffset = _windowEndAddress.remove();
                    //System.out.println("Start Offset::" + startOffset + "End Offset::" + endOffset + "File length::" + fileWriter.length() + "Buffer Size::"+bufferSize);
                    end = loadBuffer(startOffset, endOffset,0);
                    //System.out.println("332:: Start Offset is::" + startOffset+ "   End Offset::" + endOffset);
                    emitTuples(0, end);
                    sendEndOfWindowSignal(_collector);
                    System.out.println("330::: End Offset::" + endOffset);
                    break;
                }
                else{
                    endOffset = startOffset + bufferSize-1;
                    end = loadBuffer(startOffset, endOffset,0);
                    //System.out.println("341 :: Start Offset is::" + startOffset + "   End Offset::" + endOffset);
                    startOffset += emitTuples(0, end);
                }
            } else if(fileWriter.getFilePointer() < fileReader.getFilePointer()
                    && ((MAXFILESIZE-1 - fileReader.getFilePointer() + fileWriter.getFilePointer() > bufferSize -1) || !_windowEndAddress.isEmpty()))
            {
                //System.out.println("346:::!!!!!!!!!!!!!!!!!!!!FileWriter.length()::"+fileWriter.length()+ "   Start Offset::"+startOffset);
                if(!_windowEndAddress.isEmpty() && _windowEndAddress.peek() - startOffset > 0 && _windowEndAddress.peek() - startOffset + 1 < bufferSize) {
                    endOffset = _windowEndAddress.remove();
                    //System.out.println("350::: End Offset::" + endOffset);
                    //System.out.println("Start Offset::" + startOffset + "End Offset::" + endOffset + "File length::" + fileWriter.length() + "Buffer Size::"+bufferSize);
                    end = loadBuffer(startOffset, endOffset,0);
                    //System.out.println("350 :: Start Offset is::" + startOffset+ "   End Offset::" + endOffset);
                    emitTuples(0, end);
                    sendEndOfWindowSignal(_collector);
                    System.out.println("356::: End Offset::" + endOffset);
                    break;
                }
                else if(!_windowEndAddress.isEmpty() && _windowEndAddress.peek() - startOffset + 1 > bufferSize){
                    endOffset = startOffset + bufferSize - 1;
                    //System.out.println("358 :: Start Offset is::" + startOffset + "   End Offset::" + endOffset);
                    end = loadBuffer(startOffset, endOffset,0);
                    startOffset += emitTuples(0, end);
                }
                else if(!_windowEndAddress.isEmpty() && MAXFILESIZE   - startOffset + _windowEndAddress.peek() + 1 <= bufferSize)
                {
                    endOffset =MAXFILESIZE-1;
                    //System.out.println("364 :: Start Offset::" + startOffset + "End Offset::" + endOffset + "File length::" + fileWriter.length() + "Buffer Size::"+bufferSize);
                    loadBuffer(startOffset, endOffset,0);
                    int index1 = (int) (endOffset - startOffset) + 1;
                    System.out.println("print index1::"+ index1);
                    System.out.println("@@@@@@@@@@367 :: Start Offset is::" + startOffset + "   End Offset::" + endOffset);
                    endOffset = _windowEndAddress.remove();
                    startOffset =0;
                    end = loadBuffer(startOffset, endOffset,index1);
                    System.out.println("@@@@@@@@@@@@@@ End::"+end + "Start Offset::" + startOffset+"  endOffset::"+ endOffset);
                    emitTuples(0,end);
                    sendEndOfWindowSignal(_collector);
                    System.out.println("378::: End Offset::" + endOffset);
                    break;
                }
                else
                {
                    //System.out.println("Peak is printed here::"+ _windowEndAddress.peek());
                    //if(fileWriter.getFilePointer() - startOffset  > bufferSize)
                    if(MAXFILESIZE-1 - startOffset + 1  > bufferSize)
                    {
                      //  System.out.println("!!!!!!!!!!!!!!!!!!!!FileWriter.length()::"+fileWriter.length()+ "   Start Offset::"+startOffset);
                        endOffset = startOffset + bufferSize -1;
                        end = loadBuffer(startOffset, endOffset,0);
                        //System.out.println("380 :: Start Offset is::" + startOffset + "   End Offset::" + endOffset);
                        startOffset += emitTuples(0, end);
                    }
                    else {
                        endOffset = MAXFILESIZE - 1;
                        //System.out.println("not 386 :: Start Offset is::" + startOffset + "   End Offset::" + endOffset);
                        loadBuffer(startOffset, endOffset, 0);
                        int index = (int) (endOffset - startOffset) + 1;
                        //System.out.println("386 :: Start Offset is::" + startOffset + "   End Offset::" + endOffset);
                        endOffset = bufferSize - 1 - index;
                       // startOffset = 0;
                        end = loadBuffer(0, endOffset, index);
                       // System.out.println("389 :: Start Offset is::" + startOffset + "   End Offset::" + endOffset);
                       int temp = emitTuples(0, end); // - index;

                        startOffset = (startOffset + temp) % MAXFILESIZE;
                        /*if(startOffset + temp > MAXFILESIZE-1)
                        {
                            startOffset = startOffset + temp - MAXFILESIZE;
                        }
                        else
                        {
                            startOffset = startOffset + temp;
                        }*/
                        //startOffset += emitTuples(0, end) - index;
//                        if(startOffset > MAXFILESIZE-1){
                       //     startOffset = startOffset - MAXFILESIZE - 1;
                         // System.out.println("Start Offset here::"+startOffset);
                        //}
                    }
                }
            }
        }

    }

    private int loadBuffer(long sOffset, long eOffset, int index) throws IOException {

        fileReader.seek(sOffset);
        int length = (int) (eOffset - sOffset + 1);

        try {
            length = fileReader.read(readBuffer, index, length);
        }
        catch(Exception ex)
        {
            System.out.println("Exception Caught with value of index" + index + "  and length ::" + length);
            ex.printStackTrace();
            exit(1);
        }
        return length;
    }
}