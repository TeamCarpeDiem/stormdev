package storm.starter.bolt;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.apache.log4j.Logger;
import storm.starter.HelperClasses.WindowObject;
import storm.starter.Interfaces.IBaseWindowBolt;

import java.io.*;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by Sachin and Harini on 7/7/15.
 */
public abstract class BaseWindowBolt extends BaseRichBolt implements IBaseWindowBolt {

    /******************************* Configurable Parameters *******************************/
    long MAXFILESIZE;
    int WRITEBUFFERSIZE;
    int READBUFFERSIZE;
    double WRITEBUFFERTHRESHOLD;
    int MAXTHREAD;
    int TICKTUPLEFREQUENCY;
    String FILEPATH;

    /******************************* Configurable Parameters *******************************/
    final static Logger LOG = Logger.getLogger(BaseWindowBolt.class.getName());

    /******************************* from window object n windowing params  *********************/
    boolean isTimeBased;
    long windowStart; //Variable which keeps track of the window start
    long windowEnd; //Variable which keeps track of the window end
    long tupleCount; //Variable to keep track of the tuple count for time based window
    long slideBy;

    /******************************* Updated while storing tuple  *******************************/
    protected BlockingQueue<Long> _windowStartAddress;
    protected BlockingQueue<Long> _windowEndAddress;
    byte[] _writeBuffer; //Buffer which is used to perform bulk write to the disk
    int _bufferIndex;
    //RandomAccessFile _fileWriter; //File to which contents will be written
    FileOutputStream _fileWriter;
    int secondCount; //TODO Remove before releasing final code

    /********************* Updated while reading data from disk to memory  **********************/
    OutputCollector _collector;
    List<byte[]> _bufferList;
    Thread[] _diskReaderThread;
    BlockingQueue<Integer> _threadSequenceQueue;
    HashMap<Integer, Integer> _producerConsumerMap;
    long startOffset;
    RandomAccessFile _fileReader;
    //boolean sendOnlyOnce = true;
    /************************* Updated while emitting data from memory  *************************/
    Thread _memoryReader;

    /****************************** Testing variable *****************************///TODO to be removed
    int tLength;


    /****************************** Constructor *****************************/
    public BaseWindowBolt(WindowObject wObj)
    {
        Properties prop = new Properties();
        InputStream input = null;
        try {
            FILEPATH = System.getProperty("user.home")+"//WindowsContent";
            input = new FileInputStream("config.properties");
            prop.load(input);
            MAXFILESIZE = Long.valueOf(prop.getProperty("maximumFileSize"));
            WRITEBUFFERSIZE = Integer.valueOf(prop.getProperty("writeBufferSize"));
            READBUFFERSIZE = Integer.valueOf(prop.getProperty("readBufferSize"));
            WRITEBUFFERTHRESHOLD = Double.valueOf(prop.getProperty("bufferThreshold"));
            MAXTHREAD = Integer.valueOf(prop.getProperty("numberOfThreads"));
            TICKTUPLEFREQUENCY = Integer.valueOf(prop.getProperty("TickTupleFrequency"));
            startOffset = -1L; // used by disk reader thread to get the start offset oof the disk
        } catch (IOException e) {
            e.printStackTrace();
        }
        if(wObj.getWindowLength() <= 0) {
            throw new IllegalArgumentException("Window length is either null or negative");
        }
        if(wObj.getSlideBy() <= 0) {
            throw new IllegalArgumentException("Slideby should be a Positive value");
        }

//Windowing params
        isTimeBased = wObj.getIsTimeBased();
        windowStart = 1;
        windowEnd = wObj.getWindowLength();
        tupleCount = 0;
        slideBy = wObj.getSlideBy();
//end of windowing params

        _windowStartAddress = new LinkedBlockingQueue<Long>();
        _windowEndAddress = new LinkedBlockingQueue<Long>();

        _writeBuffer = new byte[WRITEBUFFERSIZE];

        _bufferIndex = 0;
        secondCount = 0;
    }

    /*    Abstract Functions   */
    public void prepare(Map conf, TopologyContext context, OutputCollector collector)
    {
        try {
            _fileWriter = new FileOutputStream(FILEPATH);
            _fileReader = new RandomAccessFile(FILEPATH,"r");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        _collector = collector;
        try {
            initiateEmitter();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * This function is called by STORM when it has to send tuple to any bolt.
     * The framework will receive the tuple and then will send it to the class extending the framework
     * @param tuple
     */
    public void execute(Tuple tuple)
    {
        delegateExecute(tuple);
    }

    protected abstract void delegateExecute(Tuple tuple);

    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declareStream("dataStream", new Fields("RandomInt"));
        declarer.declareStream("mockTickTuple", new Fields("MockTick"));

    }

    /**
     * THis function takes in a Tuple object and should check if that tuple is a tick tuple or not
     * @param tuple
     * @return boolean value
     */
    public boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
                && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
    }

    /**
     * This function is called at the end of each window to mark the end of the end of windowing signal.
     * The mock tuple sent out is understood by all other process expecting window data
     */
    private void sendEndOfWindowSignal() //OutputCollector collector
    {
        _collector.emit("mockTickTuple", new Values("__MOCKTICKTUPLE__"));
    }

    /**
     * The initiateEmitter class takes a basecollector as a parameter from the class which receives the tuple.
     * If the collector is not from the class that receives the tuple then it will cause null pointer exception.
     * The OutputCollector's object taken as a parameter should be used to emit tuple from the base class
     * @param
     */
    public void initiateEmitter() throws InterruptedException {
        Emitter();
    }


    @Override
    public void cleanup() {
        _writeBuffer = null;
        File fp = new File(FILEPATH, "a");
        fp.delete();
    }

    @Override
    /**
     * Declare configuration specific to this component.
     */
    public Map<String, Object> getComponentConfiguration() {
        if(isTimeBased) {
            Map<String, Object> conf = new HashMap<String, Object>();
            conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, TICKTUPLEFREQUENCY);
            return conf;
        }
        return null;
    }

    /**

     * @param address
     */
    protected void addStartAddress(Long address) {
        if(address >= 0) {
            _windowStartAddress.add(address);
        }
    }

    /**
     *
     * @param address
     */
    protected void addEndAddress(Long address) {
        if(address >= 0) {
            _windowEndAddress.add(address);
        }
    }

    /**
     * The initiateEmitter class takes a basecollector as a parameter from the class which receives the tuple.
     * If the collector is not from the class that receives the tuple then it will cause null pointer exception.
     * The OutputCollector's object taken as a parameter should be used to emit tuple from the base class
     * @param tuple
     * @param flag
     * @param count
     */
    //@Override
    public void storeTuple(Tuple tuple, int flag, int count)
    {
        FileChannel fc = _fileWriter.getChannel();
        try {

            //TODO Catching Up part
            if(!_windowStartAddress.isEmpty() && fc.position()  < (long)_windowStartAddress.peek()
                    && (((long)_windowStartAddress.peek() - fc.position() ) < (long)READBUFFERSIZE))
            {
                System.out.println("Writer catching up  on Reader..  Start Address::"+ _windowStartAddress.peek());
                Utils.sleep(10000);
            }

            if (flag == 0) {
                writeInParts();
                for (int i = 0; i < count; i++) {
                    _windowStartAddress.add(fc.position());
                }
            }

            if(!isTimeBased || (isTimeBased && flag == -1)) {
                String obj = tuple.getString(0);
                byte[] bytes = obj.getBytes();
                int len = bytes.length;

                //copy length of tuple in a two byte array
                byte[] length = new byte[2];
                length[1] = (byte)(len & 0xFF);
                len = len >> 8;
                length[0] = (byte)(len & 0xFF);

                //Copy the length of the tuple in two bytes and update the _bufferIndex
                System.arraycopy(length, 0, _writeBuffer, _bufferIndex, length.length);
                _bufferIndex += length.length;

                //copy the actual tuple and update the _bufferIndex
                System.arraycopy(bytes, 0, _writeBuffer, _bufferIndex, bytes.length);
                _bufferIndex += bytes.length;
            }

            if (flag == 1) {
                writeInParts();
                if(fc.position() == 0L)
                {
                    for (int i = 0; i < count; i++) {
                        _windowEndAddress.add(MAXFILESIZE - 1L);
                    }
                }
                else {

                    for (int i = 0; i < count; i++) {
                        _windowEndAddress.add(fc.position() - 1L);
                    }
                }
            }

            if(_bufferIndex >= WRITEBUFFERSIZE*WRITEBUFFERTHRESHOLD) //If the buffer is 95% full
            {
                writeInParts();
            }
        }
        catch(IOException ex)
        {
            ex.printStackTrace();
        }
    }

    /**
     * Write the data to the disk. If end of file is reached then start writing from the beginning.
     * @throws IOException
     */
    private void writeInParts() throws IOException
    {
        FileChannel fc = (_fileWriter.getChannel());

        long remainingFileSpace = (MAXFILESIZE - fc.position());
        int bufferDataLength = _bufferIndex;
        if(bufferDataLength <= remainingFileSpace) {
            long temp = fc.position();
            _fileWriter.write(_writeBuffer, 0, _bufferIndex);

            if(fc.position() == temp && _bufferIndex != 0) {
                long temp1 = temp+(long)_bufferIndex;
                fc.position(temp1);
            }
            //When FileWriter reaches the MAXFILESIZE after bulk write, wrap up should happen
            if(fc.position() == MAXFILESIZE)
                _fileWriter.getChannel().position(0L);
        }
        else{
            _fileWriter.write(_writeBuffer, 0, (int) remainingFileSpace);
            _fileWriter.getChannel().position(0L);
            _fileWriter.write(_writeBuffer, (int)remainingFileSpace, bufferDataLength - (int)remainingFileSpace);
        }
        _bufferIndex = 0;
    }

    /**
     * This function spawns all the threads needed for reading data from disk and also a thread that reads this data and send out to next process
     */
    private void Emitter() throws InterruptedException {
        _bufferList = new ArrayList<byte[]>();

        _diskReaderThread = new Thread[MAXTHREAD];
        _threadSequenceQueue = new LinkedBlockingQueue<Integer>();
        _producerConsumerMap = new HashMap<Integer, Integer>();

        _memoryReader = new Thread(new EmitFromMemory());
        _memoryReader.start();

        for (int i = 0; i < MAXTHREAD; i++) {
            _threadSequenceQueue.add(i);
            _producerConsumerMap.put(i, -1);
            //last two bytes will be used to put flag if end of window signal be sent or not
            _bufferList.add(new byte[READBUFFERSIZE + 2]);
            _diskReaderThread[i] = new Thread(new DiskToMemory(i));
        }


        for (int i = 0; i < MAXTHREAD; i++) {
            _diskReaderThread[i].start();
        }
    }


    private class DiskToMemory extends Thread {

        int __threadSequence;
        long __start1;
        long __end1;
        long __start2;
        long __end2;
        boolean __sendEOWSignal;
        boolean __isWrapLoadNeeded;

        public DiskToMemory(int threadSeq){
            __threadSequence = threadSeq;
        }

        public void run(){
            System.out.println("Disk to memory threads begin");
            while(true){
                synchronized(_threadSequenceQueue) {

                    //wait till the thread get its turn on the _threadSequenceQueue
                    while (_threadSequenceQueue.peek() != __threadSequence) {
                        try {
                            _threadSequenceQueue.wait();
                        } catch (InterruptedException e) {
                            // TODO Instead of throwing error. Put this to log statement
                            e.printStackTrace();
                        }
                    }

                    //wait till the buffer current thread is  trying to write to is read completely
                    while (_producerConsumerMap.get(__threadSequence) == 1) ;

                    if (startOffset == -1L) {
                        while (_windowStartAddress.isEmpty()) ;
                        startOffset = _windowStartAddress.remove();
                    }
                    __start1 = startOffset;
                    // try {
                    long tempFileWriter = 0;
                    while (true) {
                        try {
                            tempFileWriter = _fileWriter.getChannel().position();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        if (tempFileWriter < __start1) { //After Wrapping Up

                            //If end address is present
                            if (!_windowEndAddress.isEmpty()) {

                                long tempPeek = _windowEndAddress.peek();
                                // if the peek is ahead of the start offset
                                if (tempPeek >= __start1) {
                                    //Difference between end of window and start offset should fit in buffer
                                    if ( tempPeek - __start1 + 1 <= READBUFFERSIZE) {
                                        __end1 = _windowEndAddress.remove();
                                        __sendEOWSignal = true;
                                        __isWrapLoadNeeded = false;
                                        startOffset = -1L;
                                        break;
                                        //difference between end removed and start offset is greater than buffer capacity
                                    } else if(tempPeek - __start1 >= READBUFFERSIZE + 1L){
                                        __end1 = __start1 + READBUFFERSIZE - 1L;
                                        __sendEOWSignal = false;
                                        __isWrapLoadNeeded = false;
                                        startOffset = (long) (__end1 + 1L) % MAXFILESIZE;
                                        break;
                                    }

                                }
                                //temppeek < start1
                                else {
                                    if ((long)(MAXFILESIZE - __start1) + tempPeek + 1L <= READBUFFERSIZE) {
                                        __end1 = MAXFILESIZE - 1L;
                                        __start2 = 0L;
                                        __end2 = _windowEndAddress.remove();
                                        __sendEOWSignal = true;
                                        __isWrapLoadNeeded = true;
                                        startOffset = -1L;
                                        break;
                                    } else if((long)(MAXFILESIZE - __start1) + tempPeek +1L >= READBUFFERSIZE +1) {
                                        if (MAXFILESIZE - __start1 >= READBUFFERSIZE) {
                                            __end1 = __start1 + READBUFFERSIZE - 1L;
                                            __sendEOWSignal = false;
                                            __isWrapLoadNeeded = false;
                                            startOffset = (long) (__end1 + 1L) % MAXFILESIZE;
                                            break;
                                        } else {
                                            __end1 = MAXFILESIZE - 1L;
                                            __start2 = 0L;
                                            __end2 = READBUFFERSIZE - (MAXFILESIZE - __start1) - 1L;
                                            __sendEOWSignal = false;
                                            __isWrapLoadNeeded = true;
                                            startOffset = (__end2 + 1L) % MAXFILESIZE;
                                            break;
                                        }
                                    }

                                }
                            }
                            //The window address is not present
                            else {
                                if (_windowEndAddress.isEmpty() &&
                                        MAXFILESIZE - __start1 > READBUFFERSIZE) {
                                    __end1 = __start1 + READBUFFERSIZE - 1L;
                                    __sendEOWSignal = false;
                                    __isWrapLoadNeeded = false;
                                    startOffset = (long) (__end1 + 1L) % MAXFILESIZE;
                                    break;
                                } else if(_windowEndAddress.isEmpty() && (long)(MAXFILESIZE - __start1 ) + tempFileWriter >= 1L + READBUFFERSIZE) {
                                    //System.out.println("Here2");
                                    __end1 = MAXFILESIZE - 1L;
                                    __start2 = 0L;
                                    __end2 = READBUFFERSIZE - (MAXFILESIZE - __start1) - 1L;
                                    __sendEOWSignal = false;
                                    __isWrapLoadNeeded = true;
                                    startOffset = (__end2 + 1L) % MAXFILESIZE;
                                    break;
                                }
                            }


                        } else if(tempFileWriter > __start1 + READBUFFERSIZE || !_windowEndAddress.isEmpty()){ //Before Wrapping Up //if(tempFileWriter > __start1)
                            if (!_windowEndAddress.isEmpty()) {
                                long tempPeek = _windowEndAddress.peek();
                                if (tempPeek > __start1 && tempPeek - __start1 + 1 <= READBUFFERSIZE) {
                                    __end1 = _windowEndAddress.remove();
                                    __sendEOWSignal = true;
                                    __isWrapLoadNeeded = false;
                                    startOffset = -1L;
                                    break;
                                } else if(tempPeek > __start1 && tempPeek - __start1 + 1 >= READBUFFERSIZE+1L) {
                                    __end1 = __start1 + READBUFFERSIZE - 1L;
                                    __sendEOWSignal = false;
                                    __isWrapLoadNeeded = false;
                                    startOffset = (long) (__end1 + 1L) % MAXFILESIZE;
                                    break;
                                }

                            } else if (tempFileWriter - __start1 >= READBUFFERSIZE+1) {
                                __end1 = __start1 + READBUFFERSIZE - 1L;
                                __sendEOWSignal = false;
                                __isWrapLoadNeeded = false;
                                startOffset = (long) (__end1 + 1L) % MAXFILESIZE;
                                break;
                            }

                        }
                    }

                    int threadnum = _threadSequenceQueue.remove();
                    _threadSequenceQueue.add(threadnum);
                    _threadSequenceQueue.notifyAll();

                    try {
                        if (__isWrapLoadNeeded) {
                            int idx1 = loadBuffer(__start1, __end1, 0);
                            int idx2 = loadBuffer(__start2, __end2, idx1);
                            if (__sendEOWSignal) {
                                _bufferList.get(__threadSequence)[idx1 + idx2] = -1;
                                _bufferList.get(__threadSequence)[idx1 + idx2 + 1] = -1;
                                System.out.println("In 1:: -1 -1 is set in Buffer::" + __threadSequence);
                            } else {
                                _bufferList.get(__threadSequence)[idx1 + idx2] = 0;
                                _bufferList.get(__threadSequence)[idx1 + idx2 + 1] = 0;
                            }
                        } else {
                            int idx1 = loadBuffer(__start1, __end1, 0);
                            if (__sendEOWSignal) {
                                _bufferList.get(__threadSequence)[idx1] = -1;
                                _bufferList.get(__threadSequence)[idx1 + 1] = -1;
                            } else {
                                _bufferList.get(__threadSequence)[idx1] = 0;
                                _bufferList.get(__threadSequence)[idx1 + 1] = 0;
                            }
                        }

                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                    _producerConsumerMap.put(__threadSequence, 1);
                }
            }
        }

        /**
         * Receive start and end offest of the file from where data needs to be read.
         * The index variable marks the point in current buffer from where the data needs to be stored in the buffer
         * @param s
         * @param e
         * @param index
         * @return Index of buffer where next byte can  be added
         * @throws IOException
         */
        private int loadBuffer(long s, long e, int index) throws IOException {
            _fileReader.seek(s);

            int length = (int) (e - s + 1);
            byte[] tempArr = new byte[length];
            try {
                _fileReader.readFully(tempArr, 0, length);
                System.arraycopy(tempArr, 0, _bufferList.get(__threadSequence), index, tempArr.length);
            }
            catch(Exception ex)
            {
                System.out.println("Exception Caught with  length ::"+length + "with start ::" + s);
                ex.printStackTrace();
            }
            return length;

        }
    }

    private class EmitFromMemory extends Thread
    {
        int __currentBuffer;
        int __bufferIndex;
        int __length;
        byte __ten;
        byte __unit;

        public EmitFromMemory(){
            __currentBuffer =0;
            __bufferIndex=0;
        }

        public void run(){
            System.out.println("Emitter threads begin");
            while(_producerConsumerMap.get(__currentBuffer)==-1);
            while(true){
                getLength();
                emitTuple();
            }

        }

        /**
         * Receives two byte and convert them to short int before sending
         * @param tens
         * @param units
         * @return
         */
        private int getIntFromTwoBytes(byte tens, byte units){
            return (short)((tens & 0xFF) << 8) | ((int)units & 0xFF);
        }

        /**This function reads two bytes from current buffer(from current and next buffer if required)
         * and assign the int value stored in those bytes to __length, the __length will have 0, -1 or any other
         * positive value lesser than 32,000 (Max size of tuple supported)
         */
        private void getLength()
        {
            /*
            Bytes required to get the length are present within the buffersize as per config
             */
            if(__bufferIndex < READBUFFERSIZE - 1L)
            {
                __ten = _bufferList.get(__currentBuffer)[__bufferIndex++];
                __unit = _bufferList.get(__currentBuffer)[__bufferIndex++];

            }
            /*
            The bytes required to read the length are present across two buffer
             */
            else if(__bufferIndex == READBUFFERSIZE - 1L)
            {
                System.out.println("We are in the special case");
                __ten = _bufferList.get(__currentBuffer)[__bufferIndex];

                /*Special case*/
                __unit = _bufferList.get(__currentBuffer)[__bufferIndex+1];
                int len = getIntFromTwoBytes(__ten, __unit);
                if(len == -1)
                {
                    __length = len;
                    return;
                }

                __currentBuffer++;
                __currentBuffer = __currentBuffer%MAXTHREAD;
                __bufferIndex = 0;
                System.out.println("setting buff to 0 in special case buffer nuumber "+ __currentBuffer);

                while(_producerConsumerMap.get(__currentBuffer)==-1);

                __unit = _bufferList.get(__currentBuffer)[__bufferIndex++];

            }
            /*
            The bytes required to get the tuple length is beyond Buffersize mentioned in config file.
            This two bytes will either have -1 or 0, nothing else. This case helps when the end of window exactly fits the buffer.
             */
            else
            {
                __ten = _bufferList.get(__currentBuffer)[__bufferIndex++];
                __unit = _bufferList.get(__currentBuffer)[__bufferIndex];
                // System.out.println("This is for -1 or 0 from buffer "+ __currentBuffer);
            }
            __length = getIntFromTwoBytes(__ten, __unit);
        }

        /**
         * This function is responsible for sending the tuples out to the subsequent process.
         * It reads the __length variable and extract those many bytes from current buffer (from next buffer as well if required)
         * and forms the tuple before sending out.
         */
        private void emitTuple()
        {
            /*if length is zero, this means the buffer was complete filled
            and has been read completely without sending any end of window signal.
            Mark this buffer as read and go to the beginning of next buffer
            */
            if(__length == 0)
            {
                _producerConsumerMap.put(__currentBuffer, -1);
                //System.out.println("No data in::"+__currentBuffer);
                //System.out.println("setting buff to 0 in __length == 0 buffer nuumber "+ __currentBuffer);
                __currentBuffer++;
                __currentBuffer = __currentBuffer%MAXTHREAD;
                while(_producerConsumerMap.get(__currentBuffer)==-1);
                __bufferIndex =0;
                return;
            }
            /*if length is -1, this means the buffer had an end of window length.
            Buffer has been read completely and end of window signal. needs to be sent.
            Mark this buffer as read and go to the beginning of next buffer
            */
            else if(__length == -1)
            {
                _producerConsumerMap.put(__currentBuffer, -1);
                //System.out.println("No data in::"+__currentBuffer);
                sendEndOfWindowSignal();
                //System.out.println("EOW sent from Buffer::"+ __currentBuffer);
                //System.out.println("setting buff to 0 in __length == -1 in buffer nuumber "+ __currentBuffer);
                __currentBuffer++;
                __currentBuffer = __currentBuffer%MAXTHREAD;
                while(_producerConsumerMap.get(__currentBuffer)==-1);
                __bufferIndex =0;

                return;
            }
            /*
            The length of bytes required to form a tuple is present within same buffer
             */
            else if(__bufferIndex + __length <= READBUFFERSIZE)
            {
                byte[] tempArray = new byte[__length];
                System.arraycopy(_bufferList.get(__currentBuffer),__bufferIndex,tempArray,0, __length);
                String tupleData = new String(tempArray);
                _collector.emit("dataStream", new Values(tupleData));
                // System.out.println("Thiis normal:: Reading from current buffer "+ __currentBuffer +" and from index "+ __bufferIndex + " having length "+ __length);

                __bufferIndex = __bufferIndex + __length;

//                System.out.println("The new bufferindex "+ __bufferIndex);
                return;
            }
            /*
            he length of bytes required to form a tuple is present across multiple buffer
             */
            else {
                byte[] tempArray = new byte[__length];
                int partLength = READBUFFERSIZE - __bufferIndex;
                System.arraycopy(_bufferList.get(__currentBuffer),__bufferIndex,tempArray,0, partLength);
                //System.out.println("Switching from " + __currentBuffer + " to "+ (__currentBuffer+1)%MAXTHREAD);
                //System.out.println("Buffer Index of Incomplete tuple ::"+ _bufferIndex + " Tuple is of length::" + __length + " Amount read from this buffer is::" + partLength);
                _producerConsumerMap.put(__currentBuffer, -1);
                //System.out.println("No data in::"+__currentBuffer);
                __currentBuffer++;
                __currentBuffer = __currentBuffer%MAXTHREAD;

                while(_producerConsumerMap.get(__currentBuffer)==-1);

                __bufferIndex =0;
                //System.out.println("setting buff to 0 in __length == wrap up tuple length buffer nuumber "+ __currentBuffer);
                __length = __length - partLength;
                System.arraycopy(_bufferList.get(__currentBuffer),__bufferIndex,tempArray,partLength, __length);
                //System.out.println("Remaining bytes read from this buffer::"+ __length + " from" + __currentBuffer);
                String tupleData = new String(tempArray);
                _collector.emit("dataStream", new Values(tupleData));
                __bufferIndex = __bufferIndex + __length;
                //System.out.println("New buffer Index :: " + __bufferIndex);
                return;
            }
        }
    }
}

