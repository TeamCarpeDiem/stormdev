package storm.starter.bolt;

import backtype.storm.Constants;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import storm.starter.HelperClasses.WindowObject;
import storm.starter.Interfaces.IBaseWindowBolt;

import java.io.*;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by Sachin (Emitting logic)and Harini (Storing logic) on 7/7/15.
 */
public class BaseWindowBolt extends BaseRichBolt implements IBaseWindowBolt {

    /******************************* Configurable Parameters *******************************/
    long MAXFILESIZE;
    int WRITEBUFFERSIZE;
    int READBUFFERSIZE;
    double WRITEBUFFERTHRESHOLD;
    int MAXTHREAD;
    String FILEPATH;

    /******************************* from window object  *******************************/
    boolean isTimeBased;

    /******************************* Updated while storing tuple  *******************************/
    protected BlockingQueue<Long> _windowStartAddress;
    protected BlockingQueue<Long> _windowEndAddress;
    byte[] _writeBuffer; //Buffer which is used to perform bulk write to the disk
    int _bufferIndex;
    RandomAccessFile _fileWriter; //File to which contents will be written
    int secondCount; //TODO Remove before releasing final code

    /******************************* Updated while reading data from disk to memory  *******************************/
    OutputCollector _collector;
    List<byte[]> _bufferList;
    Thread[] _diskReaderThread;
    BlockingQueue<Integer> _threadSequenceQueue;
    HashMap<Integer, Integer> _producerConsumerMap;
    long startOffset;
    RandomAccessFile _fileReader;

    /******************************* Updated while emitting data from memory  *******************************/
    Thread _memoryReader;

    /******************************* Testing *****************************///TODO to be removed
    int tLength;

    public BaseWindowBolt(WindowObject wObj)
    {
        tLength = 3997; //TODO to be removed
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

        isTimeBased = wObj.getIsTimeBased();
        _windowStartAddress = new LinkedBlockingQueue<Long>();
        _windowEndAddress = new LinkedBlockingQueue<Long>();

        _writeBuffer = new byte[WRITEBUFFERSIZE];

        _bufferIndex = 0;
        secondCount = 0;

    }

    //TODO Remove this constructor if one with windowobject works.
    public BaseWindowBolt(long wLength, long sBy, boolean isTBased)
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
        } catch (IOException e) {
            e.printStackTrace();
        }
        if(wLength <= 0) {
            throw new IllegalArgumentException("Window length is either null or negative");
        }
        if(sBy <= 0) {
            throw new IllegalArgumentException("Slideby should be a Positive value");
        }

        isTimeBased = isTBased;
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
            _fileWriter = new RandomAccessFile(FILEPATH,"rws");
            _fileReader = new RandomAccessFile(FILEPATH,"r");
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

    //@Override
    public boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
                && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
    }

    protected void sendEndOfWindowSignal() //OutputCollector collector
    {
        _collector.emit("mockTickTuple",new Values("__MOCKTICKTUPLE__"));
    }

    //@Override
    public void initiateEmitter(OutputCollector baseCollector)
    {
        _collector = baseCollector;
        Emitter();
        //while(true);
    }


    @Override
    public void cleanup() {
        _writeBuffer = null;
        File fp = new File(FILEPATH, "a");
        fp.delete();
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
     *
     * @param tuple
     * @param flag
     * @param count
     */
    //@Override
    public void storeTuple(Tuple tuple, int flag, int count)
    {
        try {

            //TODO Catching Up part
            if(!_windowStartAddress.isEmpty() && (long)_fileWriter.getFilePointer()  < (long)_windowStartAddress.peek()
                    && (((long)_windowStartAddress.peek() - (long)_fileWriter.getFilePointer() ) < (long)READBUFFERSIZE))
            {
                System.out.println("Writer catching up  on Reader..  Start Address::"+ _windowStartAddress.peek() + "File Writer:"+_fileWriter.getFilePointer());
                Utils.sleep(10000);
            }

            if (flag == 0) {
                writeInParts();
                for (int i = 0; i < count; i++) {
                    _windowStartAddress.add(_fileWriter.getFilePointer());
                    System.out.println("Start address added in the queue: "+ _fileWriter.getFilePointer());
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
                if((long)_fileWriter.getFilePointer() == 0L)
                {
                    for (int i = 0; i < count; i++) {
                        _windowEndAddress.add(MAXFILESIZE - 1L);
                    }
                }
                else {

                    for (int i = 0; i < count; i++) {
                        System.out.println("End Address Added to the queue::"+(_fileWriter.getFilePointer() - 1L));
                        _windowEndAddress.add(_fileWriter.getFilePointer() - 1L);
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
     *
     * @throws IOException
     */
    private void writeInParts() throws IOException
    {
        long remainingFileSpace = (MAXFILESIZE - _fileWriter.getFilePointer());
        int bufferDataLength = _bufferIndex;
        if(bufferDataLength <= remainingFileSpace) {
            _fileWriter.write(_writeBuffer, 0, bufferDataLength);

            //When FileWriter reaches the MAXFILESIZE after bulk write, wrap up should happen
            if((long)_fileWriter.getFilePointer() == MAXFILESIZE)
                _fileWriter.seek(0L);
        }
        else {
            _fileWriter.write(_writeBuffer, 0, (int)remainingFileSpace);
            _fileWriter.seek(0L);
            _fileWriter.write(_writeBuffer, (int)remainingFileSpace, bufferDataLength - (int)remainingFileSpace);
        }
        _bufferIndex = 0;
    }

    /**
     *
     */
    private void Emitter() {

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

                    while (_producerConsumerMap.get(__threadSequence) == 1) ;

                    if (startOffset == -1L) {
                        while (_windowStartAddress.isEmpty()) ;
                        startOffset = _windowStartAddress.remove();
                        System.out.println("Start Address removed::" + startOffset);
                    }
                    __start1 = startOffset;
                    try {
                        long tempFileWriter;
                        while (true) {
                            tempFileWriter = _fileWriter.getFilePointer();
                            if (tempFileWriter < __start1) { //After Wrapping Up
                                //If end address is present
                                if (!_windowEndAddress.isEmpty()) {

                                    long tempPeek = _windowEndAddress.peek();
                                    if (tempPeek > __start1) {
                                        if ( tempPeek - __start1 + 1 <= READBUFFERSIZE) {
                                            __end1 = _windowEndAddress.remove();
                                            System.out.println("End address removed from temppeek > start1" + __end1);
                                            System.out.println("1 :: End Address removed::" + __end1);
                                            __sendEOWSignal = true;
                                            __isWrapLoadNeeded = false;
                                            startOffset = -1L;
                                            break;
                                        } else {
                                            __end1 = __start1 + READBUFFERSIZE - 1L;
                                            __sendEOWSignal = false;
                                            __isWrapLoadNeeded = false;
                                            startOffset = (long) (__end1 + 1L) % MAXFILESIZE;
                                            break;
                                        }

                                    } else {
                                        if (MAXFILESIZE - __start1 + tempPeek + 1 <= READBUFFERSIZE) {
                                            __end1 = MAXFILESIZE - 1L;
                                            __start2 = 0L;
                                            __end2 = _windowEndAddress.remove();
                                            System.out.println("2 :: End Address removed::" + __end2);
                                            __sendEOWSignal = true;
                                            __isWrapLoadNeeded = true;
                                            startOffset = -1L;
                                            break;
                                        } else {
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
                                } else {
                                    if (MAXFILESIZE - __start1 >= READBUFFERSIZE) {
                                        System.out.println("Here1");
                                        __end1 = __start1 + READBUFFERSIZE - 1L;
                                        __sendEOWSignal = false;
                                        __isWrapLoadNeeded = false;
                                        startOffset = (long) (__end1 + 1L) % MAXFILESIZE;
                                        break;
                                    } else if(MAXFILESIZE - __start1 + tempFileWriter + 1 >= READBUFFERSIZE) {
                                        System.out.println("Here2");
                                        __end1 = MAXFILESIZE - 1L;
                                        __start2 = 0L;
                                        __end2 = READBUFFERSIZE - (MAXFILESIZE - __start1) - 1L;
                                        __sendEOWSignal = false;
                                        __isWrapLoadNeeded = true;
                                        startOffset = (__end2 + 1L) % MAXFILESIZE;
                                        break;
                                    }
                                }


                            } else if(tempFileWriter > __start1 + READBUFFERSIZE){ //Before Wrapping Up //if(tempFileWriter > __start1)
                                if (!_windowEndAddress.isEmpty()) {
                                    long tempPeek = _windowEndAddress.peek();
                                    if (tempPeek > __start1 && tempPeek - __start1 + 1 <= READBUFFERSIZE) {
                                        __end1 = _windowEndAddress.remove();
                                        System.out.println("3 :: file writer pointer::" + tempFileWriter);
                                        System.out.println("3 :: End Address removed::" + __end1);
                                        __sendEOWSignal = true;
                                        __isWrapLoadNeeded = false;
                                        startOffset = -1L;
                                        break;
                                    } else {
                                        __end1 = __start1 + READBUFFERSIZE - 1L;
                                        __sendEOWSignal = false;
                                        __isWrapLoadNeeded = false;
                                        startOffset = (long) (__end1 + 1L) % MAXFILESIZE;
                                        break;
                                    }

                                } else if (tempFileWriter - __start1 + 1L >= READBUFFERSIZE) {
                                    __end1 = __start1 + READBUFFERSIZE - 1L;
                                    __sendEOWSignal = false;
                                    __isWrapLoadNeeded = false;
                                    startOffset = (long) (__end1 + 1L) % MAXFILESIZE;
                                    break;
                                }

                            }
                        }

                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }

                    int threadnum = _threadSequenceQueue.remove();
                    _threadSequenceQueue.add(threadnum);
                    _threadSequenceQueue.notifyAll();

                    //}//Commented Synchorized TODO: check this condition when everything else is working
                    try {
                        if (__isWrapLoadNeeded) {
                            System.out.println("Start1 :: "+ __start1 + "   End1::"+ __end1 + "Start2 :: "+ __start2 + "   End2::"+ __end2);
                            int idx1 = loadBuffer(__start1, __end1, 0);
                            int idx2 = loadBuffer(__start2, __end2, idx1);
                            if (__sendEOWSignal) {
                                _bufferList.get(__threadSequence)[idx1 + idx2] = -1;
                                _bufferList.get(__threadSequence)[idx1 + idx2 + 1] = -1;
                            } else {
                                _bufferList.get(__threadSequence)[idx1 + idx2] = 0;
                                _bufferList.get(__threadSequence)[idx1 + idx2 + 1] = 0;
                            }
                        } else {
                            int idx1 = loadBuffer(__start1, __end1, 0);
                            if (__sendEOWSignal) {
                                System.out.println("-1 -1 set by::" + __threadSequence);
                                _bufferList.get(__threadSequence)[idx1] = -1;
                                _bufferList.get(__threadSequence)[idx1 + 1] = -1;
                            } else {
                                System.out.println("Value of idx1 is::" + idx1);
                                _bufferList.get(__threadSequence)[idx1] = 0;
                                _bufferList.get(__threadSequence)[idx1 + 1] = 0;
                                // System.out.println("!!!Filling Buffer::" + _bufferList.get(__threadSequence)[47988000]);
                            }
                        }

                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }

                    System.out.println("The buffer ready to be read is:: " + __threadSequence);
                    _producerConsumerMap.put(__threadSequence, 1);
                }//to be removed
            }
        }

        private int loadBuffer(long s, long e, int index) throws IOException {
            _fileReader.seek(s);

            int length = (int) (e - s + 1);
            System.out.println("Inside LoadBuffer:: Start ::" + s + "    End::"+ e + "  Length::"+length);
            byte[] tempArr = new byte[length];
            try {
                System.out.println("Start offset::"+s + "   End Offset::"+e + "by thread::"+__threadSequence + " with File Length::"+ _fileReader.length());
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
            while(_producerConsumerMap.get(__currentBuffer)==-1);
            while(true){
                getLength();
                emitTuple();
            }

        }
        private int getIntFromTwoBytes(byte tens, byte units){
            return (short)((tens & 0xFF) << 8) | ((int)units & 0xFF);
        }

        private void getLength()
        {
            if(__bufferIndex < READBUFFERSIZE - 1L)
            {
                __ten = _bufferList.get(__currentBuffer)[__bufferIndex++];
                __unit = _bufferList.get(__currentBuffer)[__bufferIndex++];

            }
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
            else
            {
                __ten = _bufferList.get(__currentBuffer)[__bufferIndex++];
                __unit = _bufferList.get(__currentBuffer)[__bufferIndex];
                System.out.println("This is for -1 or 0 from buffer "+ __currentBuffer);
            }
            __length = getIntFromTwoBytes(__ten, __unit);
        }

        private void emitTuple()
        {
            if(__length == 0)
            {
                _producerConsumerMap.put(__currentBuffer, -1);
                System.out.println("No data in::"+__currentBuffer);
                System.out.println("setting buff to 0 in __length == 0 buffer nuumber "+ __currentBuffer);
                __currentBuffer++;
                __currentBuffer = __currentBuffer%MAXTHREAD;
                while(_producerConsumerMap.get(__currentBuffer)==-1);
                __bufferIndex =0;
                return;
            }
            else if(__length == -1)
            {
                _producerConsumerMap.put(__currentBuffer, -1);
                System.out.println("No data in::"+__currentBuffer);
                sendEndOfWindowSignal();
                System.out.println("EOW sent from Buffer::"+ __currentBuffer);
                System.out.println("setting buff to 0 in __length == -1 in buffer nuumber "+ __currentBuffer);
                __currentBuffer++;
                __currentBuffer = __currentBuffer%MAXTHREAD;
                while(_producerConsumerMap.get(__currentBuffer)==-1);
                __bufferIndex =0;

                return;
            }
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
            else {
                byte[] tempArray = new byte[__length];
                int partLength = READBUFFERSIZE - __bufferIndex;
                System.arraycopy(_bufferList.get(__currentBuffer),__bufferIndex,tempArray,0, partLength);
                System.out.println("Switching from " + __currentBuffer + " to "+ (__currentBuffer+1)%MAXTHREAD);
                System.out.println("Buffer Index of Incomplete tuple ::"+ _bufferIndex + " Tuple is of length::" + __length + " Amount read from this buffer is::" + partLength);
                _producerConsumerMap.put(__currentBuffer, -1);
                System.out.println("No data in::"+__currentBuffer);
                __currentBuffer++;
                __currentBuffer = __currentBuffer%MAXTHREAD;

                while(_producerConsumerMap.get(__currentBuffer)==-1);

                __bufferIndex =0;
                System.out.println("setting buff to 0 in __length == wrap up tuple length buffer nuumber "+ __currentBuffer);
                __length = __length - partLength;
                System.arraycopy(_bufferList.get(__currentBuffer),__bufferIndex,tempArray,partLength, __length);
                System.out.println("Remaining bytes read from this buffer::"+ __length + " from" + __currentBuffer);
                String tupleData = new String(tempArray);
                _collector.emit("dataStream", new Values(tupleData));
                __bufferIndex = __bufferIndex + __length;
                System.out.println("New buffer Index :: " + __bufferIndex);
                return;
            }
        }
    }
}


