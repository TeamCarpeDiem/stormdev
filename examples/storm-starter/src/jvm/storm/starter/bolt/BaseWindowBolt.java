package storm.starter.bolt;

import backtype.storm.Constants;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.starter.Interfaces.IBaseWindowBolt;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;


/**
 * Created by Harini Rajendran on 6/3/15.
 */

public class BaseWindowBolt extends BaseRichBolt implements IBaseWindowBolt{
    /*

		int MaxBufferSize = 98; // 100 -2 //ToDo assign in constructor from config file and subtract 2 from it


	 */
    OutputCollector _collector;
    private long windowLength;
    private long slideBy;
    private String FILEPATH = System.getProperty("user.home")+"//WindowsContent";
    private long MAXFILESIZE = ((2l * 1024 * 1024 * 1024) - 1);
    private final int WRITEBUFFERSIZE = 50 * 1024 * 1024;
    private final int MaxBufferSize = 50 * 1024 * 1024;
    private final double percentage = 0.75;
    Thread[] DiskReaderThread;
    Thread MemoryReader;
    Long startOffset = -1L;
    HashMap<Integer, Integer> ProducerConsumerMap;
    List<byte[]> _bufferList;
    BlockingQueue<Integer> ThreadSequenceQueue;
    int MaxThread = 5; //ToDo assign in constructor from config file



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

    public BaseWindowBolt(long wLength, long sBy, boolean isTBased)
    {
        tcount = 0;
        scount = 0;
        if(wLength <= 0) {
            throw new IllegalArgumentException("Window length is either null or negative");
        }
        else {
            windowLength = wLength;
        }
        if(sBy <= 0) {
            throw new IllegalArgumentException("Slideby should be a Positive value");
        }
        else {
            slideBy = sBy;
        }
        isTimeBased = isTBased;
        _windowStartAddress = new LinkedBlockingQueue<Long>();
        _windowEndAddress = new LinkedBlockingQueue<Long>();

        writeBuffer = new byte[WRITEBUFFERSIZE]; //Write Buffer size 100 MB
        //readBuffer = new byte[READBUFFERSIZE]; // Read Buffer size 100 MB

        bufferIndex = 0;
        secondCount = 0;

        System.out.println("!!!!!!!!!!!MAXFILESIZE is::" + MAXFILESIZE);

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

            if(!_windowStartAddress.isEmpty() && fileWriter.getFilePointer()  < _windowStartAddress.peek() && (fileWriter.getFilePointer()  - _windowStartAddress.peek()) < MaxBufferSize)
            {
                System.out.println("Writer catching up  on Reader");
                if(flag == -1)
                {
                    return;
                }
            }

            if (flag == 0) {
                //System.out.println(" 146 not updated file pointer:" + fileWriter.getFilePointer());
                writeInParts(writeBuffer,0,bufferIndex);
                if(fileWriter.getFilePointer() > MAXFILESIZE) {
                //    System.out.println(" 138 insert Start file pointer:" + fileWriter.getFilePointer());
                }

                //    Arrays.fill(writeBuffer, 0, bufferIndex, (byte) 0);
                bufferIndex = 0;
                for (int i = 0; i < count; i++) {
                    //System.out.println("Start Address::"+fileWriter.getFilePointer());
             //       System.out.println(" 145 insert Start file pointer:" + fileWriter.getFilePointer());
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


                //System.out.println(" 187 not updated file pointer:" + fileWriter.getFilePointer());
                writeInParts(writeBuffer,0,bufferIndex);
                if(fileWriter.getFilePointer() > MAXFILESIZE) {
                  //  System.out.println(" 173 insert end file pointer:" + fileWriter.getFilePointer());
                }

                //Arrays.fill(writeBuffer, 0, bufferIndex, (byte) 0);
                bufferIndex = 0;

                for (int i = 0; i < count; i++) {
              //      System.out.println(" 180 insert end file pointer:" + (fileWriter.getFilePointer()-1));
                    _windowEndAddress.add(fileWriter.getFilePointer()-1);
                }
            }

            if(bufferIndex >= WRITEBUFFERSIZE*percentage) //If the buffer is 75% full
            {

                //    System.out.println(" 207 not updated file pointer:" + fileWriter.getFilePointer() + "BufferSize:" + bufferIndex);
                writeInParts(writeBuffer,0,bufferIndex);
                if(fileWriter.getFilePointer() > MAXFILESIZE) {
                    //System.out.println(" 191 updated file pointer:" + fileWriter.getFilePointer());
                }
                bufferIndex = 0;
            }
        }
        catch(IOException ex)
        {
            ex.printStackTrace();
        }
    }

    private void writeInParts(byte[] buffer, int startIndex, int length) throws IOException
    {
        long remainingSpace = (MAXFILESIZE - fileWriter.getFilePointer());
        if(length <= remainingSpace)
            fileWriter.write(writeBuffer, startIndex, length);
        else {
            fileWriter.write(writeBuffer, startIndex, (int)remainingSpace);
            fileWriter.seek(0);
            fileWriter.write(writeBuffer, (int)remainingSpace, length - (int)remainingSpace);
        }
    }

    @Override
    public void initiateEmitter(OutputCollector baseCollector)
    {
        _collector = baseCollector;
                   Emitter();
        while(true);
    }


    @Override
    public void cleanup() {
        writeBuffer = null;
        readBuffer = null;
    }

    private void Emitter()
    {

        _bufferList = new ArrayList<byte[]>();

        DiskReaderThread = new Thread[MaxThread];
        ThreadSequenceQueue = new LinkedBlockingQueue<Integer>();
        ProducerConsumerMap= new HashMap<Integer, Integer>();
        MemoryReader = new Thread(new EmitFromMemory(0));


		/*Intialization of all the queues and buffers.*/
        for(int i =0; i < MaxThread; i++)
        {
            ThreadSequenceQueue.add(i);
            ProducerConsumerMap.put(i, -1);
            _bufferList.add(new byte[MaxBufferSize + 2]);
            DiskReaderThread[i] = new Thread(new DiskToMemory(i));
            DiskReaderThread[i].start();
        }

        MemoryReader.start();
    }


    private int getIntFromTwoBytes(byte tens, byte units){
        return (short)((tens & 0xFF) << 8) | ((int)units & 0xFF);
    }

    protected void sendEndOfWindowSignal() //OutputCollector collector
    {
         _collector.emit("mockTickTuple",new Values("__MOCKTICKTUPLE__"));
    }

    private class DiskToMemory extends Thread
    {
        int _threadSequence;
        Long start;
        Long endOffset;
        Long start1;
        Long endOffset1;
        boolean sendEOWSignal;
        boolean isWrapLoadNeeded;
        public DiskToMemory(int sequence)
        {
            _threadSequence = sequence;
            isWrapLoadNeeded=false;
        }

        public void run(){
            while(true){

                //System.out.println("I am thread: "+ _threadSequence);
                synchronized(ThreadSequenceQueue){
                    while(ThreadSequenceQueue.peek() != _threadSequence){
                        try {
                            ThreadSequenceQueue.wait();
                        } catch (InterruptedException e) {
                            // TODO Instead of throwing error. Put this to log statement
                            e.printStackTrace();
                        }
                    }

                    while(ProducerConsumerMap.get(_threadSequence) == 1);


                    while(_windowStartAddress.isEmpty() && startOffset != -1L);

                    if(startOffset == -1L)
                    {
                        startOffset = _windowStartAddress.remove();
                    }
                    try{
                        while(true){
                            if((!_windowEndAddress.isEmpty() && _windowEndAddress.peek() > startOffset)
                                    || (startOffset < fileWriter.getFilePointer() && fileWriter.getFilePointer() - startOffset >= MaxBufferSize)){
                                if(!_windowEndAddress.isEmpty() && _windowEndAddress.peek() - startOffset + 1 <= MaxBufferSize){
                                    start = startOffset;
                                    endOffset = _windowEndAddress.remove();

                                    startOffset = -1L;
                                    sendEOWSignal = true;
                                    break;
                                }
                                else{
                                    start = startOffset;
                                    endOffset = start + MaxBufferSize -1L;
                                    startOffset = endOffset + 1L;
                                    sendEOWSignal=false;
                                    break;
                                }

                            }
                            else if((!_windowEndAddress.isEmpty() && _windowEndAddress.peek() < startOffset)
                                    || (startOffset > fileWriter.getFilePointer() && MAXFILESIZE-startOffset + fileWriter.getFilePointer() + 1 >= MaxBufferSize)){
                                if(MAXFILESIZE - startOffset >= MaxBufferSize){
                                    start = startOffset;
                                    endOffset =start + MaxBufferSize-1L;
                                    startOffset = endOffset + 1L;
                                    sendEOWSignal=false;
                                    break;
                                }
                                else{
                                    if(MAXFILESIZE-startOffset + _windowEndAddress.peek() + 1 <= MaxBufferSize){
                                        start = startOffset;
                                        endOffset = MAXFILESIZE -1L;

                                        isWrapLoadNeeded = true;
                                        start1 = 0L;
                                        endOffset1 = _windowEndAddress.remove();
                                        startOffset = -1L;
                                        sendEOWSignal = true;
                                        break;
                                    }
                                    else{
                                        start = startOffset;
                                        endOffset = MAXFILESIZE -1L;

                                        isWrapLoadNeeded = true;
                                        start1 = 0L;
                                        endOffset1 = MaxBufferSize -2 - (endOffset - start);
                                        startOffset = endOffset1 + 1L;
                                        sendEOWSignal = false;
                                        break;
                                    }

                                }

                            }
                        }
                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                    ThreadSequenceQueue.add(ThreadSequenceQueue.remove());
                    ThreadSequenceQueue.notifyAll();
                    //ProducerConsumerMap.put(_threadSequence, 1);
                }

                try {
                    if(isWrapLoadNeeded){
                        loadBuffer(start, endOffset, 0);
                        int index = (int)(endOffset - start + 1);
                        if(sendEOWSignal)
                        {

                            loadBufferWithEOWSignal(start1, endOffset1, index);

                        }
                        else
                        {
                            loadBuffer(start1, endOffset1, index);
                        }
                    }
                    else{
                        if(sendEOWSignal)
                        {
                            loadBufferWithEOWSignal(start, endOffset, 0);

                        }
                        else
                        {
                            loadBuffer(start, endOffset, 0);
                        }
                    }
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
              //  System.out.println("I am done with this life. See you in next life, says thred: "+ _threadSequence);
                ProducerConsumerMap.put(_threadSequence, 1);
            }
        }

        private void loadBufferWithEOWSignal(long s, long e, int index) throws IOException {
            fileReader.seek(s);
            int length = (int) (e - s + 1);

            try {
                fileReader.read(_bufferList.get(_threadSequence), index, length);
                _bufferList.get(_threadSequence)[length] = -1;
                _bufferList.get(_threadSequence)[length+1] = -1;
                //System.out.println("Start address: "+  s + " End address: " + e);
               // String tupleData = new String( _bufferList.get(_threadSequence));
                //System.out.println("Tuple data from buffer: "+  tupleData);
            }
            catch(Exception ex)
            {
                System.out.println("Exception Caught with  length ::" + length);
                ex.printStackTrace();
                //exit(1);
            }

        }

        private void loadBuffer(long s, long e, int index) throws IOException {
            fileReader.seek(s);
            int length = (int) (e - s + 1);

            try {
                fileReader.read(_bufferList.get(_threadSequence), index, length);
                _bufferList.get(_threadSequence)[length] = 0;
                _bufferList.get(_threadSequence)[length+1] = 0;
              //  System.out.println("Start address: "+  s + " End address: " + e);

            }
            catch(Exception ex)
            {
                System.out.println("Exception Caught with length ::" + length);
                ex.printStackTrace();
                //exit(1);
            }
            //return length;

        }
    }

    private class EmitFromMemory extends Thread{
        int currentBuffer;
        int start;

        public EmitFromMemory(int bufferNumber)
        {
            currentBuffer = bufferNumber;
            start =0;

        }

        public void run(){

            //int end = MaxBufferSize;
            int length =0;
            while(true)
            {
                while(ProducerConsumerMap.get(currentBuffer%MaxThread) == -1);

                length = getLength();
              //  System.out.println("The length of tuple: "+ length);

                if(start + length <= MaxBufferSize)
                {
                    //System.out.println("length of the start is: "+ start);
                    //System.out.println("length of the tuple is: "+ length);
                    byte[] tempArray = Arrays.copyOfRange(_bufferList.get(currentBuffer), start, start+length);
                    String tupleData = new String(tempArray);
                    //System.out.println("String before emit::" + tupleData);
                    _collector.emit("dataStream", new Values(tupleData)); //TODO uncomment when putting real system
                   // System.out.println("yo man!!!!!!!!! Emitted a tuple..... woooohoooooooo!!!!!!!!!");
                    //count++;
                    start = start + length;
                    if(start == MaxBufferSize)
                    {
                        System.out.println(start);
                    }
                }
                else
                {
                    int partLength = MaxBufferSize - start;
                    byte[] tempArray = new byte[length];  //Arrays.copyOfRange(_bufferList.get(currentBuffer), start, partLength);
                    System.arraycopy(_bufferList.get(currentBuffer), start, tempArray, 0, partLength);

                    ProducerConsumerMap.put(currentBuffer, -1);
                    currentBuffer++;
                    currentBuffer = currentBuffer%MaxThread;
                    start =0;
                    length = length - partLength;
                    while(ProducerConsumerMap.get(currentBuffer%MaxThread) == -1);
//					tempArray = Arrays.copyOfRange(_bufferList.get(currentBuffer), start, length);

                    System.arraycopy(_bufferList.get(currentBuffer), start, tempArray, partLength, length);

                    String tupleData = new String(tempArray);
                    //System.out.println("String before emit::" + tupleData);
                    _collector.emit("dataStream", new Values(tupleData)); //TODO uncomment when putting real system
                    start = start + length;
                }



            }

        }

        public int getLength()
        {
            byte ten;
            byte unit;
            if(start < MaxBufferSize - 1){
                ten = _bufferList.get(currentBuffer)[start];
                unit = _bufferList.get(currentBuffer)[start+1];
                start = start + 2;
                int tempLength = getIntFromTwoBytes(ten,unit);

              //  System.out.println("calculated length: "+ tempLength);
                if(tempLength == -1)
                {
                    sendEndOfWindowSignal();
                }
                else
                {
                    return tempLength;
                }
                ProducerConsumerMap.put(currentBuffer, -1);
              //  System.out.println("Consumed Buffer: "+currentBuffer);
                currentBuffer++;
                currentBuffer = currentBuffer%MaxThread;
                while(ProducerConsumerMap.get(currentBuffer%MaxThread) == -1);
                start =0;
                return getLength();
            }
            else if (start < MaxBufferSize){
                ten = _bufferList.get(currentBuffer)[start];
                unit = _bufferList.get(currentBuffer)[start+1];
                int tempLength = getIntFromTwoBytes(ten,unit);
                if(tempLength == -1)
                {
                    sendEndOfWindowSignal();
                    ProducerConsumerMap.put(currentBuffer, -1);
                //    System.out.println("Consumed Buffer: "+currentBuffer);
                    currentBuffer++;
                    currentBuffer = currentBuffer%MaxThread;
                    while(ProducerConsumerMap.get(currentBuffer%MaxThread) == -1);
                    start =0;
                    return getLength();
                }

                ProducerConsumerMap.put(currentBuffer, -1);
             //   System.out.println("Consumed Buffer: "+currentBuffer);
                currentBuffer++;
                currentBuffer = currentBuffer%MaxThread;
                start =0;
                while(ProducerConsumerMap.get(currentBuffer%MaxThread) == -1);
                unit = _bufferList.get(currentBuffer)[start];
                return getIntFromTwoBytes(ten,unit);

            }
            else{
                ten = _bufferList.get(currentBuffer)[start];
                unit = _bufferList.get(currentBuffer)[start+1];
                int tempLength = getIntFromTwoBytes(ten,unit);
               // System.out.println("calculated length: "+ tempLength);
                if(tempLength == -1)
                {
                    sendEndOfWindowSignal();
                }
                ProducerConsumerMap.put(currentBuffer, -1);
             //   System.out.println("Consumed Buffer: "+currentBuffer);
                currentBuffer++;
                currentBuffer = currentBuffer%MaxThread;
                while(ProducerConsumerMap.get(currentBuffer%MaxThread) == -1);
                start = 0;
                return getLength();
            }

        }


    }


}