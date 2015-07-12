package storm.starter.bolt;

import backtype.storm.Constants;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import storm.starter.Interfaces.IBaseWindowBolt;

import java.io.*;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;


/**
 * Created by Harini Rajendran on 6/3/15.
 */

public class BaseWindowBolt1 extends BaseRichBolt implements IBaseWindowBolt{
    OutputCollector _collector;
    private long windowLength;
    private long slideBy;
    private String FILEPATH = System.getProperty("user.home")+"//WindowsContent";
    private long MAXFILESIZE;
    private int WRITEBUFFERSIZE;
    private int MaxBufferSize;
    private double percentage;
    int MaxThread;
    Thread[] DiskReaderThread;
    Thread MemoryReader;
    Long startOffset = -1L;
    HashMap<Integer, Integer> ProducerConsumerMap;
    List<byte[]> _bufferList;
    BlockingQueue<Integer> ThreadSequenceQueue;
    int tlength; //Testing


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

    public BaseWindowBolt1(long wLength, long sBy, boolean isTBased)
    {
        tlength = 3997;
        Properties prop = new Properties();
        InputStream input = null;
        try {
            input = new FileInputStream("config.properties");
            prop.load(input);
            MAXFILESIZE = Long.valueOf(prop.getProperty("maximumFileSize"));
            WRITEBUFFERSIZE = Integer.valueOf(prop.getProperty("writeBufferSize"));
            MaxBufferSize = Integer.valueOf(prop.getProperty("readBufferSize"));
            percentage = Double.valueOf(prop.getProperty("bufferThreshold"));
            MaxThread = Integer.valueOf(prop.getProperty("numberOfThreads"));
        } catch (IOException e) {
            e.printStackTrace();
        }
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

        writeBuffer = new byte[WRITEBUFFERSIZE];

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

            if(!_windowStartAddress.isEmpty() && (long)fileWriter.getFilePointer()  < (long)_windowStartAddress.peek() && (((long)_windowStartAddress.peek() - (long)fileWriter.getFilePointer() ) < (long)MaxBufferSize))
            {
                System.out.println("Writer catching up  on Reader..  Start Address::"+ _windowStartAddress.peek() + "File Writer:"+fileWriter.getFilePointer());
                Utils.sleep(10000);
            }

            if (flag == 0) {
                writeInParts(writeBuffer,0,bufferIndex);
                bufferIndex = 0;
                for (int i = 0; i < count; i++) {
                    _windowStartAddress.add(fileWriter.getFilePointer());
                    System.out.println("Start address added in the queue: "+ fileWriter.getFilePointer());
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
                if(bytes == null)
                    System.out.println("Tuple is null");
                System.arraycopy(bytes, 0, writeBuffer, bufferIndex, bytes.length);
                bufferIndex += bytes.length;
            }

            if (flag == 1) {
                writeInParts(writeBuffer,0,bufferIndex);
                bufferIndex = 0;
                if((long)fileWriter.getFilePointer() == 0L)
                {
                    for (int i = 0; i < count; i++) {
                        _windowEndAddress.add(MAXFILESIZE - 1L);
                    }
                }
                else {

                    for (int i = 0; i < count; i++) {
                        _windowEndAddress.add(fileWriter.getFilePointer() - 1L);
                    }
                }
            }

            if(bufferIndex >= WRITEBUFFERSIZE*percentage) //If the buffer is 75% full
            {
                writeInParts(writeBuffer,0,bufferIndex);
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
        if(length <= remainingSpace) {
            long temp1 = fileWriter.getFilePointer();

            fileWriter.write(writeBuffer, startIndex, length);
            if((long)fileWriter.getFilePointer() == MAXFILESIZE)
                fileWriter.seek(0L);
        }
        else {
            System.out.println("!!!!!!!!!!!!!!WRITER Wrapping Up :: Data written at the end::"+remainingSpace+" bytes   Data written on top::"+(length - (int)remainingSpace));
            System.out.println("The intial pointer before wrapping is:: "+ fileWriter.getFilePointer());

            fileWriter.write(writeBuffer, startIndex, (int)remainingSpace);
            fileWriter.seek(0);
            fileWriter.write(writeBuffer, (int)remainingSpace, length - (int)remainingSpace);
        }
    }

    @Override
    public void initiateEmitter()
    {
        Emitter();
        while(true);
    }

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

    private void Emitter() {

        _bufferList = new ArrayList<byte[]>();

        DiskReaderThread = new Thread[MaxThread];
        ThreadSequenceQueue = new LinkedBlockingQueue<Integer>();
        ProducerConsumerMap = new HashMap<Integer, Integer>();
        MemoryReader = new Thread(new EmitFromMemory(0));


		/*Intialization of all the queues and buffers.*/
        MemoryReader.start();
        for (int i = 0; i < MaxThread; i++) {
            ThreadSequenceQueue.add(i);
            ProducerConsumerMap.put(i, -1);
            _bufferList.add(new byte[MaxBufferSize + 2]);
            DiskReaderThread[i] = new Thread(new DiskToMemory(i));
        }
        for (int i = 0; i < MaxThread; i++) {
            DiskReaderThread[i].start();
        }
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


                    while(_windowStartAddress.isEmpty());

                    if(startOffset == -1L)
                    {
                        startOffset = _windowStartAddress.remove();
                        System.out.println("The startoffset removed is and by: " + startOffset +" by thread  "+ _threadSequence);
                    }
                    try{
                        while(true){
                            if((!_windowEndAddress.isEmpty() && _windowEndAddress.peek() > startOffset)
                                    || (startOffset < fileWriter.getFilePointer() && (long)fileWriter.getFilePointer() - (long)startOffset >= 1.5*MaxBufferSize)){
                                if(!_windowEndAddress.isEmpty() && _windowEndAddress.peek() - startOffset + 1 <= MaxBufferSize){
                                    start = startOffset;
                                    endOffset = _windowEndAddress.remove();
                                    System.out.println("End address removed: "+ endOffset +"  by thread "+ _threadSequence);
                                    startOffset = -1L;
                                    sendEOWSignal = true;
                                    isWrapLoadNeeded = false;
                                    break;
                                }
                                else{
                                    start = startOffset;
                                    endOffset = start + MaxBufferSize -1L;
                                    startOffset = endOffset + 1L;
                                    sendEOWSignal=false;
                                    isWrapLoadNeeded = false;
                                    break;
                                }
                            }
                            else if((!_windowEndAddress.isEmpty() && _windowEndAddress.peek() < startOffset)
                                    || (startOffset > fileWriter.getFilePointer() && (MAXFILESIZE-startOffset) + fileWriter.getFilePointer()  >= 1.5*MaxBufferSize)){

                                if(!_windowEndAddress.isEmpty() && _windowEndAddress.peek() > startOffset && _windowEndAddress.peek() - startOffset + 1 <= MaxBufferSize){
                                    start = startOffset;
                                    endOffset = _windowEndAddress.remove();
                                    System.out.println("End address removed : "+ endOffset+"by thread "+ _threadSequence);
                                    startOffset = -1L;
                                    sendEOWSignal = true;
                                    isWrapLoadNeeded = false;
                                    break;
                                }
                                else if(!_windowEndAddress.isEmpty() && _windowEndAddress.peek() > startOffset && _windowEndAddress.peek() - startOffset + 1 > MaxBufferSize){
                                    start = startOffset;
                                    endOffset = start + MaxBufferSize -1L;
                                    startOffset = endOffset + 1L;
                                    sendEOWSignal=false;
                                    isWrapLoadNeeded = false;
                                    break;
                                }
                                //else
                                else if(!_windowEndAddress.isEmpty()  && (MAXFILESIZE-startOffset) + _windowEndAddress.peek()  <= MaxBufferSize){
                                    start = startOffset;
                                    endOffset = MAXFILESIZE -1L;

                                    isWrapLoadNeeded = true;
                                    start1 = 0L;
                                    endOffset1 = _windowEndAddress.remove();
                                    System.out.println("End address removed: "+ endOffset1+"by thread "+ _threadSequence);
                                    startOffset = -1L;
                                    sendEOWSignal = true;
                                    break;
                                }
                                else if(MAXFILESIZE-startOffset >= MaxBufferSize) {
                                    start = startOffset;
                                 //   if (((long) start + (long) MaxBufferSize) - 1L <= MAXFILESIZE) {

                                        endOffset = start + MaxBufferSize - 1L;
                                        isWrapLoadNeeded = false;
                                        sendEOWSignal = false;
                                        startOffset = (endOffset + 1L) % MAXFILESIZE;
                                        break;
                                   // }
                                }
                                else {
                                    start = startOffset;
                                    endOffset = MAXFILESIZE - 1;
                                    start1 = 0L;
                                    endOffset1 = MaxBufferSize - (endOffset + 1 - start) - 1;
                                    isWrapLoadNeeded = true;
                                    sendEOWSignal = false;
                                    startOffset = endOffset1 + 1L;
                                    break;
                                }
                                }
                                /*
                                else{
                                if(MAXFILESIZE - startOffset >= MaxBufferSize){
                                    start = startOffset;
                                    endOffset =start + MaxBufferSize-1L;
                                    startOffset = (endOffset + 1L)%MAXFILESIZE;
                                    sendEOWSignal=false;
                                    isWrapLoadNeeded = false;
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
                                }*/

                            }
                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                    System.out.println();
                    int threadnum =ThreadSequenceQueue.remove();
                    System.out.println("Current thread removed:  "+ threadnum);
                    ThreadSequenceQueue.add(threadnum);
                    System.out.println("Next thread in the queue  "+ ThreadSequenceQueue.peek());
                    ThreadSequenceQueue.notifyAll();
                }

                try {
                    if(isWrapLoadNeeded){
                        loadBuffer1(start, endOffset, 0);
                        int idx1 = (int)(endOffset - start) + 1;
                        System.out.println("&&&&&&&&&&&&&&&&&&&&&&&&length of part 1::" + endOffset+ "  and new index should be "+ idx1);
                        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~start and endoffset from part1 buffer"+ start + "  " + endOffset +  "   " + _threadSequence);
                        if(sendEOWSignal)
                        {
                            System.out.println("Wrap around with eowsignal");
                            loadBufferWithEOWSignal1(start1, endOffset1, idx1);
                            System.out.println("&&&&&&&&&&&&&&&&&&&&&&&&length of part 2::" + endOffset1+ "  and start index should be "+ idx1);
                        }
                        else
                        {
                            System.out.println("Wrap around with out eowsignal");
                            loadBuffer1(start1, endOffset1, idx1);
                            System.out.println("&&&&&&&&&&&&&&&&&&&&&&&&length of part 2::" + endOffset1+ "  and start index should be "+ idx1);
                           // System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ start1 and endoffset1 prt2 ubffer"+ start1 + "  " + endOffset1  +  "   " + _threadSequence);
                            if((long)((endOffset-start+1)+(endOffset1-start1+1)) != 50000000l)
                            {
                                System.out.println("++++++++++++++++++++++this should not have happend++++++++++++++++++++++++++++");
                            }
                        }
                    }
                    else{
                        if(sendEOWSignal)
                        {
                            System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ send end of window start and endoffset buffer"+ start + "  " + endOffset  +  "   " + _threadSequence);
                            loadBufferWithEOWSignal1(start, endOffset, 0);
                        }
                        else
                        {
                            System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ no end of window start and endoffset and buffer"+ start + "  " + endOffset +  "   " + _threadSequence);
                            loadBuffer1(start, endOffset, 0);
                            if((long)(endOffset-start+1) != 50000000l)
                            {
                                System.out.println("++++++++++++++++++++++this should not have happend++++++++++++++++++++++++++++");
                            }
                        }
                    }
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
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
            }
            catch(Exception ex)
            {
                System.out.println("Exception Caught with  length ::" + length);
                ex.printStackTrace();
            }

        }
        private void loadBufferWithEOWSignal1(long s, long e, int index) throws IOException {
            fileReader.seek(s);
            long s1 =s;
            long e1 = s + (int)(e-s)/2;
            long s2= e1+1;
            long e2 = e;

            // int length = (int) (e - s + 1);
            int length1 = (int)(e1-s1+1);
            int length2 = (int)(e2-s2+1);

            // System.out.println("Copying " + length + "  bytes into buffer number "+ _threadSequence);
            //   byte[] tempArr = new byte[length1+length2+2];
            byte[] tempArr1 = new byte[length1];
            byte[] tempArr2 = new byte[length2+2];
            try {
                fileReader.read(tempArr1, 0, length1);
                fileReader.read(tempArr2, 0, length2);
                tempArr2[length2] = -1;
                tempArr2[length2+1] = -1;
                System.arraycopy(tempArr1, 0, _bufferList.get(_threadSequence), index, tempArr1.length);
                System.arraycopy(tempArr2, 0, _bufferList.get(_threadSequence), index+tempArr1.length, tempArr2.length);




                //               fileReader.read(tempArr, 0, length);
//
                //            tempArr[length] = 0;
                //              tempArr[length+1] = 0;

//                System.arraycopy(tempArr, 0, _bufferList.get(_threadSequence), index, tempArr.length);

            }
            catch(Exception ex)
            {
                System.out.println("Exception Caught with  length ::" );
                ex.printStackTrace();
            }

        }

        private void loadBuffer1(long s, long e, int index) throws IOException {
            fileReader.seek(s);
            long s1 =s;
            long e1 = s + (int)(e-s)/2;
            long s2= e1+1;
            long e2 = e;

           // int length = (int) (e - s + 1);
            int length1 = (int)(e1-s1+1);
            int length2 = (int)(e2-s2+1);

           // System.out.println("Copying " + length + "  bytes into buffer number "+ _threadSequence);
         //   byte[] tempArr = new byte[length1+length2+2];
            byte[] tempArr1 = new byte[length1];
            byte[] tempArr2 = new byte[length2+2];
            try {
                fileReader.read(tempArr1, 0, length1);
                fileReader.read(tempArr2, 0, length2);
                tempArr2[length2] = 0;
                tempArr2[length2+1] = 0;
                System.arraycopy(tempArr1, 0, _bufferList.get(_threadSequence), index, tempArr1.length);
                System.arraycopy(tempArr2, 0, _bufferList.get(_threadSequence), index+tempArr1.length, tempArr2.length);




 //               fileReader.read(tempArr, 0, length);
//
    //            tempArr[length] = 0;
  //              tempArr[length+1] = 0;

//                System.arraycopy(tempArr, 0, _bufferList.get(_threadSequence), index, tempArr.length);

            }
            catch(Exception ex)
            {
                System.out.println("Exception Caught with  length ::");
                ex.printStackTrace();
            }

        }

        private void loadBuffer(long s, long e, int index) throws IOException {
            fileReader.seek(s);
            int length = (int) (e - s + 1);

            try {
                fileReader.read(_bufferList.get(_threadSequence), index, length);
                _bufferList.get(_threadSequence)[length] = 0;
                _bufferList.get(_threadSequence)[length+1] = 0;
            }
            catch(Exception ex)
            {
                System.out.println("Exception Caught with length ::" + length);
                ex.printStackTrace();
            }
        }
    }

    private class EmitFromMemory extends Thread{
        int currentBuffer;
        int start;
        long counter;

        public EmitFromMemory(int bufferNumber)
        {
            currentBuffer = bufferNumber;
            start =0;

        }

        public void run(){
            int length =0;
            while(true)
            {
                while(ProducerConsumerMap.get(currentBuffer%MaxThread) == -1);
                length = getLength1();
                if(length != tlength) {
                    System.out.println("length of the tuple: " + length);
                }
                if(start + length <= MaxBufferSize)
                {
                    byte[] tempArray = Arrays.copyOfRange(_bufferList.get(currentBuffer), start, start+length);
                    String tupleData = new String(tempArray);
                    if(counter%10000==0)System.out.println("COUNT:"+counter);

                    _collector.emit("dataStream", new Values(tupleData)); //TODO uncomment when putting real system
                    counter++;
                    start = start + length;
                }
                else
                {
                    int partLength = MaxBufferSize - start;
                    byte[] tempArray = new byte[length];  //Arrays.copyOfRange(_bufferList.get(currentBuffer), start, partLength);
                    System.arraycopy(_bufferList.get(currentBuffer), start, tempArray, 0, partLength);
                    String Data = new String(tempArray);
                    //System.out.println("part data::" + Data);
                    System.out.println("******************* reading part from buffer:: ********************" + currentBuffer);
                    ProducerConsumerMap.put(currentBuffer, -1);
                    currentBuffer++;
                    currentBuffer = currentBuffer%MaxThread;
                    start =0;
                    length = length - partLength;
                    while(ProducerConsumerMap.get(currentBuffer%MaxThread) == -1);

                   System.out.println("******************* reading remaining part from buffer:: ********************" + currentBuffer);

                    System.arraycopy(_bufferList.get(currentBuffer), start, tempArray, partLength, length);

                    String tupleData = new String(tempArray);
                    if(counter%10000==0)System.out.println("COUNT:"+counter);
                    _collector.emit("dataStream", new Values(tupleData)); //TODO uncomment when putting real system
                    counter++;
                    start = start + length;
                }
            }
        }
        private int getLength1()
        {
            byte ten;
            byte unit;
            if(start == MaxBufferSize-1)
            {
                ten = _bufferList.get(currentBuffer)[start];
                unit = _bufferList.get(currentBuffer)[start+1];
                int tempLength = getIntFromTwoBytes(ten,unit);
                if(tempLength == -1)
                {
                    sendEndOfWindowSignal();
                    ProducerConsumerMap.put(currentBuffer, -1);
                    currentBuffer++;
                    currentBuffer = currentBuffer%MaxThread;
                    while(ProducerConsumerMap.get(currentBuffer%MaxThread) == -1);

                    start =0;
                    tempLength = getLength1();
                    return tempLength;
                }
                if(tempLength==0)
                {
                    System.out.println("Position of 0s::"+ start + "  &  "+(start+1));
                }

                ProducerConsumerMap.put(currentBuffer, -1);
                currentBuffer++;
                currentBuffer = currentBuffer%MaxThread;
                start =0;
                while(ProducerConsumerMap.get(currentBuffer) == -1);

                unit = _bufferList.get(currentBuffer)[start];
                start = start + 1;
                tempLength = getIntFromTwoBytes(ten,unit);
                return tempLength;
            }

            ten = _bufferList.get(currentBuffer)[start];
            unit = _bufferList.get(currentBuffer)[start+1];
            int tempLength = getIntFromTwoBytes(ten,unit);
            if(tempLength == -1) {
                sendEndOfWindowSignal();
            }
            else if(tempLength == 0)
            {
//                byte[] temp = new byte[1000];
//                System.arraycopy(_bufferList.get(currentBuffer),start-100,temp,0,1000);
                System.out.println("Position of 0s::"+ start + "  &  "+(start+1));
            }
            else
            {
                start = start+2;
                return tempLength;
            }
            ProducerConsumerMap.put(currentBuffer, -1);
            currentBuffer++;
            currentBuffer = currentBuffer%MaxThread;
            while(ProducerConsumerMap.get(currentBuffer) == -1);

            start =0;
            tempLength = getLength1();
            return tempLength;


        }

        public int getLength()
        {
            byte ten;
            byte unit;
            if(start < MaxBufferSize - 1){
                ten = _bufferList.get(currentBuffer)[start];
                unit = _bufferList.get(currentBuffer)[start+1];
                start = start + 2;
                System.out.println("####################### reading length from buffer:: ********************" + currentBuffer);
                int tempLength = getIntFromTwoBytes(ten,unit);

                if(tempLength == -1)
                {
                    System.out.println("####################### Mock tuple sent from ::" + currentBuffer);
                    sendEndOfWindowSignal();
                }
                else
                {
                    if(tempLength != tlength)System.out.println("Length1::"+tempLength + "   from buffer::"+currentBuffer);
                    return tempLength;
                }
                ProducerConsumerMap.put(currentBuffer, -1);
                currentBuffer++;
                currentBuffer = currentBuffer%MaxThread;
                while(ProducerConsumerMap.get(currentBuffer) == -1);
                System.out.println("####################### reading length from buffer after emitting mock:: ********************" + currentBuffer);
                start =0;
                ten = _bufferList.get(currentBuffer)[start];
                unit = _bufferList.get(currentBuffer)[start+1];
                start = start + 2;
                tempLength = getIntFromTwoBytes(ten,unit);

                //tempLength = getLength();
                if(tempLength != tlength)
                    System.out.println("Length2::"+tempLength + "   from buffer::"+currentBuffer);
                return tempLength;
            }
            else if (start < MaxBufferSize){
                ten = _bufferList.get(currentBuffer)[start];
                unit = _bufferList.get(currentBuffer)[start+1];
                int tempLength = getIntFromTwoBytes(ten,unit);
                System.out.println("####################### reading length from buffer:: ********************" + currentBuffer);
                if(tempLength == -1)
                {
                    sendEndOfWindowSignal();
                    ProducerConsumerMap.put(currentBuffer, -1);
                    currentBuffer++;
                    currentBuffer = currentBuffer%MaxThread;
                    while(ProducerConsumerMap.get(currentBuffer%MaxThread) == -1);

                    System.out.println("####################### reading length from buffer after emitting mock:: ********************" + currentBuffer);
                    start =0;
                    ten = _bufferList.get(currentBuffer)[start];
                    unit = _bufferList.get(currentBuffer)[start+1];
                    start = start + 2;
                    tempLength = getIntFromTwoBytes(ten,unit);

//                    tempLength = getLength();
                    if(tempLength != tlength)
                        System.out.println("Length3::"+tempLength);
                    return tempLength;
                }

                ProducerConsumerMap.put(currentBuffer, -1);
                currentBuffer++;
                currentBuffer = currentBuffer%MaxThread;
                start =0;
                while(ProducerConsumerMap.get(currentBuffer) == -1);

                System.out.println("####################### reading length from buffer :: ********************" + currentBuffer);
                unit = _bufferList.get(currentBuffer)[start];
                start = start + 1;
                tempLength = getIntFromTwoBytes(ten,unit);
                if(tempLength != tlength)
                    System.out.println("Length4::"+tempLength);
                return tempLength;

            }
            else{
                ten = _bufferList.get(currentBuffer)[start];
                unit = _bufferList.get(currentBuffer)[start+1];
                int tempLength = getIntFromTwoBytes(ten,unit);
                if(tempLength == -1)
                {
                    sendEndOfWindowSignal();
                    System.out.println("####################### reading mock from buffer and sending -1-1 mock:: ********************" + currentBuffer);
                }

                ProducerConsumerMap.put(currentBuffer, -1);
                currentBuffer++;
                currentBuffer = currentBuffer%MaxThread;
                while(ProducerConsumerMap.get(currentBuffer%MaxThread) == -1);
                System.out.println("####################### reading length from buffer:: ********************" + currentBuffer);

                start = 0;
                ten = _bufferList.get(currentBuffer)[start];
                unit = _bufferList.get(currentBuffer)[start+1];
                start = start + 2;
                tempLength = getIntFromTwoBytes(ten,unit);

//                tempLength =getLength();
                if(tempLength != tlength)
                    System.out.println("Length5::"+tempLength);
                return tempLength;
            }
        }
    }
}