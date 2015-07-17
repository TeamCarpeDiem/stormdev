package storm.starter.spout;

/**
 * Created by Pradheep on 6/2/15.
 */

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.Random;

public class RandomMessageSpout extends BaseRichSpout {
    int count = 0;
    final static Logger LOG = Logger.getLogger(RandomMessageSpout.class.getName());
    SpoutOutputCollector _collector;
    Random _rand;
    boolean isFirst = true;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        _rand = new Random();
    }

    @Override
    public void nextTuple() {

        //code to prevent large number of tuples sent during the first second due to the issue of
        //tick tuple being started after all the spout instances are started.
        if(isFirst)
        {
            Utils.sleep(3000);
            isFirst = false;
        }

        //sleep between the calls
        Utils.sleep(2);

        //code to simulate differnt tuple arrival rate
       /* count++;
        if(count < 500)
        {
            Utils.sleep(1);
        }
        if(count >= 500 && count < 1000)
        {
            if(count % 2 == 0)
                Utils.sleep(100);
            else
                Utils.sleep(50);
        }
        if(count >= 1000 && count < 1500)
        {
            if(count % 2 == 0)
                Utils.sleep(3);
            else
                Utils.sleep(1);
        }
        if(count == 1500)
        {
            Utils.sleep(10);
            count = 0;
        }*/

        //code to simulate to send to mixed sixe size tuples randomly
        String str[] = new String[10];
        //20 bytes message
        str[0] = "sample small message.";
        //2Kb message
        str[1] = "This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.This is a 2kb message.";
        //3KB message
        str[2] = "This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.This is a 3Kb message.";
        //4 kb message
        str[3] = "This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.This is a 4KB message.";

        //Generate randon index everytime to select a message of different size.
        int strIndex = _rand.nextInt(4);
        _collector.emit(new Values(str[strIndex]));
    }

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("RandomInt"));
    }

}
