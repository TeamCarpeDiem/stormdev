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

public class RandomIntegerSpout extends BaseRichSpout {
    final static Logger LOG = Logger.getLogger(RandomIntegerSpout.class.getName());
    SpoutOutputCollector _collector;
    Random _rand;


    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        _rand = new Random();
    }

    @Override
    public void nextTuple() {
        Utils.sleep(1);
        int randomNum = _rand.nextInt((9 - 0) + 1) + 0;
        //int randomNum = 1;
        _collector.emit(new Values(randomNum));
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
