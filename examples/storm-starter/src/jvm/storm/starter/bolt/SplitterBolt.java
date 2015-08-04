package storm.starter.bolt;

/**
 * Created by harini on 8/3/2015.
 */

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.log4j.Logger;
import storm.starter.Interfaces.IWindowBolt;

import java.util.Map;

/**
 * Created by Harini on 8/3/15.
 */
public class SplitterBolt extends BaseRichBolt implements IWindowBolt {
    final static Logger LOG = Logger.getLogger(MovingAverageBolt.class.getName());
    OutputCollector _collector;
    String tweet;

    public SplitterBolt()
    {
        tweet = null;
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;

    }

    @Override
    public void execute(Tuple tuple) {
        tweet = tuple.getString(0);
        String[] tweetSplit = tweet.split("\t");

        if(tweetSplit.length >= 3)
        {
            _collector.emit(new Values(tweetSplit[2]));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet"));
    }

    public boolean isMockTick(Tuple tuple) {
        return tuple.getSourceStreamId().equals("mockTickTuple");
    }
}
