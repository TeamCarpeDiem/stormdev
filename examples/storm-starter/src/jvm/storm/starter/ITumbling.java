package storm.starter;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by Pradheep on 6/9/15.
 */
public interface ITumbling extends Serializable{
    void prepare(Map stormConf, TopologyContext context, OutputCollector collector);

    void execute(Tuple input);

    void declareOutputFields(OutputFieldsDeclarer declarer);

}
