package storm.starter;

import java.io.Serializable;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import java.util.Map;
import backtype.storm.task.*;

/**
 * Created by Pradheep on 6/9/15.
 */
public interface ITumbling extends Serializable{
    void prepare(Map stormConf, TopologyContext context, OutputCollector collector);

    void execute(Tuple input);

    void declareOutputFields(OutputFieldsDeclarer declarer);
    //void cleanup();

}
