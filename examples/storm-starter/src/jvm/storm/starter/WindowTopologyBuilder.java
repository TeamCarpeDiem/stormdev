package storm.starter;

import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import storm.starter.Interfaces.IWindowBolt;
import storm.starter.bolt.BaseWindowBolt1;


/**
 * Created by Pradheep on 6/18/15.
 */


public class WindowTopologyBuilder extends TopologyBuilder {


    public BoltDeclarer setBolt(String id, BaseWindowBolt1 bolt, Number parallelism_hint) throws Exception {

        Class<?> x = bolt.getClass();
        BoltDeclarer obj = null;
        if (bolt instanceof IWindowBolt){
            obj = super.setBolt(id, bolt, parallelism_hint);
        }
        else throw new Exception(x + " did not implement IWindow Interface");
        return obj;
    }

    public BoltDeclarer setBolt(String id, BaseRichBolt bolt, Number parallelism_hint) throws Exception {

        Class<?> x = bolt.getClass();
        BoltDeclarer obj = null;
        if (bolt instanceof IWindowBolt){
            obj = super.setBolt(id, bolt, parallelism_hint);
        }
        else throw new Exception(x + " did not implement IWindow Interface");
        return obj;
    }
}