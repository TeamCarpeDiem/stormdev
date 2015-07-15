package storm.starter;

import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import storm.starter.Interfaces.IWindowBolt;
import storm.starter.bolt.BaseWindowBolt;

/**
 * Created by Pradheep on 6/18/15.
 */

//This is a class extended from TopologyBuilder to enforce the check if the userbolt has implemented the interface
    //IWindowBolt which contains the isMockTick function.
public class WindowTopologyBuilder extends TopologyBuilder {

    /**
     * This functions checks the bolts created by extending BaseWindowBolt if it has implemented the
     * IWindowBolt interface which contains the isMockTick function.
     * @param id
     * @param bolt
     * @param parallelism_hint
     * @return BoltDeclarer
     * @throws Exception
     */
    public BoltDeclarer setBolt(String id, BaseWindowBolt bolt, Number parallelism_hint) throws Exception {

        Class<?> x = bolt.getClass();
        BoltDeclarer obj = null;
        if (bolt instanceof IWindowBolt){
            //call the base class function if the bolt has implemented the IWindowBolt interface.
            obj = super.setBolt(id, bolt, parallelism_hint);
        }
        else throw new Exception(x + " did not implement IWindowBolt Interface");
        return obj;
    }

    /**
     * This functions checks the bolts created by extending BaseRichBolt if it has implemented the
     * IWindowBolt interface which contains the isMockTick function.
     * @param id
     * @param bolt
     * @param parallelism_hint
     * @return BoltDeclarer
     * @throws Exception
     */
    public BoltDeclarer setBolt(String id, BaseRichBolt bolt, Number parallelism_hint) throws Exception {

        Class<?> x = bolt.getClass();
        BoltDeclarer obj = null;
        if (bolt instanceof IWindowBolt){
            //call the base class function if the bolt has implemented the IWindowBolt interface.
            obj = super.setBolt(id, bolt, parallelism_hint);
        }
        else throw new Exception(x + " did not implement IWindowBolt Interface");
        return obj;
    }

    public BoltDeclarer setBolt(String id, BaseBasicBolt bolt, Number parallelism_hint) throws Exception {

        Class<?> x = bolt.getClass();
        BoltDeclarer obj = null;
        if (bolt instanceof IWindowBolt){
            obj = super.setBolt(id, bolt, parallelism_hint);
        }
        else throw new Exception(x + " did not implement IWindow Interface");
        return obj;
    }
}