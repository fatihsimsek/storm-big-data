import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.SpoutSpec;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class WordCountTopology {

    public static void main(String args[]) {

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("word-reader", new WordReaderSpout());
        builder.setBolt("word-counter", new WordCountBolt(), 1).shuffleGrouping("word-reader");
        Config conf = new Config();
        conf.setDebug(true);
        LocalCluster localCluster = new LocalCluster();
        StormTopology currentTopology = builder.createTopology();
        localCluster.submitTopology("wordcounter-topology", conf, currentTopology);

        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        localCluster.shutdown();
    }

    public static void whileTimeout(long timeoutMs, Condition condition, Runnable body) {
        long endTime = System.currentTimeMillis() + timeoutMs;
        int count = 0;
        while (condition.exec()) {
            count++;
            if (System.currentTimeMillis() > endTime) {
                throw new AssertionError("Test timed out (" + timeoutMs + "ms) " + condition);
            }
            body.run();
        }
    }

    public static <T> boolean isEvery(Collection<T> data, Predicate<T> pred) {
        return data.stream().allMatch(pred);
    }
}


