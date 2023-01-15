import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

public class WordCountBolt implements IRichBolt {

    private static final long serialVersionUID = -4130092930769665618L;
    Map<String, Integer> counters;
    Integer id;
    String name;
    String fileName;

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.counters = new HashMap();
        this.name = context.getThisComponentId();
        this.id = context.getThisTaskId();

    }

    public void execute(Tuple input) {

        String word = input.getStringByField("word");
        if (!counters.containsKey(word)) {
            counters.put(word, 1);
        } else {
            counters.put(word, counters.get(word) + 1);
        }
    }

    public void cleanup() {
        System.out.println("Result:");
        for (Map.Entry<String, Integer> entry: counters.entrySet().stream()
                                                       .sorted(Comparator.comparing(Map.Entry::getValue))
                                                       .collect(Collectors.toList())) {
            System.out.println(entry.getKey() + "-" + entry.getValue());
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    public Map<String, Object > getComponentConfiguration() {
        return null;
    }

}
