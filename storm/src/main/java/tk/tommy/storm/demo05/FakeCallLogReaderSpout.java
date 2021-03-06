package tk.tommy.storm.demo05;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * https://www.w3cschool.cn/apache_storm/apache_storm_working_example.html
 *
 * FakeCallLogReaderSpout 在我们的场景中，我们需要收集呼叫日志详细信息。呼叫日志的信息包含。
 *
 * <p>主叫号码 接收号码 持续时间 由于我们没有呼叫日志的实时信息，我们将生成假呼叫日志。假信息将使用Random类创建。完整的程序代码如下。
 */
public class FakeCallLogReaderSpout implements IRichSpout {
    // Create instance for SpoutOutputCollector which passes tuples to bolt.
    private SpoutOutputCollector collector;
    private boolean completed = false;

    // Create instance for TopologyContext which contains topology data.
    private TopologyContext context;

    // Create instance for Random class.
    private Random randomGenerator = new Random();
    private Integer idx = 0;


    /**
     * 为Spout提供执行环境。执行器将运行此方法来初始化喷头。
     * @param conf
     * @param context
     * @param collector
     */
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.context = context;
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        if (this.idx <= 1000) {
            List<String> mobileNumbers = new ArrayList<String>();
            mobileNumbers.add("1234123401");
            mobileNumbers.add("1234123402");
            mobileNumbers.add("1234123403");
            mobileNumbers.add("1234123404");

            Integer localIdx = 0;
            while (localIdx++ < 100 && this.idx++ < 1000) {
                String fromMobileNumber = mobileNumbers.get(randomGenerator.nextInt(4));
                String toMobileNumber = mobileNumbers.get(randomGenerator.nextInt(4));

                while (fromMobileNumber == toMobileNumber) {
                    toMobileNumber = mobileNumbers.get(randomGenerator.nextInt(4));
                }

                Integer duration = randomGenerator.nextInt(60);
                this.collector.emit(new Values(fromMobileNumber, toMobileNumber, duration));
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("from", "to", "duration"));
    }

    // Override all the interface methods
    @Override
    public void close() {}

    public boolean isDistributed() {
        return false;
    }

    @Override
    public void activate() {}

    @Override
    public void deactivate() {}

    @Override
    public void ack(Object msgId) {}

    @Override
    public void fail(Object msgId) {}

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
