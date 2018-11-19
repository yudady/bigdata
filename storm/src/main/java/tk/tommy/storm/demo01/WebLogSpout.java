package tk.tommy.storm.demo01;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import tk.tommy.storm.tools.LogUtils;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Map;

/** 创建spout */
public class WebLogSpout implements IRichSpout {
    public static Logger logger = LogUtils.spout;

    private static final long serialVersionUID = 1L;
    private static final String filename = "c:/tmp/website.log";

    private BufferedReader br;
    private SpoutOutputCollector collector = null;
    private String str = null;

    @Override
    public void nextTuple() {
        // 循环调用的方法
        try {
            while ((str = this.br.readLine()) != null) {
                logger.info("IRichSpout : " + str);
                logger.info("IRichSpout : " + str);
                logger.info("IRichSpout : " + str);
                logger.info("IRichSpout : " + str);
                logger.info("IRichSpout : " + str);
                logger.warn("IRichSpout : " + str);
                logger.warn("IRichSpout : " + str);
                logger.warn("IRichSpout : " + str);
                logger.warn("IRichSpout : " + str);
                logger.warn("IRichSpout : " + str);
                logger.warn("IRichSpout : " + str);
                logger.warn("IRichSpout : " + str);
                logger.info("IRichSpout : " + str);
                logger.info("IRichSpout : " + str);
                logger.info("IRichSpout : " + str);
                logger.info("IRichSpout : " + str);
                logger.info("IRichSpout : " + str);
                logger.error("IRichSpout : " + str);
                logger.info("IRichSpout : " + str);
                logger.info("IRichSpout : " + str);
                logger.info("IRichSpout : " + str);
                logger.info("IRichSpout : " + str);
                logger.info("IRichSpout : " + str);
                logger.info("IRichSpout : " + str);
                logger.info("IRichSpout : " + str);
                // 发射出去
                collector.emit(new Values(str));

                //				Thread.sleep(3000);
            }
        } catch (Exception e) {

        }
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        // 打开输入的文件
        try {
            this.collector = collector;
            this.br =
                    new BufferedReader(
                            new InputStreamReader(new FileInputStream(filename), "UTF-8"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // 声明输出字段类型
        declarer.declare(new Fields("log"));
    }

    @Override
    public void ack(Object arg0) {}

    @Override
    public void activate() {}

    @Override
    public void close() {}

    @Override
    public void deactivate() {}

    @Override
    public void fail(Object arg0) {}

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
