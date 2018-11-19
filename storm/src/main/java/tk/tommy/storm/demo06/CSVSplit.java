package tk.tommy.storm.demo06;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

public class CSVSplit extends BaseFunction {
   @Override
   public void execute(TridentTuple tuple, TridentCollector collector) {
      for(String word: tuple.getString(0).split(",")) {
         if(word.length() > 0) {
            collector.emit(new Values(word));
         }
      }
   }
}