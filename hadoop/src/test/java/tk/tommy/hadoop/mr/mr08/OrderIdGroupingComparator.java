package tk.tommy.hadoop.mr.mr08;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderIdGroupingComparator extends WritableComparator {
	
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		
		return super.compare(a, b);
	}
	
	

}
