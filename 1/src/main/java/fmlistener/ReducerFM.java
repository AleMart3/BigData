package fmlistener;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerFM extends Reducer<Text, Text, Text, IntWritable> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		Set<Text> userId = new HashSet<Text>();
		

		for (Text value : values) {
			userId.add(value);
			
		}
		context.write(key, new IntWritable(userId.size()));

		
	}
}