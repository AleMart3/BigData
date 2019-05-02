package temperature;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class ReducerT extends Reducer<Text, IntWritable, Text, IntWritable> {

	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

		Integer max=null;
		
		
		for(IntWritable i: values) {
			if(i.get()>max) {
				max=i.get();
			}
			

		context.write(key, new IntWritable(max));
		}
	}
}


