package average;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerA extends Reducer<Text, IntWritable, Text, DoubleWritable> {

	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

		Double sum = 0.0;
		Double cont=0.0;

		for (IntWritable value : values) {
			sum += value.get();
			cont++;
		}
		
		context.write(key, new DoubleWritable(sum/cont));
	}
}