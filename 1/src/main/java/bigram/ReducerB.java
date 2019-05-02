package bigram;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class ReducerB extends Reducer<Bigramma, IntWritable, Text, IntWritable> {

	public void reduce(Bigramma bigramma, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
	
		int sum=0;
		for(IntWritable i:values) {
			sum+=i.get();
			
		}
		
		context.write(new Text(bigramma.toString()), new IntWritable(sum));
		
	}

}
