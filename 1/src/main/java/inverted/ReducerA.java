package inverted;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerA extends Reducer<Text, IntWritable, Text, Set<IntWritable>> {

	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

		Set<IntWritable> set = new HashSet<IntWritable>();
		
		//non so perchè se non faccio new Int quando faccio add aggiunge il valore e sovrascrive quelli prima
		
		for (IntWritable value : values) {
			int i = value.get();
			set.add(new IntWritable(i));
			
		}
		
		context.write(key, set);
		
	}
}