package callDATA;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerCall extends Reducer<Text, LongWritable, Text, LongWritable> {

	public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
		
		Long sum=0L;

		for (LongWritable value : values) {
			sum+=value.get();
		}
		if (sum>=65)
		context.write(key, new LongWritable(sum));

		
	}
}