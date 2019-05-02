package meanMAX;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducermean extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

		Double sum = 0.0;

		int cont=0;
		for (DoubleWritable value : values) {
			sum += value.get();
			cont++;
		}

		context.write(key, new DoubleWritable(sum/cont));
	}
}