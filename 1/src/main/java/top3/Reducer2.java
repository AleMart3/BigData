package top3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer2 extends Reducer<CustomKey, Text, Text, IntWritable> {
	

	int cont=0;
	
	public void reduce(CustomKey key, Iterable<Text> values , Context context) throws IOException, InterruptedException {

		if(cont<3) {
		context.write(key.getText(), key.getNum());
		cont++;
		}
	}
}
