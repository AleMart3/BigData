package job1;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class SecondReducer extends Reducer<Text, Text, Text, FirstReducerOutputValues> {



	public void reduce(Text key, Iterable<FirstReducerOutputValues> values, Context context) throws IOException, InterruptedException {
		System.out.println("reduce");
	}
	
}