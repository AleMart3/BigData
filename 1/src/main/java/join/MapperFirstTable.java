package join;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperFirstTable extends Mapper <LongWritable, Text, Text, Text>
{
	public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
		String record = value.toString();
		String[] parts = record.split(",");
		context.write(new Text(parts[0]), new Text("firstTable\t" + parts[1]));
	}
}