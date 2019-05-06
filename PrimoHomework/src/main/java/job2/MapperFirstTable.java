package job2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperFirstTable extends Mapper <LongWritable, Text, Text, Text>
{
	public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
		if(key.get()==0) return;

		String[] line = value.toString().split("[,]");
		String settore = line[line.length-1];

		context.write(new Text(line[Costanti_FirstTable.ticker]), new Text("firstTable\t" + line[line.length - 2 ]));

	}

}