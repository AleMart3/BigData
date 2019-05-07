package job3;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ThirdMapper extends Mapper <LongWritable, Text, Text, ThirdMapperCustomValues> {
	
	private Map<Text, List<CustomValues>> map = new HashMap<Text, List<CustomValues>>();
	
	public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
	
		String[] line = value.toString().split("[,]");
		String nome= line[0];
		String settore= line[1];
		String anno= line[2];
		String diff= line[3];
		
		
		// ciao
		//context.write(new Text(anno+":"+diff+"%"), new ThirdMapperCustomValues(new Text(nome), new Text(settore)));

	}


}
