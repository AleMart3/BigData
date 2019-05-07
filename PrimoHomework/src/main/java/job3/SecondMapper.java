package job3;
import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;




public class SecondMapper extends Mapper <LongWritable, Text, CustomKey, CustomValues> {
	
	private Map<Text, List<CustomValues>> map = new HashMap<Text, List<CustomValues>>();

	public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {

		String[] line = value.toString().split("[,]");

		String nome = line[CostantiSecondoMapper.nome].trim();
		String settore = line[CostantiSecondoMapper.settore].trim();
		String data = line[CostantiSecondoMapper.data].trim(); 
		double close = Double.parseDouble(line[CostantiSecondoMapper.close].trim());

		DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd");
		LocalDate date = LocalDate.parse(data, format);
		
		/*context.write(new CustomKey(new Text(nome), new Text(settore), new IntWritable(date.getYear())), 
				new CustomValues(new Text(data), new DoubleWritable(close)));*/
		map.put(new Text(nome+" "+settore),  )

	}
}
