package job3;
import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import utilsJob3.CostantiSecondoMapper;
import utilsJob3.CustomKey;
import utilsJob3.CustomValues;


public class SecondMapper extends Mapper <LongWritable, Text, CustomKey, CustomValues> {

	public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {

		String[] line = value.toString().split("[,]");

		String nome = line[CostantiSecondoMapper.nome];
		String settore = line[CostantiSecondoMapper.settore];
		String data = line[CostantiSecondoMapper.data]; 
		double close = Double.parseDouble(line[CostantiSecondoMapper.close]);

		DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd");
		LocalDate date = LocalDate.parse(data, format);
		
		context.write(new CustomKey(new Text(nome), new Text(settore), new IntWritable(date.getYear())), 
				new CustomValues(new Text(data), new DoubleWritable(close)));

	}
}
