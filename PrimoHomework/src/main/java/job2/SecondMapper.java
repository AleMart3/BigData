package job2;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import utilsJob2.Costanti_SecondMapper;
import utilsJob2.CustomKey2;
import utilsJob2.CustomValues;


public class SecondMapper extends Mapper <LongWritable, Text, CustomKey2, CustomValues>{


	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

		String[] line = value.toString().split("[,]");

		String ticker = line[Costanti_SecondMapper.ticker];
		String settore= line[Costanti_SecondMapper.settore];
		String data= line[Costanti_SecondMapper.data];
		Double close= Double.parseDouble(line[Costanti_SecondMapper.close]);
		Double volume= Double.parseDouble(line[Costanti_SecondMapper.volume]);


		DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    	

		try {
			LocalDate date = LocalDate.parse(data, format);
			int year= date.getYear();
			context.write(new CustomKey2(new Text(settore),new IntWritable(year), new Text(ticker)),new CustomValues(new Text(data),
			new DoubleWritable(close),new DoubleWritable(volume)));

		}catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		

	}
}
