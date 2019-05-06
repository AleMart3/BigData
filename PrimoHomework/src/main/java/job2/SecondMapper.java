package job2;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class SecondMapper extends Mapper <LongWritable, Text, CustomKey, CustomValues>{


	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

		String[] line = value.toString().split("[,]");

		String settore= line[Costanti_SecondMapper.settore];
		String data= line[Costanti_SecondMapper.data];
		Double close= Double.parseDouble(line[Costanti_SecondMapper.close]);
		Double volume= Double.parseDouble(line[Costanti_SecondMapper.volume]);


		DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    	

		try {
			LocalDate date = LocalDate.parse(data, format);
			int year= date.getYear();
			context.write(new CustomKey(new Text(settore),new IntWritable(year)),new CustomValues(new Text(data),
			new DoubleWritable(close),new DoubleWritable(volume)));

		}catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		

	}
}
