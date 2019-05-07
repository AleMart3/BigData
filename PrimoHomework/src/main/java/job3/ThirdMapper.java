package job3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import utilsJob3.CustomValues;
import utilsJob3.ThirdMapperCustomValues;

public class ThirdMapper extends Mapper <LongWritable, Text, Text, ThirdMapperCustomValues> {
	
	private Map<ThirdMapperCustomValues, List<CustomValues>> map = new HashMap<ThirdMapperCustomValues, List<CustomValues>>();
	
	public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
	
		String[] line = value.toString().split("[,]");
		String nome= line[0].trim();
		String settore= line[1].trim();
		String anno= line[2].trim();
		double diff= Double.parseDouble(line[3].trim());
		
		aggiungiElemento(new ThirdMapperCustomValues(new Text(nome), new Text(settore)), 
				new CustomValues(new Text(anno), new DoubleWritable(diff)));
		
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {	
		for (ThirdMapperCustomValues key : map.keySet()){
			DoubleWritable anno16 = null;  
			DoubleWritable anno17 = null; 
			DoubleWritable anno18 = null;
			
			for (CustomValues el : map.get(key)){
			
				switch(el.getData().toString()) {
				case "2016": anno16 = el.getClose();
				case "2017": anno17 = el.getClose();
				case "2018": anno18 = el.getClose();
				}
			}
			
			if (anno16!= null && anno17!=null && anno18!=null){
				context.write(new Text(anno16.toString() + ", " + anno17.toString() + ", " + anno18.toString()), 
						key);
			}
		}
		
	}
	
	
	private void aggiungiElemento(ThirdMapperCustomValues key, CustomValues cv ){
		if (map.containsKey(key))
			map.get(key).add(cv);
		else {
			List<CustomValues> list = new ArrayList<CustomValues>(); 
			list.add(cv); 
			map.put(key, list);
		}
	}

}
