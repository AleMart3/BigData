package job3;
import java.io.IOException;
import java.util.ArrayList;

import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import utilsJob3.ThirdMapperCustomValues;

public class ThirdReducer extends Reducer<Text, ThirdMapperCustomValues, Text, Text> {

	public void reduce(Text key, Iterable<ThirdMapperCustomValues> values, Context context) throws IOException, InterruptedException {
		
		List<ThirdMapperCustomValues> list = iterableToCollection(values);

		String[] valori = key.toString().split("[,]");
		String a2016 = valori[0];
		String a2017 = valori[1];
		String a2018 = valori[2]; 



			for (int i = 0; i <list.size(); i++) {
			for (int j = i+1 ; j <list.size(); j++) {
				if (!(list.get(i).getSettore().equals(list.get(j).getSettore())) 
						&& !(list.get(i).getNome().equals(list.get(j).getNome()))){
					context.write(new Text(list.get(i).getNome()+ ", " + list.get(j).getNome() + ","), 
							new Text("Anno 2016: " + a2016 + "%, Anno 2017:" + a2017 + "%, Anno 2018:"+ a2018+ "%"));
				}
			

			}}}


	public List<ThirdMapperCustomValues> iterableToCollection(Iterable<ThirdMapperCustomValues> iterable)
	{
		List<ThirdMapperCustomValues> collection = new ArrayList<ThirdMapperCustomValues>();
		for (ThirdMapperCustomValues e : iterable) {
			ThirdMapperCustomValues cv = new ThirdMapperCustomValues(e.getNome(),e.getSettore());
			collection.add(cv);
		}
		return collection;
	}


}

