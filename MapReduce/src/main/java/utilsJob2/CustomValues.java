package utilsJob2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CustomValues implements WritableComparable<CustomValues>{
	
	private Text data;	
	private DoubleWritable close;
	private DoubleWritable volume;
	
	
	
	public CustomValues() {
	}
	
	public CustomValues(Text data,DoubleWritable close, DoubleWritable volume) {
		 this.data=data;
		 this.close = close;
		 this.volume = volume;
		 
		 }
	
	public Text getData() {
		return data;
	}

	public void setData(Text data) {
		this.data = data;
	}
	
	public DoubleWritable getClose() {
		return close;
	}

	public void setClose(DoubleWritable close) {
		this.close = close;
	}
	
	public DoubleWritable getVolume() {
		return volume;
	}

	public void setVolume(DoubleWritable volume) {
		this.volume = volume;
	}

	
	public int getAnno() {
		return Integer.parseInt(this.data.toString().substring(0, 4));
	}
	
	public int getMese() {
		return Integer.parseInt(this.data.toString().substring(5, 7));
	}
	public int getGiorno() {
		return Integer.parseInt(this.data.toString().substring(8));
	}
	
	
	@Override
	public int hashCode() {
		return data.hashCode()+ volume.hashCode() + close.hashCode();
	}
	
	@Override
	 public String toString() {
	 return this.data.toString()+ " " +this.close.toString() + " " + this.volume.toString();
	 }


	@Override
	public boolean equals(Object o){
		CustomValues a= (CustomValues)o;
		return this.data.equals(a.getData()) && this.volume.equals(a.getVolume()) && this.close.equals(a.getClose());
	}
	
	@Override
	public int compareTo(CustomValues that) {
		int cmp = this.volume.hashCode()-that.volume.hashCode();
		if(cmp==0) {
			cmp= this.close.hashCode()-that.close.hashCode();
			if(cmp==0)
				cmp= this.data.hashCode()-that.data.hashCode();
		}
			
		return cmp;
	}

	public void readFields(DataInput in) throws IOException {
		this.data= new Text(in.readUTF());
		this.volume = new DoubleWritable(in.readDouble());
		this.close = new DoubleWritable(in.readDouble());
	}
	
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.data.toString());
		out.writeDouble(this.volume.get());
		out.writeDouble(this.close.get());
		
	}

	
}
