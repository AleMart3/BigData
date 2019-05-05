package job2;

import org.apache.hadoop.io.DoubleWritable;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CustomValues implements WritableComparable<CustomValues>{
	
	private DoubleWritable close;
	private DoubleWritable volume;
	
	
	public CustomValues() {
	}
	
	public CustomValues(DoubleWritable close, DoubleWritable volume) {
		 this.close = close;
		 this.volume = volume;
		 
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

	

	@Override
	public int hashCode() {
		return volume.hashCode() + close.hashCode();
	}
	
	@Override
	 public String toString() {
	 return this.close.toString() + " " + this.volume.toString();
	 }


	@Override
	public boolean equals(Object o){
		CustomValues a= (CustomValues)o;
		return this.volume.equals(a.getVolume()) && this.close.equals(a.getClose());
	}
	
	@Override
	public int compareTo(CustomValues that) {
		int cmp = this.volume.hashCode()-that.volume.hashCode();
		if(cmp==0)
			cmp= this.close.hashCode()-that.close.hashCode();
		return cmp;
	}

	public void readFields(DataInput in) throws IOException {
		this.volume = new DoubleWritable(in.readDouble());
		this.close = new DoubleWritable(in.readDouble());
	}
	
	public void write(DataOutput out) throws IOException {
		out.writeDouble(this.volume.get());
		out.writeDouble(this.close.get());
		
	}

	
}
