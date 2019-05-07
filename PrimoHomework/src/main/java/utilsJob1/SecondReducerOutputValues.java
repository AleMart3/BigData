package utilsJob1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SecondReducerOutputValues implements WritableComparable<SecondReducerOutputValues>{

	private DoubleWritable differenzaChiusura;
	private DoubleWritable minLow;
	private DoubleWritable maxHigh;
	private DoubleWritable mediaVolumi;
	private DoubleWritable incrementoPercentuale; 

	public SecondReducerOutputValues() {
	}


	public SecondReducerOutputValues(DoubleWritable differenzaChiusura, DoubleWritable minLow, 
			DoubleWritable maxHigh,DoubleWritable mediaVolumi, DoubleWritable incrementoPercentuale) {
		
		this.differenzaChiusura = differenzaChiusura;
		this.minLow = minLow;
		this.maxHigh = maxHigh;
		this.mediaVolumi = mediaVolumi;
		this.incrementoPercentuale = incrementoPercentuale;
	}



	public DoubleWritable getDifferenzaChiusura() {
		return differenzaChiusura;
	}


	public void setDifferenzaChiusura(DoubleWritable differenzaChiusura) {
		this.differenzaChiusura = differenzaChiusura;
	}


	public DoubleWritable getMinLow() {
		return minLow;
	}


	public void setMinLow(DoubleWritable minLow) {
		this.minLow = minLow;
	}


	public DoubleWritable getMaxHigh() {
		return maxHigh;
	}


	public void setMaxHigh(DoubleWritable maxHigh) {
		this.maxHigh = maxHigh;
	}


	public DoubleWritable getMediaVolumi() {
		return mediaVolumi;
	}


	public void setMediaVolumi(DoubleWritable mediaVolumi) {
		this.mediaVolumi = mediaVolumi;
	}


	public DoubleWritable getIncrementoPercentuale() {
		return incrementoPercentuale;
	}


	public void setIncrementoPercentuale(DoubleWritable incrementoPercentuale) {
		this.incrementoPercentuale = incrementoPercentuale;
	}


	@Override
	public int hashCode() {
		return this.differenzaChiusura.hashCode() + this.minLow.hashCode() + 
				+ this.maxHigh.hashCode() + this.mediaVolumi.hashCode() + this.incrementoPercentuale.hashCode();

	}

	

	@Override
	public String toString() {
		return "[differenzaChiusura=" + differenzaChiusura + ", minLow=" + minLow
				+ ", maxHigh=" + maxHigh + ", mediaVolumi=" + mediaVolumi + ", incrementoPercentuale="
				+ incrementoPercentuale + "]";
	}


	@Override
	public boolean equals(Object o){
		SecondReducerOutputValues a= (SecondReducerOutputValues)o;
		return this.differenzaChiusura.equals(a.getDifferenzaChiusura()) &&
			   this.minLow.equals(a.getMinLow()) && this.maxHigh.equals(a.getMaxHigh()) &&
			   this.mediaVolumi.equals(a.getMediaVolumi()) && this.incrementoPercentuale.equals(a.getIncrementoPercentuale());

	}

	@Override
	public int compareTo(SecondReducerOutputValues that) {
		return 0;
	}

	public void readFields(DataInput in) throws IOException {
		this.differenzaChiusura = new DoubleWritable(in.readDouble());
		this.minLow = new DoubleWritable(in.readDouble());
		this.maxHigh = new DoubleWritable(in.readDouble());
		this.mediaVolumi = new DoubleWritable(in.readDouble());
		this.incrementoPercentuale = new DoubleWritable(in.readDouble());
		
	}

	public void write(DataOutput out) throws IOException {
		out.writeDouble(this.differenzaChiusura.get());
		out.writeDouble(this.minLow.get());
		out.writeDouble(this.maxHigh.get());
		out.writeDouble(this.mediaVolumi.get());
		out.writeDouble(this.incrementoPercentuale.get());

	}


	




}
