package utilsJob2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CustomKey2 implements WritableComparable<CustomKey2>{

	private Text settore;
	private IntWritable anno;
	private Text ticker;

	public Text getTicker() {
		return ticker;
	}

	public void setTicker(Text ticker) {
		this.ticker = ticker;
	}

	public CustomKey2() {
	}

	public CustomKey2(Text settore, IntWritable anno, Text ticker) {
		this.settore = settore;
		this.anno = anno;
		this.ticker = ticker; 
	}

	
	public CustomKey2(Text settore, IntWritable anno) {
		this.settore = settore;
		this.anno = anno;

	}


	public Text getSettore() {
		return settore;
	}

	public void setSettore(Text settore) {
		this.settore = settore;
	}

	public IntWritable getAnno() {
		return anno;
	}

	public void setAnno(IntWritable anno) {
		this.anno = anno;
	}


	@Override
	public int hashCode() {
		return settore.hashCode() + anno.hashCode() + ticker.hashCode();
	}

	@Override
	public String toString() {
		return this.settore.toString() + " " + this.anno.toString() + " " + this.ticker.toString();
	}


	@Override
	public boolean equals(Object o){
		CustomKey2 a= (CustomKey2)o;
		return this.settore.equals(a.getSettore()) && this.anno.equals(a.getAnno()) && this.ticker.equals(a.getTicker());
	}

	public int compareTo(CustomKey2 that) {
		int cmp = this.settore.hashCode()-that.settore.hashCode();
		if(cmp==0) {
			cmp= this.anno.hashCode()-that.getAnno().hashCode();
			if (cmp == 0)
				cmp = this.ticker.hashCode()-that.getTicker().hashCode();
		}	
		return cmp;
	}

	public void readFields(DataInput in) throws IOException {
		this.settore = new Text(in.readUTF());
		this.anno = new IntWritable(in.readInt());
		this.ticker = new Text(in.readUTF());
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.settore.toString());
		out.writeInt(this.anno.get());
		out.writeUTF(this.ticker.toString());
	}




}
