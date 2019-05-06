package job2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CustomKey implements WritableComparable<CustomKey>{
	
	private Text settore;
	private IntWritable anno;
	
	public CustomKey() {
	}
	
	public CustomKey(Text settore, IntWritable anno) {
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
		return settore.hashCode() + anno.hashCode();
	}
	
	@Override
	 public String toString() {
	 return this.settore.toString() + " " + this.anno.toString();
	 }


	@Override
	public boolean equals(Object o){
		CustomKey a= (CustomKey)o;
		return this.settore.equals(a.getSettore()) && this.anno.equals(a.getAnno());
	}
	
	public int compareTo(CustomKey that) {
		int cmp = this.settore.hashCode()-that.settore.hashCode();
		if(cmp==0)
			cmp= this.anno.hashCode()-that.getAnno().hashCode();
		return cmp;
	}

	public void readFields(DataInput in) throws IOException {
		this.settore = new Text(in.readUTF());
		this.anno = new IntWritable(in.readInt());
	}
	
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.settore.toString());
		out.writeInt(this.anno.get());
	}
	
	
	

}
