package job3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CustomKey implements WritableComparable<CustomKey>{

	private Text nome;
	private Text settore;

	private IntWritable anno;

	public CustomKey() {
	}

	public CustomKey(Text nome, Text settore, IntWritable anno) {
		this.nome = nome;
		this.settore = settore;
		this.anno = anno;
	}


	public Text getNome() {
		return nome;
	}

	public void setNome(Text nome) {
		this.nome = nome;
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
		return nome.hashCode() + settore.hashCode() + anno.hashCode();
	}

	@Override
	public String toString() {
		return this.nome.toString() + " " + this.settore.toString() + " " + this.anno.toString();
	}


	@Override
	public boolean equals(Object o){
		CustomKey a= (CustomKey)o;
		return this.nome.equals(a.getNome()) && this.settore.equals(a.getSettore())&& this.anno.equals(a.getAnno());
	}

	public int compareTo(CustomKey that) {
		int cmp = this.nome.hashCode()-that.nome.hashCode();
		if(cmp==0){
			cmp= this.anno.hashCode()-that.getAnno().hashCode();
			if (cmp == 0)
				cmp= this.settore.hashCode()-that.getSettore().hashCode();
		}
			
		return cmp;
	}

	public void readFields(DataInput in) throws IOException {
		this.nome = new Text(in.readUTF());
		this.settore = new Text(in.readUTF());
		this.anno = new IntWritable(in.readInt());
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.nome.toString());
		out.writeUTF(this.settore.toString());
		out.writeInt(this.anno.get());
	}




}
