package FilesOperation;

import java.io.IOException;
import contants.DataTypes;
import contants.Config;

public class Page {
    TableFile file;
	public int rowid;
	public int pagetype;
	public String[] coltype;
	public String[] colvalue;
	public int cellsize;
	public int pageno;
	static int pagesize = new Config().PAGE_SIZE;
	
	public Page(TableFile file){
		this.file = file;
	}
	
	public Page(TableFile file,int rowid,String[] coltype, String[] colvalue, int cellsize,int pageno,int pagetype){
		this.file = file;
		this.coltype = coltype;
		this.colvalue = colvalue;
		this.pageno = pageno;
		this.rowid = rowid;
		this.cellsize = cellsize;
		this.pagetype = pagetype;
	}
	
	public Page(TableFile file, int pageno, String[] colvalue, int pagetype){
		this.file = file;
		this.pageno = pageno;
		this.colvalue = colvalue;
		this.pagetype = pagetype;
	}
	
	public int writeleaf() throws IOException{
		int pos = pageno * pagesize;
		file.seek(pos);
		Byte curpagetype = file.readByte();
		short payloadsize=0;
		short cellpos=0;
		if(curpagetype == 0){
			file.seek(pos);
			file.writeByte(pagetype);
			file.seek(pos+1);
			file.writeByte(0x01);
			file.seek(pos+4);
			file.writeInt(-1);
			payloadsize = (short)(coltype.length+cellsize+1);
			cellpos = (short)(pagesize - (6 + payloadsize));		
		}else{
			int num_cells = 0;
			file.seek(pos+1);
			num_cells = file.readByte();
			file.seek(pos+2);
			cellpos = file.readShort();
			if(cellpos < (cellsize+coltype.length+6+1+8+((num_cells+1)*2))){
				return -1;
			}
			file.seek(pos+1);
			num_cells = file.readByte();
			file.seek(pos+1);
			file.writeByte(num_cells+1);
			cellpos -= (cellsize+coltype.length+6+1);
			payloadsize = (short)(coltype.length+cellsize+1);
		}
		file.seek(pos+8);
		int j = 0;
		while(file.readShort() != 0){
			j +=2;
			file.seek(pos+8+j);
		}
		file.seek(pos+8+j);
		file.writeShort(cellpos);
		file.seek(pos+2);
		file.writeShort(cellpos);
		file.seek(pos+cellpos);
		file.writeShort(payloadsize);
		file.seek(pos+cellpos+2);
		file.writeInt(rowid);
		file.seek(pos+cellpos+6);
		file.writeByte(coltype.length);
		for(int i=0; i < coltype.length; i++){
			Byte type = DataTypes.getTypeCode(coltype[i]);
			if(type == 0x0C){
				int len = colvalue[i].length() & 0xFF;
				type = (byte) (type + (len & 0xFF));
			}
			file.seek(pos+cellpos+7+i);
			file.writeByte(type);
		}
		int newpos = pos +cellpos+7+coltype.length;
		for(int i = 0; i < colvalue.length; i++){
			file.seek(newpos);
			file.writeByType(colvalue[i], DataTypes.getTypeCode(coltype[i]));
			newpos += DataTypes.getTypeSize(coltype[i]);
			if(DataTypes.getTypeCode(coltype[i]) == 0x0C){
				newpos += colvalue[i].length();
			}
		}
		return 1;
	}
	
	public void writeinterior(int cellrowid,int leafpageno) throws IOException{
		int pos = pageno * pagesize;
		file.seek(pos);
		Byte curpagetype = file.readByte();
		short cellpos = 0;
		int currleafno = 0;
		int currrowid = 0;
		if(curpagetype != 0x05){
			file.seek(pos);
			file.writeByte(pagetype);
			file.seek(pos+1);
			file.writeByte(0x01);
			file.seek(pos+4);
			file.writeInt(-1);
			cellpos = (short)(pagesize - 10);
		}else{
			file.seek(pos+4);
			int rightptr = file.readInt();
			if(leafpageno == rightptr){
				return;
			}
			file.seek(pos+2);
			cellpos = file.readShort();
			if(rightptr != -1){
				currleafno = rightptr;
				int newpos = currleafno * pagesize;
				file.seek(newpos+2);
				int newcellpos = file.readShort();
				file.seek(newpos+newcellpos+2);
				currrowid = file.readInt();
				cellpos -= 8;
				file.seek(pos+cellpos);
				file.writeInt(rightptr);
				file.seek(pos+cellpos+4);
				file.writeInt(currrowid);
				file.seek(pos+4);
				file.writeInt(leafpageno);
				return;
			}else{
				file.seek(pos+2);
				cellpos = file.readShort();
				file.seek(pos+cellpos);
				currleafno = file.readInt();
				file.seek(pos+cellpos+4);
				currrowid = file.readInt();
				if(currleafno == leafpageno){
					if(cellrowid > currrowid){
						file.seek(pos+cellpos+4);
						file.writeByType(colvalue[1], DataTypes.getTypeCode(DataTypes.INT));
					}
					return;
				}else{
					file.seek(pos+4);
					rightptr = file.readInt();
					if(rightptr == -1){
						file.seek(pos+4);
						file.writeInt(leafpageno);
					}
					return;
				}
			}
		}
		file.seek(pos+8);
		int j = 0;
		while(file.readByte() != 0){
			j +=2;
			file.seek(pos+8+j);
		}
		file.seek(pos+8+j);
		file.writeShort(cellpos);
		file.seek(pos+2);
		file.writeShort(cellpos);
		file.seek(pos+cellpos);
		file.writeByType(colvalue[0], DataTypes.getTypeCode(DataTypes.INT));
		file.writeByType(colvalue[1], DataTypes.getTypeCode(DataTypes.INT));
	}
	
	public int findleafnode(int serrowid,int serpageno,int cellsize) throws IOException{
		pageno = serpageno;
		rowid = serrowid;
		
		int pos = pageno * pagesize;
		file.seek(pos+1);
		int num_cells = file.readByte();
		int leafpage = 0;
		int cellpos = 0;
		int cellrowid = 0;
		file.seek(pos+4);
		int leafnodefound = 0;
		int rightptr = file.readInt();
		file.seek(pos+8);
		for(int i = 0; i < num_cells; i++){
			file.seek(pos+8+(2*i));
			cellpos = file.readShort();
			if(cellpos == 0){
				continue;
			}
			file.seek(pos+cellpos);
			leafpage = file.readInt();
			file.seek(pos+cellpos+4);
			cellrowid = file.readInt();
			if(rowid > cellrowid){
				continue;
			}else{
				leafnodefound = 1;
				break;
			}
		}
		if(leafnodefound == 0){
			if(rightptr!= -1){
				return rightptr;
			}
		}
		return leafpage;	
		
	}

}
