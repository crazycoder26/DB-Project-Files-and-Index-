import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.Scanner;
import java.io.EOFException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Set;
import java.util.Stack;
import java.util.TreeMap;
import java.util.Map.Entry;

public class MyDatabase {
	static String dbName="information_schema";
	static String table;
	public static void help() {
		System.out.println(line("*",80));
		System.out.println();
		System.out.println("\tdisplay all;   Display all records in the table.");
		System.out.println("\tdisplay; <id>; Display records whose ID is <id>.");
		System.out.println("\tversion;       Show the program version.");
		System.out.println("\thelp;          Show this help information");
		System.out.println("\texit;          Exit the program");
		System.out.println();
		System.out.println(line("*",80));
	}
	
	public static void splashScreen() {
		System.out.println(line("*",80));
        System.out.println("Welcome to SvSQL");
		version();
		System.out.println("Type \"help;\" to display supported commands.");
		System.out.println(line("*",80));
	}
	
	public static String line(String s,int num) {
		StringBuilder sb = new StringBuilder();
		for(int i=0;i<num;i++) {
			sb.append(s);
		}
		return sb.toString();
	}
	public static void newline(int num) {
		for(int i=0;i<num;i++) {
			System.out.println();
		}
	}
	
	public static void version() {
		System.out.println("SvSQL v5.0");
	}
	
	static void displaySchema() throws EOFException
	{
		try{
			RandomAccessFile schemataTableFile = new RandomAccessFile("information_schema.schemata.tbl", "rw");
			
			/*
			*System.out.println(schemataTableFile.getFilePointer());
			*System.out.println(schemataTableFile.length());
			*/
			
			while(schemataTableFile.getFilePointer()<schemataTableFile.length()){
				byte len=schemataTableFile.readByte();
				for( int i=0;i<len;i++)
					System.out.print((char)schemataTableFile.readByte());
				System.out.println();
				}
			}
			catch (Exception e) {
				System.out.println(e);	
			}
	}
	
	static void createTable(String query) throws EOFException
	{
		if(dbName.equals("information_schema"))
			System.out.println("Database not selected");
		else
		{	
		String subquery;
		subquery=query.substring(query.indexOf("(")+1,query.length()-1);
		String[] b=subquery.split(",");
		int flag=0;
		try{
				RandomAccessFile infoschemafile = new RandomAccessFile("information_schema.table.tbl", "rw");
				do
				{
					String db="";
					String tb="";
					
					Byte length=infoschemafile.readByte();
					for(int i=0;i<length;i++)
					{
						db=db+(char)infoschemafile.readByte();
					}
					length=infoschemafile.readByte();
					
					for(int i=0;i<length;i++)
					{
						tb=tb+(char)infoschemafile.readByte();
					}
					
					infoschemafile.readLong();
					if((tb.equals(table))&&(db.equals(dbName)))
						flag=1; //tables existence check
				}while(infoschemafile.getFilePointer()!=infoschemafile.length());
				if(flag==1)
					System.out.println("Table name exists");
				else
				{
				String filename = dbName+"."+table+"."+"tbl";
				RandomAccessFile latestTblFile=new RandomAccessFile(filename,"rw");
				infoschemafile.writeByte(dbName.length());
				infoschemafile.writeBytes(dbName);
				infoschemafile.writeByte(table.length());
				infoschemafile.writeBytes(table);
				infoschemafile.writeLong(0);
				infoschemafile = new RandomAccessFile("information_schema.columns.tbl", "rw");
				for(int i=0;i<b.length;i++)
				{
					String priKey="";
					String isnull="YES";
					String[] c=b[i].split(" ");
					int j=0;
					filename=dbName+"."+table+"."+c[0]+".ndx";
					latestTblFile=new RandomAccessFile(filename,"rw");
					infoschemafile.skipBytes((int)infoschemafile.length());
					infoschemafile.writeByte(dbName.length());
					infoschemafile.writeBytes(dbName);
					infoschemafile.writeByte(table.length());
					infoschemafile.writeBytes(table);
					infoschemafile.writeByte(c[j].length());
					infoschemafile.writeBytes(c[j]);
					j++;
					infoschemafile.writeInt(i);
					infoschemafile.writeByte(c[j].length());
					infoschemafile.writeBytes(c[j]);
					for(j=2;j<c.length;j++)
					{
						if(c[j].equals("not null"))
							isnull="NO";
													
						if(c[j].equals("primary key")){
								priKey="PRI";
								isnull="NO";
						}
					}
					infoschemafile.writeByte(isnull.length());
					infoschemafile.writeBytes(isnull);
					infoschemafile.writeByte(priKey.length());
					infoschemafile.writeBytes(priKey);
				}
				}
			}
		catch(Exception e)
		{
			System.out.println(e);
		}
		}
	}
	static void createSchema() throws EOFException
	{
		String db="";
		int flag=0;
		try{
				RandomAccessFile infoschemafile = new RandomAccessFile("information_schema.schemata.tbl", "rw");
				do
				{
					Byte length=infoschemafile.readByte();
					for(int i=0;i<length;i++)
						db=db+(char)infoschemafile.readByte();
					if((db.equals(dbName)))
						flag=1; //schema existence check
				}while(infoschemafile.getFilePointer()!=infoschemafile.length());
				
				if(flag==1)
				{
					throw new Exception("Schema name exists!");
				}
				else	
				{
					infoschemafile.skipBytes((int) infoschemafile.length());
					infoschemafile.writeByte(dbName.length());
					infoschemafile.writeBytes(dbName);
				}
			}
			catch (Exception e) {
				System.out.println(e);
			}
	}
	
	// Method to drop the table
	
	static void dropTable(String tbName) throws EOFException
	{
		System.out.println(tbName);
		try{
			RandomAccessFile infoschemafile = new RandomAccessFile("information_schema.table.tbl", "rw");
			do
			{
				String db="";
				String tb="";
				
				Byte length=infoschemafile.readByte();
				for(int i=0;i<length;i++)
				{
					db=db+(char)infoschemafile.readByte();
				}
				length=infoschemafile.readByte();
				
				for(int i=0;i<length;i++)
				{
					tb=tb+(char)infoschemafile.readByte();
				}
				
				infoschemafile.readLong();
				
				if((tb.equals(table))&&(db.equals(dbName)))
				{
					System.out.println("table" + table + "dbname" + dbName);
					infoschemafile.seek(infoschemafile.getFilePointer());
					infoschemafile.writeBytes("");
					
				}
			}while(infoschemafile.getFilePointer()!=infoschemafile.length());
		}
		catch(Exception e)
		{
			System.out.println(e);
		}
	}
	static void displayTables() throws EOFException
	{
		if(dbName.equals("information_schema"))
		{
			//information_schema default tables
			System.out.println("columns");
			System.out.println("schemata");
			System.out.println("tables");
		}
		else
		{	
		try{
			RandomAccessFile schTblFile = new RandomAccessFile("information_schema.table.tbl", "rw");
			while(schTblFile.getFilePointer()<schTblFile.length()){
				String db="";
				String tb="";
				byte len=schTblFile.readByte();
				for( int i=0;i<len;i++)
					db=db+(char)schTblFile.readByte();
				len=schTblFile.readByte();
				for( int i=0;i<len;i++)
				{
					tb=tb+(char)schTblFile.readByte();
				}
				if(db.equals(dbName))
				{
					System.out.println(tb);	
				}
				schTblFile.readLong();
			}
		}
		catch (Exception e) {
			System.out.println(e);
		}
	}		
}
	//use schema
	static void use() throws EOFException
	{
		int flag=0;
		try
		{
			RandomAccessFile infoschemafile = new RandomAccessFile("information_schema.schemata.tbl", "rw");
			do{
				Byte length=infoschemafile.readByte();
				//System.out.println("length:" + length);
				String db="";
				for(int i=0;i<length;i++){
					db=db+(char)infoschemafile.readByte();
					}
				if(db.equals(dbName))
					flag=1;
			}while(infoschemafile.getFilePointer()!=infoschemafile.length());
			if(flag==0)
				System.out.println("No database");
			else
				System.out.println("Database Changed");
		}
		catch(Exception e)
		{
			System.out.println(e);
		}
		
	}	
	public static void main(String []args) throws IOException
	{
		
		splashScreen();
		Scanner scanner = new Scanner(System.in).useDelimiter(";");
		String query;
		do
		{
		System.out.print("SvSQL>");
		query =scanner.next().trim();
		String[] a=query.split(" ");
		switch(a[0])
		{
			case "show":
				if(a[1].equals("schemas"))
						displaySchema();
				else
				if(a[1].equals("tables"))
				{
					displayTables();
				}
				break;
			case "use":
				dbName=a[1];
				use();
				break;
			case "select":
				table=a[3];
			case "create":
						if(a[1].equals("schema"))
						{
							dbName=a[2];
							createSchema();
						}
						else
						{
							if(a[1].equals("table"))
							{
								
								String[] t1=a[2].replace("("," ").split(" ");
								table=t1[0];
								createTable(query);
							}
						}
				break;
			case "drop":
				table = a[2];
				dropTable(table);
			case "exit":
				break;
			case "help": help(); break;
			case "insert": table=a[2];
			new InsertQuery(query, dbName, table);
			System.out.println("Do not give spaces before and after ',' and '()'  ");
			System.out.println("Ex: insert into tbname values(val1,val2);");
			System.out.println("----"+query+" "+dbName+" "+table);
			break;
			default:
				System.out.println("Invalid command: " +query);
		}
		}while(!query.equals("exit"));
		System.out.println("Bye!");
	}
}	