import subprocess
import pickle
from pprint import pprint
from time import sleep

supported_dtypes = ['str', 'int']
supported_agg = ["max", "min", "count"]

def shell(line):
	#Send a terminal command to this function to receive the output
	l = line.split()
	#In case of loading schema (File mode - 'rb'/'wb')
	if '-cat' in l and '/myhive/schemas' in l[-1]:
		f = open("tst.txt", 'wb')
		subprocess.run(l, stdout=f)
		f.close()
		f = open("tst.txt", 'rb')
		result = f.read()
		return result
	
	if l[0] == 'hadoop':
		l[4] = l[4].replace(':::', ' ')
		l[6] = l[6].replace(':::', ' ')
		
	f = open("tst.txt", 'w')
	subprocess.run(l, stdout=f)
	f.close()
	f = open("tst.txt", 'r')
	result = f.readlines()
	f.close()
	return result


def check_syntax(line):
	#Check the type of query and return the same

	l = line.split()
	if len(l) > 1:
		if l[0] == 'load' and '/' in l[1] and l[1].endswith(".csv") and "as" in l:
			return "load"
		elif l[0] == 'select' and 'from' in l:
			return 'select'
		elif l[0] == "drop":
			return "drop"
		elif line == "show tables":
			return "show"
		elif l[0] == 'describe':
			return "describe"
		elif l[0] == "put":
			return "put"


def load(line):
	sep = ","
	if "sep" in line:
		tmp = line.split("sep")
		sep = tmp[1].strip() #Extract the seperator of the csv file
		line = tmp[0]
		print("Found seperator: ", sep)
	line = line.strip()
	comm, schema = line.strip(')').split("(")  #Extracting the command and the schema specified
	#schema = schema.strip(')')
	comm = comm.split()
	db, tb = comm[1].split("/")  		#Extracting the database and table name 
	schema = schema.split(',')

	#Comparing the schema specified in the command with the format of the CSV on HDFS
	data_firstline = shell("hdfs dfs -head /myhive/data/"+tb.strip(".csv")+'/'+tb)[0][:-1].split(sep)
	sch_store = {}		#Creating a dictionary to store the schema.
	if len(schema) != len(data_firstline):
		print("Mismatched column dimensions")
		return
	for i in range(len(data_firstline)):
		colname, dtype = schema[i].split(':')   #schema structure example: (name:str,age:int)
		colname = colname.strip()
		dtype = dtype.strip()

		if dtype not in supported_dtypes:
			# Handles unsupported data type
			print("Invalid data type:",dtype)
			print("Use on of these:", ", ".join(supported_dtypes))
			return

		if dtype == "int":
			try:
				tmp1 = int(data_firstline[i])
			except ValueError:
				print("Mismatched data types")
				return
		sch_store[colname] = (i, dtype) #Adding (column number,datatype) to the sch_store dictionary
	sch_store["seperator"] = sep
	sch_filename = 'schemas/' + db + "___" + tb + '.bd'
	f = open(sch_filename, 'wb')
	pickle.dump(sch_store, f)   #Storing the schema in the local buffer file in /schemas/
	f.close()
	o = shell('hdfs dfs -put ' + sch_filename + ' /myhive/schemas')  #Loading the local buffer file into HDFS

def getCond(line, schema):
	#If query contains a condition, extract if and return as a dictionary

	d = {}
	comparator = 'None'
	for i in ['<=', '>=', '<', '>', '=']: 	#Selecting the specified condition from the supported conditions
		if i in line:
			comparator = i
			break
	if comparator == 'None':		#If unsupported comparator is given
		print("No comparator found")
		return 
	attr, value = line.split(comparator)	#Extracting the attribute and its value to compare 
	value = value.split()[0]		
	attr = attr.strip()
	value = value.strip()
	if attr in schema:
		if 'int' not in schema[attr][1]:	
			if comparator != "=":	
				#If data type is not integer, only '=' comparator should be used
				print("Invalid comparator on column")
				return
		attr = schema[attr][0] 	#Mapping attribute name and attribute index
	else:
		print("Conditional attribute not in schema")
		return
	if "'" in value or '"' in value:
		value = value[1:-1]
	d[attr] = (comparator, value)
	return d


def select(line):
	line = line.lower()
	l = line.split()
	try:
		db, tb = l[l.index('from')+1].split('/')	#Extracting the database and tablename
	except ValueError:					#If only database or tablename is specified
		print("No database/table provided")
		return
	out = shell("hdfs dfs -cat /myhive/schemas/" + db + "___" + tb + ".bd")
	try:
		schema = pickle.loads(out)   			#If wrong database/tablename is specified
	except EOFError:
		print("Database/table does not exist")
		return
	col = 1
	indices = []
	while l[col] != 'from':
		if l[col] == '*':
			indices = list(range(len(schema)-1)) #-1 because schema has column info AND seperator
			break
		try:
			indices.append(str(schema[l[col]][0])) #Mapping of column_name and column_index using the schema
		except KeyError:			#If column_name is not in schema
			print("Column:", l[col], "does not exist")
			return
		col += 1
	cond_dict = "None"
	agg="0"
	if "where" in line:
		_, cond = line.split('where')
		cond_dict = getCond(cond, schema) #Get the specified condition
		if not cond_dict:
			return -1 
	#For aggregate operations
	if "aggregate_by" in l:
		try:
			agg= l[l.index("aggregate_by") + 1]
		except IndexError:
			print("No aggregation specified")
			return
		if agg in supported_agg:
			# print("Found aggregation", agg)
			if agg in ['max', 'min']:	#Only one column has to be specified in case of max and min
				if len(indices) != 1:
					print("Inconsistent number of columns")
					return 
		else:
			print("Aggregation not supported")
			return
	change_mapper(indices, cond_dict, schema["seperator"])
	change_reducer(agg)
	apply_mapred(db, tb)


def drop(line):
	l = line.strip().split()
	print("Dropping", l[1])
	db, tb = l[1].split("/")
	shell("hdfs dfs -rm /myhive/schemas/" + db + "___" + tb + ".bd")
	
	

#Mapper requires column indices, condition specified and the seperator
def change_mapper(indices, cond, sep):
	f = open('mapper.py', 'r')
	lines = f.readlines()
	f.close()
	# print(lines)
	a = 'ind=' + str(indices) + ';'
	a += 'cond=' + str(cond) + ';'
	a += 'sep=' + "'" + sep + "'" + ';'
	# a += "agg="+ "'" + agg + "'"
	a += "\n"
	lines[0] = a
	f = open('mapper.py', 'w')
	f.writelines(lines)
	f.close()
	# return 

#Reducer requires only the aggregation
def change_reducer(agg):
	f = open('reducer.py', 'r')
	lines = f.readlines()
	f.close()
	# print(lines)
	a = "agg="+ "'" + agg + "'" + '\n'
	lines[0] = a
	f = open('reducer.py', 'w')
	f.writelines(lines)
	f.close()

def apply_mapred(db, tb):
	o = shell('hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.2.0.jar -mapper "python3:::mapper.py" -reducer "python3:::reducer.py" -input /myhive/data/'+tb.strip(".csv")+'/* -output /output1')
	while 1:
		res = '\n'.join(shell('hdfs dfs -ls /'))
		if "output1" in res:
			res = shell("hdfs dfs -ls /output1")
			res_str = "\n".join(res)
			if "SUCCESS" in res_str: 	#Handle multiple reducer output files.
				for i in res:
					if "part" in i:
						f = i.split("/output1/")[1].strip()
						result = shell('hdfs dfs -cat /output1/'+f)
						for i in result:
							if i != '\t\n':
								i=i.strip('\t\n')
							print(i)
						
				o = shell('hdfs dfs -rm -r /output1')
				break

	
	

def showtables():
	res = shell("hdfs dfs -ls /myhive/schemas")
	sep = "myhive/schemas/"
	for i in res:
		if sep in i:
			tmp = i.split(sep)
			tmp1 = tmp[1].split("___")
			db = tmp1[0]
			tb = tmp1[1].strip(".bd\n")
			print(db + "/" + tb)


def describe(line):
	l = line.split()
	try:
		db, tb = l[1].split('/')
	except ValueError:
		print("Database/table not specified")
		return
	try:
		res = shell('hdfs dfs -cat /myhive/schemas/'+db+'___'+tb+'.bd')
		res = pickle.loads(res)
	except EOFError:
		print("Database/table does not exist")
		return 
	pprint(res)
		
	

def put(line):
	l = line.split()
	filepath = l[1].strip('"').strip("'")
	fname = filepath.split('/')[-1].strip(".csv")
	shell("hdfs dfs -mkdir /myhive/data/"+fname)
	shell("hdfs dfs -put " + filepath + " /myhive/data/"+fname) 


def comlist():
	print("\nPlease enter queries in one of these formats:\n\n")
	l = ["PUT tablename.csv",
	     "LOAD dbname/tablename.csv AS (colname1:dtype1, colname2:dtype2) SEP separator",
	     "SHOW TABLES",
	     "DESCRIBE tablename",
	     "SELECT * FROM dbname/tablename.csv",
	     "SELECT col1 col2 coln from dbname/tablename.csv WHERE cond",
	     "SELECT col from dbname/tablename.csv WHERE cond AGGREGATE_BY aggregation",
	     ]
	print("\n".join(l), "\n")


def hive():	
	print("Welcome to MyHive.\nThis is an SQL query engine using Hadoop MapReduce as backend.\n")	
	print('\nType "HELP" for Help')
	while True: 					#Infinite loop for shell execution
		command = input("\n#>").lower()		#Accepting input command
		if command == "exit":			
			break
		if command == "test":
			change_mapper()
			break
		
			
		typeofcom = check_syntax(command)	#Checking the syntax of the typed command
		if typeofcom == "load":
			load(command)
		elif typeofcom == 'select':
			flag = select(command)
		elif typeofcom == "drop":
			drop(command)
		elif typeofcom == "show":
			showtables()
		elif typeofcom == 'describe':
			describe(command)
		elif typeofcom == "put":
			put(command)
		elif command == "help":
			comlist()
		else:
			print("Invalid command")
		

#Start of execution		
hive()


