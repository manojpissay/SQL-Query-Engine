agg='min'
import sys

current_key = None
current_value = 0
if agg=='min':
	current_value = 10**9

key = None

# input comes from STDIN
for line in sys.stdin:
	line = line.strip()
	key, value = line.split('\t')
	if agg=='0':	#No aggregation specified
		v1, v2 = value.split(":::")
		print(v1)#.strip(":::1"))
	else:
		#Aggragation specified
		v1, v2 = value.split(":::")
		try:
			if agg=='count':	
				value = v2
			else:
				value = v1		
			value = int(value)
		except ValueError:
			continue
		if current_key == key:
			if agg == "count":
				current_value += value
			if agg == "max":
				if current_value < value:
					current_value = value
			if agg == "min":
				if current_value > value:
					current_value = value
		else:
			current_value = value
			current_key = key
if current_key == key and agg!='0':
	print(agg, "\t",  current_value)
