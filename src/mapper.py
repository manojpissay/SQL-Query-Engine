ind=['2'];cond=None;sep=',';


import sys

key = "key"

if cond:
	for line in sys.stdin:
		flag = 1
		line = line.strip()
		words = line.split(sep)
		for i in cond:
			if cond[i][0] == "=":
				if words[i] != cond[i][1]:
					flag = 0
					break
			elif cond[i][0] == ">":
				if int(words[i]) <= int(cond[i][1]):
					flag = 0
					break
			elif cond[i][0] == "<":
				if int(words[i]) >= int(cond[i][1]):
					flag = 0
					break
			elif cond[i][0] == ">=":
				if int(words[i]) < int(cond[i][1]):
					flag = 0
					break
			elif cond[i][0] == "<=":
				if int(words[i]) > int(cond[i][1]):
					flag = 0
					break

		if flag == 1:
				# print("got it", words)
				tmp = []
				for i in ind:
					i = int(i)
					tmp.append(words[i])
				print(key + '\t' +  ' | '.join(tmp)+":::"+"1")
		
			

else:
	for line in sys.stdin:
		
		line = line.strip()
		words = line.split(sep)
		# print(words)
		tmp = []
		
		for i in ind:
			i = int(i)
			tmp.append(words[i])
		print(key+'\t'+' | '.join(tmp)+":::"+"1")

		
