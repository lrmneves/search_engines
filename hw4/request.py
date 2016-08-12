import sys



baseline = sys.argv[1]
comparison = sys.argv[2]

with open(baseline) as b_file:
	with open(comparison) as c_file:

		baseline_lines = b_file.readlines()
		comparison_lines = c_file.readlines()
		better = 0
		worse = 0
		for i in range(len(baseline_lines)):
			b_line = baseline_lines[i]
			c_line = comparison_lines[i]

			if b_line.startswith("map"):
				b_line_list = b_line.split("\t")
				c_line_list = c_line.split("\t")
				if b_line_list[1] != "all": 
					print b_line_list[1]
					base_map = float(b_line_list[-1].strip("\n")) 
					

					new_map = float(c_line_list[-1].strip("\n"))
					if base_map > new_map:
						print "worse"
						print new_map/base_map - 1.0

						worse+=1.0
						
					if new_map > base_map:
						print "better"
						print new_map/base_map - 1.0
						better+=1.0
		if worse == 0:
			worse = 1
		print better/worse