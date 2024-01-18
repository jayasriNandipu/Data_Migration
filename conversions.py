#-------converting list to tuple------
print("----converting list to tuple-----")
# Python3 program to convert a
# list into a tuple
def convert(list):
	return tuple(list)

# Driver function
list = [1, 2, 3, 4]
print(convert(list))

#---------converting tuple to list----
print("---converting tuple to list---")
# Define a tuple
GFG_tuple = (1, 2, 3)

# Convert the tuple to a list
GFG_list = "list(GFG_tuple)"
print(GFG_list)

#------------------converting dictonary to list-------
# Python code to convert dictionary into list of tuples
print("------converting dictonary to list-------")
# Initialization of dictionary
dict = {'Geeks': 10, 'for': 12, 'Geek': 31}

# Converting into list of tuple
list = [(k, v) for k, v in dict.items()]

# Printing list of tuple
print(list)

