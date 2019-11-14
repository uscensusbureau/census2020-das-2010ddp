import numpy as np

def sliceArray(np_array):
    #slice off the combined child solution to make separate arrays for each child
    n = np_array.shape[-1]
    thelist = []
    for i in range(n):
        temp = np_array[tuple( [ slice(0, np_array.shape[x]) for x in range(len(np_array.shape) - 1) ] + [slice(i,i+1)] )] #this is really ugly - feel free to improve, trying to subset to each geography
        temp = temp.squeeze() #gets rid of dimensions of size 1
        thelist.append(temp)
    return thelist 