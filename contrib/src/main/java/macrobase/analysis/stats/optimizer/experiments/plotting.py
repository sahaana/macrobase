import sys
import matplotlib
import matplotlib.pyplot as plt
import numpy as np

file_name = sys.argv[1]
matplotlib.rcParams['figure.figsize'] = 1.11, .8
matplotlib.rcParams['font.size'] = 7
matplotlib.rcParams['font.sans-serif'] = "Helvetica"
matplotlib.rcParams['font.family'] = "sans-serif"

data = np.loadtxt(file_name, delimiter=',')
sorted_data = data[np.argsort(data[:,0])]
plt.figure()
plt.plot(sorted_data[:,0],sorted_data[:,1])
plt.show()

