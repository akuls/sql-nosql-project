import numpy as np
import matplotlib.pyplot as plt

#Plots the bar charts
def my_plot(val_list, x_label, y_label, title, fig_name):
	# data to plot
	n_groups = 7
	val = (0, val_list[0], val_list[1], val_list[2], val_list[3], val_list[4], val_list[5])

	# create plot
	f1 = plt.figure()
	index = np.arange(n_groups)
	bar_width = 0.2
	opacity = 0.5

	rects1 = plt.bar(index, val, bar_width,
	                 alpha=opacity,
	                 color='b',
	                 label=x_label)

	plt.xlabel(x_label)
	plt.ylabel(y_label)
	plt.title(title)
	plt.xticks(index, ('', 'Artist_identity', 'Artist_info', 'Track_identity', 'Track_basic_info', 'Track_tech_info', 'Sang'))
	#plt.legend()

	plt.tight_layout()
	fig_name = "../Figures/" + fig_name
	plt.savefig(fig_name)
	plt.show()

#Plots the line graphs
def line_plot(score, training_error):

	K_range = range(1, 11)
	
	f2 = plt.figure()
	plt.plot(K_range, score,'or-', label="Hyperparameter Score Error")
	plt.plot(K_range, training_error, 'sb-', label="Training error")
	
	plt.grid(True)
	plt.xlabel("Neighbors(K)")
	plt.ylabel("Percentage Error")
	plt.title("5 Fold CV using KNN on Email Spam Data")
	
	plt.xlim(0, 11)
	plt.ylim(0, 100) 

	plt.gca().set_xticks(K_range)
	
	plt.legend(loc="upper left")
	
	fig_name = "../Figures/CVV_result.png"
	plt.savefig(fig_name)
	#plt.show()