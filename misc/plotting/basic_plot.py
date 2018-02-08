import sys
import numpy as np
from bokeh.plotting import figure, show, output_file, save
import simplejson

#Read data
def read_json(filename):
    with open(filename) as f:
        data = simplejson.load(f)

    return data['data']['values']

if __name__ == '__main__':

    low = 0
    high = 100
    nbins = 11

    test_baseline = read_json(sys.argv[1])
    test_w2 = read_json(sys.argv[2])
    x = np.linspace(low,high,nbins)

    p = figure(title="Bill similarity distribution", y_axis_label="Bill pairs/bin", x_axis_label='Jaccard similarity'
           ,y_axis_type="log")

    p.quad(left=x[:-1], right=x[1:], top=test_baseline, bottom=0,legend="Baseline, max(left,right), random sample",
       fill_color=None,line_color="tomato", line_width=1.5) #line_dash="dotdash")

    p.quad(left=x[:-1], right=x[1:], top=test_w2, bottom=0,legend="MinHash LSH (w2), max(left,right), random sample",
       fill_color = None, line_color="green", line_width=1.5)

    p.legend.location = "top_right"
    output_file("logplot.html", title="Bill similarity distribution")

    save(p)
