import numpy as np
from bokeh.plotting import figure, show, output_file, save
import simplejson

def reader(filename):
    with open(filename) as f:
        data = simplejson.load(f)

    area = sum(data['data']['values'])
    return list(map(lambda x: x/area,data['data']['values']))

palette = {0:'blue',1:'red',2:'orange',3:'magenta',4:'indigo',5:'black'}
styles = {0:'solid',1:'solid',2:'dashed',3:'dotted',4:'dotdash',5:'dotdash'}
sizes = {0:1,1:1,2:1.5,3:1.5,4:1.5,5:2}

def plotter(nbins, low, high, **kwargs): #title="Bill similarity distribution", y_axis_label="Bill pairs/bin", x_axis_label="Jaccard similarity", **kwargs):
    title="Bill similarity distribution"
    y_axis_label="Bill pairs/bin"
    x_axis_label="Jaccard similarity"

    x = np.linspace(low,high,nbins)

    p = figure(title=title, y_axis_label=y_axis_label, x_axis_label=x_axis_label) #, y_axis_type="log")

    i = 0
    for key, value in kwargs.iteritems():
        p.line(x, value, legend=key, line_color=palette[i], line_dash=styles[i], line_width=sizes[i]) 
        i+=1

    from bokeh.models import Range1d
    p.y_range = Range1d(-0.1,1.5)


    p.legend.location = "top_right"
    output_file("logplot.html", title="Bill similarity distribution")
    save(p)


if __name__ == '__main__':
    #Read data
    low = 0
    high = 100
    nbins = 10

    #w2_all = reader("overall.json")
    aggag = reader("aggag.json")
    aggag_both = reader("aggag_both.json")
    #darkSky = reader("darkSky.json")
    #fairShare = reader("fairShare.json")
    #healthCare = reader("unitedHealth.json")
    #foodSchools = reader("foodSchools.json")

    #kwargs = {'MinHash LSH (w2), max(left,right) from 51 states':w2_all,'Baseline, Ag-gag':aggag,'Baseline, Dark sky':darkSky,'Baseline, Fair share':fairShare, 'Baseline, health care':healthCare, 'Baeline, food schools':foodSchools}
    kwargs = {'Baseline, Ag-gag, 2/pair':aggag_both,'Baseline, Ag-gag, > 1/pair':aggag}     


    plotter(nbins,low,high,**kwargs)
