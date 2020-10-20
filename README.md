# Covid19TrendPerProvincia
Simple python3 script to produce plots about new daily infections in each italian province.<br>
It includes also an estimation of the number of tests per province, by considering a linear correlation between the population of each province and the regional data (if a city has 1000 abitants, probably they will have done 10 times the number of tests than a city with 100 abitants). This assumption is extremely naif and may not be accurate. <br>
The script relies on [Protezione Civile's csv data](https://github.com/pcm-dpc/COVID-19/) about total cases per province per day.<br>
__Provided data is unreliable__ in the short period, as you can see on the graphs. It will improve when I'll smooth the data with the previous and following days data.


## How to use
Simply do 

```python3.8  csv_covid.py```

The updated plots will be produced in Covid,Covid_Tests_est,Covid_growing_est directory as PNG images.

All the requirements should be already satisfied, if not, check the file requirements.txt.


![example_plot](Covid/Covid%20new%20infections%20per%20day%20in%20Roma%20RM.png)
![example_plot2](Covid/Covid%20new%20infections%20per%20day%20in%20Milano%20MI.png)
![example_plot3](Covid_growing_est/Covid%20estimated%20growing%20rate%20in%20Milano%20MI.png)
