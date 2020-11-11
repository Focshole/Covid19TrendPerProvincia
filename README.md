# Covid19TrendPerProvincia
Python3 script to produce plots about new daily infections in each italian province. It needs to be hugely refactored, by using Pandas and other better stuff.<br>
It includes also an estimation of the number of tests per province, by considering a linear correlation between the population of each province and the regional data (if a city has 1000 abitants, probably they will have done 10 times the number of tests than a city with 100 abitants). This assumption is extremely simplistic and may not be accurate. <br>
The script relies on [Protezione Civile's csv data](https://github.com/pcm-dpc/COVID-19/) about total cases per province per day.<br>
__Provided data is unreliable__ in the short period, as you can see on the graphs. It will improve when I'll smooth the data with the previous and following days data.<br>
I'm thinking about a better estimation for the most recent 3 days of data, cause they're alike raw data.

## Update

 - I have smoothed data, the result is much better. All the "normalized" directories contain the adjusted data. It is still a WIP
 - There is still a lot to fix/correct, I didn't realize that the tests data column was cumulative, so I had to change a lot of stuff
 - Added caching for requests
 - Multiprocess image save (nproc - 1)


## How to use
Simply do 

```python3.8  csv_covid.py```

The updated plots will be produced in Covid,Covid_Tests_est,Covid_growing_est directory as PNG images.

All the requirements should be already satisfied, if not, check the file requirements.txt.


![example_plot](Covid/Covid%20new%20infections%20per%20day%20in%20Milano%20MI.png)
![example_plot_normalized](Covid_n/Covid%20new%20infections%20per%20day%20in%20Milano%20MI%20normalized.png)
![example_plot3](Covid_infection_per_test_est_n/Covid%20infections%20per%20tests%20in%20Milano%20MI%20normalized.png)
