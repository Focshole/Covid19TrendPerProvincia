import csv
import math
import requests
import matplotlib.pyplot as plt
from datetime import date, timedelta
verbose=False



fn=["data","stato","codice_regione","denominazione_regione","codice_provincia","denominazione_provincia","sigla_provincia","lat","long","totale_casi","note_it","note_en"]

def getCsv(d):
    assert type(d)==date
    url = f"https://raw.github.com/pcm-dpc/COVID-19/master/dati-province/dpc-covid19-ita-province-{d.year}{d.month:02}{d.day:02}.csv"
    r = requests.get(url, stream = True)
    if verbose:
        print(f"Downloading {d} csv data form {url}...")

    csv_file_content=bytearray()
    for chunk in r.iter_content(chunk_size = 1024):
        if chunk:
            csv_file_content+=chunk
    if r.status_code!=200:
        if verbose:
            print(f"Downloading {d} csv data failed!")
        return None
    else:
        csv_file_splitted=(csv_file_content.decode("UTF-8")).splitlines()
        csv_file_reader=csv.DictReader(csv_file_splitted)
        csv_as_a_list=list(csv_file_reader)
        return csv_as_a_list

def getProvinceDataFromCsv(c,province):
    assert len(province)==2 and province.isupper()
    assert type(c)==list 
    assert len(c)>0 
    assert type(c[0])==dict
   
    for row in c:
        if "sigla_provincia" in row and province == row["sigla_provincia"]:
            return row



def diffInfectionsPerCsv(province,csv0,csv1):
    assert len(province)==2 and province.isupper()
    assert csv0 is not None and csv1 is not None
    assert type(csv0)==list and type(csv1)==list
    assert len(csv0)>0 and len(csv1)>0
    d0_prov_data=getProvinceDataFromCsv(csv0,province)
    d1_prov_data=getProvinceDataFromCsv(csv1,province)
    if d0_prov_data is not None and d1_prov_data is not None:
        return int(d1_prov_data["totale_casi" ]) - int(d0_prov_data["totale_casi"])
    else:
        return None

def diffInfectionsPerDate(province,d0,d1):
    assert len(province)==2 and province.isupper()
    assert type(d0)==date and type(d1)==date
    assert d0<d1
    d0_data=getCsv(d0)
    d1_data=getCsv(d1)
    if d0_data is not None and d1_data is not None:
        return diffInfectionsPerCsv(province,d0_data,d1_data)
    else:
        return None


def diffInfectionsPerDay(province,d):
    d0=d-timedelta(days=1)#d0= previous day
    return diffInfectionsPerDate(province,d0,d)





def main():
    starting_date=date(2020,2,24)
    ending_date=date.today()
    province="MI"
    oldCsv=getCsv(starting_date)
    plotx=list()
    ploty=list()
    d=starting_date+timedelta(days=1)
    while d<=ending_date:
        print(f"Fetching {d} data...")
        newCsv=getCsv(d)
        if newCsv==None:
            print(f"No data available for date {d}!")
        else:
            newInfections=diffInfectionsPerCsv(province,oldCsv,newCsv)
            if verbose:
                print(f"{d} new infections in {province}: {newInfections}")
            plotx.append(str(d))
            ploty.append(newInfections)
            oldCsv=newCsv
        d+=timedelta(days=1)
    plot_graph(plotx,ploty,province)

def main2():
    #all provinces
    starting_date=date(2020,2,24)
    ending_date=date.today()
    oldCsv=getCsv(starting_date)
    d=starting_date+timedelta(days=1)
    with open("provinces.txt","r") as f:
        provinces=f.read().splitlines()
    
    plotx=[list() for index in provinces]
    ploty=[list() for index in provinces]
    while d<=ending_date:
        print(f"Fetching {d} data...")
        newCsv=getCsv(d)
        if newCsv==None:
            print(f"No data available for date {d}!")
        else:
            for i,province in enumerate(provinces):
                province=province.split(' ')[1]
                newInfections=diffInfectionsPerCsv(province,oldCsv,newCsv)
                if newInfections is not None:
                    if verbose:
                        print(f"{d} new infections in {province}: {newInfections}")
                    plotx[i].append(f"{d.day:02}/{d.month:02}")
                    ploty[i].append(newInfections)
            oldCsv=newCsv
        d+=timedelta(days=1)
    print("Saving graphs, may require some time...")
    for i,province in enumerate(provinces):
        save_graph(plotx[i],ploty[i],province)
    print("Done!")


def save_graph(x,y,province):
    return plot_graph(x,y,province,save=True,dont_print=True)

def plot_graph(x,y,province,save=True,dont_print=False):
    plt.plot(x,y, linestyle='dashed', linewidth = 2, marker='o', markerfacecolor='black', markersize=3)
    plt.xlabel('Day') 
    plt.ylabel('New Infections') 
    plt.title(f'Covid new infections per day in {province}')
    plt.grid(True)
    howManyLabelsToPlot=30
    slidingWindow=0#useful to shift the grid
    ticks=[(i*math.floor(len(x)/howManyLabelsToPlot)+slidingWindow)%len(x) for i in range(howManyLabelsToPlot) ]
    lastTick=len(x)-1
    if  lastTick not in ticks:
        ticks.append(lastTick)
    #firstTick=0 # from now on, this should be always inserted. Probably i may change the logic above and always insert the first tick. who knows
    #if firstTick not in ticks:
    #    ticks.insert(0,0)
    plt.xticks(ticks,rotation="vertical")#was 45, but with 45 it is not aligned as you may intuitively think when watching the plot
    if save:
        if verbose:
            print("Saving {province} graph in ./Covid/Covid new infections in {province} per day.png...")
        plt.savefig(f'./Covid/Covid new infections in {province} per day.png', dpi=300)
        if verbose:
            print("Saved {province} graph!")
    if not dont_print:
        plt.show()
    plt.clf()

if __name__=="__main__":
    main2()