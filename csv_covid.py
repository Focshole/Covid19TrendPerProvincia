import csv
import math
import requests
import matplotlib.pyplot as plt
from datetime import date, timedelta
import concurrent.futures

# I know, i should use panda for CSVs, cause Fiat Pandas are reliable, but I'm too lazy, sry

MAX_CONNECTIONS = 6  # https://stackoverflow.com/questions/985431/max-parallel-http-connections-in-a-browser
SAVE_IMAGE_DPI = 300 # image saving quality

def preProcessing(csv_as_a_list):
    rename = ["Ascoli Piceno", "La Spezia", "Reggio Calabria",
              "Reggio Emilia", "Vibo Valentia", "Sud Sardegna", "Friuli-Venezia Giulia","Friuli Venezia Giulia"]
    for i, row in enumerate(csv_as_a_list):
        if "denominazione_regione" in row:  # Provinces and regions
            region_name = row["denominazione_regione"]
            if "P.A." in region_name: # Allows me to have a higher precision in those provinces
                csv_as_a_list[i]["denominazione_regione"] = csv_as_a_list[i]["denominazione_regione"].replace("P.A. ","PA-")
            elif region_name == "Valle d'Aosta":
                csv_as_a_list[i]["denominazione_regione"] = "Valle-d-Aosta"
            elif region_name in rename:
                csv_as_a_list[i]["denominazione_regione"] = csv_as_a_list[i]["denominazione_regione"].replace(" ", "-")

        if "Territorio" in row:
            csv_as_a_list[i]['Territorio'] = row['Territorio'].split(' /')[0]  # remove dialect names
            if csv_as_a_list[i]["Territorio"] == "Valle d'Aosta":
                if len(row['\ufeff"ITTER107"'])==4:  # region
                    csv_as_a_list[i]["Territorio"] = "Valle-d-Aosta"
                else:
                    csv_as_a_list[i]["Territorio"] = "Aosta"
            elif csv_as_a_list[i]["Territorio"] == "L'Aquila":
                csv_as_a_list[i]["Territorio"] = "Aquila"  # to avoid problems in file naming
            elif csv_as_a_list[i]["Territorio"] == "Forlì-Cesena":
                csv_as_a_list[i]["Territorio"] = "Forli-Cesena"  # same here
            elif csv_as_a_list[i]["Territorio"] == "Monza e della Brianza":
                csv_as_a_list[i]["Territorio"] = "Monza-Brianza"
            elif csv_as_a_list[i]["Territorio"] == "Pesaro e Urbino":
                csv_as_a_list[i]["Territorio"] = "Pesaro-Urbino"
            elif csv_as_a_list[i]["Territorio"] == "Reggio di Calabria":
                csv_as_a_list[i]["Territorio"] = "Reggio-Calabria"
            elif csv_as_a_list[i]["Territorio"] == "Reggio nell'Emilia":
                csv_as_a_list[i]["Territorio"] = "Reggio-Emilia"
            elif csv_as_a_list[i]["Territorio"] in rename:
                csv_as_a_list[i]["Territorio"] = csv_as_a_list[i]["Territorio"].replace(" ","-")
                # to avoid problems in file naming
            elif csv_as_a_list[i]["Territorio"]== "Provincia Autonoma Bolzano" or csv_as_a_list[i]["Territorio"]== "Provincia Autonoma Trento":
                csv_as_a_list[i]["Territorio"] = csv_as_a_list[i]["Territorio"].replace("Provincia Autonoma ", "PA-")
    return csv_as_a_list


def download_csv(url):
    r = requests.get(url, stream=True)
    file_content = bytearray()
    for chunk in r.iter_content(chunk_size=1024):
        if chunk:
            file_content += chunk
    if r.status_code != 200:  # ISTAT does a 302 redirect to their warning messages
        print(f"Downloading failed for {url}!")
        return None
    else:
        csv_file_splitted = (file_content.decode("UTF-8")).splitlines()
        csv_file_reader = csv.DictReader(csv_file_splitted)
        csv_as_a_list = list(csv_file_reader)
        # Pre processing, it is extremely ugly. I'm lazy and I don't want to refactor it by now
        return preProcessing(csv_as_a_list)


fn = ["data", "stato", "codice_regione", "denominazione_regione", "codice_provincia", "denominazione_provincia",
      "sigla_provincia", "lat", "long", "totale_casi", "note_it", "note_en"]


def get_population_per_territory_csv(territory_names):
    url = "https://dati.istat.it/Download.ashx?type=csv&Delimiter=%2c&IncludeTimeSeriesIdentifiers=False&LabelType=CodeAndLabel&LanguageCode=it"
    csv_as_a_list = download_csv(url)
    if csv_as_a_list is None:  # istat services is offline
        # fetch the local file
        with open("DCIS_POPRES1_14102020171844268.csv", "r") as f:
            csv_file_splitted = f.read().splitlines()
        csv_file_reader = csv.DictReader(csv_file_splitted)
        csv_as_a_list = preProcessing(list(csv_file_reader))
    reduced_csv = list()

    for entry in csv_as_a_list:
        if "Territorio" in entry:
            if entry["SEXISTAT1"] == '9' and entry["ETA1"] == "TOTAL" and entry["STATCIV2"] == '99' and entry[
                "Territorio"] in territory_names:  # include only stats for all sex, age and civil status
                reduced_csv.append(entry)
        else:
            print(f"Formato non riconosciuto. Url:{url}")
            exit(-1)
    return reduced_csv


def get_provinces_csv(d):
    return get_provinces_csv_multithread(d)[0]


def get_regions_csv(d):
    return get_regions_csv_multithread(d)[0]


def get_regions_csv_multithread(d):
    assert type(d) == date
    url = f"https://raw.github.com/pcm-dpc/COVID-19/master/dati-regioni/dpc-covid19-ita-regioni-{d.year}{d.month:02}{d.day:02}.csv"
    print(f"Fetching {d} regions data...")
    return download_csv(url), d


def get_provinces_csv_multithread(d):
    assert type(d) == date
    url = f"https://raw.github.com/pcm-dpc/COVID-19/master/dati-province/dpc-covid19-ita-province-{d.year}{d.month:02}{d.day:02}.csv"
    print(f"Fetching {d} provinces data...")
    return download_csv(url), d


def get_province_data_from_csv(c, province):
    assert len(province) == 2 and province.isupper()
    assert type(c) == list
    assert len(c) > 0
    assert type(c[0]) == dict

    for row in c:
        if "sigla_provincia" in row and province == row["sigla_provincia"]:
            return row


def diff_infections_between_csv(province, csv0, csv1):
    assert len(province) == 2 and province.isupper()
    assert csv0 is not None and csv1 is not None
    assert type(csv0) == list and type(csv1) == list
    assert len(csv0) > 0 and len(csv1) > 0
    d0_prov_data = get_province_data_from_csv(csv0, province)
    d1_prov_data = get_province_data_from_csv(csv1, province)
    if d0_prov_data is not None and d1_prov_data is not None:
        return int(d1_prov_data["totale_casi"]) - int(d0_prov_data["totale_casi"])
    else:
        return None


def diff_infections_per_date(province, d0, d1):
    assert len(province) == 2 and province.isupper()
    assert type(d0) == date and type(d1) == date
    assert d0 < d1
    d0_data = get_provinces_csv(d0)
    d1_data = get_provinces_csv(d1)
    if d0_data is not None and d1_data is not None:
        return diff_infections_between_csv(province, d0_data, d1_data)
    else:
        return None


def diff_infections_per_day(province, d):
    d0 = d - timedelta(days=1)  # d0 = previous day
    return diff_infections_per_date(province, d0, d)


def get_provinces_data_csv_indexed(starting_date, ending_date):
    distance = (ending_date - starting_date).days
    indexed_csv = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_CONNECTIONS) as executor:
        future_results = (executor.submit(get_provinces_csv_multithread, starting_date + timedelta(days=x)) for x in
                          range(distance))
        for future in concurrent.futures.as_completed(future_results):
            try:
                data = future.result()
            finally:
                indexed_csv[data[1]] = data[0]
    return indexed_csv

def get_regions_data_csv_indexed(starting_date, ending_date):
    distance = (ending_date - starting_date).days
    indexed_csv = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_CONNECTIONS) as executor:
        future_results = (executor.submit(get_regions_csv_multithread, starting_date + timedelta(days=x)) for x in
                          range(distance))
        for future in concurrent.futures.as_completed(future_results):
            data = future.result()
            indexed_csv[data[1]] = data[0]
    return indexed_csv

def save_graph(x, y, title, xlabel, ylabel,path="./Covid"):
    return plot_graph(x, y, title, xlabel, ylabel, save=True, dont_print=True,path=path)


def plot_graph(x, y, title, xlabel, ylabel, save=True, dont_print=False,path="./Covid"):
    plt.plot(x, y, linestyle='dashed', linewidth=2, marker='o', markerfacecolor='black', markersize=3)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.title(title)
    plt.grid(True)

    howManyLabelsToPlot = math.floor(len(x) / 7)  # one label every 7 days
    slidingWindow = (len(x) - 1) % 7  # useful to shift the grid
    ticks = [(i * 7 + slidingWindow) % len(x) for i in range(howManyLabelsToPlot)]
    # take one tick every 7 days. the " % len(x)" is to make it circular if the last tick is not included

    lastTick = len(x) - 1
    if lastTick not in ticks:
        ticks.append(lastTick)
    # firstTick=0 # from now on, this should be always inserted. Probably i may change the logic above and always insert the first tick. who knows
    # if firstTick not in ticks:
    #    ticks.insert(0,0)
    plt.xticks(ticks,
               rotation="vertical")  # was 45, but with 45 it is not aligned as you may intuitively think when watching the plot
    if save:
        plt.savefig(f'{path}/{title}.png', dpi=SAVE_IMAGE_DPI)
    if not dont_print:
        plt.show()
    plt.clf()


def get_prov_region_mapping(prov_csv_sample, provinces_name, provinces_abbr):
    prov_region_mapping = {}
    for row in prov_csv_sample:
        if "sigla_provincia" in row and row['sigla_provincia'] in provinces_abbr:
            corresponding_prov_name = provinces_name[provinces_abbr.index(row["sigla_provincia"])]
            region_name=row["denominazione_regione"]
            prov_region_mapping[corresponding_prov_name] = region_name
            prov_region_mapping[row["sigla_provincia"]] =  region_name# duplicated to avoid any further pain
    assert len(prov_region_mapping) == 2*len(provinces_name)
    return prov_region_mapping


def get_province_ratio(pop_csv, provinces_name, provinces_abbr, regions, prov_region_mapping):
    assert len(provinces_name) == len(provinces_abbr)  # I assume that they are ordered properly. It sucks, I know
    pop_provs = {}
    pop_regions = {}
    for row in pop_csv:
        if row["Territorio"] in regions and len(row['\ufeff"ITTER107"'])==4: #len=4 identifies a region
            assert row["Territorio"] not in pop_regions
            pop_regions[row["Territorio"]] = int(row["Value"])
        if row["Territorio"] in provinces_name and len(row['\ufeff"ITTER107"'])==5: #len=4 identifies a province
            assert row["Territorio"] not in pop_provs  # shouldn't have been inserted by now
            assert row["Territorio"] in prov_region_mapping
            # but it should be present in the province/region mapping indexed array
            pop_provs[row["Territorio"]] = int(row["Value"])  # save it for long name cities

    assert len(pop_regions) == len(regions)
    for k in pop_provs:
        if k not in provinces_name:
            print("pop_provs",k)
    for k in provinces_name:
        if k not in pop_provs:
            print("provinces_name",k)
    assert len(pop_provs) == len(provinces_name)
    # we got the pop data, I can calculate the ratio
    ratio_per_province = {}
    for p in zip(provinces_abbr, provinces_name):
        region = prov_region_mapping[p[0]]
        pop_r = pop_regions[region]
        pop_p = pop_provs[p[1]]  # it is indexed by long name
        ratio = pop_p / pop_r
        ratio_per_province[p[0]] = ratio
        ratio_per_province[p[1]] = ratio

    #if you want to add some post processing (eg gaussian distribution), add it here

    return ratio_per_province


def estimated_daily_tests_per_province(provinces_abbr, prov_region_mapping,ratio, daily_regs_csv):
    if daily_regs_csv is not None:
        tests_per_province={}
        tests_per_region={}
        for l in daily_regs_csv:
            tests_per_region[l["denominazione_regione"]]=int(l["tamponi"])-int(l["dimessi_guariti"])
        for p in provinces_abbr:
            tests_per_province[p]=tests_per_region[prov_region_mapping[p]]*ratio[p]
        return tests_per_province
    return None




def main():
    starting_date = date(2020, 2, 24)
    ending_date = date.today()
    provinces_name = list()
    provinces_abbr = list()
    with open("provinces.txt", "r") as f:
        for l in f.read().splitlines():
            tmp = l.split(' ')
            provinces_name.append(tmp[0])
            provinces_abbr.append(tmp[1])
            # I would need some kind of a bidirectional map, but it is much harder to handle. I'll stick to this awful method by now
    with open("regions.txt", "r") as f:
        regions = f.read().splitlines()
    pop_csv = get_population_per_territory_csv(provinces_name + regions)
    prov_region_mapping = get_prov_region_mapping(get_provinces_csv(date(2020, 2, 25)), provinces_name, provinces_abbr) # provides the mapping for each province to its region
    ratio = get_province_ratio(pop_csv, provinces_name, provinces_abbr, regions, prov_region_mapping)
    provs_indexed_csv = get_provinces_data_csv_indexed(starting_date, ending_date)
    regs_indexed_csv = get_regions_data_csv_indexed(starting_date, ending_date)
    plotx={}
    ploty={}
    plotx["infect"] = [list() for index in provinces_abbr]
    ploty["infect"] = [list() for index in provinces_abbr]
    plotx["rat"] = [list() for index in provinces_abbr]
    ploty["rat"] = [list() for index in provinces_abbr]
    plotx["tests"] = [list() for index in provinces_abbr]
    ploty["tests"] = [list() for index in provinces_abbr]
    distance = (ending_date - starting_date).days
    for x in range(1, distance):
        d = starting_date + timedelta(days=x)
        tests = estimated_daily_tests_per_province(provinces_abbr, prov_region_mapping, ratio, regs_indexed_csv[d])
        for i, province in enumerate(provinces_abbr):
            newInfections = diff_infections_between_csv(province, provs_indexed_csv[d - timedelta(days=1)],
                                                        provs_indexed_csv[d])
            if newInfections is not None:
                plotx["infect"][i].append(f"{d.day:02}/{d.month:02}")
                ploty["infect"][i].append(newInfections)
                if tests[province] is not None:

                    if newInfections==0:
                        plotx["rat"][i].append(f"{d.day:02}/{d.month:02}")
                        ploty["rat"][i].append(0)
                    else:
                        if tests[province]==0:
                            print(f"This dataset is really unreliable. Zero tests made on day {d} and {newInfections} new infections at {provinces_name[i]} {province}")
                            plotx["rat"][i].append(f"{d.day:02}/{d.month:02}")
                            ploty["rat"][i].append(1 * 100)
                        else:
                            plotx["rat"][i].append(f"{d.day:02}/{d.month:02}")
                            val=newInfections/tests[province]
                            if val>1:
                                print(
                                    f"This dataset is really unreliable. There are more infections than tests made on day {d} at {provinces_name[i]} {province}")
                                val=1
                            elif val<0:
                                print(
                                    f"This dataset is really unreliable. There is a negative variation on day {d} at {provinces_name[i]} {province}")
                                val=0
                            ploty["rat"][i].append(val*100)
                    plotx["tests"][i].append(f"{d.day:02}/{d.month:02}")
                    ploty["tests"][i].append(tests[province])

    total_saves=len(plotx)*len(provinces_name)
    j=0
    for k in plotx:
        print("Saving infection graphs, may require some time...")
        for i,province in enumerate(zip(provinces_name,provinces_abbr)):
            print(f"{int(j/total_saves*100)}% done")
            if k == "infect":
                save_graph(plotx[k][i], ploty[k][i], f'Covid new infections per day in {province[0]} {province[1]}', 'Day', 'New infections')
            elif k == "rat":
                save_graph(plotx[k][i], ploty[k][i], f'Covid estimated growing rate in {province[0]} {province[1]}', 'Day',
                           '% new infections/tests','./Covid_growing_est')
            elif k == "tests":
                save_graph(plotx[k][i], ploty[k][i], f'Covid estimated tests per day in {province[0]} {province[1]}',
                           'Day',
                           'Tests','./Covid_Tests_est')
            j+=1

    print("Done!")


if __name__ == "__main__":
    main()
