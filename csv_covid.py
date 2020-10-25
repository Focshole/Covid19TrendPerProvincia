import csv
import math
import requests
import matplotlib.pyplot as plt
from datetime import date, timedelta
import concurrent.futures
import sys
import requests_cache
import pathlib

# TODO add some decent comments to this code
# I know, i should use panda for CSVs, cause Fiat Pandas are reliable, but I'm too lazy, sry

SMOOTH_DATA_DAYS_FACTOR = 2  # how many days before and after should be considered to smooth data. if value is set to 1, each value gets smoothed with the day before and the day after
STDDEV_CRISPNESS = 1  # how the smoothness should be. if 1, 68.3% of the values are set considering the central day

MAX_CONNECTIONS = 6  # https://stackoverflow.com/questions/985431/max-parallel-http-connections-in-a-browser
SAVE_IMAGE_DPI = 300  # image saving quality


# Possible future work, model with a markov chain smh
# Markov chain: no_inf -> exp beginning -> stall -> decreasing ->no_inf
#                                        --------->
class Plot:

    def __init__(self):
        self.x = list()
        self.y = list()

    def save_plot(self, title, xlabel, ylabel, path):
        return self.__plot(title, xlabel, ylabel, save=True, show=False, path=path)

    def show_plot(self, title, xlabel, ylabel):
        return self.__plot(title, xlabel, ylabel, save=False, show=True, path=None)

    def __plot(self, title, xlabel, ylabel, save=True, show=False, path=None):
        if type(path) == str and save:
            path = pathlib.Path(path)
        plt.plot(self.x, self.y, linestyle='dashed', linewidth=1, marker='o', markerfacecolor='blue', markersize=2)
        plt.xlabel(xlabel)
        plt.ylabel(ylabel)
        plt.title(title)
        plt.grid(True)

        n_values = len(self.x)
        howManyLabelsToPlot = math.ceil(n_values / 7) # one label per week
        slidingWindow = (n_values-1)%7

        ticks = [ (slidingWindow + i * 7) for i in range(howManyLabelsToPlot)]
        # take one tick every 7 days. the " % len(x)" is to make it circular if the last tick is not included
        lastTick = n_values - 1
        assert lastTick in ticks
        plt.xticks(ticks, rotation="vertical")
        if save:
            plt.savefig(f'{path / title}.png', dpi=SAVE_IMAGE_DPI)
        if show:
            plt.show()
        plt.clf()

    def append(self, x, y):
        self.x.append(x)
        self.y.append(y)





def fdr_norm(value, dev_std, avg=0):  # fdr =
    z = (value - avg) / dev_std  # normalization
    return 0.5 * (1 + math.erf(z / (math.sqrt(2))))  # Cumulative distribution function for norm distr


def data_norm(values, center_index=None):  # we need to normalize data
    assert type(values) == list  # center data with day in position math.floor(len(values)/2)
    if center_index is None:
        center_index = math.floor(len(values) / 2)  # as default it is set to its central value,
        # cause we want to consider data relevance as shown here https://commons.wikimedia.org/wiki/File:Gaussian_Filter.svg
    normalized_value = 0
    coeff_sum = 0
    for i, val in enumerate(values):
        upper_bound = (i - center_index + 0.5)  # useless for last element
        lower_bound = upper_bound - 1  # useless for first element
        if i == len(values) - 1 and i == 0:
            coeff = 1
        elif i == len(values) - 1:  # last
            coeff = 1 - fdr_norm(lower_bound, STDDEV_CRISPNESS)
        elif i == 0:
            coeff = fdr_norm(upper_bound, STDDEV_CRISPNESS)
        else:
            coeff = (fdr_norm(upper_bound, STDDEV_CRISPNESS) - fdr_norm(lower_bound, STDDEV_CRISPNESS))
        normalized_value += coeff * val
        coeff_sum += coeff
    assert coeff_sum > 1 - sys.float_info.epsilon and coeff_sum < 1 + sys.float_info.epsilon
    return normalized_value


def pre_processing(csv_as_a_list):
    rename = ["Ascoli Piceno", "La Spezia", "Reggio Calabria",
              "Reggio Emilia", "Vibo Valentia", "Sud Sardegna", "Friuli-Venezia Giulia", "Friuli Venezia Giulia"]
    for i, row in enumerate(csv_as_a_list):
        if "denominazione_regione" in row:  # Provinces and regions
            region_name = row["denominazione_regione"]
            if "P.A." in region_name:  # Allows me to have a higher precision in those provinces
                csv_as_a_list[i]["denominazione_regione"] = csv_as_a_list[i]["denominazione_regione"].replace("P.A. ",
                                                                                                              "PA-")
            elif region_name == "Valle d'Aosta":
                csv_as_a_list[i]["denominazione_regione"] = "Valle-d-Aosta"
            elif region_name in rename:
                csv_as_a_list[i]["denominazione_regione"] = csv_as_a_list[i]["denominazione_regione"].replace(" ", "-")

        if "Territorio" in row:
            csv_as_a_list[i]['Territorio'] = row['Territorio'].split(' /')[0]  # remove dialect names
            if csv_as_a_list[i]["Territorio"] == "Valle d'Aosta":
                if len(row['\ufeff"ITTER107"']) == 4:  # region
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
                csv_as_a_list[i]["Territorio"] = csv_as_a_list[i]["Territorio"].replace(" ", "-")
                # to avoid problems in file naming
            elif csv_as_a_list[i]["Territorio"] == "Provincia Autonoma Bolzano" or csv_as_a_list[i][
                "Territorio"] == "Provincia Autonoma Trento":
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
        return pre_processing(csv_as_a_list)


fn = ["data", "stato", "codice_regione", "denominazione_regione", "codice_provincia", "denominazione_provincia",
      "sigla_provincia", "lat", "long", "totale_casi", "note_it", "note_en"]


def get_population_per_territory_csv(territory_names):
    url = "https://dati.istat.it/Download.ashx?type=csv&Delimiter=%2c&IncludeTimeSeriesIdentifiers=False&LabelType=CodeAndLabel&LanguageCode=it"
    csv_as_a_list = download_csv(url)
    if csv_as_a_list is None:  # istat services are offline
        # fetch the local file
        with open("DCIS_POPRES1_14102020171844268.csv", "r") as f:
            csv_file_splitted = f.read().splitlines()
        csv_file_reader = csv.DictReader(csv_file_splitted)
        csv_as_a_list = pre_processing(list(csv_file_reader))
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


def get_province_infections_from_csv(c):
    assert type(c) == list
    assert len(c) > 0
    assert type(c[0]) == dict  # I know, it's a dumb assert but it saved me some times
    infections = {}
    for row in c:
        if "sigla_provincia" in row:
            infections[row["sigla_provincia"]] = int(row["totale_casi"])
    return infections


def diff_infections_between_csv(csv0, csv1, provinces_abbr):  # convenient for batch downloads
    assert csv0 is not None and csv1 is not None
    assert type(csv0) == list and type(csv1) == list
    assert len(csv0) > 0
    infections = get_province_infections_from_csv(csv0)
    infections1 = get_province_infections_from_csv(csv1)
    for p in provinces_abbr:
        if p not in infections or p not in infections1:
            infections[p] = None
        else:
            infections[p] = infections1[p] - infections[p]
    return infections


def diff_infections_per_date(d0, d1, provinces_abbr):  # convenient for a few downloads
    assert type(d0) == date and type(d1) == date
    assert d0 < d1
    d0_data = get_provinces_csv(d0)
    d1_data = get_provinces_csv(d1)
    if d0_data is not None and d1_data is not None:
        return diff_infections_between_csv(d0_data, d1_data, provinces_abbr)


def diff_infections_per_day(d, provinces_abbr):  # convenient for a few downloads
    d0 = d - timedelta(days=1)  # d0 = previous day
    return diff_infections_per_date(d0, d, provinces_abbr)


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


def get_prov_region_mapping(prov_csv_sample, provinces_name, provinces_abbr):
    prov_region_mapping = {}
    for row in prov_csv_sample:
        if "sigla_provincia" in row and row['sigla_provincia'] in provinces_abbr:
            corresponding_prov_name = provinces_name[provinces_abbr.index(row["sigla_provincia"])]
            region_name = row["denominazione_regione"]
            prov_region_mapping[corresponding_prov_name] = region_name
            prov_region_mapping[row["sigla_provincia"]] = region_name  # duplicated to avoid any further pain
    assert len(prov_region_mapping) == 2 * len(provinces_name)
    return prov_region_mapping


def get_province_ratio(pop_csv, provinces_name, provinces_abbr, regions, prov_region_mapping):
    assert len(provinces_name) == len(provinces_abbr)  # I assume that they are ordered properly. It sucks, I know
    pop_provs = {}
    pop_regions = {}
    for row in pop_csv:
        if row["Territorio"] in regions and len(row['\ufeff"ITTER107"']) == 4:  # len=4 identifies a region
            assert row["Territorio"] not in pop_regions
            pop_regions[row["Territorio"]] = int(row["Value"])
        if row["Territorio"] in provinces_name and len(row['\ufeff"ITTER107"']) == 5:  # len=4 identifies a province
            assert row["Territorio"] not in pop_provs  # shouldn't have been inserted by now
            assert row["Territorio"] in prov_region_mapping
            # but it should be present in the province/region mapping indexed array
            pop_provs[row["Territorio"]] = int(row["Value"])  # save it for long name cities

    assert len(pop_regions) == len(regions)
    assert len(pop_provs) == len(provinces_name)
    # we got the pop data, I can calculate the ratio
    ratio_per_province = {}
    for p_abb, p_name in zip(provinces_abbr, provinces_name):
        region = prov_region_mapping[p_abb]
        pop_r = pop_regions[region]
        pop_p = pop_provs[p_name]  # it is indexed by long name
        ratio = pop_p / pop_r
        ratio_per_province[p_abb] = ratio
        ratio_per_province[p_name] = ratio

    # if you want to add some post processing (eg gaussian distribution), add it here

    return ratio_per_province


def estimated_cumulative_tests_per_province(provinces_abbr, prov_region_mapping, ratio, daily_regs_csv):
    if daily_regs_csv is not None:
        tests_per_province = {}
        tests_per_region = {}
        for lin in daily_regs_csv:
            tests_per_region[lin["denominazione_regione"]] = int(lin["tamponi"])
        for p in provinces_abbr:
            tests_per_province[p] = tests_per_region[prov_region_mapping[p]] * ratio[p]
        return tests_per_province
    return None


def estimated_daily_tests_per_province(provinces_abbr, prov_region_mapping, ratio, daily_regs_csv0, daily_regs_csv):
    tests_per_province = {}
    t0 = estimated_cumulative_tests_per_province(provinces_abbr, prov_region_mapping, ratio, daily_regs_csv0)
    t = estimated_cumulative_tests_per_province(provinces_abbr, prov_region_mapping, ratio, daily_regs_csv)
    for p in provinces_abbr:
        tests_per_province[p] = t[p] - t0[p]
    return tests_per_province


def main():
    starting_date = date(2020, 2, 24)
    ending_date = date.today()
    provinces_name = list()
    provinces_abbr = list()
    with open("provinces.txt", "r") as f:
        for lin in f.read().splitlines():
            tmp = lin.split(' ')
            provinces_name.append(tmp[0])
            provinces_abbr.append(tmp[1])
    with open("regions.txt", "r") as f:
        regions = f.read().splitlines()
    pop_csv = get_population_per_territory_csv(provinces_name + regions)
    prov_region_mapping = get_prov_region_mapping(get_provinces_csv(date(2020, 2, 25)), provinces_name,
                                                  provinces_abbr)  # provides the mapping for each province to its region
    ratio = get_province_ratio(pop_csv, provinces_name, provinces_abbr, regions, prov_region_mapping)
    provs_indexed_csv = get_provinces_data_csv_indexed(starting_date, ending_date)
    regs_indexed_csv = get_regions_data_csv_indexed(starting_date, ending_date)
    plots = {}
    categories = ["infect", "infects per tests", "tests", "infect_n", "infects per tests_n", "tests_n"]

    for c in categories:
        plots[c] = {p: Plot() for p in provinces_abbr}

    # for normalization
    distance = (ending_date - starting_date).days
    window_size = SMOOTH_DATA_DAYS_FACTOR * 2 + 1  # smooth data over
    infection_window = list()
    test_window = list()
    last_index = distance - 2
    central_element_index = math.floor(window_size / 2)

    for x in range(distance - 1):
        d0 = starting_date + timedelta(days=x)
        d = starting_date + timedelta(days=x + 1)
        tests = estimated_daily_tests_per_province(provinces_abbr, prov_region_mapping, ratio, regs_indexed_csv[d0],
                                                   regs_indexed_csv[d])
        newInfections = diff_infections_between_csv(provs_indexed_csv[d0], provs_indexed_csv[d], provinces_abbr)

        # window management
        test_window.append(tests)
        if len(test_window) > window_size:
            test_window.pop(0)
        infection_window.append(newInfections)
        if len(infection_window) > window_size:
            infection_window.pop(0)
        for p_a, p_n in zip(provinces_abbr, provinces_name):
            # classical plots
            formatted_date_str = f"{d.day:02}/{d.month:02}"
            if newInfections[p_a] is not None:
                plots["infect"][p_a].append(formatted_date_str, newInfections[p_a])
                if tests[p_a] is not None:

                    if newInfections[p_a] == 0:
                        plots["infects per tests"][p_a].append(formatted_date_str, 0)
                    else:
                        if tests[p_a] == 0:  # I know, it's bad
                            plots["infects per tests"][p_a].append(formatted_date_str, 100)
                        else:

                            val = newInfections[p_a] / tests[p_a]
                            if val > 1:  # I know, it's bad
                                val = 1
                            elif val < 0:  # I know, it's bad
                                val = 0
                            plots["infects per tests"][p_a].append(formatted_date_str, val * 100)
                    plots["tests"][p_a].append(formatted_date_str, tests[p_a])

            # normalized plots
            province_inf_window = [ni[p_a] for ni in infection_window]
            # todo refactor this sh1t
            province_test_window = [ni[p_a] for ni in test_window]
            # if the window had been filled for the first time
            if x == window_size - 1:
                skipped_days = math.ceil(window_size / 2)  # days still not registered
                for sd in range(skipped_days):
                    iterator_d = d - timedelta(days=skipped_days - (sd + 1))
                    formatted_date_str = f"{iterator_d.day:02}/{iterator_d.month:02}"
                    inf_val = data_norm(province_inf_window, sd)
                    plots["infect_n"][p_a].append(formatted_date_str, inf_val)

                    test_val = data_norm(province_test_window, sd)
                    plots["tests_n"][p_a].append(formatted_date_str, test_val)

                    if test_val == 0 and inf_val > 0:  # it is bad, i know
                        rat_val = 100
                    elif test_val == 0 and inf_val <= 0:  # adjustments
                        rat_val = 0
                    elif inf_val / test_val > 1:  # still impossible, but it may happen with imprecise data
                        rat_val = 100
                    else:
                        rat_val = inf_val / test_val * 100
                    plots["infects per tests_n"][p_a].append(formatted_date_str, rat_val)

            elif x >= window_size and x + central_element_index <= last_index:  #
                inf_val = data_norm(province_inf_window)
                plots["infect_n"][p_a].append(formatted_date_str, inf_val)

                test_val = data_norm(province_test_window)
                plots["tests_n"][p_a].append(formatted_date_str, test_val)

                if test_val == 0 and inf_val > 0:  # it is bad, i know
                    rat_val = 100
                elif test_val == 0 and inf_val <= 0:  # adjustments
                    rat_val = 0
                elif inf_val / test_val > 1:  # still impossible, but it may happen with imprecise data
                    rat_val = 100
                else:
                    rat_val = inf_val / test_val * 100
                plots["infects per tests_n"][p_a].append(formatted_date_str, rat_val)
            elif x + central_element_index > last_index:  # emptying the window, it is less precise
                index = central_element_index + x
                inf_val = data_norm(province_inf_window, index)
                plots["infect_n"][p_a].append(formatted_date_str, inf_val)
                test_val = data_norm(province_test_window, index)
                plots["tests_n"][p_a].append(formatted_date_str, test_val)

                if test_val == 0 and inf_val > 0:  # it is bad, i know
                    rat_val = 100
                elif test_val == 0 and inf_val <= 0:  # adjustments
                    rat_val = 0
                elif inf_val / test_val > 1:  # still impossible, but it may happen with imprecise data
                    rat_val = 100
                else:
                    rat_val = inf_val / test_val * 100
                plots["infects per tests_n"][p_a].append(formatted_date_str, rat_val)

    total_saves = len(plots) * len(provinces_name)
    j = 0
    print("Saving infection graphs, may require some time...")
    path = pathlib.Path(__file__).parent
    for k in plots:
        for i, (p_name, p_abbr) in enumerate(zip(provinces_name, provinces_abbr)):
            sys.stdout.write(f"\r{int(j / total_saves * 100)}% done")
            sys.stdout.flush()
            if k == "infect":
                plots[k][p_abbr].save_plot(f'Covid new infections per day in {p_name} {p_abbr}',
                                           'Day', 'New infections', path / 'Covid')
            elif k == "infect_n":
                plots[k][p_abbr].save_plot(f'Covid new infections per day in {p_name} {p_abbr} normalized',
                                           'Day', 'New infections', path / 'Covid_n')
            elif k == "infects per tests":
                plots[k][p_abbr].save_plot(f'Covid infections per tests in {p_name} {p_abbr}',
                                           'Day', '% new infections/tests', path / 'Covid_infection_per_test_est')
            elif k == "infects per tests_n":
                plots[k][p_abbr].save_plot(f'Covid infections per tests in {p_name} {p_abbr} normalized',
                                           'Day', '% new infections/tests', path / 'Covid_infection_per_test_est_n')
            elif k == "tests":
                plots[k][p_abbr].save_plot(f'Covid estimated tests per day in {p_name} {p_abbr}',
                                           'Day', 'Tests', path / 'Covid_Tests_est')
            elif k == "tests_n":
                plots[k][p_abbr].save_plot(f'Covid estimated tests per day in {p_name} {p_abbr} normalized', 'Day',
                                           'Tests', path / 'Covid_Tests_est_n')
            j += 1

    print("\nDone!")


if __name__ == "__main__":
    requests_cache.install_cache('req_cache.sqlite')
    main()
