#' Create vintages in spradsheet
# Initialize ----------------------------------------------------------
VARNAME = 'ffr'

## Load Libs ----------------------------------------------------------
library(macropredictions)
library(tidyverse)
library(httr2)
library(rvest)
load_env()

## Load Data ----------------------------------------------------------
pg = connect_pg()


# Get Data ----------------------------------------------------------
raw_data = get_query(pg, str_glue(
	"SELECT date, vdate, value
	FROM forecast_values_v2
	WHERE
		varname = '{VARNAME}'
		AND form = 'd1'
		AND freq = 'm'
		AND forecast = 'int'
	"
))

# Analysis ----------------------------------------------------------
raw_data %>%
	pivot_wider(., id_cols = vdate, names_from = date, values_from = value) %>%
	select(., all_of(c('vdate', sort(keep(colnames(.), \(x) x != 'vdate'))))) %>%
	arrange(., vdate) %>%
	rename(., forecast_date = vdate) %>%
	write_csv(., str_glue('{VARNAME}-vintages-{today()}.csv'))
