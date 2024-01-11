# Initialize ----------------------------------------------------------
# If T, adds data to the database that belongs to data vintages already existing in the database.
# Set to F only when there are model updates, new variable pulls, or old vintages are unreliable.
STORE_NEW_ONLY = T
validation_log <<- list()

## Load Libs ----------------------------------------------------------
library(macropredictions)
library(tidyverse)
library(httr2)
library(rvest)
library(pdftools, include.only = 'pdf_text')

## Load Connection Info ----------------------------------------------------------
load_env()
pg = connect_pg()

# Import ------------------------------------------------------------------

## Get Data ------------------------------------------------------------------
local({

	varnames_map = tribble(
		~ varname, ~ fullname,
		'gdp', 'Real GDP',
		'pce', 'Real consumer spending',
		'pdir', 'Residential investment',
		'pdin', 'Nonresidential investment',
		'govt', 'Total gov\'t spending',
		'ex', 'Exports',
		'im', 'Imports',
		'unemp', 'Unemployment rate (%)',
		'cpi', 'Core PCE Inflation (%Y/Y)', # should be pcepi!
		'ffr', 'Fed Funds (%, Midpoint, Period End)'
		) %>%
		mutate(., fullname = str_to_lower(fullname))

	html_content =
		request('https://www.conference-board.org/research/us-forecast/us-forecast') %>%
		req_perform %>%
		resp_body_html

	vintage_date =
		html_content %>%
		html_node(., 'a.downloadBtn') %>%
		html_attr(., 'href') %>%
		paste0('https://www.conference-board.org', .) %>%
		pdf_text() %>%
		str_extract(., 'Updated.*\n') %>%
		.[1] %>%
		str_remove_all(., 'Updated|\\n') %>%
		str_squish(.) %>%
		dmy()

	iframe_src =
		html_content %>%
		html_element(., 'iframe[data-external]') %>%
		html_attr(., 'src') %>%
		str_extract(., "https://datawrapper.dwcdn.net/(.*?)/") # strip off version numbers

	# Redirect page
	redirect_src =
		request(iframe_src) %>%
		req_perform() %>%
		resp_body_html() %>%
		as.character() %>%
		str_match(., "window.location.href='(.*?)'+") %>%
		.[1, 2]

	table_content = paste0(redirect_src, 'dataset.csv') %>% read_tsv(., col_names = F, col_types = 'c')

	# First two rows are headers
	headers_fixed =
		table_content %>%
		.[1:2, ] %>%
		t %>%
		as.data.frame %>%
		as_tibble %>%
		fill(., V1, .direction = 'down') %>%
		mutate(., V2 = str_replace_all(V2, c('IV Q' = 'Q4', 'III Q' = 'Q3', 'II Q' = 'Q2', 'I Q' = 'Q1'))) %>%
		mutate(., col_index = 1:nrow(.), quarter = ifelse(is.na(V1) | is.na(V2), NA, paste0(V1, V2))) %>%
		mutate(., quarter = ifelse(col_index == 1, 'fullname', ifelse(is.na(quarter), paste0('drop_', 1:nrow(.)), quarter))) %>%
		.$quarter

	table_fixed =
		table_content %>%
		tail(., -2) %>%
		set_names(., headers_fixed) %>%
		select(., -contains(coll('*'))) %>%
		select(., -contains('drop'))

	final_data =
		table_fixed %>%
		pivot_longer(., cols = -fullname, names_to = 'date', values_to = 'value') %>%
		mutate(., date = from_pretty_date(date, 'q'), fullname = str_to_lower(fullname)) %>%
		inner_join(., varnames_map, by = 'fullname') %>%
		transmute(
			.,
			forecast = 'cb',
			form = 'd1',
			freq = 'q',
			varname,
			vdate = vintage_date,
			date,
			value
		)

	print(unique(final_data$varname))
	if (length(unique(final_data$varname)) != nrow(varnames_map)) stop('Missing variables!')

	raw_data <<- final_data
})


## Export Forecasts ------------------------------------------------------------------
local({

	# Store in SQL
	model_values = transmute(raw_data, forecast, form, vdate, freq, varname, date, value)

	rows_added = store_forecast_values_v2(pg, model_values, .store_new_only = STORE_NEW_ONLY, .verbose = T)

	# Log
	validation_log$store_new_only <<- STORE_NEW_ONLY
	validation_log$rows_added <<- rows_added
	validation_log$last_vdate <<- max(raw_data$vdate)

	disconnect_db(pg)
})
