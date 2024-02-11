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

## Load Connection Info ----------------------------------------------------------
load_env()
pg = connect_pg()

# Import ------------------------------------------------------------------

## Get Data ------------------------------------------------------------------
local({

	cbo_vintages = map(0:2, function(page)
		request(paste0('https://www.cbo.gov/data/publications-with-data-files?page=', page, '')) %>%
			req_perform %>%
			resp_body_html %>%
			html_nodes('#block-cbo-cbo-system-main div.item-list > ul > li') %>%
			keep(., \(x) str_detect(html_text(html_node(x, 'span.views-field-title')), coll('Economic Outlook'))) %>%
			map_chr(., \(x) html_text(html_node(x, 'div.views-field-field-display-date')))
		) %>%
		unlist %>%
		mdy %>%
		tibble(release_date = .) %>%
		group_by(., month(release_date), year(release_date)) %>%
		summarize(., release_date = min(release_date), .groups = 'drop') %>%
		select(., release_date) %>%
		arrange(., release_date)

	url_params =
		request('https://www.cbo.gov/data/budget-economic-data') %>%
		req_perform %>%
		resp_body_html %>%
		html_nodes('div .view-content') %>%
		.[[9]] %>%
		html_nodes(., 'a') %>%
		map_dfr(., \(x) tibble(date = html_text(x), url = html_attr(x, 'href'))) %>%
		transmute(., date = mdy(paste0(str_sub(date, 1, 3), ' 1 ' , str_sub(date, -4))), url) %>%
		mutate(., date = as_date(date)) %>%
		inner_join(., cbo_vintages %>% mutate(., date = floor_date(release_date, 'months')), by = 'date') %>%
		transmute(., vdate = release_date, url = paste0('https://www.cbo.gov', url))

	cbo_params = tribble(
		~ varname, ~ cbo_category, ~ cbo_name, ~ cbo_units,
		'ngdp', 'Output', 'Gross Domestic Product (GDP)', 'Percentage change, annual rate',
		'gdp', 'Output', 'Real GDP', 'Percentage change, annual rate',
		'pce', 'Components of GDP (Real)', 'Personal Consumption Expenditures', 'Percentage change, annual rate',
		'pdi', 'Components of GDP (Real)', 'Gross Private Domestic Investment', 'Percentage change, annual rate',
		'pdin', 'Components of GDP (Real)', 'Nonresidential fixed investment', 'Percentage change, annual rate',
		'pdir', 'Components of GDP (Real)', 'Residential fixed investment', 'Percentage change, annual rate',
		'cbi', 'Components of GDP (Real)', 'Change in private inventories', 'Billions of chained (2012) dollars',
		'govt', 'Components of GDP (Real)', 'Government Consumption Expenditures and Gross Investment',
		'Percentage change, annual rate',
		'govtf', 'Components of GDP (Real)', 'Federal', 'Percentage change, annual rate',
		'govts', 'Components of GDP (Real)', 'State and local', 'Percentage change, annual rate',
		'nx', 'Components of GDP (Real)', 'Net Exports of Goods and Services', 'Billions of chained (2012) dollars',
		'ex', 'Components of GDP (Real)', 'Exports', 'Percentage change, annual rate',
		'im', 'Components of GDP (Real)', 'Imports', 'Percentage change, annual rate',
		'cpi', 'Prices', 'Consumer Price Index, All Urban Consumers (CPI-U)', 'Percentage change, annual rate',
		'pcepi', 'Prices', 'Price Index, Personal Consumption Expenditures (PCE)',
		'Percentage change, annual rate',
		'oil', 'Prices', 'Price of Crude Oil, West Texas Intermediate (WTI)', 'Dollars per barrel',
		'ffr', 'Interest Rates', 'Federal Funds Rate', 'Percent',
		't03m', 'Interest Rates', '3-Month Treasury Bill', 'Percent',
		't10y', 'Interest Rates', '10-Year Treasury Note', 'Percent',
		'unemp', 'Labor', 'Unemployment Rate, Civilian, 16 Years or Older', 'Percent',
		'lfpr', 'Labor', 'Labor Force Participation Rate, 16 Years or Older', 'Percent'
		)


	cbo_data = imap(df_to_list(url_params), function(x, i) {

		download.file(x$url, file.path(tempdir(), 'cbo.xlsx'), mode = 'wb', quiet = TRUE)

		# Not all spreadsheets start at the same row
		skip_rows =
			suppressMessages(readxl::read_excel(
				file.path(tempdir(), 'cbo.xlsx'),
				sheet = '1. Quarterly',
				skip = 0
			)) %>%
			mutate(., idx = 1:nrow(.)) %>%
			filter(., .[[1]] == 'Output') %>%
			{.$idx - 1}

		if (x$vdate < '2024-02-07') {
			xl =
				suppressMessages(readxl::read_excel(
					file.path(tempdir(), 'cbo.xlsx'),
					sheet = '1. Quarterly',
					skip = skip_rows
				)) %>%
				rename(., cbo_category = 1, cbo_name = 2, cbo_name_2 = 3, cbo_units = 4) %>%
				mutate(., cbo_name = ifelse(is.na(cbo_name), cbo_name_2, cbo_name)) %>%
				select(., -cbo_name_2) %>%
				tidyr::fill(., cbo_category, .direction = 'down') %>%
				tidyr::fill(., cbo_name, .direction = 'down') %>%
				na.omit
		} else {
			xl =
				suppressMessages(readxl::read_excel(
					file.path(tempdir(), 'cbo.xlsx'),
					sheet = '1. Quarterly',
					skip = skip_rows
				))
		}

		xl %>%
			inner_join(., cbo_params, by = c('cbo_category', 'cbo_name', 'cbo_units')) %>%
			select(., -cbo_category, -cbo_name, -cbo_units) %>%
			pivot_longer(., -varname, names_to = 'date') %>%
			mutate(., date = from_pretty_date(date, 'q')) %>%
			filter(., date >= as_date(x$vdate)) %>%
			mutate(., vdate = as_date(x$vdate))

		}) %>%
		list_rbind %>%
		transmute(
			.,
			forecast = 'cbo',
			form = 'd1',
			freq = 'q',
			varname,
			vdate,
			date,
			value
		)

	missing_vars = cbo_params$varname %>% set_names(., .) %>% keep(., function(x) !x %in% cbo_data$varname)

	message('Missing variables...', missing_vars)

	validation_log$missing_variables <<- missing_vars
	raw_data <<- cbo_data
})


## CBO Data ----------------------------------------------------------------
local({

	cbo_vintages = map(0:2, function(page)
		request(paste0('https://www.cbo.gov/data/publications-with-data-files?page=', page, '')) %>%
			req_perform %>%
			resp_body_html %>%
			html_nodes('#block-cbo-cbo-system-main div.item-list > ul > li') %>%
			keep(., \(x) str_detect(html_text(html_node(x, 'span.views-field-title')), coll('Economic Outlook'))) %>%
			map_chr(., \(x) html_text(html_node(x, 'div.views-field-field-display-date')))
		) %>%
		unlist %>%
		mdy %>%
		tibble(release_date = .) %>%
		group_by(., month(release_date), year(release_date)) %>%
		summarize(., release_date = min(release_date), .groups = 'drop') %>%
		select(., release_date) %>%
		arrange(., release_date)

	url_params =
		request('https://www.cbo.gov/data/budget-economic-data') %>%
		req_perform %>%
		resp_body_html %>%
		html_nodes('div .view-content') %>%
		.[[10]] %>%
		html_nodes(., 'a') %>%
		map(., \(x) tibble(date = html_text(x), url = html_attr(x, 'href'))) %>%
		list_rbind() %>%
		transmute(., date = mdy(paste0(str_sub(date, 1, 3), ' 1 ' , str_sub(date, -4))), url) %>%
		mutate(., date = as_date(date)) %>%
		inner_join(., cbo_vintages %>% mutate(., date = floor_date(release_date, 'months')), by = 'date') %>%
		transmute(., vdate = release_date, url = paste0('https://www.cbo.gov', url))


	cbo_params = tribble(
		~ varname, ~ cboname, ~ transform,
		'ngdp', 'gdp', 'apchg',
		'gdp', 'real_gdp', 'apchg',
		'pce', 'pce', 'apchg',
		'pdi', 'real_gross_pri_dom_invest', 'apchg',
		'pdin', 'real_nonres_fixed_invest', 'apchg',
		'pdir', 'real_res_fixed_invest', 'apchg',
		'cbi', 'real_change_pri_invest', 'base',
		'govt', 'real_government_c_gi', 'apchg',
		'govtf', 'real_federal_government_c_gi', 'apchg',
		'govts', 'real_sl_government_c_gi', 'apchg',
		'nx', 'real_net_exports', 'base',
		'ex', 'real_exports', 'apchg',
		'im', 'real_imports', 'apchg',
		'cpi', 'cpiu', 'yoy',
		'pcepi', 'pce_price_index', 'yoy',
		'oil', 'oil_price_wti_spot', 'base',
		'ffr', 'fed_funds_rate', 'base',
		't03m', 'treasury_bill_rate_3mo', 'base',
		't10y', 'treasury_note_rate_10yr', 'base',
		'unemp', 'unemployment_rate', 'base',
		'lfpr', 'lfpr_16yo', 'base'
	)

	cleaned = map(df_to_list(url_params), function(x) {

		if (fs::dir_exists(file.path(tempdir(), 'cbo'))) fs::dir_delete(file.path(tempdir(), 'cbo'))

		download.file(x$url, destfile = file.path(tempdir(), 'cbo.zip'), quiet = T)
		unzip(file.path(tempdir(), 'cbo.zip'), exdir = file.path(tempdir(), 'cbo'), overwrite = T)

		quarterly_csvs =
			fs::dir_ls(file.path(tempdir(), 'cbo'), recurse = T) %>%
			keep(., \(f) str_detect(str_to_lower(f), 'quarter') & !str_detect(str_to_lower(f), 'potential'))

		if (length(quarterly_csvs) != 1) stop('Error, wrong length of matching CSVs')

		cleaned_res =
			read_csv(quarterly_csvs[[1]], col_types = cols(date = col_character(), .default = col_double())) %>%
			filter(., !is.na(date)) %>%
			mutate(., date = from_pretty_date(str_to_upper(date), 'q')) %>%
			select(., c(date, cbo_params$cboname)) %>%
			mutate(
				.,
				across(filter(cbo_params, transform == 'apchg')$cboname, \(x) apchg(x)),
				across(filter(cbo_params, transform == 'yoy')$cboname, \(x) (x/lag(x, 4) - 1) * 100)
				) %>%
			na.omit() %>%
			filter(., date >= floor_date(as_date(x$vdate), 'quarter')) %>%
			pivot_longer(., cols = -date, values_to = 'value', names_to = 'cboname') %>%
			inner_join(., cbo_params, by = 'cboname') %>%
			transmute(., vdate = as_date(x$vdate), date, varname, value)

		fs::dir_delete(file.path(tempdir(), 'cbo'))

		return(cleaned_res)
	})

	cbo_data =
		cleaned %>%
		list_rbind %>%
		transmute(
			.,
			forecast = 'cbo',
			form = 'd1',
			freq = 'q',
			varname,
			vdate,
			date,
			value
		)

	raw_data <<- cbo_data
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
