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
