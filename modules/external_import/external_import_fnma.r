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
library(pdftools)
library(reticulate)

use_virtualenv(file.path(Sys.getenv('MP_DIR'), '.venv'))

## Load Connection Info ----------------------------------------------------------
load_env()
pg = connect_pg()

# Import ------------------------------------------------------------------

## Get Data
local({

	message('***** Getting FNMA Links')
	
	fnma_links_raw =
		request('https://www.fanniemae.com/research-and-insights/forecast/forecast-monthly-archive') %>%
		req_perform %>%
		resp_body_html %>%
		html_nodes('div.fm-accordion ul li a') %>%
		keep(., \(x) str_detect(html_text(x), 'News Release| Forecast')) %>%
		map_dfr(., \(x) tibble(
			url = str_trim(html_attr(x, 'href')),
			type = case_when(
				str_detect(html_text(x), 'News Release') ~ 'article',
				str_detect(html_text(x), 'Economic Forecast') ~ 'econ_forecast',
				str_detect(html_text(x), 'Housing Forecast') ~ 'housing_forecast',
				TRUE ~ NA_character_
			)
		)) %>%
		na.omit %>%
		mutate(., post2026 = ifelse(cumsum(ifelse(str_detect(url, 'december-2025'), 1, 0)) == 1, 0, 1))

	# Get vdates for 2026
	
	# 2026+: Announcement posts for 2026 and later aren't available. We'll use file modification date instead.
	post2026_by_month =
		fnma_links_raw %>%
		filter(., post2026 == 1) %>%
		mutate(., fname_vdate = as_date(parse_date_time(str_sub(url, -6), 'mY'))) %>%
		pivot_wider(., id_cols = c(fname_vdate), names_from = type, values_from = url)
	
	if(nrow(na.omit(post2026_by_month)) != nrow(post2026_by_month))	stop('Post-2026 data error')

	get_mod_date = \(url) request(paste0('https://www.fanniemae.com', url)) %>%
		req_perform %>%
		resp_body_raw %>%
		pdf_info %>%
		.$modified %>%
		as_date()

	post2026_with_vdates = 
		post2026_by_month %>%
		mutate(
			., 
			econ_mod_vdate = map_vec(econ_forecast, get_mod_date),
			housing_mod_vdate = map_vec(housing_forecast, get_mod_date)
		) %>%
		rowwise() %>%
		mutate(., vdate = max(econ_mod_vdate, housing_mod_vdate)) %>%
		ungroup()
	
	# Confirm filename vdate matches modified time vdate
	if(any(floor_date(post2026_with_vdates$vdate, 'month') != floor_date(post2026_with_vdates$fname_vdate, 'month'))) {
		stop('Post-2026 vdate mismatch')
	}
	
	fnma_links_post2026 = select(post2026_with_vdates, vdate, econ_forecast, housing_forecast)
	
	
	# 2025-: Use announcement posts. Pull last 12 only; change if needed.
	fnma_links_pre2026 = 
		fnma_links_raw %>%
		filter(., post2026 == 0) %>%
		mutate(., group = (1:nrow(.) - 1) %/% 3) %>%
		pivot_wider(., id_cols = group, names_from = type, values_from = url) %>%
		na.omit %>%
		head(12) %>%
		mutate(., vdate = map_vec(article, \(x) {
			
			vdate =
				request(paste0('https://www.fanniemae.com', x)) %>%
				req_perform %>%
				resp_body_html %>%
				html_node(., '.field-space-sm.font-weight-light') %>%
				html_text %>%
				mdy
			
			vdate
		})) %>%
		select(., -article, -group)

	
	fnma_links <<- bind_rows(fnma_links_post2026, fnma_links_pre2026)
})

## Get Data ------------------------------------------------------------------
local({

	message('***** Downloading Fannie Mae Data')

	fnma_dir = file.path(tempdir(), 'fnma')
	fs::dir_create(fnma_dir)

	message('***** FNMA dir: ', fnma_dir)

	fnma_details =
		fnma_links %>%
		df_to_list %>%
		imap(., .progress = T, function(x, i) {

			# if (i %% 10 == 0) message('Downloading ', i)

			request(paste0('https://www.fanniemae.com', x$econ_forecast)) %>%
				req_perform(., path = file.path(fnma_dir, paste0('econ_', x$vdate, '.pdf')))

			request(paste0('https://www.fanniemae.com', x$housing_forecast)) %>%
				req_perform(., path = file.path(fnma_dir, paste0('housing_', x$vdate, '.pdf')))

			tibble(
				vdate = x$vdate,
				econ_forecast_path = normalizePath(file.path(fnma_dir, paste0('econ_', x$vdate, '.pdf'))),
				housing_forecast_path = normalizePath(file.path(fnma_dir, paste0('housing_', x$vdate, '.pdf')))
			)
		}) %>%
		list_rbind(.)

	camelot = import('camelot')

	fnma_clean_macro = imap(df_to_list(fnma_details), function(x, i) {

		message(str_glue('Importing macro {i}'))

		raw_import = camelot$read_pdf(
			x$econ_forecast_path,
			pages = '1',
			flavor = 'stream',
			# Below needed to prevent split wrapping columns correctly https://www.fanniemae.com/media/42376/display
			column_tol = -3.5, # -5
			edge_tol = {
				if (str_detect(x$econ_forecast_path, '2025-09-23')) 30
				else 60
			}, # 60
			# table_areas = list('100, 490, 700, 250')
			)[0]$df %>%
			as_tibble(.)

		col_names =
			df_to_list(raw_import[3, ])[[1]] %>%
			keep(., \(x) x != '' & !is.na(x)) %>%
			map(., \(x) {
				if (str_detect(x, coll('.'))) paste0('20', str_replace(x, coll('.'), 'Q'))
				else x
				}) %>%
			c('varname', .) %>%
			unname(.)

		clean_import =
			raw_import[4:nrow(raw_import), ] %>%
			set_names(., col_names) %>%
			pivot_longer(., -varname, names_to = 'date', values_to = 'value') %>%
			filter(., str_length(date) == 6 & str_detect(date, 'Q') & value != '') %>%
			mutate(
				.,
				varname = case_when(
					str_detect(varname, 'Gross Domestic Product') ~ 'gdp',
					str_detect(varname, 'Personal Consumption') ~ 'pce',
					str_detect(varname, 'Residential Fixed Investment') ~ 'pdir',
					str_detect(varname, 'Business Fixed Investment') ~ 'pdin',
					str_detect(varname, 'Government Consumption') ~ 'govt',
					str_detect(varname, 'Net Exports') ~ 'nx',
					str_detect(varname, 'Change in Business Inventories') ~ 'cbi',
					str_detect(varname, 'Consumer Price Index') & !str_detect(varname, 'Core') ~ 'cpi',
					str_detect(varname, 'PCE Chain Price Index') & !str_detect(varname, 'Core') ~ 'pcepi',
					str_detect(varname, 'Unemployment Rate') ~ 'unemp',
					str_detect(varname, 'Federal Funds Rate') ~ 'ffr',
					str_detect(varname, '1-Year Treasury') ~ 't01y',
					str_detect(varname, '10-Year Treasury') ~ 't10y'
				),
				date = from_pretty_date(date, 'q'),
				value = as.numeric(str_replace_all(value, c(',' = '')))
			) %>%
			na.omit(.) %>%
			transmute(., vdate = as_date(x$vdate), varname, date, value)

		if (length(unique(clean_import$varname)) < 12) stop('Missing variable')

		return(clean_import)
		}) %>%
		list_rbind()

	# fnma_clean_macro %>% count(vdate, varname) %>% pivot_wider(names_from = varname, values_from = n)
	
	fnma_clean_housing = imap(df_to_list(fnma_details), function(x, i) {

		message(str_glue('Importing housing {i}'))

		raw_import = camelot$read_pdf(
			x$housing_forecast_path,
			pages = '1',
			flavor = 'stream',
			column_tol = -1,
			edge_tol = {
				if (str_detect(x$housing_forecast_path, '2025-09-23')) 30
				else 60
			}, # 60
			)[0]$df %>%
			as_tibble(.)

		col_names =
			df_to_list(raw_import[3, ])[[1]] %>%
			str_replace_all(., coll(c('.' = 'Q'))) %>%
			paste0('20', .) %>%
			{ifelse(. == '20', 'varname', .)}

		clean_import =
			raw_import[4:nrow(raw_import), ] %>%
			set_names(., col_names) %>%
			pivot_longer(., -varname, names_to = 'date', values_to = 'value') %>%
			filter(., str_length(date) == 6 & str_detect(date, 'Q') & value != '') %>%
			mutate(
				.,
				varname = case_when(
					str_detect(varname, 'Total Housing Starts') ~ 'houst',
					# str_detect(varname, 'Total Home Sales') ~ 'hsold',
					str_detect(varname, '30-Year') ~ 'mort30y',
					str_detect(varname, '5-Year') ~ 'mort05y' # Dropped 5 year forecast
				),
				date = from_pretty_date(date, 'q'),
				value = as.numeric(str_replace_all(value, c(',' = '')))
			) %>%
			na.omit(.) %>%
			transmute(., vdate = as_date(x$vdate), varname, date, value)
		# As of Oct. 2022, 5 year mortgage forecast is dropped so only requires 2
		if (length(unique(clean_import$varname)) < 2) stop('Missing variable')

		return(clean_import)
		}) %>%
		list_rbind()
	
	fnma_clean_housing %>% count(vdate, varname) %>% pivot_wider(names_from = varname, values_from = n)
	
	fnma_data =
		bind_rows(fnma_clean_macro, fnma_clean_housing) %>%
		transmute(
			.,
			forecast = 'fnma',
			form = 'd1',
			freq = 'q',
			varname,
			vdate,
			date,
			value
		)

	message('***** Missing Variables:')
	message(
		c('gdp', 'pce', 'pdir', 'pdin', 'govt', 'nx', 'cbi', 'cpi', 'pcepi', 'unemp', 'ffr', 't01y', 't10y',
			'houst', 'mort30y') %>%
			keep(., ~ !. %in% unique(fnma_data$varname)) %>%
			paste0(., collapse = ' | ')
		)

	raw_data <<- fnma_data
})


 ## Export Forecast ------------------------------------------------------------------
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
