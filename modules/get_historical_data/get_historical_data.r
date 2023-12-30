#' Get historical data for all variables in the forecast_variables table.
#'
#' Should run after composite inf model for accurate vintage dates

# Initialize ----------------------------------------------------------
IMPORT_DATE_START = '2007-01-01'
validation_log <<- list() # Stores logging data when called via controller.r
data_dump <<- list() # Stores logging data when called via controller.r

## Load Libs ----------------------------------------------------------'
library(macropredictions)
library(tidyverse)
library(data.table)
library(httr2)
library(rvest)
library(roll)

## Load Connection Info ----------------------------------------------------------
load_env()
pg = connect_pg()

releases = list()
hist = list()

## Load Variable Defs ----------------------------------------------------------
variable_params = get_query(pg, 'SELECT * FROM forecast_variables')
release_params = get_query(pg, 'SELECT * FROM forecast_hist_releases')

# Release Data ----------------------------------------------------------

## 1. Get Data Releases ----------------------------------------------------------
local({

	message(str_glue('*** Getting Releases History | {format(now(), "%H:%M")}'))
	api_key = Sys.getenv('FRED_API_KEY')

	fred_releases = list_rbind(map(df_to_list(filter(release_params, source == 'fred')), function(x) {
		request(str_glue(
			'https://api.stlouisfed.org/fred/release/dates?',
			'release_id={x$source_key}&realtime_start=2010-01-01',
			'&include_release_dates_with_no_data=true&api_key={api_key}&file_type=json'
			)) %>%
			req_retry(max_tries = 10) %>%
			req_perform() %>%
			resp_body_json() %>%
			.$release_dates %>%
			{tibble(release = x$id, date = sapply(., function(y) y$date))}
	}))

	releases$raw$fred <<- fred_releases
})

## 2. Combine ----------------------------------------------------------
local({
	releases$final <<- bind_rows(releases$raw)
})

## 3. SQL ----------------------------------------------------------
local({

	initial_count = get_rowcount(pg, 'forecast_hist_release_dates')
	message('***** Initial Count: ', initial_count)

	sql_result =
		releases$final %>%
		write_df_to_sql(pg, ., 'forecast_hist_release_dates', 'ON CONFLICT (release, date) DO NOTHING')

	final_count = get_rowcount(pg, 'forecast_hist_release_dates')
	message('***** Rows Added: ', final_count - initial_count)
})

# Historical Data ----------------------------------------------------------
## 1. FRED ----------------------------------------------------------
local({

	message(str_glue('*** Importing FRED Data | {format(now(), "%H:%M")}'))
	api_key = Sys.getenv('FRED_API_KEY')

	fred_data = list_rbind(imap(df_to_list(filter(variable_params, hist_source == 'fred')), function(x, i) {

		# message(str_glue('**** Pull {i}: {x$varname}'))

		res =
			get_fred_obs_with_vintage(
				list(c(x$hist_source_key, x$hist_source_freq)),
				api_key,
				.obs_start = IMPORT_DATE_START,
				.verbose = F
			) %>%
			transmute(., varname = x$varname, freq = x$hist_source_freq, date, vdate = vintage_date, value) %>%
			filter(., date >= as_date(IMPORT_DATE_START), vdate >= as_date(IMPORT_DATE_START)) %>%
			# Fix for EFFR
			mutate(., vdate = as_date(ifelse(
				varname == 'ffr' & vdate == '2016-03-01',
				get_next_fed_business_day(date),
				vdate
				)))

		message(str_glue('**** Count: {nrow(res)}'))
		return(res)

	}))

	hist$raw$fred <<- fred_data
})

## 2. TREAS ----------------------------------------------------------
local({

	message(str_glue('*** Importing Treasury Data | {format(now(), "%H:%M")}'))

	treasury_data =
		get_treasury_yields() %>%
		pivot_wider(., id_cols = c(date), names_from = varname, values_from = value) %>%
		mutate(
			.,
			t10yt02yspread = t10y - t02y,
			t10yt03mspread = t10y - t03m
		) %>%
		pivot_longer(., cols = -date, names_to = 'varname', values_to = 'value', values_drop_na = T) %>%
		transmute(
			.,
			varname,
			freq = 'd',
			date,
			vdate = date,
			value
		) %>%
		filter(., varname %in% filter(variable_params, hist_source == 'treas')$varname) %>%
		filter(., date >= as_date(IMPORT_DATE_START), vdate >= as_date(IMPORT_DATE_START))

	hist$raw$treas <<- treasury_data
})

## 3. Yahoo Finance ----------------------------------------------------------
local({

	message(str_glue('*** Importing Yahoo Finance Data | {format(now(), "%H:%M")}'))

	yahoo_data = list_rbind(map(df_to_list(filter(variable_params, hist_source == 'yahoo')), function(x) {

		get_yahoo_data(x$hist_source_key, .obs_start = IMPORT_DATE_START) %>%
			transmute(
				.,
				varname = x$varname,
				freq = x$hist_source_freq,
				date = as_date(date),
				vdate = date,
				value = as.numeric(value)
			)
	}))

	hist$raw$yahoo <<- yahoo_data
})

## 4. BLOOM  ----------------------------------------------------------
local({

	message(str_glue('*** Importing Bloom Data | {format(now(), "%H:%M")}'))

	bloom_data = list_rbind(map(df_to_list(filter(variable_params, hist_source == 'bloom')), function(x) {

		res =
			request(paste0(
				'https://www.bloomberg.com/markets2/api/history/', x$hist_source_key, '%3AIND/PX_LAST?',
				'timeframe=5_YEAR&period=daily&volumePeriod=daily'
			)) %>%
			# add_standard_headers %>%
			list_merge(., headers = list(
				'Host' = 'www.bloomberg.com',
				'Referer' = str_glue('https://www.bloomberg.com/quote/{x$source_key}:IND'),
				'User-Agent' = 'PostmanRuntime/7.32.2'
				)) %>%
			req_perform %>%
			resp_body_json %>%
			.[[1]] %>%
			.$price %>%
			map(., \(x) as_tibble(x)) %>%
			list_rbind %>%
			transmute(
				.,
				varname = x$varname,
				freq = 'd',
				date = as_date(dateTime),
				vdate = date,
				value
			) %>%
			na.omit

		# Add sleep due to bot detection
		Sys.sleep(runif(1, 5, 10))

		return(res)
	}))

	hist$raw$bloom <<- bloom_data
})

## 5. AFX  ----------------------------------------------------------
local({

	afx_data =
		request('https://us-central1-ameribor.cloudfunctions.net/api/rates') %>%
		req_perform %>%
		resp_body_json %>%
		keep(., \(x) all(c('date', 'ON', '1M', '3M', '6M', '1Y', '2Y') %in% names(x))) %>%
		map(., function(x)
			as_tibble(x) %>%
				select(., all_of(c('date', 'ON', '1M', '3M', '6M', '1Y', '2Y'))) %>%
				mutate(., across(-date, function(x) as.numeric(x)))
		) %>%
		list_rbind %>%
		mutate(., date = ymd(date)) %>%
		pivot_longer(., -date, names_to = 'varname_scrape', values_to = 'value') %>%
		inner_join(
			.,
			select(filter(variable_params, hist_source == 'afx'), varname, hist_source_key),
			by = c('varname_scrape' = 'hist_source_key')
		) %>%
		distinct(.) %>%
		transmute(., varname, freq = 'd', date, vdate = date, value)

	hist$raw$afx <<- afx_data
})

## 6. ECB ---------------------------------------------------------------------
local({

	estr_data =
		request('https://data.ecb.europa.eu/data-detail-api/EST.B.EU000A2X2A25.WT') %>%
		req_perform %>%
		resp_body_json %>%
		map(., \(x) {if (is.null(x$OBS)) NULL else tibble(date = as_date(x$PERIOD), value = as.numeric(x$OBS))}) %>%
		compact %>%
		list_rbind %>%
		transmute(., varname = 'estr', freq = 'd', date, vdate = date, value)

	hist$raw$ecb <<- estr_data
})



## 7. BOE ---------------------------------------------------------------------
local({

	# Bank rate
	boe_keys = tribble(
		~ varname, ~ url,
		'ukbankrate', 'https://www.bankofengland.co.uk/boeapps/database/Bank-Rate.asp'
	)

	boe_data = list_rbind(map(df_to_list(boe_keys), function(x)
		request(x$url) %>%
			req_perform %>%
			resp_body_html %>%
			# content(., 'parsed', encoding = 'UTF-8') %>%
			html_node(., '#stats-table') %>%
			html_table(.) %>%
			set_names(., c('date', 'value')) %>%
			mutate(., date = dmy(date)) %>%
			arrange(., date) %>%
			filter(., date >= as_date('2010-01-01')) %>%
			# Fill in missing dates - to the latter of yesterday or max available date in dataset
			left_join(tibble(date = seq(min(.$date), to = max(max(.$date), today('GMT') - days(1)), by = '1 day')), ., by = 'date') %>%
			mutate(., value = zoo::na.locf(value)) %>%
			transmute(., varname = x$varname, freq = 'd', date, value)
		)) %>%
		transmute(
			.,
			varname = 'ukbankrate',
			freq = 'd',
			date,
			vdate = date,
			value
		)

	hist$raw$boe <<- boe_data
})

## 8. Calculated Variables ----------------------------------------------------------
local({

	message('*** Adding Calculated Variables')

	# For each vintage date, pull all CPI values from the latest available vintage for each date
	# This intelligently handles revisions and prevents
	# duplications of CPI values by vintage dates/obs date
	cpi_df =
		hist$raw$fred %>%
		filter(., varname == 'cpi' & freq == 'm') %>%
		select(., date, vdate, value)

	cpi_data =
		cpi_df %>%
		group_split(., vdate) %>%
		map(., function(x)
			filter(cpi_df, vdate <= x$vdate[[1]]) %>%
				group_by(., date) %>%
				filter(., vdate == max(vdate)) %>%
				ungroup(.) %>%
				arrange(., date) %>%
				transmute(date, vdate = roll::roll_max(vdate, 13), value = 100 * (value/lag(value, 12) - 1))
		) %>%
		list_rbind %>%
		na.omit %>%
		distinct(., date, vdate, value) %>%
		transmute(
			.,
			varname = 'cpi',
			freq = 'm',
			date,
			vdate,
			value
		)

	pcepi_df =
		hist$raw$fred %>%
		filter(., varname == 'pcepi' & freq == 'm') %>%
		select(., date, vdate, value)

	pcepi_data =
		pcepi_df %>%
		group_split(., vdate) %>%
		map(., function(x)
			filter(pcepi_df, vdate <= x$vdate[[1]]) %>%
				group_by(., date) %>%
				filter(., vdate == max(vdate)) %>%
				ungroup(.) %>%
				arrange(., date) %>%
				transmute(date, vdate = roll::roll_max(vdate, 13), value = 100 * (value/lag(value, 12) - 1))
		) %>%
		list_rbind %>%
		na.omit %>%
		distinct(., date, vdate, value) %>%
		transmute(
			.,
			varname = 'pcepi',
			freq = 'm',
			date,
			vdate,
			value
		)

	## Calculate rffr and real Treasury yields by difference the expected inflation model
	inf_curve = get_query(pg, "SELECT vdate, ttm, value FROM composite_inflation_yield_curves")

	real_yields =
		hist$raw$treas %>%
		filter(., freq == 'd' & varname %in% c('t03m', 't06m', 't01y', 't02y', 't05y', 't10y', 't20y', 't30y')) %>%
		mutate(., ttm = as.numeric(str_sub(varname, 2, 3)) * ifelse(str_sub(varname, 4, 4) == 'y', 12, 1)) %>%
		select(., varname, date, vdate, value, ttm) %>%
		inner_join(
			.,
			transmute(inf_curve, ttm, inf_vdate = vdate, inf_value = value),
			join_by(ttm, closest(vdate >= inf_vdate))
			) %>%
		transmute(
			.,
			varname = paste0('r', varname),
			freq = 'd',
			date,
			vdate,
			value = value - inf_value
			)

	real_spreads =
		real_yields %>%
		pivot_wider(., id_cols = c(date, vdate), names_from = varname, values_from = value) %>%
		mutate(., rt10yrt02yspread = rt10y - rt02y) %>%
		transmute(
			.,
			varname = 'rt10yrt02yspread',
			freq = 'd',
			date,
			vdate,
			value = rt10yrt02yspread
		)

	real_effr =
		hist$raw$fred %>%
		filter(., varname == 'ffr' & freq == 'd') %>%
		inner_join(
			.,
			transmute(filter(inf_curve, ttm == 1), inf_vdate = vdate, inf_value = value),
			join_by(closest(vdate >= inf_vdate))
		) %>%
		transmute(
			.,
			varname = 'rffr',
			freq = 'd',
			date,
			vdate,
			value = value - inf_value
		)

	## Mortgage spread
	mortgage_spread =
		hist$raw$fred %>%
		filter(., varname == 'mort30y' & freq == 'w') %>%
		mutate(., end_date = date + days(6)) %>%
		inner_join(
			.,
			hist$raw$treas %>%
				filter(., varname == 't10y') %>%
				transmute(., t_date = date, t_vdate = vdate, t_value = value)
				,
			join_by(end_date >= t_date, date <= t_date, vdate >= t_vdate)
		) %>%
		group_by(., varname, freq, date, vdate) %>%
		summarize(., value = mean(value - t_value), .groups = 'drop') %>%
		transmute(
			.,
			varname = 'mort30yt10yspread',
			freq,
			date,
			vdate,
			value = value
		)

	hist_calc = bind_rows(cpi_data, pcepi_data, real_yields, real_spreads, real_effr, mortgage_spread)


	hist$raw$calc <<- hist_calc
})

## 9. Verify ----------------------------------------------------------
local({

	missing_varnames = variable_params$varname[!variable_params$varname %in% unique(bind_rows(hist$raw)$varname)]

	message('*** Missing Variables: ')
	cat(paste0(missing_varnames, collapse = '\n'))
})


## 10. Aggregate Frequencies ----------------------------------------------------------
local({

	message(str_glue('*** Aggregating Monthly & Quarterly Data | {format(now(), "%H:%M")}'))

	hist_agg_0 =
		hist$raw$calc %>%
		bind_rows(
			.,
			filter(hist$raw$fred, !varname %in% unique(.$varname)),
			filter(hist$raw$treas, !varname %in% unique(.$varname)),
			filter(hist$raw$yahoo, !varname %in% unique(.$varname)),
			filter(hist$raw$bloom, !varname %in% unique(.$varname)),
			filter(hist$raw$afx, !varname %in% unique(.$varname)),
			filter(hist$raw$ecb, !varname %in% unique(.$varname)),
			filter(hist$raw$boe, !varname %in% unique(.$varname))
		)

	monthly_agg =
		# Get all daily/weekly varnames with pre-existing data
		hist_agg_0 %>%
		filter(., freq %in% c('d', 'w')) %>%
		# Add in month of date
		mutate(., this_month = lubridate::floor_date(date, 'month')) %>%
		as.data.table(.) %>%
		# For each variable, for each month, create a dataframe of all month obs across all vintages
		# Then for each vintage date, take the latest vintage date for every obs in the month and
		# calculate the rolling mean
		split(., by = c('this_month', 'varname')) %>%
		lapply(., function(x)
			x %>%
				dcast(., vdate ~ date, value.var = 'value') %>%
				.[order(vdate)] %>%
				.[, colnames(.) := lapply(.SD, function(x) zoo::na.locf(x, na.rm = F)), .SDcols = colnames(.)] %>%
				# {data.table(vdate = .$vdate, value = rowMeans(.[, -1], na.rm = T))} %>%
				melt(., id.vars = 'vdate', value.name = 'value', variable.name = 'input_date', na.rm = T) %>%
				.[, input_date := as_date(input_date)] %>%
				.[, list(value = mean(value, na.rm = T), count = .N), by = 'vdate'] %>%
				.[, c('varname', 'date') := list(x$varname[[1]], date = x$this_month[[1]])]
		) %>%
		rbindlist(.) %>%
		as_tibble(.) %>%
		transmute(., varname, freq = 'm', date, vdate, value)

	# Works similarly as monthly aggregation but does not create new quarterly data unless all
	# 3 monthly data points are available -this can still result in premature quarterly calcs if the last
	# month value has been agrgegated before that last month was finished
	quarterly_agg =
		monthly_agg %>%
		bind_rows(., filter(hist_agg_0, freq == 'm')) %>%
		mutate(., this_quarter = lubridate::floor_date(date, 'quarter')) %>%
		as.data.table(.) %>%
		split(., by = c('this_quarter', 'varname')) %>%
		lapply(., function(x)
			x %>%
				dcast(., vdate ~ date, value.var = 'value') %>%
				.[order(vdate)] %>%
				.[, colnames(.) := lapply(.SD, function(x) zoo::na.locf(x, na.rm = F)), .SDcols = colnames(.)] %>%
				melt(., id.vars = 'vdate', value.name = 'value', variable.name = 'input_date', na.rm = T) %>%
				.[, input_date := as_date(input_date)] %>%
				.[, list(value = mean(value, na.rm = T), count = .N), by = 'vdate'] %>%
				.[, c('varname', 'date') := list(x$varname[[1]], date = x$this_quarter[[1]])] %>%
				.[count == 3]
		) %>%
		rbindlist(.) %>%
		as_tibble(.) %>%
		transmute(., varname, freq = 'q', date, vdate, value)

	hist_agg =
		bind_rows(hist_agg_0, monthly_agg, quarterly_agg) %>%
		filter(., freq %in% c('m', 'q'))

	hist$agg <<- hist_agg
})


# Split & Transform ----------------------------------------------------------

## 1. Split By Vintage Date ----------------------------------------------------------
local({

	message(str_glue('*** Splitting By Vintage Date | {format(now(), "%H:%M")}'))

	# Check min dates
	message('***** Variables Dates:')
	hist$agg %>%
		group_by(., varname) %>%
		summarize(., min_dt = min(date)) %>%
		arrange(., desc(min_dt)) %>%
		print(., n = 5)

	# Get table indicating which variables can have their historical data be revised well after the fact
	# For these variables, not necessary to build out a full history for each vdate for later stationary transforms
	# This saves significant time
	# Only include variables which definitely have final vintage data within 30 days of original data
	revisable =
		variable_params %>%
		mutate(
			.,
			hist_revisable =
			   	!dispgroup %in% c('Interest_Rates', 'Stocks_and_Commodities') &
				!hist_source_freq %in% c('d', 'w')
			) %>%
		select(., varname, hist_revisable) %>%
		as.data.table(.)

	last_obs_by_vdate =
		hist$agg %>%
		as.data.table(.) %>%
		split(., by = c('varname', 'freq')) %>%
		lapply(., function(x)  {

			# message(str_glue('**** Getting last vintage dates for {x$varname[[1]]}'))
			# For every vintage date within last 6 months, get last observation for every past date
			# This creates lots of duplicates, especially for monthly variables - clean them later
			# after doing stationary transforms
			last_obs_for_all_vdates =
				x %>%
				.[order(vdate)] %>%
				dcast(., varname + freq + vdate ~  date, value.var = 'value') %>%
				.[, colnames(.) := lapply(.SD, function(x) zoo::na.locf(x, na.rm = F)), .SDcols = colnames(.)] %>%
				melt(
					.,
					id.vars = c('varname', 'freq', 'vdate'),
					value.name = 'value',
					variable.name = 'date',
					na.rm = T
				) %>%
				.[, date := as_date(date)] %>%
				# For non revisable table, only minimal data is  needed for calculating stationary transformations
				merge(., revisable, by = 'varname', keep.x = T) %>%
				.[hist_revisable == T | (hist_revisable == F & vdate <= date + days(60))] %>%
				select(., -hist_revisable)
			return(last_obs_for_all_vdates)
		}) %>%
		rbindlist(.)

	hist$base <<- last_obs_by_vdate
})

## 2. Add Stationary Transformations ----------------------------------------------------------
local({

	message(str_glue('*** Adding Stationary Transforms | {format(now(), "%H:%M")}'))

	# Microbenchmark @ 1.8s per 100k rows
	stat_final =
		copy(hist$base) %>%
		merge(., variable_params[, c('varname', 'd1', 'd2')], by = c('varname'), all = T) %>%
		melt(
			.,
			id.vars = c('varname', 'freq', 'vdate', 'date', 'value'),
			variable.name = 'form',
			value.name = 'transform'
			) %>%
		.[transform != 'none'] %>%
		.[order(varname, freq, form, vdate, date)] %>%
		.[,
			value := frollapply(
				value,
				n = 2,
				FUN = function(z) { # Z is a vector of length equal to the window size, asc order
					if (transform[[1]] == 'base') z[[2]]
					else if (transform[[1]] == 'log') log(z[[2]])
					else if (transform[[1]] == 'dlog') log(z[[2]]/z[[1]])
					else if (transform[[1]] == 'diff1') z[[2]] - z[[1]]
					else if (transform[[1]] == 'pchg') (z[[2]]/z[[1]] - 1) * 100
					else if (transform[[1]] == 'apchg') ((z[[2]]/z[[1]])^{if (freq[[1]] == 'q') 4 else 12} - 1) * 100
					else stop ('Error')
					},
				fill = NA
				),
			by = c('varname', 'freq', 'form', 'vdate')
			] %>%
		.[, transform := NULL] %>%
		bind_rows(., copy(hist$base)[, form := 'base']) %>%
		na.omit(.)

	stat_final_last =
		stat_final %>%
		group_by(., varname) %>%
		mutate(., max_vdate = max(vdate)) %>%
		filter(., vdate == max(vdate)) %>%
		select(., -max_vdate) %>%
		ungroup(.) %>%
		as.data.table(.)

	hist$flat <<- stat_final
	hist$flat_last <<- stat_final_last
})

## 3. Create Monthly/Quarterly Matrices ----------------------------------------------------------
local({

	# message(str_glue('*** Creating Wide Matrices | {format(now(), "%H:%M")}'))
	#
	# wide =
	# 	hist$flat %>%
	# 	split(., by = 'freq', keep.by = F) %>%
	# 	lapply(., function(x)
	# 		split(x, by = 'form', keep.by = F) %>%
	# 			lapply(., function(y)
	# 				split(y, by = 'vdate', keep.by = T) %>%
	# 					lapply(., function(z) {
	# 						# message(z$vdate[[1]])
	# 						dcast(select(z, -vdate), date ~ varname, value.var = 'value') %>%
	# 							.[, date := as_date(date)] %>%
	# 							.[order(date)] %>%
	# 							as_tibble(.)
	# 					})
	# 			)
	# 	)
	#
	# wide_last <<- lapply(wide, function(x) lapply(x, function(y) tail(y, 1)[[1]]))
	#
	# hist$wide <<- wide
	# hist$wide_last <<- wide_last
})

## 4. Strip Duplicates ---------------------------------------------------------------------
local({

	message(str_glue('*** Stripping Vintage Date Dupes | {format(now(), "%H:%M")}'))

	hist_stripped =
		copy(hist$flat) %>%
		# Rleid changes when value changes (by vdate)
		.[order(vdate, date), value_runlength := rleid(value), by = c('varname', 'freq', 'date', 'form')] %>%
		# Get first rleid only for each group -
		.[, .SD[which.min(vdate)], by = c('varname', 'freq', 'date', 'form', 'value_runlength')] %>%
		.[, value_runlength := NULL]

	hist$flat_final <<- hist_stripped
})

# Finalize ----------------------------------------------------------------

## 1. SQL ----------------------------------------------------------------
local({

	message(str_glue('*** Sending Historical Data to SQL: {format(now(), "%H:%M")}'))

	# Store in SQL
	hist_values =
		hist$flat_final %>%
		select(., vdate, form, freq, varname, date, value)

	rows_added = store_forecast_hist_values_v2(pg, hist_values, .verbose = T)

	# Log
	validation_log$rows_added <<- rows_added
	validation_log$last_vdate <<- max(hist$flat_final$vdate)
	validation_log$missing_varnames <<- variable_params$varname[!variable_params$varname %in% unique(hist$flat_final$varname)]
	validation_log$rows_pulled <<- nrow(hist$flat_final)

	disconnect_db(pg)
})
