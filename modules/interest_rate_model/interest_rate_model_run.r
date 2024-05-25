#' Historical data lags used for vintage assignments (val 12/24/23)
#'
#' Assuming run at 9:30am ET:
#' - FFR/SOFR: 1 day lag (updated next-day at 9am ET)
#' - Treasury data: 1 day lag (publishes same-day at 3pm ET)
#' - Bloomberg indices: day of/realtime (but still assume 1 day lag, see hist BLOOM sec)
#' - AFX indices: 1 day lag (updated next-day at 8am ET)
#' - BOE: 1 day lag
#' - Futures: 1 day lag: (updated same-day at 3pm ET)

# Initialize ----------------------------------------------------------
BACKTEST_MONTHS = 12 # T-AFNSv3 is limited to 2016 due to FFR_LT variable
STORE_NEW_ONLY = T # Generally T; only F if there's a reason to believe that past vdates are incorrect

validation_log <<- list()

## Load Libs ----------------------------------------------------------
library(macropredictions)
library(tidyverse)
library(httr2)
library(rvest)
library(data.table)
load_env()

## Load Data ----------------------------------------------------------
pg = connect_pg()

hist <- list()
submodels <- list()

## Load Variable Defs ----------------------------------------------------------
input_sources = get_query(pg, 'SELECT * FROM interest_rate_model_variables')

# Historical Data ----------------------------------------------------------

## FRED ----------------------------------------------------------
local({

	# Get historical data with latest release date
	# Note that only a single vintage date is pulled (last available value for each data point).
	# This assumes for rate historical data there are no revisions
	message('***** Importing FRED Data')
	api_key = Sys.getenv('FRED_API_KEY')

	fred_data =
		filter(input_sources, hist_source == 'fred') %>%
		df_to_list	%>%
		map(., \(x) c(x$hist_source_key, x$hist_source_freq)) %>%
		get_fred_obs_with_vintage(., api_key, .obs_start = '2005-01-01', .verbose = F) %>%
		left_join(
			.,
			select(input_sources, 'varname', 'hist_source_key'),
			by = c('series_id' = 'hist_source_key'),
			relationship = 'many-to-one'
			) %>%
		select(., -series_id) %>%
		rename(., vdate = vintage_date)

	hist$fred <<- fred_data
})

## TREAS ----------------------------------------------------------
local({

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
			vdate = date + days(1),
			freq = 'd',
			varname,
			date,
			value
		) %>%
		filter(., varname %in% filter(input_sources, hist_source == 'treas')$varname) %>%
		arrange(., vdate)

	hist$treasury <<- treasury_data
})

## BLOOM  ----------------------------------------------------------
local({

	# Note: fingerprinting is heavily UA based
	r1 =
		request('https://www.bloomberg.com') %>%
		list_merge(., headers = list(
			'User-Agent' = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0'
		)) %>%
		req_perform()

	if (str_detect(resp_body_string(r1), 'Are you a robot?')) stop(paste0('Bot detection - ', r1$url))

	r1_cookies =
		r1 %>%
		resp_headers() %>%
		imap(., \(x, i) if (i == 'set-cookie') str_extract(x, '^[^;]*;') else NULL) %>%
		compact %>%
		unlist() %>%
		paste0(., collapse = ' ')


	bloom_data =
		filter(input_sources, hist_source == 'bloom') %>%
		df_to_list	%>%
		map(., function(x) {

			req = request(str_glue(
				'https://www.bloomberg.com/markets2/api/history/{x$hist_source_key}%3AIND/PX_LAST?',
				'timeframe=5_YEAR&period=daily&volumePeriod=daily'
				))

			res =
				req %>%
				req_headers(
					'Host' = 'www.bloomberg.com',
					'User-Agent' = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0',
					'Cookie' = r1_cookies
				) %>%
				req_retry(max_tries = 5) %>%
				req_perform %>%
				resp_body_json %>%
				.[[1]] %>%
				.$price %>%
				map(., \(row) as_tibble(row)) %>%
				list_rbind %>%
				transmute(
					.,
					varname = x$varname,
					freq = 'd',
					date = as_date(dateTime),
					vdate = date + days(1),
					value
					) %>%
				na.omit

			Sys.sleep(runif(10, 10, 20))

			return(res)
		}) %>%
		list_rbind

	hist$bloom <<- bloom_data
})

## AFX  ----------------------------------------------------------
local({

	afx_data =
		request('https://us-central1-ameribor.cloudfunctions.net/api/rates') %>%
		req_retry(max_tries = 5) %>%
		req_perform %>%
		resp_body_json %>%
		keep(., ~ all(c('date', 'ON', '1M', '3M', '6M', '1Y', '2Y') %in% names(.))) %>%
		map(., function(x)
			as_tibble(x) %>%
				select(., all_of(c('date', 'ON', '1M', '3M', '6M', '1Y', '2Y'))) %>%
				mutate(., across(-date, \(y) as.numeric(y)))
			) %>%
		list_rbind %>%
		mutate(., date = ymd(date)) %>%
		pivot_longer(., -date, names_to = 'varname_scrape', values_to = 'value') %>%
		inner_join(
			.,
			select(filter(input_sources, hist_source == 'afx'), varname, hist_source_key),
			by = c('varname_scrape' = 'hist_source_key')
			) %>%
		distinct(.) %>%
		transmute(., vdate = date + days(1), varname, freq = 'd', date, value)

	hist$afx <<- afx_data
})


## BOE  ----------------------------------------------------------
local({

	boe_keys = tribble(
		~ varname, ~ url,
		'sonia',
		str_glue(
			'https://www.bankofengland.co.uk/boeapps/database/fromshowcolumns.asp?',
			'Travel=NIxAZxSUx&FromSeries=1&ToSeries=50&DAT=RNG',
			'&FD=1&FM=Jan&FY=2017',
			'&TD=31&TM=Dec&TY={year(today("GMT"))}',
			'&FNY=Y&CSVF=TT&html.x=66&html.y=26&SeriesCodes=IUDSOIA&UsingCodes=Y&Filter=N&title=IUDSOIA&VPD=Y'
			),
		'ukbankrate', 'https://www.bankofengland.co.uk/boeapps/database/Bank-Rate.asp'
	)

	boe_data = lapply(df_to_list(boe_keys), function(x)
		request(x$url) %>%
			req_retry(max_tries = 5) %>%
			req_perform %>%
			resp_body_html %>%
			html_node(., '#stats-table') %>%
			html_table(.) %>%
			set_names(., c('date', 'value')) %>%
			mutate(., date = dmy(date)) %>%
			arrange(., date) %>%
			filter(., date >= as_date('2010-01-01')) %>%
			# Fill in missing dates - to the latter of yesterday or max available date in dataset
			left_join(
				tibble(date = seq(min(.$date), to = max(max(.$date), today('GMT') - days(1)), by = '1 day')),
				.,
				by = 'date'
				) %>%
			mutate(., value = zoo::na.locf(value)) %>%
			transmute(., vdate = date + days(1), varname = x$varname, freq = 'd', date, value)
		) %>%
		bind_rows(.)

	 hist$boe <<- boe_data
})

## Yahoo -----------------------------------------------------------
local({

	obs =
		get_yahoo_data('^GSPC') %>%
		mutate(., value = (value/dplyr::lag(value, 90) - 1) * 100) %>%
		na.omit() %>%
		transmute(., vdate = date + days(1), varname = 'sp500tr', date = date, freq = 'd', value)

	hist$yahoo <<- obs
})

## Store in SQL ----------------------------------------------------------
local({

	message('**** Storing SQL Data')

	hist_values = bind_rows(hist)
	#%>%
		# bind_rows(
		# 	.,
		# 	filter(., freq %in% c('d', 'w')) %>%
		# 		mutate(., date = floor_date(date, 'months'), freq = 'm') %>%
		# 		group_by(., varname, freq, date) %>%
		# 		summarize(., value = mean(value), .groups = 'drop')
		# 	) %>%
		# # vdate is the same as date
		# mutate(., vdate = date, form = 'd1')

	# initial_count = get_rowcount(pg, 'interest_rate_model_input_values')
	# message('***** Initial Count: ', initial_count)
	#
	# sql_result = write_df_to_sql(
	# 	pg,
	# 	hist_values,
	# 	'interest_rate_model_input_values',
	# 	'ON CONFLICT (vdate, form, varname, freq, date) DO UPDATE SET value=EXCLUDED.value'
	# )
	#
	# message('***** Rows Added: ', get_rowcount(pg, 'interest_rate_model_input_values') - initial_count)

	hist_values <<- hist_values
})

# Sub-Models  ----------------------------------------------------------

## FFR/SOFR/BSBY  ----------------------------------------------------------
local({

	backtest_vdates = seq(
		from = today('US/Eastern') %m-% months(BACKTEST_MONTHS + 12),
		to = today('US/Eastern'),
		by = '1 day'
	)

	# Get prev day's closes, current day usually missing data
	# Need to get an extra 12 months of back-history to satisfy TDNS regression reqs
	futures_df = get_query(pg, str_glue(
		"SELECT varname, tradedate, expdate AS date, tenor, value
		FROM interest_rate_model_futures_values
		WHERE
			(
				varname IN ('bsby', 'ffr')
				OR (varname IN ('sofr') AND tenor = '3m')
			)
			AND is_final IS TRUE
			AND scrape_source IN ('bc')
			AND tradedate >= '{min_tradedate}'",
		min_tradedate = backtest_vdates[1] - days(7) - years(1)
	))

	# Want to create a dataframe with months out starting at 0 and until max available data (or 120 for ffr)
	raw_data =
		expand_grid(vdate = backtest_vdates, varname = c('bsby', 'ffr', 'sofr'), months_out = 0:120) %>%
		mutate(., date = floor_date(vdate, 'month') %m+% months(months_out), min_tradedate = vdate - days(7)) %>%
		left_join(
			.,
			futures_df,
			join_by(varname, date, closest(vdate >= tradedate), min_tradedate <= tradedate)
			) %>%
		# select(., vdate, varname, months_out, date, value) %>%
		mutate(., max_months_out = max(ifelse(is.na(value), 0, months_out)), .by = c(vdate, varname)) %>%
		# Keep vdate x varnames with at least some non-NA values
		filter(., max_months_out != 0) %>%
		# Keep month-forward forecasts if leq max_months_out (or for ffr, keep through 60 months min)
		filter(
			(varname == 'ffr' & months_out <= ifelse(max_months_out <= 120, 120, max_months_out) ) |
				(varname != 'ffr' & months_out <= max_months_out)
			)

	# Backfill start-0 values with historical data if there's no forecast (BSBY & SOFR issues)
	hist_daily = filter(hist_values, freq == 'd' & varname %in% c('ffr', 'bsby', 'sofr'))

	# Get existing same-month mean values for each vdate
	hist_by_vdate =
		distinct(hist_daily, varname, vdate) %>%
		mutate(., date = floor_date(vdate, 'month')) %>%
		inner_join(
			.,
			transmute(hist_daily, varname, trailing_vdate = vdate, value, date = floor_date(date, 'month')),
			join_by(date, varname, vdate >= trailing_vdate)
			) %>%
		group_by(., varname, vdate, date) %>%
		summarize(., month_avg = mean(value), .groups = 'drop')

	filled_data =
		raw_data %>%
		left_join(
			.,
			transmute(hist_by_vdate, varname, hist_vdate = vdate, date, months_out = 0, month_avg),
			join_by(varname, months_out, date, closest(vdate >= hist_vdate))
			) %>%
		mutate(., value = ifelse(is.na(value), month_avg, value)) %>%
		# Some vdate/varname/dates still empty, so join in last available data as well even if not in the same month
		left_join(
			.,
			transmute(
				hist_daily, varname, months_out = 0,
				hist_d_date = date, hist_d_vdate = vdate, hist_d_value = value
				),
			join_by(varname, months_out, date >= hist_d_date, closest(vdate >= hist_d_vdate))
		) %>%
		# Needed in case there are different hist_d_dates with same hist_d_vdate
		slice_max(., hist_d_date, by = c(vdate, varname, date)) %>%
		mutate(., value = ifelse(is.na(value), hist_d_value, value)) %>%
		select(., vdate, varname, months_out, date, value, max_months_out)

	# Check if any vdate is missing data
	filled_data %>%
		filter(., months_out == 0 & is.na(value)) %>%
		summarize(., missing = sum(is.na(value)), .by = 'varname')

	# Interp ff to length == 20
	ffr_lt_raw_data =
		get_fred_obs('FEDTARMDLR', Sys.getenv('FRED_API_KEY')) %>%
		transmute(lt_vdate = date, lt = value)

	ffr_fix =
		filled_data %>%
		filter(., varname == 'ffr' & months_out == 72 & max_months_out < 72) %>%
		left_join(., ffr_lt_raw_data, join_by(closest(vdate >= lt_vdate))) %>%
		mutate(., value = lt) %>%
		select(., -lt, -lt_vdate)

	cleaned_data = bind_rows(
		anti_join(filled_data, ffr_fix, join_by(vdate, varname, months_out, date)),
		ffr_fix
		)

	# Smooth
	smoothed_data =
		cleaned_data %>%
		group_by(., vdate, varname) %>%
		mutate(., value = ifelse(is.na(value), zoo::na.approx(value), value)) %>%
		# Smooth all values more than 2 years out due to low liquidity
		mutate(., value_smoothed = zoo::rollmean(value, 4, fill = NA, align = 'right')) %>%
		# %m+% handles leap years and imaginary days, regular addition of years does not
		mutate(., value = ifelse(!is.na(value_smoothed) & date >= vdate %m+% months(24), value_smoothed, value)) %>%
		ungroup(.) %>%
		filter(., months_out <= 60) %>%
		transmute(
			.,
			varname,
			freq = 'm',
			vdate,
			date,
			value
		)

	# Print diagnostics
	smoothed_data %>%
		filter(., vdate == max(vdate)) %>%
		pivot_wider(., id_cols = 'date', names_from = 'varname', values_from = 'value') %>%
		arrange(., date)

	series_chart =
		smoothed_data %>%
		filter(., vdate == max(vdate), .by = varname) %>%
		ggplot() +
		geom_line(aes(x = date, y = value, color = varname, group = varname)) +
		facet_grid(rows = vars(varname))

	print(series_chart)

	submodels$cme <<- smoothed_data
})

## SONIA/BR  ----------------------------------------------------------
local({

	backtest_vdates = seq(
		from = today('US/Eastern') %m-% months(BACKTEST_MONTHS),
		to = today('US/Eastern'),
		by = '1 day'
	)

	# vdate in futures table represents trade dates, not pull dates!
	futures_df = get_query(pg, str_glue(
		"SELECT varname, tradedate, expdate AS date, tenor, value
		FROM interest_rate_model_futures_values
		WHERE
			scrape_source = 'bc'
			AND varname IN ('sonia', 'estr')
			AND is_final IS TRUE
			AND tradedate >= '{min_tradedate}'",
		min_tradedate = backtest_vdates[1] - days(7)
	))

	raw_data_0 =
		futures_df %>%
		# Now use only one-month futures if it's the only available data
		group_by(varname, tradedate, date) %>%
		filter(., tenor == min(tenor)) %>%
		arrange(., date) %>%
		ungroup %>%
		# Get rid of forecasts for old observations
		filter(., date >= floor_date(tradedate, 'month')) %>%
		transmute(., varname, tradedate, date, value)

	raw_data =
		expand_grid(vdate = backtest_vdates, varname = c('sonia', 'estr'), months_out = 0:120) %>%
		mutate(., date = floor_date(vdate, 'month') %m+% months(months_out), min_tradedate = vdate - days(7)) %>%
		left_join(raw_data_0, join_by(varname, date, closest(vdate >= tradedate), min_tradedate <= tradedate)) %>%
		mutate(., max_months_out = max(ifelse(is.na(value), 0, months_out)), .by = c(vdate, varname)) %>%
		# Keep vdate x varnames with at least some non-NA values
		filter(., max_months_out != 0) %>%
		# Keep month-forward forecasts if leq max_months_out (or for ffr, keep through 60 months min)
		filter(., months_out <= max_months_out)

	# Backfill start-0 values with historical data if there's no forecast
	hist_daily = filter(hist_values, freq == 'd' & varname %in% c('sonia', 'estr'))

	# Get existing same-month mean values for each vdate, and the prev-month final values
	hist_by_vdate =
		distinct(hist_daily, varname, vdate) %>%
		mutate(., date = floor_date(vdate, 'month')) %>%
		inner_join(
			.,
			transmute(hist_daily, varname, trailing_vdate = vdate, value, date = floor_date(date, 'month')),
			join_by(date, varname, vdate >= trailing_vdate)
		) %>%
		group_by(., varname, vdate, date) %>%
		summarize(., month_avg = mean(value), .groups = 'drop')

	filled_data =
		raw_data %>%
		left_join(
			.,
			transmute(hist_by_vdate, varname, hist_vdate = vdate, date, months_out = 0, month_avg),
			join_by(varname, months_out, date, closest(vdate >= hist_vdate))
		) %>%
		mutate(., value = ifelse(is.na(value), month_avg, value)) %>%
		# Some are still empty, so join in last available data as well even if not in the same month
		left_join(
			.,
			transmute(
				hist_daily, varname, months_out = 0,
				hist_d_date = date, hist_d_vdate = vdate, hist_d_value = value
			),
			join_by(varname, months_out, date >= hist_d_date, closest(vdate >= hist_d_vdate))
		) %>%
		# Needed in case there are different hist_d_dates with same hist_d_vdate
		slice_max(., hist_d_date, by = c(vdate, varname, date)) %>%
		mutate(., value = ifelse(is.na(value), hist_d_value, value)) %>%
		select(., vdate, varname, months_out, date, value, max_months_out)

	# Check if any vdate is missing data
	filled_data %>%
		filter(., months_out == 0 & is.na(value)) %>%
		summarize(., missing = sum(is.na(value)), .by = 'varname')

	ice_data =
		filled_data %>%
		group_by(., vdate, varname) %>%
		mutate(., value = ifelse(is.na(value), zoo::na.approx(value), value)) %>%
		ungroup(.) %>%
		filter(., months_out <= 60) %>%
		transmute(
			.,
			varname,
			freq = 'm',
			vdate,
			date,
			value
		)

	## Now calculate BOE Bank Rate
	spread_df =
		hist$boe %>%
		filter(freq == 'd') %>%
		pivot_wider(., id_cols = c(vdate, date), names_from = varname, values_from = value) %>%
		mutate(., spread = sonia - ukbankrate)

	spread_df %>%
		ggplot(.) +
		geom_line(aes(x = date, y = spread))

	# Monthly baserate forecasts (daily forecasts can be calculated later; see ENG section)
	ukbankrate =
		ice_data %>%
		filter(., varname == 'sonia') %>%
		expand_grid(., tibble(lag = 0:10)) %>%
		mutate(., lagged_vdate = vdate - days(lag)) %>%
		left_join(
			.,
			spread_df %>% transmute(., lagged_vdate = vdate, spread),
			by = 'lagged_vdate'
		) %>%
		group_by(., varname, freq, vdate, date, value)  %>%
		summarize(., trailing_lag_mean = mean(spread, na.rm = T), .groups = 'drop') %>%
		mutate(
			.,
			varname = 'ukbankrate',
			value = value - trailing_lag_mean
		) %>%
		select(., varname, freq, vdate, date, value)

	final_df = bind_rows(
		ice_data,
		ukbankrate
	)

	series_chart =
		final_df %>%
		filter(., vdate == max(vdate)) %>%
		ggplot() +
		geom_line(aes(x = date, y = value, color = varname, group = varname))

	print(series_chart)

	submodels$ice <<- final_df
})

## T-AFNSv3 ------------------------------------------------------------------
# https://www.bis.org/publ/qtrpdf/r_qt1809h.htm
# Uses spot forward risk-free rates instead of risk-free curve.
# Risk-free curve generates forecasts that are "too" smooth.
# Vintage date min is bounded by FFRLT series which begins in 2012

### 1. Get Input data ----------------------------------------------------------
local({

	message('***** Adding Treasury Forecasts')
	message('****** Getting input data')

	# Subtract a year  for EH forecasts backtests, less for others
	earliest_ffr = min(filter(submodels$cme, varname == 'ffr')$vdate)
	backtest_vdates_eh = seq(
		from = max(earliest_ffr, today('US/Eastern') %m-% months(BACKTEST_MONTHS + 12)),
		to = today('US/Eastern'),
		by = '1 day'
	)

	backtest_vdates = seq(
		from = max(earliest_ffr, today('US/Eastern') %m-% months(BACKTEST_MONTHS)),
		to = today('US/Eastern'),
		by = '1 day'
	)

	# Create tibble mapping tyield_3m to 3, tyield_1y to 12, etc.
	ttm_varname_map =
		input_sources %>%
		filter(., str_detect(varname, '^t\\d{2}[m|y]$')) %>%
		select(., varname) %>%
		mutate(., ttm = as.numeric(str_sub(varname, 2, 3)) * ifelse(str_sub(varname, 4, 4) == 'y', 12, 1)) %>%
		arrange(., ttm)

	fred_data =
		hist_values %>%
		filter(., str_detect(varname, '^t\\d{2}[m|y]$') | varname == 'ffr')

	ffr_d_obs = fred_data %>% filter(., varname == 'ffr')
	treas_d_obs = fred_data %>% filter(., varname != 'ffr')

	tdns <<- list()
	tdns$backtest_vdates_eh <<- backtest_vdates_eh
	tdns$backtest_vdates <<- backtest_vdates
	tdns$ttm_varname_map <<- ttm_varname_map
	tdns$ffr_d_obs <<- ffr_d_obs
	tdns$treas_d_obs <<- treas_d_obs
})

### 2. Riskless Forecasts ------------------------------------------------------
local({

	message('****** Adding riskless forecasts')

	ffr_lt_obs = get_fred_obs('FEDTARMDLR', Sys.getenv('FRED_API_KEY')) %>% transmute(lt_vdate = date, lt = value)

	rf_annualized =
		submodels$cme %>%
		filter(., vdate %in% tdns$backtest_vdates_eh & varname == 'ffr') %>%
		transmute(., vdate, value = 100 * ((1 + value/100/360)^360 - 1), date) %>%
		arrange(vdate) %>%
		mutate(., months_out = interval(floor_date(vdate, 'month'), date) %/% months(1))

	rf_plot =
		bind_rows(
			tdns$ffr_d_obs %>%
				mutate(., type = 'hist', vdate = date, period = 0) %>%
				mutate(., value = zoo::rollmax(value, 7, fill = NA, align = 'right')),
			rf_annualized %>%
				filter(., vdate == floor_date(vdate, 'month') & month(vdate) %% 3 == 0) %>%
				mutate(., type = 'forecast', period = cur_group_id(), .by = vdate),
			get_query(pg, sql(
				"SELECT vdate, varname, date, d1 AS value, 'survey' AS type
				FROM forecast_values_v2_all
				WHERE forecast = 'spf' AND varname IN ('t03m')"
			)) %>%
			arrange(., date) %>%
			filter(., date >= vdate %m-% months(1)) %>%
			mutate(., period = cur_group_id(), .by = vdate)
		) %>%
		filter(., vdate >= tdns$backtest_vdates_eh[[1]]) %>%
		ggplot() +
		geom_line(aes(x = date, y = value, color = type, group = period, linetype = type, linewidth = type)) +
		scale_color_manual(
			labels = c(hist = 'Historical Data', forecast = 'Forecast', survey = 'Survey'),
			values = c(hist = 'black', forecast = 'red', survey = 'blue')) +
		scale_linetype_manual(
			labels = c(hist = 'Historical Data', forecast = 'Forecast', survey = 'Survey'),
			values = c(hist = 'solid', forecast = 'dotted', survey = 'dotted')
		) +
		scale_linewidth_manual(
			labels = c(hist = 'Historical Data', forecast = 'Forecast', survey = 'Survey'),
			values = c(hist = 1.5, forecast = .75, survey = .9)
		) +
		ggthemes::theme_igray() +
		labs(title = '5-year fed funds forecasts', color = NULL, x = NULL, y = NULL) +
		theme(legend.position = 'none')

	print(rf_plot)

	# FFR performance testing
	if (F) {
		perf_df_0 =
			bind_rows(
				# Get riskless-rate forecasts at quarterly intervals
				rf_annualized %>%
					mutate(., date = floor_date(date, 'month')) %>%
					summarize(., value = mean(value), .by = c(date, vdate)) %>%
					group_split(., vdate) %>%
					# Aggregate to quarterly - may need historical data joined in
					map(., .progress = T, function(df) {
						# Get previous data - assume ffr vintage date lag of 1 day
						bind_rows(
							tdns$ffr_d_obs %>%
								filter(., date < df$vdate[[1]]) %>%
								mutate(., date = floor_date(date, 'month')) %>%
								summarize(., value = mean(value), .by = c(date)) %>%
								filter(., date <= df$date[[1]]),
							df
							) %>%
							mutate(., date = floor_date(date, 'quarter')) %>%
							summarize(., value = mean(value), .by = date) %>%
							filter(., date >= floor_date(df$vdate[[1]], 'quarter')) %>%
							mutate(., vdate = df$vdate[[1]]) %>%
							mutate(., type = 'futures')
					}) %>%
					list_rbind(),
				get_query(pg, sql(
					"SELECT vdate, date, d1 AS value
						FROM forecast_values_v2_all
						WHERE forecast = 'spf' AND varname IN ('t03m')"
					)) %>%
					mutate(., vdate) %>%
					filter(., date >= floor_date(vdate, 'quarter')) %>%
					mutate(., type = 'spf'),
				get_query(pg, sql(
					"SELECT vdate, date, d1 AS value
						FROM forecast_values_v2_all
						WHERE forecast = 'fnma' AND varname IN ('ffr')"
					)) %>%
					mutate(., vdate = floor_date(vdate, 'month')) %>%
					filter(., date >= floor_date(vdate, 'quarter')) %>%
					mutate(., type = 'fnma')
			)

		perf_df =
			expand_grid(vdate = unique(perf_df_0$vdate), type = unique(perf_df_0$type)) %>%
			left_join(., rename(perf_df_0, release_vdate = vdate), join_by(type, closest(vdate >= release_vdate))) %>%
			inner_join(
				.,
				tdns$ffr_d_obs %>%
					mutate(., date = floor_date(date, 'quarter')) %>%
					summarize(., hist = mean(value), .by = date),
				by = 'date'
			) %>%
			mutate(
				.,
				weeks_out = interval(vdate, ceiling_date(date, 'quarter')) %/% days(7),
				data_lag = interval(release_vdate, vdate) %/% days(1)
				)

		perf_df %>%
			mutate(., error = value - hist) %>%
			arrange(., vdate, date) %>%
			group_by(., type, weeks_out) %>%
			summarize(
				.,
				average_data_delay = mean(data_lag),
				mae = mean(abs(error)),
				q80e = quantile(abs(error), .8),
				q20e = quantile(abs(error), .2),
				n = n(),
				.groups = 'drop'
				) %>%
			filter(., n >= 1 & weeks_out <= 80 & weeks_out >= 0) %>%
			# pivot_wider(., id_cols = weeks_out, names_from = type, values_from = c(mae, n)) %>%
			ggplot() +
			geom_ribbon(
				aes(x = weeks_out, ymax = q80e, ymin = q20e, fill = type, group = type),
				alpha = .25
				) +
			geom_line(aes(x = weeks_out, y = mae, color = type, group = type)) +
			geom_point(aes(x = weeks_out, y = mae, color = type, group = type)) +
			scale_color_manual(
				labels = c(fnma = 'Blue Chip', futures = 'Futures-Derived', spf = 'Survey of Professional Forecasters'),
				values = c(fnma = 'blue', futures = 'forestgreen', spf = 'red'),
			) +
			scale_fill_manual(
				labels = c(fnma = 'Blue Chip', futures = 'Futures-Derived', spf = 'Survey of Professional Forecasters'),
				values = c(fnma = 'blue', futures = 'forestgreen', spf = 'salmon'),
			) +
			scale_x_reverse() +
			ggthemes::theme_igray() +
			labs(x = 'Weeks until end forecast value is realized', y = 'MAE', color = 'Forecast', fill = 'Forecast')

		perf_df %>%
			mutate(., error = value - hist) %>%
			arrange(., vdate, date) %>%
			group_by(., type, weeks_out) %>%
			summarize(
				.,
				lag = median(data_lag),
				mae = mean(abs(error)),
				mape = mean(abs(error)/abs(value)),
				n = n(),
				.groups = 'drop'
			) %>%
			filter(., n >= 1 & weeks_out <= 36 & weeks_out >= 0) %>%
			mutate(
				.,
				lag = as.character(lag),
				across(c(mae, mape), \(x) ifelse(x == min(x), paste0('\\textbf{', round(x, 3),'}'), round(x, 3))),
				.by = 'weeks_out'
			) %>%
			pivot_wider(., id_cols = weeks_out, names_from = c(type), values_from = c(lag, mae, mape)) %>%
			select(
				.,
				weeks_out,
				lag_fnma, mae_fnma, mape_fnma,
				lag_spf, mae_spf, mape_spf,
				lag_futures, mae_futures, mape_futures
				) %>%
			arrange(., weeks_out) %>%
			xtable::xtable(., digits = c(0, 0, 0, 4, 4, 0, 4, 4, 0, 4, 4)) %>%
			print(., include.rownames = F, include.colnames = F, hline.after = NULL, sanitize.text.function=\(x) x)
	}

	tdns$rf_annualized <<- rf_annualized
})


### 3. TDNS fit ------------------------------------------------------------------
local({

	message('****** Adding expectations forecasts')

	# Get available-vintage data
	# Get available historical data at each vintage date, up to 36 months of history
	hist_df_unagg =
		tibble(vdate = tdns$backtest_vdates_eh) %>%
		mutate(., floor_vdate = floor_date(vdate, 'month'), min_date = floor_vdate %m-% months(36)) %>%
		left_join(
			.,
			bind_rows(tdns$ffr_d_obs, tdns$treas_d_obs) %>%
				transmute(
					.,
					varname, value, date,
					floor_date = floor_date(date, 'month'), hist_vdate = vdate
				),
			join_by(vdate >= hist_vdate, min_date <= date)
		) %>%
		mutate(., is_current_month = floor_vdate == floor_date) %>%
		select(., -min_date, -floor_vdate)

	# Aggregate monthly using latest data value for each month
	hist_df_0 =
		hist_df_unagg %>%
		group_split(., is_current_month) %>%
		lapply(., function(df)
			if (df$is_current_month[[1]] == TRUE) {
				# If current month, use last available date
				df %>%
					group_by(., vdate, varname) %>%
					filter(., date == max(date)) %>%
					ungroup(.) %>%
					select(., vdate, varname, date, value) %>%
					mutate(., date = floor_date(date, 'months'))
			} else {
				# Otherwise use monthly average for history
				df %>%
					mutate(., date = floor_date) %>%
					group_by(., vdate, varname, date) %>%
					summarize(., value = mean(value), .groups = 'drop')
			}
		) %>%
		bind_rows(.) %>%
		arrange(., vdate, varname, date)

	# Create TDNS training dataset on spread
	hist_df =
		hist_df_0 %>%
		inner_join(., tdns$ttm_varname_map, by = 'varname') %>%
		inner_join(
			.,
			hist_df_0 %>%
				filter(., varname == 'ffr') %>%
				transmute(., date, ffr_vdate = vdate, ffr = value),
			join_by(date, closest(vdate >= ffr_vdate)) # Always equality in practice
		) %>%
		mutate(., value = value - ffr) %>%
		select(., -ffr)

	# Filter to only last 4 months
	train_df =
		hist_df %>%
		mutate(., months_ago = interval(floor_date(date, 'month'), vdate) %/% months(1)) %>%
		filter(., months_ago <= 4)

	# Find MSE-minimizing lambda value by vintage date
	optim_lambdas =
		train_df %>%
		group_split(., vdate) %>%
		map(., .progress = T, function(x) {
			opt = optimize(
				estimate_diebold_li_fit,
				df = x,
				return_all = F,
				interval = .0609 + c(-.5, .5),
				maximum = F
			)
			list(
				vdate = x$vdate[[1]],
				train_df = x,
				lambda = opt$minimum,
				mse = opt$objective
			)
		})

	# Get historical DNS fits by vintage date
	dns_fit_hist = list_rbind(map(optim_lambdas, \(x) estimate_diebold_li_fit(df = x$train_df, x$lambda, T)))
	dns_fit_hist %>%
		ggplot() +
		geom_point(aes(x = date, y = f2), color = 'violet') +
		geom_point(aes(x = date, y = f3), color = 'pink')

	dns_hp_validate = list_rbind(map(optim_lambdas, \(x) tibble(vdate = x$vdate, l = x$lambda, mse = x$mse)))
	dns_hp_validate %>%
		ggplot() +
		geom_line(aes(x = vdate, y = l), color = 'skyblue') +
		# geom_line(aes(x = vdate, y = mse), color = 'forestgreen') +
		ggthemes::theme_igray() +
		labs(title = 'Lambda estimates', color = NULL, x = NULL, y = NULL) +
		theme(legend.position = 'none')

	# Get historical DNS hists by vintage date
	dns_coefs_hist =
		dns_fit_hist %>%
		group_by(., vdate, date) %>%
		summarize(
			.,
			lambda = unique(lambda), tdns1 = unique(b1), tdns2 = unique(b2), tdns3 = unique(b3),
			.groups = 'drop'
			)

	# Check fits for last 12 vintage date (last historical fit for each vintage)
	dns_fit_plots =
		dns_fit_hist %>%
		filter(., vdate %in% sample(unique(dns_coefs_hist$vdate), size = 20)) %>%
		group_by(., vdate) %>%
		filter(., date == max(date)) %>%
		ungroup(.) %>%
		arrange(., vdate, ttm) %>%
		ggplot(.) +
		geom_point(aes(x = ttm, y = value)) +
		geom_line(aes(x = ttm, y = fitted)) +
		facet_wrap(vars(vdate)) +
		ggthemes::theme_igray() +
		labs(x = 'm', y = 'y')

	print(dns_fit_plots)

	### 5. Recompose Treasury yield curves

	# For each vintage date, gets:
	#  monthly returns (annualized & compounded) up to 10 years (minus FFR) using TDNS
	#  decomposition to enforce smoothness, reweighted with historical values (via spline)
	# For each vdate, use the last possible historical date (b1, b2, b3) for TDNS.
	fitted_curve =
		dns_coefs_hist %>%
		group_by(., vdate) %>%
		slice_max(., date) %>%
		ungroup(.) %>%
		transmute(., vdate, tdns1, tdns2, tdns3, lambda) %>%
		expand_grid(., ttm = 1:480) %>%
		mutate(., annualized_yield_dns = get_dns_fit(tdns1, tdns2, tdns2, lambda, ttm)) %>%
		# Join on last historical datapoints for each vintage (for spline fitting)
		left_join(
			.,
			dns_fit_hist %>%
				group_by(., vdate) %>%
				slice_max(., date) %>%
				ungroup(.) %>%
				transmute(., vdate, ttm, yield_hist = value),
			by = c('vdate', 'ttm')
		) %>%
		group_split(., vdate) %>%
		map(., .progress = T, function(x)
			x %>%
				mutate(
					.,
					spline_fit = zoo::na.spline(
						c(.$yield_hist[1:360], rep(tail(keep(.$yield_hist, \(x) !is.na(x)), 1), 120)),
						method = 'natural',
					),
					annualized_yield = .5 * annualized_yield_dns + .5 * spline_fit,
					# % change after this month
					cum_return = (1 + annualized_yield/100)^(ttm/12)
				)
		) %>%
		bind_rows(.) %>%
		select(., -tdns1, -tdns2, -tdns3, -lambda)

	# Check how well the spline combined with TDNS fits the history
	full_fit_plots =
		fitted_curve %>%
		filter(., vdate %in% c(head(unique(.$vdate, 4)), sample(unique(.$vdate), 4), tail(unique(.$vdate), 4))) %>%
		ggplot(.) +
		geom_line(aes(x = ttm, y = annualized_yield_dns), color = 'skyblue', linewidth = 1.5) +
		geom_line(aes(x = ttm, y = spline_fit), color = 'slategray') +
		geom_point(aes(x = ttm, y = yield_hist), color = 'black') +
		geom_line(aes(x = ttm, y = annualized_yield), color = 'firebrick', linewidth = 2, alpha = .5) +
		facet_wrap(vars(vdate))

	print(full_fit_plots)

	### 6. Build EH forecasts

	# Iterate over "yttms" tyield_1m, tyield_3m, ..., etc.
	# and for each, iterate over the original "ttms" 1, 2, 3,
	# ..., 120 and for each forecast the cumulative return for the yttm period ahead.
	expectations_forecasts =
		tdns$ttm_varname_map$ttm %>%
		lapply(., function(yttm)
			fitted_curve %>%
				arrange(., vdate, ttm) %>%
				group_by(., vdate) %>%
				mutate(
					.,
					yttm_ahead_cum_return = dplyr::lead(cum_return, yttm, order_by = ttm)/cum_return,
					yttm_ahead_annualized_yield = (yttm_ahead_cum_return^(12/yttm) - 1) * 100
				) %>%
				ungroup(.) %>%
				filter(., ttm <= 120) %>%
				mutate(., yttm = yttm) %>%
				inner_join(., tdns$ttm_varname_map, c('yttm' = 'ttm'))
		) %>%
		list_rbind %>%
		mutate(., date = add_with_rollback(floor_date(vdate, 'months'), months(ttm - 1))) %>%
		transmute(., vdate, ttm = yttm, varname, date, value = yttm_ahead_annualized_yield) %>%
		inner_join(
			.,
			tdns$rf_annualized %>% transmute(., date, ffr_vdate = vdate, ffr = value),
			join_by(date, closest(vdate >= ffr_vdate)) # In practice, always equal
		) %>%
		transmute(., vdate, varname, date, value_norf = value, value = value + ffr)

	# Plot point forecasts
	expectations_forecasts %>%
		filter(., vdate == max(vdate)) %>%
		ggplot(.) +
		geom_line(aes(x = date, y = value, color = varname)) +
		labs(title = 'Current forecast')

	expectations_forecasts %>%
		filter(., vdate == max(vdate)) %>%
		ggplot(.) +
		geom_line(aes(x = date, y = value_norf, color = varname)) +
		labs(title = 'Current forecast - ffr removed')

	# Plot curve forecasts
	expectations_forecasts %>%
		filter(
			.,
			vdate %in% c(sample(unique(.$vdate), 7), max(.$vdate)),
			date <= vdate %m+% months(5 * 12),
		) %>%
		mutate(., months_ahead = interval(floor_date(vdate, 'month'), date) %/% months(1)) %>%
		filter(., months_ahead %in% c(1, 6, 12, 24, 36, 60)) %>%
		left_join(., tdns$ttm_varname_map, by = 'varname') %>%
		ggplot() +
		geom_line(aes(x = ttm, y = value, color = as.factor(months_ahead), group = months_ahead)) +
		facet_wrap(vars(vdate), nrow = 2) +
		# scale_color_viridis_b() +
		ggthemes::theme_igray() +
		labs(title = 'Expectations curve forecasts', color = 'Forward months', x = 'm', y = 'y')

	# Plot curve forecasts vs ffr
	expectations_forecasts %>%
		filter(., vdate == max(vdate) & (date <= today('US/Eastern') %m+% months(5 * 12) & month(date) %in% c(1, 6))) %>%
		left_join(., tdns$ttm_varname_map, by = 'varname') %>%
		ggplot() +
		geom_line(aes(x = ttm, y = value, color = as.factor(date), group = date))

	# Plot forecasts over time for current period
	expectations_forecasts %>%
		filter(., date == floor_date(today('US/Eastern'), 'months')) %>%
		ggplot(.) +
		geom_line(aes(x = vdate, y = value, color = varname))

	# Check spread history
	# bind_rows(
	# 	mutate(expectations_forecasts, type = 'forecast') %>%
	# 		filter(., vdate >= min(expectations_forecasts$vdate)),
	# 	mutate(tdns$treas_d_obs, vdate = date, type = 'hist')
	# ) %>%


	# mutate(tdns$treas_d_obs, vdate = date, type = 'hist') %>%
	mutate(expectations_forecasts, type = 'forecast') %>%
		pivot_wider(., id_cols = c(vdate, date, type), names_from = varname, values_from = value) %>%
		mutate(., spread = t10y - t02y) %>%
		mutate(., months_ahead = ifelse(type == 'hist', -1, interval(vdate, date) %/% months(1))) %>%
		select(., vdate, date, spread, months_ahead) %>%
		group_by(., vdate, months_ahead) %>%
		summarize(., spread = mean(spread), .groups = 'drop') %>%
		arrange(., vdate, months_ahead) %>%
		filter(., months_ahead %in% c(-1, 0, 12, 36, 60)) %>%
		ggplot(.) +
		geom_line(aes(x = vdate, y = spread, color = as.factor(months_ahead))) +
		labs(title = 'Forecasted 10-2 Spread', subtitle = 'By forecast period, -1 = hist', x = 'vdate', y = 'spread')

	# Calculate TDNS1, TDNS2, TDNS3 forecasts
	# Forecast vintage date should be bound to historical data vintage
	# date since reliant purely on historical data
	# Calculated TDNS1: TYield_10y
	# Calculated TDNS2: -1 * (t10y - t03m)
	# Calculated TDNS3: .3 * (2*t02y - t03m - t10y)
	dns_coefs_forecast =
		expectations_forecasts %>%
		select(., vdate, varname, date, value) %>%
		pivot_wider(id_cols = c('vdate', 'date'), names_from = 'varname', values_from = 'value') %>%
		transmute(
			.,
			vdate,
			date,
			tdns1 = t10y,
			tdns2 = -1 * (t10y - t03m),
			tdns3 = .3 * (2 * t02y - t03m - t10y)
		)

	tdns$hist_df_unagg <<- hist_df_unagg
	tdns$dns_coefs_host <<- dns_coefs_hist
	tdns$optim_lambdas <<- optim_lambdas
	tdns$expectations_forecasts <<- expectations_forecasts
	tdns$dns_coefs_forecast <<- dns_coefs_forecast
})

### 4. Term Premium ----------------------------------------------------------
local({

	#### Add in LT term premium
	message('****** Add LT term premium')

	# Current spreads
	spf_1 = collect(tbl(pg, sql(
		"SELECT vdate, varname, date, d1 AS value
		FROM forecast_values_v2_all
		WHERE forecast = 'spf' AND varname IN ('t03m', 't10y')"
	)))

	# 4Y spreads
	spf_2 = list_rbind(map(c('tbill', 'tbond'), function(var) {
		request(paste0(
			'https://www.philadelphiafed.org/-/media/frbp/assets/surveys-and-data/',
			'survey-of-professional-forecasters/data-files/files/median_', var, '_level.xlsx?la=en'
			)) %>%
			req_perform(., path = file.path(tempdir(), paste0(var, '.xlsx')))

		readxl::read_excel(file.path(tempdir(), paste0(var, '.xlsx')), na = '#N/A', sheet = 'Median_Level') %>%
			select(., c('YEAR', 'QUARTER', paste0(str_to_upper(var), 'D'), paste0(str_to_upper(var), 'C'))) %>%
			na.omit %>%
			transmute(
				.,
				reldate = from_pretty_date(paste0(YEAR, 'Q', QUARTER), 'q'),
				varname = {if (var == 'tbill') 't03m' else 't10y'},
				D = .[[paste0(str_to_upper(var), 'D')]],
				C = .[[paste0(str_to_upper(var), 'C')]]
			)
		})) %>%
		left_join(., spf_1 %>% group_by(., vdate) %>% summarize(., reldate = min(date)), by = 'reldate') %>%
		arrange(., vdate) %>%
		pivot_longer(., -c(reldate, vdate, varname), names_to = 'out') %>%
		mutate(., new_year = add_with_rollback(floor_date(vdate, 'year'), years(ifelse(out == 'C', 2, 3)))) %>%
		expand_grid(., quarters = 2:4) %>%
		mutate(., date = add_with_rollback(new_year, months((quarters - 1) * 3))) %>%
		transmute(., vdate, varname, value, date)

	# Assign each quarterly forecast to the middle month of the forecast
	spf =
		bind_rows(spf_1, spf_2) %>%
		mutate(., date = add_with_rollback(floor_date(date, 'quarter'), months(1)))

	# TPs
	# request('https://www.newyorkfed.org/medialibrary/media/research/data_indicators/ACMTermPremium.xls') %>%
	# 	req_perform(., path = file.path(tempdir(), 'acm.xls'))
	#
	# acm_tps =
	# 	readxl::read_excel(file.path(tempdir(), 'acm.xls'), sheet = 'ACM Monthly') %>%
	# 	select(., DATE, contains('ACMTP')) %>%
	# 	mutate(., vdate = dmy(DATE)) %>%
	# 	select(., -DATE) %>%
	# 	pivot_longer(-vdate, names_to = 'duration', values_to = 'value') %>%
	# 	mutate(., duration = as.numeric(str_remove(duration, 'ACMTP')) * 12)

	# Get term premiums for time ranges
	tps_by_block =
		spf %>%
		transmute(., varname, date, spf = value, spf_vdate = vdate) %>%
		inner_join(
			.,
			transmute(tdns$expectations_forecasts, varname, date, eh = value, eh_vdate = vdate),
			join_by(date, varname, closest(spf_vdate >= eh_vdate))
		) %>%
		filter(., eh_vdate >= spf_vdate - days(7)) %>%
		left_join(., tdns$ttm_varname_map, by = 'varname') %>%
		mutate(
			.,
			months_ahead_start = interval(floor_date(spf_vdate, 'month'), date) %/% months(1),
			months_ahead_end = months_ahead_start + ttm,
			diff = spf - eh,
		) %>%
		# Set TP to zero at 0 months forward
		inner_join(
			.,
			transmute(filter(., months_ahead_start == 0), spf_vdate, varname, diff_0 = diff),
			by = c('spf_vdate', 'varname')
		) %>%
		mutate(
			.,
			diff = diff - diff_0,
			monthly_tp = 100 * ((1 + diff/100)^(1/12) - 1)
		)

	tps_by_block_plot =
		tps_by_block %>%
		filter(., months_ahead_start > 0 & varname == 't10y') %>%
		ggplot() +
		geom_line(
			aes(x = spf_vdate, y = monthly_tp, color = as.factor(months_ahead_start), group = months_ahead_start)
		) +
		ggthemes::theme_igray() +
		labs(title = 'Estimated Monthly Term Premia', y = 'Estimated Term Premia', color = 'Months Forward', x = 'Release Date') +
		theme(legend.position = 'right')

	print(tps_by_block_plot)

	sp500 = filter(hist$yahoo, varname == 'sp500tr') %>% rename(., sp500 = value)

	ffr_forecasts = transmute(tdns$rf_annualized, vdate, months_out, ffr_forecast = value)

	models_by_vdate =
		tps_by_block %>%
		group_split(., spf_vdate) %>%
		map(., function(df) {

			input_df =
				tps_by_block %>%
				filter(., spf_vdate <= df$spf_vdate[[1]] & spf_vdate >= df$spf_vdate[[1]] %m-% months(12)) %>%
				inner_join(
					tdns$dns_coefs_forecast %>% select(., date, vdate, tdns1, tdns2, tdns3),
					join_by(date, closest(spf_vdate >= vdate))
				) %>%
				inner_join(
					ffr_forecasts,
					join_by(months_ahead_start == months_out, closest(spf_vdate >= vdate))
					) %>%
				inner_join(sp500, join_by(closest(spf_vdate >= vdate))) %>%
				mutate(
					.,
					logttm = ttm ^ .0609 - 1,
					logttm_int = months_ahead_start * logttm,
					logttm_int_2 = months_ahead_start^.5 * logttm,
					tdns1_int = tdns1 * months_ahead_start,
					tdns1_int2 = tdns1 * logttm,
					forecast_diff = ffr_forecast - tdns1,
					forecast_diff_int = forecast_diff * months_ahead_start
					)

			if (nrow(input_df) <= 30) return(NULL)

			covs = c(
				'logttm', 'logttm_int',
				'tdns1', 'tdns1_int', 'tdns1_int2', 'tdns2', 'tdns3',
				'sp500',
				'forecast_diff', 'forecast_diff_int'
				)
			x_in = input_df %>% select(all_of(covs)) %>% as.matrix(.)

			cv = glmnet::cv.glmnet(
				x = x_in,
				y = as.matrix(input_df[, 'monthly_tp']),
				alpha = 1,
				intercept = F,
				type.measure = "mse",
				weights = .9^(interval(floor_date(input_df$spf_vdate, 'month'), df$spf_vdate[[1]]) %/% months(1)),
				nfolds = 10
			)

			reg = glmnet::glmnet(
				x = x_in,
				y = as.matrix(input_df[, 'monthly_tp']),
				alpha = 1,
				intercept = F,
				lambda = cv$lambda.min,
				weights = .9 ^ (interval(floor_date(input_df$spf_vdate, 'month'), df$spf_vdate[[1]]) %/% months(1))
			)

			# reg =
			# 	lm(
			# 		monthly_tp ~ logttm +
			# 			logttm_int + tdns1 + tdns1_int + tdns1_int2 + tdns2 + tdns3 + sp500 + ffr_forecast,
			# 		data = input_df
			# 		)
			#
			# reg %>% summary(.)

			test =
				tdns$dns_coefs_forecast %>%
				filter(., vdate <= max(df$spf_vdate)) %>%
				slice_max(., vdate) %>%
				mutate(., months_ahead_start = interval(floor_date(vdate, 'months'), date) %/% months(1)) %>%
				mutate(., sp500 = tail(filter(sp500, vdate <= df$spf_vdate[[1]]), 1)$sp500) %>%
				left_join(
					.,
					ffr_forecasts %>% filter(., vdate <= df$spf_vdate[[1]]) %>% filter(., vdate == max(vdate)),
					join_by(months_ahead_start == months_out, closest(vdate >= vdate))
				) %>%
				expand_grid(tdns$ttm_varname_map) %>%
				mutate(
					.,
					logttm = ttm ^ .0609 - 1,
					logttm_int = months_ahead_start * logttm,
					logttm_int_2 = months_ahead_start^.5 * logttm,
					tdns1_int = tdns1 * months_ahead_start,
					tdns1_int2 = tdns1 * logttm,
					forecast_diff = ffr_forecast - tdns1,
					forecast_diff_int = forecast_diff * months_ahead_start
				)

			x_new = test %>% select(., all_of(covs)) %>% as.matrix(.)

			yhat = predict(reg, x_new, type = 'response')[, 1]
			# yhat =
			# 	test %>%
			# 	transmute(., logttm, logttm_int, logttm_int_2, tdns1, tdns1_int, tdns2, tdns3, sp500, forecast_diff_int) %>%
			# 	{as.matrix(.) %*% as.matrix(reg$coefficients)} %>%
			# 	.[, 1]

			fitted = tibble(
				months_ahead_start = test$months_ahead_start,
				varname = test$varname,
				value = yhat,
				type = 'predicted'
				) %>%
				bind_rows(., transmute(df, varname, months_ahead_start, value = monthly_tp, type = 'actual')) %>%
				transmute(
					.,
					varname,
					spf_vdate = df$spf_vdate[[1]],
					months_out = months_ahead_start,
					date = floor_date(df$spf_vdate[[1]], 'months')  %m+% months(months_ahead_start),
					value,
					type
					)

			plot =
				ggplot(fitted) +
				geom_point(aes(x = months_out, y = value, color = type, group = type)) +
				facet_wrap(vars(varname))

			list(
				pred = fitted,
				coefs = bind_cols(varname = df$varname[[1]], vdate = df$spf_vdate[[1]], broom::tidy(reg)),
				plot = plot,
				model = reg,
				vdate = df$spf_vdate[[1]]
			)
		}) %>%
		compact(.)

	model_coefs_plot =
		models_by_vdate %>%
		map(., \(x) x$coefs) %>%
		list_rbind() %>%
		ggplot() +
		geom_line(aes(x = vdate, y = estimate)) +
		facet_wrap(vars(term), scales = 'free')

	print(model_coefs_plot)

	models_tps_plot =
		models_by_vdate %>%
		map(., \(x) x$pred) %>%
		list_rbind() %>%
		filter(., varname %in% c('t01m', 't03m', 't10y', 't30y')) %>%
		mutate(., type_varname = paste0(varname, type)) %>%
		mutate(., spf_year = year(spf_vdate)) %>%
		slice_min(., order_by = spf_vdate, by = spf_year) %>%
		ggplot() +
		geom_point(aes(x = months_out, y = value, color = varname, group = type_varname, size = type), shape = 18) +
		facet_wrap(vars(spf_vdate), nrow = 2) +
		scale_size_manual(values = c(actual = 2, predicted = .8)) +
		ggthemes::theme_fivethirtyeight() +
		labs(title = 'Fitted term premia estimates by t', x = 'Months forward', y = 'Term premia') +
		theme(legend.position = 'none')

	print(models_tps_plot)

	# Get TPS for all vdates & scale down small forecast horizons
	tps = map(tdns$backtest_vdates, .progress = T, function(test_vdate) {

		models = models_by_vdate %>% keep(., \(x) x$vdate <= test_vdate) %>% tail(., 1)
		if (length(models) == 0) return(NULL)
		model = models[[1]]$model

		test =
			tdns$dns_coefs_forecast %>%
			filter(., vdate <= max(test_vdate)) %>%
			filter(., vdate == max(vdate)) %>%
			mutate(., months_ahead_start = interval(floor_date(vdate, 'months'), date) %/% months(1)) %>%
			mutate(., sp500 = tail(filter(sp500, vdate <= test_vdate), 1)$sp500) %>%
			left_join(
				.,
				ffr_forecasts %>% filter(., vdate <= test_vdate) %>% filter(., vdate == max(vdate)),
				join_by(months_ahead_start == months_out, closest(vdate >= vdate))
			) %>%
			expand_grid(tdns$ttm_varname_map) %>%
			mutate(
				.,
				logttm = ttm ^ .0609 - 1,
				logttm_int = months_ahead_start * logttm,
				logttm_int_2 = months_ahead_start^.5 * logttm,
				tdns1_int = tdns1 * months_ahead_start,
				tdns1_int2 = tdns1 * logttm,
				forecast_diff = ffr_forecast - tdns1,
				forecast_diff_int = forecast_diff * months_ahead_start
			)

		x_new =
			test %>%
			transmute(., logttm, logttm_int, tdns1, tdns1_int, tdns1_int2, tdns2, tdns3, sp500, forecast_diff, forecast_diff_int) %>%
			as.matrix(.)

		yhat = predict(model, x_new, type = 'response')[, 1]

		fitted = tibble(
			months_out = test$months_ahead_start,
			varname = test$varname,
			value = ((1 + yhat/100)^12 - 1) * 100,
			) %>%
			mutate(
				.,
				test_vdate = test_vdate,
				model_vdate = models[[1]]$vdate,
				date = floor_date(test_vdate, 'months')  %m+% months(months_out),
				value = log(ifelse(months_out >= 60, 60, months_out) + 1)/log(61) * value
			)

		return(fitted)
		}) %>%
		compact() %>%
		list_rbind()

	fixed_tp_plot =
		tps %>%
		filter(., date == floor_date(today('US/Eastern') %m-% months(3), 'month') & varname == 't10y') %>%
		ggplot() +
		geom_line(aes(x = test_vdate, y = value, color = as.factor(model_vdate), group = model_vdate)) +
		ggthemes::theme_igray() +
		guides(color=guide_legend(ncol = 2)) +
		labs(title = 'Forecasted term premiums for Dec-23 by forecast date', x = 't', y = 'term premium', color = 'model tau')

	print(fixed_tp_plot)
	# Reconstruct 10-year forward TP & annualize it!
	tp_10y_plot =
		tps %>%
		filter(., varname == 't10y') %>%
		filter(., months_out == 60) %>%
		ggplot() +
		geom_line(aes(x = test_vdate, y = value))

	print(tp_10y_plot)

	latest_tp_plot =
		tps %>%
		filter(., test_vdate == max(test_vdate))  %>%
		ggplot() +
		geom_line(aes(x = date, y = value, color = varname)) +
		labs(title = 'Latest term premiums (smoothed)')

	print(latest_tp_plot)

	# Plot TP curve forecasts
	tp_curve_plot =
		tps %>%
		filter(
			.,
			test_vdate %in% c(sample(unique(.$test_vdate), 9), max(.$test_vdate)),
			months_out <= 60,
			months_out %% 12 == 0
		) %>%
		inner_join(., tdns$ttm_varname_map, by = 'varname') %>%
		ggplot() +
		geom_line(aes(x = ttm, y = value, color = as.factor(months_out), group = months_out)) +
		geom_point(aes(x = ttm, y = value, color = as.factor(months_out), group = months_out)) +
		facet_wrap(vars(test_vdate))

	print(tp_curve_plot)

	# Plot TP forecasts over time for current period
	tp_over_time_plot =
		tps %>%
		filter(., date == floor_date(today('US/Eastern'), 'months')) %>%
		ggplot(.) +
		geom_line(aes(x = test_vdate, y = value, color = varname)) +
		labs(title = 'TPs over time for current month')

	print(tp_over_time_plot)

	adj_forecasts =
		tdns$expectations_forecasts %>%
		rename(norf = value_norf, eh = value) %>%
		inner_join(
			transmute(tps, date, varname, tp_vdate = test_vdate, tp = value),
			join_by(date, varname, closest(vdate >= tp_vdate))
		) %>%
		mutate(., value = eh + tp)

	current_forecast_plot =
		adj_forecasts %>%
		filter(., vdate == max(vdate)) %>%
		ggplot(.) +
		geom_line(aes(x = date, y = value, color = varname)) +
		labs(title = 'Current forecast')

	print(current_forecast_plot)

	current_curve_plot =
		adj_forecasts %>%
		filter(., vdate == max(vdate) & (date <= today('US/Eastern') + years(5) & month(date) %in% c(1, 6))) %>%
		left_join(., tdns$ttm_varname_map, by = 'varname') %>%
		ggplot() +
		geom_line(aes(x = ttm, y = value, color = as.factor(date), group = date)) +
		labs(title = 'Current curve forecast')

	print(current_curve_plot)

	# adj_forecasts %>%
	# 	filter(vdate == max(vdate) & varname %in% c('t03m', 't10y', 't30y')) %>%
	# 	pivot_longer(., cols = c(eh, tp, norf, value)) %>%
	# 	ggplot() +
	# 	geom_line(aes(x = date, y = value, color = name)) +
	# 	facet_wrap(vars(varname)) +
	# 	labs(title = 'Current forecast, split by component types') +
	# 	ggthemes::theme_fivethirtyeight()

	sampled_curve_plots =
		adj_forecasts %>%
		mutate(., months_ahead = interval(floor_date(vdate, 'month'), date) %/% months(1)) %>%
		filter(
			.,
			vdate %in% c(sample(unique(.$vdate), 9), max(.$vdate)),
			months_ahead <= 60,
			months_ahead %in% c(0, 6, 12, 24, 48)
		) %>%
		left_join(tdns$ttm_varname_map, by = 'varname') %>%
		ggplot() +
		geom_line(aes(x = ttm, y = value, color = as.factor(months_ahead), group = months_ahead)) +
		facet_wrap(vars(vdate))

	print(sampled_curve_plots)

	tdns$tps <<- tps
	tdns$adj_forecasts <<- adj_forecasts
})

### 5. Combine with Historical -----------------------------------------------------------
local({

	### Smooth with historical data
	message('****** Smoothing with historical data')

	# https://en.wikipedia.org/wiki/Logistic_function
	get_logistic_x0 = function(desired_y_intercept, k = 1) {
		res = log(1/desired_y_intercept - 1)/k
		return(res)
	}
	logistic = function(x, x0, k = 1) {
		res = 1/(1 + exp(-1 * k * (x - x0)))
		return(res)
	}

	# Join forecasts with historical data to smooth out bump between historical data and TDNS curve
	# TBD: Instead of merging against last_hist, it should be adjusted by the difference between the last_hist
	# and the last forecast for that last_hist
	# Use logistic smoother to reduce weight over time
	hist_merged_df =
		tdns$adj_forecasts %>%
		mutate(., months_ahead = interval(floor_date(vdate, 'months'), date) %/% months(1)) %>%
		left_join(
			.,
			# Use hist proper mean of existing data
			tdns$hist_df_unagg %>%
				mutate(., date = floor_date) %>%
				group_by(., vdate, varname, date) %>%
				summarize(., value = mean(value), .groups = 'drop') %>%
				# Keep latest hist obs for each vdate
				group_by(., vdate) %>%
				filter(., date == max(date)) %>%
				ungroup(.) %>%
				transmute(., vdate, varname, date, last_hist = value),
			by = c('vdate', 'varname', 'date')
		) %>%
		group_by(., vdate, varname) %>%
		mutate(
			.,
			last_hist = zoo::na.locf(last_hist, na.rm = F),
			last_hist_diff = ifelse(is.na(last_hist), NA, last_hist - value)
		) %>%
		ungroup(.) %>%
		# Map weights with sigmoid function and fill down forecast month 0 histoical value for full vdate x varname
		mutate(
			.,
			# Scale multiplier for origin month (.5 to 1).
			# In the next step, this is the sigmoid curve's y-axis crossing point.
			# Swap to k = 1 in both logistic functions to force smoothness (original 4, .5)
			forecast_origin_y = logistic((1 - day(vdate)/days_in_month(vdate)), get_logistic_x0(1/4, k = 2), k = 2),
			# Goal: for f(x; x0) = 1/(1 + e^-k(x  - x0)) i.e. the logistic function,
			# find x0 s.t. f(0; x0) = forecast_origin_y => x0 = log(1/.75 - 1)/k
			forecast_weight =
				# e.g., see y=(1/(1+e^-x)+.25)/1.25 - starts at .75
				logistic(months_ahead, get_logistic_x0(forecast_origin_y, k = 2), k = 2) #log(1/forecast_origin_y - 1)/.5, .5)
			# (1/(1 + 1 * exp(-1 * months_ahead)) + (forecast_origin_y - (log(1/forecast_origin_y - 1))))
		) %>%
		# A vdate x varname with have all NAs if no history for forecast month 0 was available
		# (e.g. first day of the month)
		mutate(., final_value = ifelse(
			is.na(last_hist),
			value,
			forecast_weight * value + (1 - forecast_weight) * (value + last_hist_diff)
		)) %>%
		arrange(., desc(vdate))

	fw_over_time_plot =
		hist_merged_df %>%
		filter(., vdate >= today('US/Eastern') - days(90)) %>%
		filter(., date == floor_date(today('US/Eastern'), 'month'), varname == 't10y') %>%
		ggplot(.) +
		geom_line(aes(x = vdate, y = forecast_weight, color = format(vdate, '%Y%m'))) +
		labs(title = 'Forecast weights for this months forecast over time')

	print(fw_over_time_plot)

	fw_over_fut_plot =
		hist_merged_df %>%
		filter(., vdate == max(vdate), varname == 't30y') %>%
		ggplot(.) +
		geom_line(aes(x = date, y = forecast_weight)) +
		labs(title = 'Forecast weights generated on this vdate for future forecasts')

	print(fw_over_fut_plot)

	# Test plot 1
	test_plot_1 =
		hist_merged_df %>%
		filter(., vdate %in% head(unique(hist_merged_df$vdate), 10)) %>%
		group_by(., vdate) %>%
		filter(., date == min(date)) %>%
		inner_join(., tdns$ttm_varname_map, by = 'varname') %>%
		arrange(., ttm) %>%
		ggplot(.) +
		geom_point(aes(x = ttm, y = final_value), alpha = .5, color = 'forestgreen') +
		geom_point(aes(x = ttm, y = value), alpha = .5, color = 'blue') +
		geom_point(aes(x = ttm, y = last_hist), alpha = .5, color = 'black') +
		facet_wrap(vars(vdate)) +
		labs(
			title = 'Current month forecasts - last 10 vdates',
			subtitle = 'green = joined, blue = unjoined_forecast, black = hist'
		)

	print(test_plot_1)

	# Test plot 2
	test_plot_2 =
		hist_merged_df %>%
		filter(., vdate == max(vdate) & date <= today('US/Eastern') %m+% months(12)) %>%
		inner_join(., tdns$ttm_varname_map, by = 'varname') %>%
		arrange(., ttm) %>%
		ggplot(.) +
		geom_point(aes(x = ttm, y = final_value), alpha = .5, color = 'forestgreen') +
		geom_point(aes(x = ttm, y = value), alpha = .5, color = 'blue') +
		geom_point(aes(x = ttm, y = last_hist), alpha = .5, color = 'black') +
		facet_wrap(vars(date)) +
		labs(
			title = 'Max vdate forecasts - forecasts by date',
			subtitle = 'green = joined, blue = unjoined_forecast, black = hist'
		)

	print(test_plot_2)

	merged_forecasts =
		hist_merged_df %>%
		transmute(., vdate, varname, freq = 'm', date, value = final_value)

	forecasts_comparison = bind_rows(
		tdns$expectations_forecasts %>% mutate(., value = value_norf, type = '1. EH'),
		tdns$expectations_forecasts  %>% mutate(., type = '2. EH + RF'),
		tdns$adj_forecasts %>% mutate(., type = '3. EH + TP'),
		merged_forecasts %>% mutate(., type = '4. EH + TP + Hist (Final)'),
	)

	subcomponent_plots =
		forecasts_comparison %>%
		filter(., vdate == max(vdate) - days(0)) %>%
		ggplot(.) +
		geom_line(aes(x = date, y = value, color = type), alpha = .5) +
		facet_wrap(vars(varname)) +
		ggthemes::theme_igray() +
		labs(title = 'Subcomponent forecasts at lastest vintage', color = 'Component')

	print(subcomponent_plots)

	if (F) {
		forecasts_composition = bind_rows(
			tdns$expectations_forecasts %>% mutate(., value = value - value_norf, type = 'RF'),
			tdns$expectations_forecasts  %>% mutate(., value = value_norf, type = 'EH'),
			tdns$adj_forecasts %>% mutate(., value = tp, type = 'TP'),
		)

		forecasts_composition %>%
			filter(., varname == 't10y') %>%
			mutate(., months_out = interval(floor_date(vdate, 'month'), date) %/% months(1)) %>%
			filter(., months_out == 12) %>%
			ggplot(.) +
			geom_line(aes(x = vdate, color = type, y = value)) +
			ggthemes::theme_igray() +
			labs(title = '12-month ahead forecast of the 10-year Treasury yield', x = 't', y = 'y', color = 'Component')

		forecasts_composition %>%
			filter(., varname == 't10y' & vdate %in% c(sample(.$vdate, size = 10), max(.$vdate))) %>%
			mutate(., months_out = interval(floor_date(vdate, 'month'), date) %/% months(1)) %>%
			ggplot(.) +
			geom_area(aes(x = months_out, y = value, fill = type), alpha = .5) +
			ggthemes::theme_igray() +
			facet_wrap(vars(vdate)) +
			labs(title = 'Subcomponent forecasts at lastest vintage', color = 'Component')

		forecasts_comparison %>%
			filter(., vdate %in% sample(merged_forecasts$vdate, size = 5) & type != '1. EH') %>%
			mutate(., months_ahead = interval(floor_date(vdate, 'month'), floor_date(date, 'month')) %/% months(1)) %>%
			filter(., months_ahead %in% c(0, 12, 60)) %>%
			left_join(., tdns$ttm_varname_map, by = 'varname') %>%
			ggplot() +
			geom_line(aes(x = ttm, y = value, color = as.factor(months_ahead), group = months_ahead)) +
			facet_grid(rows = vars(vdate), cols = vars(type)) +
			ggthemes::theme_fivethirtyeight() +
			labs(title = 'Subcomponent curve forecasts by vdate', color = 'Months ahead')
	}

	# Take same-month t01m forecasts and merge with last historical period
	# at the short end during financial risk periods
	# Weight of last period is .5 at start of month and decreases to 0 at end of month
	final_forecasts =
		filter(merged_forecasts, floor_date(vdate, 'month') == date & varname == 't01m') %>%
		left_join(
			tibble(vdate = tdns$backtest_vdates) %>%
				left_join(., rename(tdns$treas_d_obs, hist_vdate = vdate), join_by(closest(vdate > hist_vdate))) %>%
				transmute(., vdate, varname, last_date = date, last_value = value),
			by = c('varname', 'vdate'),
			relationship = 'one-to-one'
		) %>%
		mutate(., hist_weight = .5 - .5/30 * ifelse(day(vdate) >= 30, 30, day(vdate))) %>%
		mutate(., weighted_value = hist_weight * last_value + (1 - hist_weight) * value) %>%
		transmute(
			.,
			vdate,
			varname,
			freq,
			date,
			value = weighted_value
		) %>%
		arrange(., vdate) %>%
		{bind_rows(., anti_join(merged_forecasts, ., by = c('vdate', 'varname', 'freq', 'date')))}

	final_forecast_plots =
		final_forecasts %>%
		filter(., vdate %in% sample(final_forecasts$vdate, size = 10)) %>%
		mutate(., months_ahead = interval(floor_date(vdate, 'month'), floor_date(date, 'month')) %/% months(1)) %>%
		filter(., months_ahead %in% c(0, 12, 60)) %>%
		left_join(., tdns$ttm_varname_map, by = 'varname') %>%
		ggplot() +
		geom_line(aes(x = ttm, y = value, color = as.factor(months_ahead), group = months_ahead)) +
		facet_wrap(vars(vdate))

	print(final_forecast_plots)

	### Performance testing
	if (F) {

		ets_forecasts = list_rbind(map(unique(model_forecasts$vdate), .progress = T, \(this_vdate) {
			hist$treasury %>%
				filter(., varname == 't10y' & date < this_vdate) %>%
				tail(., 90) %>%
				bind_rows(
					tibble(date = seq(
						from = floor_date(max(.$date), 'month')  %m+% months(1),
						to = floor_date(this_vdate, 'quarter') %m+% months(4 * 12),
						by = '1 month'
					))
				) %>%
				mutate(., value = zoo::na.locf(value)) %>%
				mutate(., date = floor_date(date, 'quarter')) %>%
				summarize(., value = mean(value), .by = 'date') %>%
				filter(., date >= floor_date(as_date(this_vdate), 'quarter')) %>%
				bind_cols(vdate = this_vdate, type = 'ets', .)
		}))

		fut_forecasts = list_rbind(map(unique(model_forecasts$vdate), .progress = T, \(this_vdate) {

			last_treas = tail(filter(hist$treasury, varname == 't10y' & date < this_vdate), 1)$value
			last_ffr = tail(filter(hist$fred, varname == 'ffr' & date < this_vdate), 1)$value
			last_spread = last_treas - last_ffr

			forecasts_out =
				tdns$adj_forecasts %>%
				filter(., varname == 't10y' & vdate == this_vdate) %>%
				mutate(., value = eh - norf) %>%
				select(., vdate, date, value) %>%
				mutate(., value = value + last_spread)

			hist$treasury %>%
				filter(., varname == 't10y' & date < forecasts_out$date[[1]]) %>%
				bind_rows(., forecasts_out) %>%
				mutate(., date = floor_date(date, 'quarter')) %>%
				summarize(., value = mean(value), .by = 'date') %>%
				filter(., date >= floor_date(as_date(this_vdate), 'quarter')) %>%
				bind_cols(vdate = this_vdate, type = 'fut', .)
		}))

		# Get model forecasts at quarterly intervals
		model_forecasts =
			final_forecasts %>%
			filter(., varname == 't10y') %>%
			group_split(., vdate) %>%
			# Aggregate to quarterly - may need historical data joined in
			map(., .progress = T, function(df) {
				# Get previous data - assume t10y vintage date lag of 1 day
				bind_rows(
					hist$treasury %>%
						filter(., varname == 't10y' & date < df$vdate[[1]]) %>%
						mutate(., date = floor_date(date, 'month')) %>%
						summarize(., value = mean(value), .by = c(date)) %>%
						filter(., date < df$date[[1]]),
					df
					) %>%
					mutate(., date = floor_date(date, 'quarter')) %>%
					summarize(., value = mean(value), .by = date) %>%
					filter(., date >= floor_date(df$vdate[[1]], 'quarter')) %>%
					mutate(., vdate = df$vdate[[1]]) %>%
					mutate(., type = 'model')
			}) %>%
			list_rbind()

		# Test No_TP model
		model_forecasts_2 =
			tdns$adj_forecasts %>%
			mutate(., value = value - tp) %>%
			filter(., varname == 't10y') %>%
			group_split(., vdate) %>%
			# Aggregate to quarterly - may need historical data joined in
			map(., .progress = T, function(df) {
				# Get previous data - assume t10y vintage date lag of 1 day
				bind_rows(
					hist$treasury %>%
						filter(., varname == 't10y' & date < df$vdate[[1]]) %>%
						mutate(., date = floor_date(date, 'month')) %>%
						summarize(., value = mean(value), .by = c(date)) %>%
						filter(., date < df$date[[1]]),
					df
				) %>%
					mutate(., date = floor_date(date, 'quarter')) %>%
					summarize(., value = mean(value), .by = date) %>%
					filter(., date >= floor_date(df$vdate[[1]], 'quarter')) %>%
					mutate(., vdate = df$vdate[[1]]) %>%
					mutate(., type = 'model_2')
			}) %>%
		list_rbind()

		perf_df_0 = bind_rows(
			ets_forecasts,
			model_forecasts,
			fut_forecasts,
			get_query(pg, sql(
				"SELECT vdate, date, d1 AS value
					FROM forecast_values_v2_all
					WHERE forecast = 'spf' AND varname IN ('t10y')"
				)) %>%
				mutate(., vdate) %>%
				filter(., date >= floor_date(vdate, 'quarter')) %>%
				mutate(., type = 'spf'),
			get_query(pg, sql(
				"SELECT vdate, date, d1 AS value
					FROM forecast_values_v2_all
					WHERE forecast = 'fnma' AND varname IN ('t10y')"
				)) %>%
				mutate(., vdate = floor_date(vdate, 'month')) %>%
				filter(., date >= floor_date(vdate, 'quarter')) %>%
				mutate(., type = 'fnma')
		) %>%
			filter(., vdate >= '2014-01-01')

		perf_df =
			expand_grid(vdate = unique(perf_df_0$vdate), type = unique(perf_df_0$type)) %>%
			left_join(., rename(perf_df_0, release_vdate = vdate), join_by(type, closest(vdate >= release_vdate))) %>%
			inner_join(
				.,
				hist$treasury %>%
					filter(., varname == 't10y') %>%
					mutate(., date = floor_date(date, 'quarter')) %>%
					summarize(., hist = mean(value), .by = date),
				by = 'date'
			) %>%
			mutate(
				.,
				weeks_out = ceiling(interval(vdate, floor_date(date, 'quarter')) / days(7)),
				data_lag = interval(release_vdate, vdate) %/% days(1)
				) %>%
			filter(., year(vdate) >= 2016)

		perf_df %>%
			filter(., year(vdate) >= 2017 & weeks_out <= 52) %>%
			mutate(., error = 100 * (value - hist)) %>%
			arrange(., vdate, date) %>%
			group_by(., type) %>%
			mutate(., type = case_when(
				type == 'fnma' ~ 'Blue Chip',
				type == 'spf' ~ 'SPF',
				type == 'ets' ~ 'ETS',
				type == 'fut' ~ 'Futures-Derived',
				type == 'model' ~ 'Model'
			)) %>%
			summarize(
				.,
				'Average Data Delay in Days' = mean(data_lag),
				'MAE (bps)' = (mean(abs(error))),
				'MAPE (bps)' = mean(abs(error)/hist),
				'20th Error Quantile (bps)' = quantile((abs(error)), .2),
				'80th Error Quantile (bps)' = quantile((abs(error)), .8),
				.groups = 'drop'
			) %>%
			xtable::xtable(., digits = c(0, 0, 0, 2, 2, 2, 2)) %>%
			print(., include.rownames = F)

		perf_df %>%
			filter(., year(vdate) >= 2017) %>%
			mutate(., error = value - hist) %>%
			arrange(., vdate, date) %>%
			group_by(., type, weeks_out) %>%
			summarize(., lag = median(data_lag), mae = mean(abs(error)), n = n(), .groups = 'drop') %>%
			filter(., n >= 4 & weeks_out %in% c(1:4, 6, 12, 24, 36, 48, 60) & weeks_out >= 0) %>%
			mutate(
				.,
				lag = as.character(lag),
				across(c(mae), \(x) ifelse(x == min(x), paste0('\\textbf{', round(x, 3),'}'), round(x, 3))),
				.by = 'weeks_out'
			) %>%
			pivot_wider(., id_cols = weeks_out, names_from = c(type), values_from = c(lag, mae)) %>%
			select(
				.,
				weeks_out,
				lag_fnma, mae_fnma,
				lag_spf, mae_spf,
				lag_ets, mae_ets,
				lag_fut, mae_fut,
				lag_model, mae_model,
				) %>%
			arrange(., weeks_out) %>%
			xtable::xtable(., digits = c(0, 0, rep(c(0, 4), 5))) %>%
			print(., include.rownames = F, include.colnames = F, hline.after = NULL, sanitize.text.function=\(x) x)

		perf_df %>%
			filter(., year(vdate) >= 2017) %>%
			mutate(., error = 100 * (value - hist)) %>%
			arrange(., vdate, date) %>%
			group_by(., type, weeks_out) %>%
			summarize(., mae = (mean(abs(error))), n = n(), .groups = 'drop') %>%
			filter(., n >= 4 & weeks_out <= 52 * 1 & weeks_out >= 1) %>%
			ggplot() +
			geom_line(aes(x = weeks_out, y = mae, color = type, group = type)) +
			geom_point(aes(x = weeks_out, y = mae, color = type, group = type), size = .5) +
			scale_color_manual(
				labels = c(fnma = 'Blue Chip', fut = 'Futures-Derived', spf = 'Survey of Professional Forecasters', ets = 'Naive ETS', model = 'Model'),
				values = c(fnma = 'turquoise', fut = 'slategray', spf = 'firebrick', ets = 'violet', model = 'gold'),
			) +
			scale_x_reverse() +
			ggthemes::theme_igray() +
			labs(x = 'Weeks before forecasted quarter begins', y = 'MAE in basis points', color = 'Forecast', fill = 'Forecast')

		quantiles =
			vix %>%
			group_by(., date = floor_date(date, 'quarter')) %>%
			summarize(., vix = mean(value)) %>%
			mutate(., quantile = ntile(vix, 4))

		perf_df %>%
			inner_join(., quantiles, by = 'date') %>%
			filter(., year(vdate) >= 2014 & weeks_out <= 52) %>%
			mutate(., error = 100 * (value - hist)) %>%
			arrange(., vdate, date) %>%
			group_by(., type, quantile) %>%
			mutate(., type = case_when(
				type == 'fnma' ~ 'Blue Chip',
				type == 'spf' ~ 'SPF',
				type == 'ets' ~ 'ETS',
				type == 'fut' ~ 'Futures-Derived',
				type == 'model' ~ 'Model'
			)) %>%
			summarize(., mae = mean(abs(error)), .groups = 'drop') %>%
			pivot_wider(., id_cols = c(type), names_from = quantile, values_from = mae)

		ets_forecasts_d = list_rbind(map(unique(model_forecasts$vdate), .progress = T, \(this_vdate) {
			hist$treasury %>%
				filter(., varname == 't10y' & date < this_vdate) %>%
				tail(., 31) %>%
				bind_rows(
					tibble(date = seq(
						from = floor_date(max(.$date), 'month')  %m+% months(1),
						to = floor_date(this_vdate, 'month') %m+% months(24),
						by = '1 month'
					))
				) %>%
				mutate(., value = zoo::na.locf(value)) %>%
				mutate(., date = floor_date(date, 'month')) %>%
				summarize(., value = mean(value), .by = 'date') %>%
				filter(., date >= floor_date(as_date(this_vdate), 'month')) %>%
				bind_cols(vdate = this_vdate, type = 'ets', .)
		}))

		fut_forecasts_d = list_rbind(map(unique(model_forecasts$vdate), .progress = T, \(this_vdate) {

			last_treas = tail(filter(hist$treasury, varname == 't10y' & date < this_vdate), 1)$value
			last_ffr = tail(filter(hist$fred, varname == 'ffr' & date < this_vdate), 1)$value
			last_spread = last_treas - last_ffr

			forecasts_out =
				tdns$adj_forecasts %>%
				filter(., varname == 't10y' & vdate == this_vdate) %>%
				mutate(., value = eh - norf) %>%
				select(., vdate, date, value) %>%
				mutate(., value = value + last_spread)

			hist$treasury %>%
				filter(., varname == 't10y' & date < forecasts_out$date[[1]]) %>%
				bind_rows(., forecasts_out) %>%
				mutate(., date = floor_date(date, 'month')) %>%
				summarize(., value = mean(value), .by = 'date') %>%
				filter(., date >= floor_date(as_date(this_vdate), 'month')) %>%
				bind_cols(vdate = this_vdate, type = 'fut', .)
		}))

		# Get model forecasts at quarterly intervals
		model_forecasts_d =
			final_forecasts %>%
			filter(., varname == 't10y') %>%
			filter(., date >= floor_date(vdate, 'month')) %>%
			mutate(., type = 'model')
			list_rbind()

		perf_df_d = bind_rows(ets_forecasts_d, fut_forecasts_d, model_forecasts_d)

		perf_df =
			expand_grid(vdate = unique(perf_df_d$vdate), type = unique(perf_df_d$type)) %>%
			left_join(., rename(perf_df_d, release_vdate = vdate), join_by(type, closest(vdate >= release_vdate))) %>%
			inner_join(
				.,
				hist$treasury %>%
				   	filter(., varname == 't10y') %>%
				   	mutate(., date = floor_date(date, 'month')) %>%
				   	summarize(., hist = mean(value), .by = date),
				by = 'date'
				) %>%
			mutate(
				.,
				days_out = interval(vdate, ceiling_date(date, 'month')) %/% days(7),
				data_lag = interval(release_vdate, vdate) %/% days(1)
			)

		perf_df %>%
			filter(., year(vdate) >= 2016) %>%
			mutate(., error = value - hist) %>%
			arrange(., vdate, date) %>%
			group_by(., type, days_out) %>%
			summarize(
				.,
				average_data_delay = mean(data_lag),
				mae = (mean(abs(error))),
				q80e = quantile((abs(error)), .8),
				q20e = quantile((abs(error)), .2),
				n = n(),
				.groups = 'drop'
			) %>%
			filter(., days_out <= 360) %>%
			ggplot() +
			geom_line(aes(x = days_out, y = mae, color = type, group = type)) +
			geom_point(aes(x = days_out, y = mae, color = type, group = type), size = .5) +
			scale_x_reverse() +
			ggthemes::theme_igray() +
			labs(x = 'Weeks until forecast value is realized', y = 'Log MAE', color = 'Forecast', fill = 'Forecast')
	}

	tdns$final_forecasts <<- final_forecasts
	submodels$tdns <<- final_forecasts
})

## REAL ----------------------------------------------------------
local({

	backtest_vdates = seq(
		from = today('US/Eastern') %m-% months(BACKTEST_MONTHS),
		to = today('US/Eastern'),
		by = '1 day'
	)

	nominal_bases = bind_rows(
		submodels$tdns %>%
			filter(., varname %in% c('t03m', 't06m', 't01y', 't02y', 't05y', 't10y', 't20y', 't30y')) %>%
			left_join(., tdns$ttm_varname_map, by = 'varname'),
		submodels$cme %>%
			filter(., varname == 'ffr') %>%
			mutate(., ttm = 1)
		) %>%
		filter(., vdate %in% backtest_vdates) %>%
		select(., vdate, varname, ttm, date, value)

	# Get inflation curves from TIPS and full compinf curves
	# compinf curves have multiple forward dates per each vdate and need to be constructed
	inf_tips_curves = get_query(
		pg,
		"(SELECT vdate, ttm, value FROM composite_inflation_yield_curves WHERE curve_source = 'tips_spread')
		UNION ALL
		(SELECT vdate, ttm, value FROM composite_inflation_yield_curves
		WHERE curve_source = 'tips_spread_smoothed'
		AND ttm NOT IN (SELECT DISTINCT ttm FROM composite_inflation_yield_curves WHERE curve_source = 'tips_spread'))"
	)

	inf_compinf_values = get_query(
		pg,
		"SELECT vdate, date, d2 AS value -- Get monthly % change
		FROM forecast_values_v2_all
		WHERE forecast = 'compinf' AND freq = 'm' AND varname = 'cpi'"
	)

	# At each previous forecast, create 40 years of forward forecasts by dragging down the monthly-change at year 30
	inf_compinf_values_interp =
		inf_compinf_values %>%
		mutate(., months_out = interval(floor_date(vdate, 'month'), date) %/% months(1)) %>%
		right_join(., expand_grid(vdate = unique(.$vdate), months_out = 0:480), by = c('vdate', 'months_out')) %>%
		arrange(., vdate, months_out) %>%
		mutate(., value = zoo::na.locf(value, na.rm = F), .by = vdate)

	# Get annualized inflation forecasts for each maturity date and vintage time
	inf_compinf_curves = map(unique(nominal_bases$ttm), .progress = F, \(yttm) {
		inf_compinf_values_interp %>%
			arrange(., vdate, months_out) %>%
			mutate(
				.,
				ttm = yttm,
				cum_return = cumprod(1 + value/100),
				yttm_ahead_cum_return = lead(cum_return, yttm, order_by = months_out)/cum_return,
				yttm_ahead_annualized_yield = (yttm_ahead_cum_return^(12/yttm) - 1) * 100,
				.by = vdate
			) %>%
			filter(., months_out <= 120)
		}) %>%
		list_rbind() %>%
		transmute(., vdate, ttm, date, value = yttm_ahead_annualized_yield)

	# Join Treasury yields with latest annualized inf forecasts from both the forecast curve and the TIPS curve
	rt_components =
		nominal_bases %>%
		inner_join(
			.,
			inf_compinf_curves %>% transmute(., ttm, date, inf_f_vdate = vdate, inf_f_value = value),
			join_by(ttm, date, closest(vdate >= inf_f_vdate))
		) %>%
		inner_join(
			.,
			inf_tips_curves %>% transmute(., ttm, inf_h_vdate = vdate, inf_h_value = value),
			join_by(ttm, closest(vdate >= inf_h_vdate))
		) %>%
		mutate(
			.,
			months_out = interval(floor_date(vdate, 'month'), date) %/% months(1),
			inf_f_weight = tanh((months_out + 3)/12),
			inf = inf_f_value * inf_f_weight + inf_h_value * (1 - inf_f_weight)
			) %>%
		transmute(., vdate, varname, date, months_out, nominal = value, inf, real = nominal - inf)

	rt_plot =
		rt_components %>%
		filter(., vdate == max(vdate)) %>%
		pivot_longer(., cols = c(nominal, real, inf)) %>%
		ggplot() +
		geom_line(aes(x = date, y = value, color = name, group = name)) +
		facet_wrap(vars(varname)) +
		labs(title = 'Last forecast, nominal yield and inflation components')

	print(rt_plot)

	real_final =
		rt_components %>%
		transmute(
			.,
			vdate,
			varname = paste0('r', varname),
			freq = 'm',
			date,
			value = real
		)

	submodels$real <<- real_final
})

## AMB ---------------------------------------------------------------------
local({

	# AMB1
	# AMT1 <->
	# Get FFR spread <-> compare historical change from AMERIBOR
	backtest_vdates = seq(
		from = max(today('US/Eastern') %m-% months(BACKTEST_MONTHS), as_date('2023-07-01')),
		# Ameribor missing close-dates before that
		to = today('US/Eastern'),
		by = '1 day'
	)

	# vdate in futures table represents trade dates, not pull dates!
	futures_df = get_query(pg, str_glue(
		"SELECT varname, tradedate, expdate AS date, tenor, value
		FROM interest_rate_model_futures_values
		WHERE
			scrape_source = 'cboe'
			AND varname IN ('ameribor')
			AND is_final IS TRUE
			AND tenor = '30d'
			AND tradedate >= '{min_tradedate}'",
		min_tradedate = backtest_vdates[1] - days(7)
	))

	raw_data =
		expand_grid(vdate = backtest_vdates, varname = c('ameribor'), months_out = 0:60) %>%
		mutate(., date = floor_date(vdate, 'month') %m+% months(months_out), min_tradedate = vdate - days(7)) %>%
		left_join(futures_df, join_by(varname, date, closest(vdate >= tradedate), min_tradedate <= tradedate)) %>%
		mutate(., max_months_out = max(ifelse(is.na(value), 0, months_out)), .by = c(vdate, varname)) %>%
		filter(., max_months_out != 0) # Keep vdate x varnames with at least some non-NA values

	# Backfill start-0 values with historical data if there's no forecast
	hist_daily = filter(bind_rows(hist), freq == 'd' & varname %in% c('ameribor'))

	# Get existing same-month mean values for each vdates
	hist_by_vdate =
		distinct(hist_daily, varname, vdate) %>%
		mutate(., date = floor_date(vdate, 'month')) %>%
		inner_join(
			.,
			transmute(hist_daily, varname, trailing_vdate = vdate, value, date = floor_date(date, 'month')),
			join_by(date, varname, vdate >= trailing_vdate)
		) %>%
		group_by(., varname, vdate, date) %>%
		summarize(., month_avg = mean(value), .groups = 'drop')

	filled_data =
		raw_data %>%
		left_join(
			.,
			transmute(hist_by_vdate, varname, hist_vdate = vdate, date, months_out = 0, month_avg),
			join_by(varname, months_out, date, closest(vdate >= hist_vdate))
		) %>%
		mutate(., value = ifelse(is.na(value), month_avg, value)) %>%
		# Some are still empty, so join in last available data as well even if not in the same month
		left_join(
			.,
			transmute(
				hist_daily, varname, months_out = 0,
				hist_d_date = date, hist_d_vdate = vdate, hist_d_value = value
			),
			join_by(varname, months_out, date >= hist_d_date, closest(vdate >= hist_d_vdate))
		) %>%
		# Needed in case there are different hist_d_dates with same hist_d_vdate
		slice_max(., hist_d_date, by = c(vdate, varname, date)) %>%
		mutate(., value = ifelse(is.na(value), hist_d_value, value)) %>%
		select(., vdate, varname, months_out, date, value, max_months_out)

	# Check if any vdate is missing data
	filled_data %>%
		filter(., months_out == 0 & is.na(value)) %>%
		summarize(., missing = sum(is.na(value)), .by = 'varname')

	cboe_data =
		filled_data %>%
		left_join(
			.,
			hist$afx %>%
				filter(., varname == 'ameribor') %>%
				slice_max(., date, with_ties = F, by = vdate) %>%
				transmute(., afx_vdate = vdate, ameribor_hist = value),
			join_by(closest(vdate >= afx_vdate))
		) %>%
		left_join(
			.,
			hist$fred %>%
				filter(., varname == 'sofr') %>%
				slice_max(., date, with_ties = F, by = vdate) %>%
				transmute(., ffr_vdate = vdate, ffr_hist = value),
			join_by(closest(vdate >= ffr_vdate))
		) %>%
		inner_join(
			.,
			submodels$cme %>% filter(., varname == 'sofr') %>% transmute(vdate, date, ffr_pred = value),
			join_by(date, vdate)
		) %>%
		group_split(., vdate) %>%
		map(., function(df) {
			# 50% FFR + Ameribor constant spread, 50% Ameribor futures
			df %>%
				mutate(., value = zoo::na.locf(value)) %>%
				mutate(., ffr_based_forecast = ameribor_hist - ffr_hist + ffr_pred) %>%
				mutate(., value = .15 * value + .85 * ffr_based_forecast)
		}) %>%
		list_rbind() %>%
		transmute(., vdate, date, freq = 'm', value, varname = 'ameribor')

	# Plot comparison against TDNS
	cboe_plot =
		cboe_data %>%
		filter(., vdate == max(vdate)) %>%
		bind_rows(
			.,
			filter(submodels$cme, varname == 'ffr' & vdate == max(vdate)),
			filter(submodels$cme, varname == 'sofr' & vdate == max(vdate))
			) %>%
		ggplot(.) +
		geom_line(aes(x = date, y = value, color = varname, group = varname))

	print(cboe_plot)

	submodels$cboe <<- cboe_data
})


## MORT ----------------------------------------------------------
local({

	backtest_vdates = seq(
		from = today('US/Eastern') %m-% months(BACKTEST_MONTHS),
		to = today('US/Eastern'),
		by = '1 day'
	)

	desired_hists = expand_grid(
		vdate = backtest_vdates,
		months_back = -36:0,
		varname = c('t10y', 't20y', 't30y', 'mort15y', 'mort30y', 'ffr')
		) %>%
		mutate(., date = add_with_rollback(floor_date(vdate, 'month'), months(months_back)))

	# Backfill start-0 values with historical data if there's no forecast
	hist_daily = filter(bind_rows(hist$fred, hist$treasury), varname %in% desired_hists$varname)

	# Month-avgs
	month_avgs =
		desired_hists %>%
		inner_join(
			.,
			transmute(hist_daily, varname, trailing_vdate = vdate, value, date = floor_date(date, 'month')),
			join_by(date, varname, vdate >= trailing_vdate)
		) %>%
		group_by(., varname, vdate, date) %>%
		summarize(., value = mean(value), .groups = 'drop')

	filled_data = left_join(desired_hists, month_avgs, join_by(varname, date, vdate))

	reg_data =
		filled_data %>%
		# pivot to vdate x date level
		pivot_wider(., id_cols = c(vdate, date), names_from = 'varname', values_from = 'value') %>%
		mutate(
			.,
			t15y = (t10y + t20y)/2,
			spread15 = mort15y - t15y, spread30 = mort30y - t30y
		) %>%
		arrange(., vdate, date) %>%
		mutate(., spread15.l1 = lag(spread15, 1), spread30.l1 = lag(spread30, 1), spread30.l2 = lag(spread30, 2))


	spread_forecast = list_rbind(map(group_split(reg_data, vdate), function(df) {

		spread30_lt = mean(df$spread30, na.rm = T)
		spread15_lt = mean(df$spread15, na.rm = T)

		spread30_l1 = tail(na.omit(df$spread30), 1)
		spread15_l1 = tail(na.omit(df$spread15), 1)

		# Diff a variant
		tibble(vdate = df$vdate[[1]], months_out = 0:60) %>%
			mutate(., lag_weight = 1 - tanh(months_out/30)) %>%
			mutate(
				.,
				spread30 = spread30_l1 * lag_weight + spread30_lt * (1 - lag_weight),
				spread15 = spread15_l1 * lag_weight + spread15_lt * (1 - lag_weight),
			)
	}))

	mortgage_data =
		spread_forecast %>%
		mutate(., date = floor_date(vdate, 'month') %m+% months(months_out)) %>%
		inner_join(
			.,
			submodels$tdns %>%
				filter(., varname %in% c('t10y', 't20y', 't30y')) %>%
				pivot_wider(., id_cols = c(date, vdate), names_from = varname, values_from = value),
				# transmute(., ffr_vdate, date, value),
			join_by(vdate, date)
		) %>%
		mutate(., mort15y = (t10y + t30y)/2 + spread15, mort30y = t30y + spread30) %>%
		select(., vdate, date, mort15y, mort30y) %>%
		pivot_longer(., -c(vdate, date), names_to = 'varname', values_to = 'value') %>%
		transmute(
			.,
			varname,
			freq = 'm',
			vdate,
			date,
			value
		)

	mort_plots =
		mortgage_data %>%
		filter(., vdate == max(vdate)) %>%
		ggplot() +
		geom_line(aes(x = date, y = value, color = varname))

	print(mort_plots)

	submodels$mort <<- mortgage_data
})

## SPREADS ----------------------------------------------------------
local({

	backtest_vdates = seq(
		from = today('US/Eastern') %m-% months(BACKTEST_MONTHS),
		to = today('US/Eastern'),
		by = '1 day'
	)

	# Intra-Treasury spreads (10-3, 10-2)
	intra_treasury_spreads =
		submodels$tdns %>%
		pivot_wider(., id_cols = c(vdate, date, freq), names_from = varname, values_from = value) %>%
		transmute(
			.,
			vdate, date, freq,
			t10yt02yspread = t10y - t02y,
			t10yt03mspread = t10y - t03m
		) %>%
		pivot_longer(
			.,
			cols = -c(vdate, date, freq),
			names_to = 'varname',
			values_to = 'value',
			values_drop_na = T
			) %>%
		transmute(
			.,
			varname,
			freq = 'm',
			vdate,
			date,
			value
		)

	# Treasury-mortgage spread
	mortgage_spreads =
		submodels$mort %>%
		filter(., varname == 'mort30y') %>%
		inner_join(
			.,
			submodels$tdns %>%
				filter(., varname == 't10y' & freq == 'm') %>%
				transmute(., t_vdate = vdate, date, t_value = value),
			join_by(date, closest(vdate >= t_vdate))
			) %>%
		mutate(., value = value - t_value) %>%
		transmute(
			.,
			varname = 'mort30yt10yspread',
			freq = 'm',
			vdate,
			date,
			value
		)

	# Real intra-Treasury spreads
	intra_treasury_real_spreads =
		submodels$real %>%
		pivot_wider(., id_cols = c(vdate, date, freq), names_from = varname, values_from = value) %>%
		transmute(
			.,
			vdate, date, freq,
			rt10yrt02yspread = rt10y - rt02y
		) %>%
		pivot_longer(
			.,
			cols = -c(vdate, date, freq),
			names_to = 'varname',
			values_to = 'value',
			values_drop_na = T
		) %>%
		transmute(
			.,
			varname,
			freq = 'm',
			vdate,
			date,
			value
		)

	spreads = bind_rows(
		intra_treasury_spreads,
		mortgage_spreads,
		intra_treasury_real_spreads
	)

	submodels$spreads <<- spreads
})

# Finalize ----------------------------------------------------------

## Store in SQL ----------------------------------------------------------
local({

	submodel_values =
		bind_rows(
			submodels$ice,
			submodels$cme,
			submodels$tdns,
			submodels$real,
			submodels$cboe,
			submodels$mort,
			submodels$spreads
		) %>%
		transmute(., forecast = 'int', form = 'd1', vdate, freq, varname, date, value)

	rows_added = store_forecast_values_v2(pg, submodel_values, .store_new_only = STORE_NEW_ONLY, .verbose = T)

	# Log
	validation_log$store_new_only <<- STORE_NEW_ONLY
	validation_log$rows_added <<- rows_added
	validation_log$last_vdate <<- max(submodel_values$vdate)

	disconnect_db(pg)

	submodel_values <<- submodel_values
})
