#' Historical data lags:
#' - FFR/SOFR: 1 day lag (9am ET)
#' - Treasury data: day of
#' - Bloomberg indices: day of
#' - AFX indices: 1 day lag (8am ET)
#' Historical vintage dates are assigned given these assumptions!
#'
#' TBD:
#' - Merge TFUT
#' - Add vintage testing for mortgage rates
#'
#' Validated 11/16/23

# Initialize ----------------------------------------------------------
BACKTEST_MONTHS = 120
STORE_NEW_ONLY = F

validation_log <<- list()
data_dump <<- list()

## Load Libs ----------------------------------------------------------
library(macropredictions)
library(tidyverse)
library(httr2)
library(rvest)
library(data.table)
library(forecast, include.only = c('forecast', 'Arima'))
load_env()

## Load Data ----------------------------------------------------------
pg = connect_pg()

hist <- list()
submodels <- list()

## Load Variable Defs ----------------------------------------------------------
input_sources = get_query(pg, 'SELECT * FROM interest_rate_model_variables')

# Historical Data ----------------------------------------------------------

## FRED ----------------------------------------------------------
# Vintages release with 1-day lag relative to current vals.
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
		get_fred_obs(., api_key, .obs_start = '2010-01-01', .verbose = T) %>%
		left_join(
			.,
			select(input_sources, 'varname', 'hist_source_key'),
			by = c('series_id' = 'hist_source_key'),
			relationship = 'many-to-one'
			) %>%
		select(., -series_id) %>%
		# For simplicity, assume all data with daily frequency is released the same day.
		# Only weekly/monthly data keeps the true vintage date.
		mutate(., vdate = case_when(
			freq == 'd' ~ date + days(1),
			str_detect(varname, 'mort') ~ date + days(7),
			TRUE ~ NA_Date_
		))

	hist$fred <<- fred_data
})

## TREAS ----------------------------------------------------------
local({

	treasury_data = c(
		'https://home.treasury.gov/system/files/276/yield-curve-rates-2011-2020.csv',
		paste0(
			'https://home.treasury.gov/resource-center/data-chart-center/interest-rates/daily-treasury-rates.csv/',
			2021:year(today('US/Eastern')),
			'/all?type=daily_treasury_yield_curve&field_tdr_date_value=2023&page&_format=csv'
			)
		) %>%
		map(., .progress = F, \(x) read_csv(x, col_types = 'c')) %>%
		list_rbind %>%
		pivot_longer(., cols = -c('Date'), names_to = 'varname', values_to = 'value') %>%
		separate(., col = 'varname', into = c('ttm_1', 'ttm_2'), sep = ' ') %>%
		mutate(
			.,
			varname = paste0('t', str_pad(ttm_1, 2, pad = '0'), ifelse(ttm_2 == 'Mo', 'm', 'y')),
			date = mdy(Date),
			) %>%
		transmute(
			.,
			vdate = date,
			freq = 'd',
			varname,
			date,
			value
		) %>%
		filter(., !is.na(value)) %>%
		filter(., varname %in% filter(input_sources, hist_source == 'treas')$varname) %>%
		arrange(., vdate)

	hist$treasury <<- treasury_data
})

## BLOOM  ----------------------------------------------------------
local({

	request('https://www.bloomberg.com') %>%
		add_standard_headers %>%
		req_perform

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
					# 'Referer' = str_glue('https://www.bloomberg.com/quote/{x$source_key}:IND'),
					'Accept' = 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
					`User-Agent` = paste0(
						'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 ',
						'(KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'
						),
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
					vdate = date,
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
			transmute(., vdate = date, varname = x$varname, freq = 'd', date, value)
		) %>%
		bind_rows(.)

	 hist$boe <<- boe_data
})

## Store in SQL ----------------------------------------------------------
local({

	message('**** Storing SQL Data')

	hist_values =
		bind_rows(hist) %>%
		bind_rows(
			.,
			filter(., freq %in% c('d', 'w')) %>%
				mutate(., date = floor_date(date, 'months'), freq = 'm') %>%
				group_by(., varname, freq, date) %>%
				summarize(., value = mean(value), .groups = 'drop')
			) %>%
		# vdate is the same as date
		mutate(., vdate = date, form = 'd1')

	initial_count = get_rowcount(pg, 'interest_rate_model_input_values')
	message('***** Initial Count: ', initial_count)

	sql_result = write_df_to_sql(
		pg,
		hist_values,
		'interest_rate_model_input_values',
		'ON CONFLICT (vdate, form, varname, freq, date) DO UPDATE SET value=EXCLUDED.value'
	)

	message('***** Rows Added: ', get_rowcount(pg, 'interest_rate_model_input_values') - initial_count)

	hist_values <<- hist_values
})

# Sub-Models  ----------------------------------------------------------

## CME: Futures  ----------------------------------------------------------
local({

	# Get prev day's closes, current day usually missing data
	futures_df = get_query(pg, sql(
		"SELECT scrape_source, varname, vdate, expdate AS date, tenor, value
		FROM interest_rate_model_futures_values
		WHERE
			varname IN ('bsby', 'ffr', 'sofr')
			AND scrape_source IN ('bc')"
	))

	barchart_data =
		futures_df %>%
		filter(., varname != 'sofr' | (varname == 'sofr' & tenor == '3m')) %>%
		mutate(., vdate = vdate) %>%
		# Adjust futures forecast date forward compared to hist date
		transmute(varname, vdate = vdate + days(ifelse(varname == 'bsby', 2, 1)), date, value)

	# Fill in missing forward forecasts using prior forecasts of same day within month
	bc_interp =
		expand_grid(distinct(barchart_data, vdate, varname), months_forward = 0:360) %>%
		mutate(., date = floor_date(vdate, 'month') + months(months_forward)) %>%
		inner_join(
			.,
			rename(barchart_data, prev_vdate = vdate),
			join_by(varname, date, closest(vdate >= prev_vdate))
			) %>%
		filter(., prev_vdate >= vdate - days(7))

	## Bloom forecasts
	# These are necessary to fill in missing BSBY forecasts for first 3 months before
	# date of first CME future
	bloom_data =
		hist$bloom %>%
		filter(., date == max(date) & varname == 'bsby') %>%
		transmute(
			.,
			varname = 'bsby',
			vdate = max(filter(barchart_data, varname == 'bsby')$vdate),
			date = floor_date(date, 'months'), value
			)

	## Combine datasets and add monthly interpolation
	message('Adding monthly interpolation ...')
	final_df =
		bc_interp %>%
		# Replace Bloom futures with data
		full_join(., rename(bloom_data, bloom = value), by = c('varname', 'vdate', 'date')) %>%
		mutate(., value = ifelse(is.na(value), bloom, value)) %>%
		select(., -bloom) %>%
		# If this months forecast missing for BSBY, add it in for interpolation purposes
		# Otherwise the dataset starts 3 months out
		group_split(., vdate, varname) %>%
		map(., .progress = T, function(x) {
			x %>%
				# Join on missing obs dates
				right_join(
					.,
					tibble(
						varname = unique(x$varname),
						vdate = unique(x$vdate),
						date = seq(from = min(x$date), to = max(x$date), by = '1 month')
					),
					by = c('varname', 'vdate', 'date')
				) %>%
				arrange(date) %>%
				transmute(
					.,
					varname = unique(varname),
					freq = 'm',
					vdate = unique(vdate),
					date,
					value = zoo::na.approx(value)
				)
		}) %>%
		list_rbind %>%
		# Now smooth all values more than 2 years out due to low liquidity
		group_split(., vdate, varname) %>%
		map(., function(x)
			x %>%
				mutate(., value_smoothed = zoo::rollmean(value, 6, fill = NA, align = 'right')) %>%
				# %m+% handles leap years and imaginary days, regular addition of years does not
				mutate(., value = ifelse(!is.na(value_smoothed) & date >= vdate %m+% years(2), value_smoothed, value))
		) %>%
		list_rbind %>%
		select(., -value_smoothed)

	# Print diagnostics
	final_df %>%
		filter(., vdate == max(vdate)) %>%
		pivot_wider(., id_cols = 'date', names_from = 'varname', values_from = 'value') %>%
		arrange(., date)

	series_chart =
		final_df %>%
		filter(., vdate == max(vdate), .by = varname) %>%
		ggplot() +
		geom_line(aes(x = date, y = value, color = varname, group = varname)) +
		facet_grid(rows = vars(varname))

	print(series_chart)

	submodels$cme <<- final_df
})

## ICE: Futures  ----------------------------------------------------------
local({

	futures_df = get_query(pg, sql(
		"SELECT varname, vdate, expdate AS date, tenor, value
		FROM interest_rate_model_futures_values
		WHERE scrape_source = 'ice'"
	))

	ice_data =
		futures_df %>%
		# Now use only one-month futures if it's the only available data
		group_by(varname, vdate, date) %>%
		filter(., tenor == min(tenor)) %>%
		arrange(., date) %>%
		ungroup %>%
		# Get rid of forecasts for old observations
		filter(., date >= floor_date(vdate, 'month')) %>%
		transmute(., varname, freq = 'm', vdate, date, value)

	# ice_data %>%
	# 	filter(., vdate == max(vdate)) %>%
	# 	ggplot(.) +
	# 	geom_line(aes(x = date, y = value, color = varname))

	## Now calculate BOE Bank Rate
	spread_df =
		hist$boe %>%
		filter(freq == 'd') %>%
		pivot_wider(., id_cols = date, names_from = varname, values_from = value) %>%
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
			spread_df %>% transmute(., lagged_vdate = date, spread),
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

	final_df %>%
		filter(., vdate == max(vdate)) %>%
		ggplot() +
		geom_line(aes(x = date, y = value, color = varname, group = varname))

	submodels$ice <<- final_df
})


## TDNS: Nelson-Siegel Treasury Yield Forecast ----------------------------------------------------------
local({

	#' This relies heavily on expectations theory weighted to federal funds rate;
	#' Consider taking a weighted average with Treasury futures directly
	#' (see CME micro futures)
	#' https://www.bis.org/publ/qtrpdf/r_qt1809h.htm
	message('***** Adding Treasury Forecasts')

	# Create tibble mapping tyield_3m to 3, tyield_1y to 12, etc.
	yield_curve_names_map =
		input_sources %>%
		filter(., str_detect(varname, '^t\\d{2}[m|y]$')) %>%
		select(., varname) %>%
		mutate(., ttm = as.numeric(str_sub(varname, 2, 3)) * ifelse(str_sub(varname, 4, 4) == 'y', 12, 1)) %>%
		arrange(., ttm)

	fred_data  =
		bind_rows(hist$fred, hist$treasury) %>%
		filter(., str_detect(varname, '^t\\d{2}[m|y]$') | varname == 'ffr') %>%
		select(., -vdate)

	# Get all vintage dates that need to be backtested
	backtest_vdates =
		submodels$cme %>%
		filter(., varname == 'ffr') %>%
		transmute(., ffr = value, vdate, date) %>%
		# Only backtest vdates with at least 36 months of FFR forecasts
		group_by(., vdate) %>%
		mutate(., vdate_forecasts = n()) %>%
		filter(., vdate_forecasts >= 36) %>%
		# Limit to backtest vdates
		filter(., vdate >= today() - months(BACKTEST_MONTHS)) %>%
		group_by(., vdate) %>%
		summarize(.) %>%
		.$vdate

	# Get available historical data at each vintage date, up to 36 months of history
	hist_df_unagg =
		tibble(vdate = backtest_vdates) %>%
		mutate(., floor_vdate = floor_date(vdate, 'month')) %>%
		left_join(
			.,
			fred_data %>% mutate(., floor_date = floor_date(date, 'month')),
			# !! Only pulls dates that are strictly less than the vdate (1 day EFFR delay in FRED)
			join_by(vdate > date)
			) %>%
		# Get floor month diff
		left_join(
			.,
			distinct(., floor_vdate, floor_date) %>%
				mutate(., months_diff = interval(floor_date, floor_vdate) %/% months(1)),
			by = c('floor_vdate', 'floor_date')
		) %>%
		filter(., months_diff <= 36) %>%
		mutate(., is_current_month = floor_vdate == floor_date)

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

	# Create training dataset on spread
	hist_df =
		filter(hist_df_0, varname %in% yield_curve_names_map$varname) %>%
		right_join(., yield_curve_names_map, by = 'varname') %>%
		left_join(.,
			transmute(filter(hist_df_0, varname == 'ffr'), vdate, date, ffr = value),
			by = c('vdate', 'date')
			) %>%
		mutate(., value = value - ffr) %>%
		select(., -ffr)

	# Filter to only last 3 months
	train_df = hist_df %>% filter(., date >= add_with_rollback(vdate, months(-4)))

	#' Calculate DNS fit
	#'
	#' @param df: A tibble continuing columns date, value, and ttm
	#' @param return_all: FALSE by default.
	#' If FALSE, will return only the MSE (useful for optimization).
	#' Otherwise, will return a tibble containing fitted values, residuals, and the beta coefficients.
	#'
	#' @details
	#' Recasted into data.table as of 12/21/22, significant speed improvements (600ms optim -> 110ms)
	#'	test = train_df %>% group_split(., vdate) %>% .[[1]]
	#'  microbenchmark::microbenchmark(
	#'  	optimize(get_dns_fit, df = test, return_all = F, interval = c(-1, 1), maximum = F)
	#'  	times = 10,
	#'  	unit = 'ms'
	#'	)
	#' @export
	get_dns_fit = function(df, lambda, return_all = FALSE) {
		df %>%
			# mutate(f1 = 1, f2 = (1 - exp(-1 * lambda * ttm))/(lambda * ttm), f3 = f2 - exp(-1 * lambda * ttm)) %>%
			# group_split(., date) %>%
			# map_dfr(., function(x) {
			# 	reg = lm(value ~ f1 + f2 + f3 - 1, data = x)
			# 	bind_cols(x, fitted = fitted(reg)) %>%
			# 		mutate(., b1 = coef(reg)[['f1']], b2 = coef(reg)[['f2']], b3 = coef(reg)[['f3']]) %>%
			# 		mutate(., resid = value - fitted)
			# 	}) %>%
			as.data.table(.) %>%
			.[, c('f1', 'f2') := list(1, (1 - exp(-1 * lambda * ttm))/(lambda * ttm))] %>%
			.[, f3 := f2 - exp(-1*lambda*ttm)] %>%
			split(., by = 'date') %>%
			lapply(., function(x) {
				reg = lm(value ~ f1 + f2 + f3 - 1, data = x)
				x %>%
					.[, fitted := fitted(reg)] %>%
					.[, c('b1', 'b2', 'b3') := list(coef(reg)[['f1']], coef(reg)[['f2']], coef(reg)[['f3']])] %>%
					.[, resid := value - fitted] %>%
					.[, lambda := lambda]
				}) %>%
			rbindlist(.) %>%
			{if (return_all == FALSE) mean(abs(.$resid)) else as_tibble(.)}
	}

	# Find MSE-minimizing lambda value by vintage date
	optim_lambdas =
		train_df %>%
		group_split(., vdate) %>%
		map(., .progress = T, function(x) {
			list(
				vdate = x$vdate[[1]],
				train_df = x,
				lambda = optimize(
					get_dns_fit,
					df = x,
					return_all = F,
					interval = c(-1, 1),
					maximum = F
					)$minimum
				)
			})

	# Get historical DNS fits by vintage date
	dns_fit_hist = map_dfr(optim_lambdas, \(x) get_dns_fit(df = x$train_df, x$lambda, return_all = T))

	# Get historical DNS hists by vintage date
	dns_coefs_hist =
		dns_fit_hist %>%
		group_by(., vdate, date) %>%
		summarize(., lambda = unique(lambda), tdns1 = unique(b1), tdns2 = unique(b2), tdns3 = unique(b3), .groups = 'drop')

	# Check fits for last 12 vintage date (last historical fit for each vintage)
	dns_fit_plots =
		dns_fit_hist %>%
		filter(., vdate %in% tail(unique(dns_coefs_hist$vdate, 12))) %>%
		group_by(., vdate) %>%
		filter(., date == max(date)) %>%
		ungroup(.) %>%
		arrange(., vdate, ttm) %>%
		ggplot(.) +
		geom_point(aes(x = ttm, y = value)) +
		geom_line(aes(x = ttm, y = fitted)) +
		facet_wrap(vars(vdate))

	print(dns_fit_plots)

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
		mutate(
			.,
			annualized_yield_dns =
				tdns1 +
				tdns2 * (1-exp(-1 * lambda * ttm))/(lambda * ttm) +
				tdns3 *((1-exp(-1 * lambda * ttm))/(lambda * ttm) - exp(-1 * lambda * ttm)),
			) %>%
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
		filter(., vdate %in% c(head(unique(.$vdate, 4)), tail(unique(.$vdate), 4))) %>%
		ggplot(.) +
		geom_line(aes(x = ttm, y = annualized_yield_dns), color = 'blue', linewidth = 1.5) +
		geom_line(aes(x = ttm, y = spline_fit), color = 'black') +
		geom_point(aes(x = ttm, y = yield_hist), color = 'black') +
		geom_line(aes(x = ttm, y = annualized_yield), color = 'red', linewidth = 2, alpha = .5) +
		facet_wrap(vars(vdate))

	print(full_fit_plots)

	# Iterate over "yttms" tyield_1m, tyield_3m, ..., etc.
	# and for each, iterate over the original "ttms" 1, 2, 3,
	# ..., 120 and for each forecast the cumulative return for the yttm period ahead.
	expectations_forecasts =
		yield_curve_names_map$ttm %>%
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
				inner_join(., yield_curve_names_map, c('yttm' = 'ttm'))
		) %>%
		list_rbind %>%
		mutate(., date = add_with_rollback(floor_date(vdate, 'months'), months(ttm - 1))) %>%
		select(., vdate, varname, date, value = yttm_ahead_annualized_yield) %>%
		inner_join(
			.,
			submodels$cme %>%
				filter(., varname == 'ffr') %>%
				transmute(., ffr_vdate = vdate, ffr = value, date),
			join_by(date, closest(vdate >= ffr_vdate)) # Use last available ffr_vdate (but in practice always ==)
			) %>%
		# Only include forecasts where the ffr vdate was within 7 days of tyield vdate
		filter(., interval(ffr_vdate, vdate) %/% days(1) <= 7) %>%
		transmute(., vdate, varname, date, value = value + ffr)

	# Plot point forecasts
	expectations_forecasts %>%
		filter(., vdate == max(vdate)) %>%
		ggplot(.) +
		geom_line(aes(x = date, y = value, color = varname)) +
		labs(title = 'Current forecast')

	# Plot curve forecasts
	expectations_forecasts %>%
		filter(., vdate == max(vdate)) %>%
		left_join(., yield_curve_names_map, by = 'varname') %>%
		ggplot() +
		geom_line(aes(x = ttm, y = value, color = date, group = date))

	# Plot forecasts over time for current period
	expectations_forecasts %>%
		filter(., date == floor_date(today(), 'months')) %>%
		ggplot(.) +
		geom_line(aes(x = vdate, y = value, color = varname))

	# Check spread history
	expectations_forecasts %>%
		pivot_wider(., id_cols = c(vdate, date), names_from = varname, values_from = value) %>%
		mutate(., spread = t10y - t02y) %>%
		mutate(., months_ahead = interval(vdate, date) %/% months(1)) %>%
		select(., vdate, date, spread, months_ahead) %>%
		group_by(., vdate, months_ahead) %>%
		summarize(., spread = mean(spread), .groups = 'drop') %>%
		arrange(., vdate, months_ahead) %>%
		filter(., months_ahead %in% c(0, 12, 36, 60)) %>%
		ggplot(.) +
		geom_line(aes(x = vdate, y = spread, color = as.factor(months_ahead))) +
		labs(title = 'Forecasted 10-2 Spread (Months Ahead)', x = 'vdate', y = 'spread')

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

	## Now add historical ratios in for LT term premia

	# For 10-year forward forecast, use long-term spread forecast
	# Exact interpolation at 10-3 10 years ahead
	# These forecasts only account for expectations theory without accounting for
	# https://www.bis.org/publ/qtrpdf/r_qt1809h.htm
	# Get historical term premium
	term_prems_raw = collect(tbl(pg, sql(
		"SELECT vdate, varname, date, d1 AS value
		FROM forecast_values_v2_all
		WHERE forecast = 'spf' AND varname IN ('t03m', 't10y')"
	)))

	# Get long-term spreads forecast
	forecast_spreads = lapply(c('tbill', 'tbond'), function(var) {

		request(paste0(
			'https://www.philadelphiafed.org/-/media/frbp/assets/surveys-and-data/',
			'survey-of-professional-forecasters/data-files/files/median_', var, '_level.xlsx?la=en'
			)) %>%
			req_perform(., path = file.path(tempdir(), paste0(var, '.xlsx')))

		readxl::read_excel(file.path(tempdir(), paste0(var, '.xlsx')), na = '#N/A', sheet = 'Median_Level') %>%
			select(., c('YEAR', 'QUARTER', paste0(str_to_upper(var), 'D'))) %>%
			na.omit %>%
			transmute(
				.,
				reldate = from_pretty_date(paste0(YEAR, 'Q', QUARTER), 'q'),
				varname = {if (var == 'tbill') 't03m_lt' else 't10y_lt'},
				value = .[[paste0(str_to_upper(var), 'D')]]
			)
		}) %>%
		list_rbind(.) %>%
		# Guess release dates
		left_join(., term_prems_raw %>% group_by(., vdate) %>% summarize(., reldate = min(date)), by = 'reldate') %>%
		pivot_wider(., id_cols = vdate, names_from = varname, values_from = value) %>%
		mutate(., spread = t10y_lt - t03m_lt) %>%
		arrange(., vdate) %>%
		mutate(., spread = zoo::rollmean(spread, 2, fill = NA, align = 'right')) %>%
		tail(., -1)

	# 1. At each historical vdate, get the trailing 36-month historical spread between all Treasuries with the 3m yield
	# 2. Get the relative ratio of these historical spreads relative to the 10-3 spread (%diff from 0)
	# 3. Get the forecasted SPF long-term spread for the latest forecast available at each historical vdate
	# 4. Compare the forecast SPF long-term spread to the hist_spread_ratio
	# - Ex. Hist 10-3 spread = .5; hist 30-3 spread = 2 => 4x multiplier (spread_ratio)
	# - Forecast 10-3 spread = 1; want to get forecast 30-3; so multiply historical spread_ratio * 1
	# Get relative ratio of historical diffs to 10-3 year forecast to generate spread forecasts (from 3mo) for all variables
	# Then reweight these back in time towards other
	historical_ratios =
		hist_df %>%
		{left_join(
			filter(., varname != 't03m') %>% transmute(., date, vdate, ttm, varname, value),
			filter(., varname == 't03m') %>% transmute(., date, vdate, t03m = value),
			by = c('vdate', 'date')
		)} %>%
		mutate(., spread = value - t03m) %>%
		group_by(., vdate, varname, ttm) %>%
		summarize(., mean_spread_above_3m = mean(spread), .groups = 'drop') %>%
		arrange(., ttm) %>%
		{left_join(
			transmute(., vdate, varname, ttm, mean_spread_above_3m),
			filter(., varname == 't10y') %>% transmute(., vdate, t10y_mean_spread_above_3m = mean_spread_above_3m),
			by = c('vdate')
		)} %>%
		mutate(., hist_spread_ratio_to_10_3 = case_when(
			t10y_mean_spread_above_3m <= 0 ~ max(0, mean_spread_above_3m),
			mean_spread_above_3m <= 0 ~ 0,
			TRUE ~ mean_spread_above_3m /t10y_mean_spread_above_3m
		)) %>%
		# Sanity check
		mutate(., hist_spread_ratio_to_10_3 = ifelse(hist_spread_ratio_to_10_3 > 3, 3, hist_spread_ratio_to_10_3)) %>%
		arrange(., vdate)

	if (!all(sort(unique(historical_ratios$vdate)) == sort(backtest_vdates))) {
		stop ('Error: lost vintage dates!')
	}

	# Why so many *discontinuous* drops
	forecast_lt_spreads =
		historical_ratios %>%
		select(., vdate, ttm, varname, hist_spread_ratio_to_10_3) %>%
		left_join(
			.,
			transmute(forecast_spreads, hist_vdate = vdate, forecast_spread_10_3 = spread),
			join_by(closest(vdate >= hist_vdate))
		) %>%
		mutate(., forecast_lt_spread = hist_spread_ratio_to_10_3 * forecast_spread_10_3) %>%
		select(., vdate, varname, forecast_spread_10_3, forecast_lt_spread)

	forecast_lt_spreads %>%
		ggplot(.) +
		geom_line(aes(x = vdate, y = forecast_lt_spread, color = varname)) +
		geom_point(aes(x = vdate, y = forecast_spread_10_3, color = varname))


	# LT spread forecasts by date
	adj_forecasts_raw =
		expectations_forecasts %>%
		filter(., varname != 't03m') %>%
		left_join(
			.,
			transmute(filter(expectations_forecasts, varname == 't03m'), vdate, date, t03m = value),
			by = c('vdate', 'date')
		) %>%
		mutate(
			.,
			months_ahead = interval(floor_date(vdate, 'months'), date) %/% months(1),
			lt_weight = ifelse(months_ahead >= 60, 1/3, months_ahead/60 * 2/3),
			spread = value - t03m
			) %>%
		left_join(., forecast_lt_spreads, by = c('varname', 'vdate')) %>%
		mutate(., adj_spread = (1 - lt_weight) * spread + lt_weight * forecast_lt_spread) %>%
		mutate(., adj_value = adj_spread + t03m)

	adj_forecasts_raw %>%
		pivot_longer(., cols = c(value, adj_value)) %>%
		filter(., vdate == max(vdate)) %>%
		ggplot(.) +
		geom_line(aes(x = date, y = value, color = name)) +
		facet_wrap(vars(varname))

	adj_forecasts =
		adj_forecasts_raw %>%
		transmute(., vdate, varname, date, value = adj_value) %>%
		bind_rows(., filter(expectations_forecasts, varname == 't03m')) %>%
		arrange(., vdate, varname, date)


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
		adj_forecasts %>%
		mutate(., months_ahead = interval(floor_date(vdate, 'months'), date) %/% months(1)) %>%
		left_join(
			.,
			# Use hist proper mean of existing data
			hist_df_unagg %>%
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
			forecast_origin_y = logistic((1 - day(vdate)/days_in_month(vdate)), get_logistic_x0(1/3, k = 2), k = 2),
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

	hist_merged_df %>%
		filter(., date == floor_date(today('US/Eastern'), 'month'), varname == 't10y') %>%
		ggplot(.) +
		geom_line(aes(x = vdate, y = forecast_weight, color = format(vdate, '%Y%m'))) +
		labs(title = 'Forecast weights for this months forecast over time')

	hist_merged_df %>%
		filter(., vdate == max(vdate), varname == 't30y') %>%
		ggplot(.) +
		geom_line(aes(x = date, y = forecast_weight)) +
		labs(title = 'Forecast weights generated on this vdate for future forecasts')

	# Test plot 1
	hist_merged_df %>%
		filter(., vdate %in% head(unique(hist_merged_df$vdate), 10)) %>%
		group_by(., vdate) %>%
		filter(., date == min(date)) %>%
		inner_join(., yield_curve_names_map, by = 'varname') %>%
		arrange(., ttm) %>%
		ggplot(.) +
		geom_point(aes(x = ttm, y = final_value), color = 'green') +
		geom_point(aes(x = ttm, y = value), color = 'blue') +
		geom_point(aes(x = ttm, y = last_hist), color = 'black') +
		facet_wrap(vars(vdate)) +
		labs(title = 'Forecasts - last 10 vdates', subtitle = 'green = joined, blue = unjoined_forecast, black = hist')

	# Test plot 2
	hist_merged_df %>%
		filter(., vdate == max(vdate) & date <= today() + years(1)) %>%
		inner_join(., yield_curve_names_map, by = 'varname') %>%
		arrange(., ttm) %>%
		ggplot(.) +
		geom_point(aes(x = ttm, y = final_value), color = 'green') +
		geom_point(aes(x = ttm, y = value), color = 'blue') +
		geom_point(aes(x = ttm, y = last_hist), color = 'black') +
		facet_wrap(vars(date)) +
		labs(title = 'Forecasts - 1vdate', subtitle = 'green = joined, blue = unjoined_forecast, black = hist')

	## TBD: WEIGHT OF MONTH 0 SHOULD DEPEND ON HOW FAR IT IS INTO TE MONTH
	merged_forecasts =
		hist_merged_df %>%
		transmute(., vdate, varname, freq = 'm', date, value = final_value)

	merged_forecasts %>%
		filter(., vdate == max(vdate)) %>%
		ggplot(.) +
		geom_line(aes(x = date, y = value)) +
		facet_wrap(vars(varname))

	bind_rows(
		expectations_forecasts %>% filter(., vdate == max(vdate) - days(0)) %>% mutate(., type = 'expectations'),
		adj_forecasts %>% filter(., vdate == max(vdate) - days(0)) %>% mutate(., type = 'adj'),
		merged_forecasts %>% filter(., vdate == max(vdate) - days(0)) %>% mutate(., type = 'merged')
		) %>%
		ggplot(.) +
		geom_line(aes(x = date, y = value, color = type)) +
		facet_wrap(vars(varname))

	# Take same-month t01m forecasts and merge with last historical period
	# at the short end during financial risk periods
	# Weight of last period is .5 at start of month and decreases to 0 at end of month
	final_forecasts =
		filter(merged_forecasts, floor_date(vdate, 'month') == date & varname == 't01m') %>%
		left_join(
			hist_df_unagg %>%
				group_by(., vdate, varname) %>%
				filter(., date == max(date)) %>%
				ungroup(.) %>%
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

	tdns <<- list(
		dns_coefs_hist = dns_coefs_hist,
		dns_coefs_forecast = dns_coefs_forecast
		)

	submodels$tdns <<- final_forecasts
})


## TDNSv2 ------------------------------------------------------------------
# https://www.bis.org/publ/qtrpdf/r_qt1809h.htm
local({

	message('***** Adding Treasury Forecasts')

	### 1. Get input data
	# Create tibble mapping tyield_3m to 3, tyield_1y to 12, etc.
	yield_curve_names_map =
		input_sources %>%
		filter(., str_detect(varname, '^t\\d{2}[m|y]$')) %>%
		select(., varname) %>%
		mutate(., ttm = as.numeric(str_sub(varname, 2, 3)) * ifelse(str_sub(varname, 4, 4) == 'y', 12, 1)) %>%
		arrange(., ttm)

	fred_data =
		bind_rows(hist$fred, hist$treasury) %>%
		filter(., str_detect(varname, '^t\\d{2}[m|y]$')) %>%
		select(., -vdate)

	ffr_lt = get_fred_obs('FEDTARMDLR', Sys.getenv('FRED_API_KEY')) %>% transmute(vdate = date, value)

	# Get all vintage dates that need to be backtested
	backtest_vdates =
		submodels$cme %>%
		filter(., varname == 'ffr') %>%
		transmute(., ffr = value, vdate, date) %>%
		# Only backtest vdates with at least 28 months of FFR forecasts
		group_by(., vdate) %>%
		mutate(., vdate_forecasts = n()) %>%
		filter(., vdate_forecasts >= 28) %>%
		# Limit to backtest vdates
		filter(., vdate >= today() - months(BACKTEST_MONTHS)) %>%
		group_by(., vdate) %>%
		summarize(.) %>%
		.$vdate

	# Get available historical data at each vintage date, up to 36 months of history
	hist_df_unagg =
		tibble(vdate = backtest_vdates) %>%
		mutate(., floor_vdate = floor_date(vdate, 'month'), min_date = floor_vdate - months(36)) %>%
		left_join(
			.,
			mutate(fred_data, floor_date = floor_date(date, 'month')),
			# !! Only pulls dates that are strictly less than the vdate (1 day EFFR delay in FRED)
			join_by(vdate > date, min_date <= date)
		) %>%
		mutate(., is_current_month = floor_vdate == floor_date) %>%
		select(., -min_date, -floor_vdate)

	gc()

	### 2. Get annualized risk-free rates
	# For each vdate v, get the forecasted annualized risk-free rate between v+j and v+j+m periods ahead
	# (to cast into same periods as bond durations)
	# This returns a df of estimated annualized risk-free rates at vintage v for forecast month v+j and m duration
	rf_annualized =
		submodels$cme %>%
		filter(., vdate %in% backtest_vdates & varname == 'ffr') %>%
		transmute(., vdate, value, date) %>%
		arrange(vdate) %>%
		mutate(., j = interval(floor_date(vdate, 'month'), date) %/% months(1)) %>%
		# Add long-run forecasts & interpolate if necessary
		group_split(., vdate) %>%
		map(., function(df) {
			df %>%
				full_join(., tibble(j = seq(from = max(df$j) + 1, to = 480, by = 1)), by = 'j') %>%
				mutate(
					.,
					value = ifelse(
						j == max(j),
						1/3 * tail(df$value, 1) + 2/3 * tail(filter(ffr_lt, vdate <= df$vdate[[1]]), 1)$value,
						value
						),
					value = zoo::na.approx(value, na.rm = F),
					vdate = unique(df$vdate)
					)
		}) %>%
		list_rbind()

	rf_test_1 =
		rf_annualized %>%
		filter(., vdate %in% sample(vdate, 10)) %>%
		ggplot() +
		geom_line(aes(x = date, y = value, color = as.factor(vdate)))

	print(rf_test_1)

	# Expand grid with Treasury durations - for each vdate, returns annualized rf between date and date+ttm
	rf_df = list_rbind(lapply(yield_curve_names_map$ttm, function(ttm) {
		rf_annualized %>%
			# https://quant.stackexchange.com/questions/22395/central-bank-interest-rates-are-they-quoted-annualized
			mutate(
				.,
				yield_j_to_j_plus_1 = value/12,
				# The cumulative return expected going through period j
				cumret_v_to_v_plus_j = cumprod(1 + yield_j_to_j_plus_1/100),
				cumret_j_to_j_plus_m = lead(cumret_v_to_v_plus_j, ttm, order_by = j)/cumret_v_to_v_plus_j,
				annret_j_to_j_plus_m = (cumret_j_to_j_plus_m - 1) * 100 * 12/(ttm),
				# annualized_return_v_to_v_plus_j = ((1 + cum_return_v_to_v_plus_j/100)^(12/(j + 1)) - 1) * 100,
				.by = 'vdate'
				) %>%
			filter(., !is.na(annret_j_to_j_plus_m)) %>%
			transmute(
				.,
				vdate,
				value = annret_j_to_j_plus_m,
				date = floor_date(vdate, 'month') + months(j),
				ttm
				)
	}))

	rf_test_2 =
		rf_df %>%
		filter(., vdate == max(vdate) & date <= today() + years(10)) %>%
		ggplot() +
		geom_line(aes(x = date, y = value, color = as.factor(ttm), group = ttm)) +
		labs(title = 'At max vintage, forecasts for rf_1m, rf_3m, etc', x = 'j', color = 'm')

	print(rf_test_2)


	### 3. Monthly aggregation & remove rf rate

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

	# Create training dataset on spread
	# Find the last FFR forecast of the same maturity at each date
	hist_df =
		hist_df_0 %>%
		inner_join(., yield_curve_names_map, by = 'varname') %>%
		inner_join(
			.,
			rf_df %>%
				filter(., floor_date(vdate, 'month') == floor_date(date, 'month')) %>%
				transmute(., date, ttm, ffr_vdate = vdate, ffr = value),
			join_by(ttm, date, closest(vdate >= ffr_vdate))
		) %>%
		mutate(., value = value - ffr) %>%
		select(., -ffr)


	### 4. Smoothing of Treasury yields

	# Filter to only last 3 months
	train_df =
		hist_df %>%
		mutate(., months_ago = interval(vdate, date) %/% months(1)) %>%
		filter(., months_ago >= -6)

	#' Calculate DNS fit
	#'
	#' @param df: A tibble continuing columns date, value, and ttm
	#' @param return_all: FALSE by default.
	#' If FALSE, will return only the MSE (useful for optimization).
	#' Otherwise, will return a tibble containing fitted values, residuals, and the beta coefficients.
	#'
	#' @details
	#' Recasted into data.table as of 12/21/22, significant speed improvements (600ms optim -> 110ms)
	#'	test = train_df %>% group_split(., vdate) %>% .[[1]]
	#'  microbenchmark::microbenchmark(
	#'  	optimize(get_dns_fit, df = test, return_all = F, interval = c(-1, 1), maximum = F)
	#'  	times = 10,
	#'  	unit = 'ms'
	#'	)
	#' @export
	get_dns_fit = function(df, lambda, return_all = FALSE) {
		df %>%
			# mutate(f1 = 1, f2 = (1 - exp(-1 * lambda * ttm))/(lambda * ttm), f3 = f2 - exp(-1 * lambda * ttm)) %>%
			# group_split(., date) %>%
			# map_dfr(., function(x) {
			# 	reg = lm(value ~ f1 + f2 + f3 - 1, data = x)
			# 	bind_cols(x, fitted = fitted(reg)) %>%
			# 		mutate(., b1 = coef(reg)[['f1']], b2 = coef(reg)[['f2']], b3 = coef(reg)[['f3']]) %>%
			# 		mutate(., resid = value - fitted)
			# 	}) %>%
			as.data.table(.) %>%
			.[, c('f1', 'f2') := list(1, (1 - exp(-1 * lambda * ttm))/(lambda * ttm))] %>%
			.[, f3 := f2 - exp(-1*lambda*ttm)] %>%
			split(., by = 'date') %>%
			lapply(., function(x) {
				reg = lm(value ~ f1 + f2 + f3 - 1, data = x)
				x %>%
					.[, fitted := fitted(reg)] %>%
					.[, c('b1', 'b2', 'b3') := list(coef(reg)[['f1']], coef(reg)[['f2']], coef(reg)[['f3']])] %>%
					.[, resid := value - fitted] %>%
					.[, lambda := lambda]
			}) %>%
			rbindlist(.) %>%
			{if (return_all == FALSE) mean((.$resid)^2) else as_tibble(.)}
	}

	# Find MSE-minimizing lambda value by vintage date
	optim_lambdas =
		train_df %>%
		group_split(., vdate) %>%
		map(., .progress = T, function(x) {
			list(
				vdate = x$vdate[[1]],
				train_df = x,
				lambda = optimize(
					get_dns_fit,
					df = x,
					return_all = F,
					interval = .0609 + c(-.1, .1),
					maximum = F
					)$minimum
			)
		})

	# Get historical DNS fits by vintage date
	dns_fit_hist = list_rbind(map(optim_lambdas, \(x) get_dns_fit(df = x$train_df, x$lambda, return_all = T)))

	optim_lambdas %>%
		map(., \(x) tibble(vdate = x$vdate, l = x$lambda)) %>%
		list_rbind() %>%
		ggplot() +
		geom_line(aes(x = vdate, y = l))

	# Get historical DNS hists by vintage date
	dns_coefs_hist =
		dns_fit_hist %>%
		group_by(., vdate, date) %>%
		summarize(., lambda = unique(lambda), tdns1 = unique(b1), tdns2 = unique(b2), tdns3 = unique(b3), .groups = 'drop')

	# Check fits for last 12 vintage date (last historical fit for each vintage)
	dns_fit_plots =
		dns_fit_hist %>%
		filter(., vdate %in% tail(unique(dns_coefs_hist$vdate, 12))) %>%
		group_by(., vdate) %>%
		filter(., date == max(date)) %>%
		ungroup(.) %>%
		arrange(., vdate, ttm) %>%
		ggplot(.) +
		geom_point(aes(x = ttm, y = value)) +
		geom_line(aes(x = ttm, y = fitted)) +
		facet_wrap(vars(vdate))

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
		mutate(
			.,
			annualized_yield_dns =
				tdns1 +
				tdns2 * (1-exp(-1 * lambda * ttm))/(lambda * ttm) +
				tdns3 *((1-exp(-1 * lambda * ttm))/(lambda * ttm) - exp(-1 * lambda * ttm)),
		) %>%
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
		geom_line(aes(x = ttm, y = annualized_yield_dns), color = 'blue', linewidth = 1.5) +
		geom_line(aes(x = ttm, y = spline_fit), color = 'black') +
		geom_point(aes(x = ttm, y = yield_hist), color = 'black') +
		geom_line(aes(x = ttm, y = annualized_yield), color = 'red', linewidth = 2, alpha = .5) +
		facet_wrap(vars(vdate))

	print(full_fit_plots)

	### 6. Build expectations forecasts

	# Iterate over "yttms" tyield_1m, tyield_3m, ..., etc.
	# and for each, iterate over the original "ttms" 1, 2, 3,
	# ..., 120 and for each forecast the cumulative return for the yttm period ahead.
	expectations_forecasts =
		yield_curve_names_map$ttm %>%
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
				inner_join(., yield_curve_names_map, c('yttm' = 'ttm'))
		) %>%
		list_rbind %>%
		mutate(., date = add_with_rollback(floor_date(vdate, 'months'), months(ttm - 1))) %>%
		transmute(., vdate, ttm = yttm, varname, date, value = yttm_ahead_annualized_yield) %>%
		inner_join(
			.,
			rf_df %>% transmute(., date, ttm, ffr_vdate = vdate, ffr = value),
			join_by(ttm, date, closest(vdate >= ffr_vdate))
		) %>%
		transmute(., vdate, varname, date, value_og = value, value = value + ffr)

	# Plot point forecasts
	expectations_forecasts %>%
		filter(., vdate == max(vdate)) %>%
		ggplot(.) +
		geom_line(aes(x = date, y = value, color = varname)) +
		labs(title = 'Current forecast')

	expectations_forecasts %>%
		filter(., vdate == max(vdate)) %>%
		ggplot(.) +
		geom_line(aes(x = date, y = value_og, color = varname)) +
		labs(title = 'Current forecast - ffr removed')

	# Plot curve forecasts
	expectations_forecasts %>%
		filter(
			.,
			vdate %in% c(sample(unique(.$vdate), 9), max(.$vdate)),
			date <= vdate + years(5),
			month(date) %in% c(1, 6)
			) %>%
		left_join(., yield_curve_names_map, by = 'varname') %>%
		ggplot() +
		geom_line(aes(x = ttm, y = value, color = as.factor(date), group = date)) +
		facet_wrap(vars(vdate))

	# Plot curve forecasts vs ffr
	expectations_forecasts %>%
		filter(., vdate == max(vdate) & (date <= today() + years(5) & month(date) %in% c(1, 6))) %>%
		left_join(., yield_curve_names_map, by = 'varname') %>%
		ggplot() +
		geom_line(aes(x = ttm, y = value, color = as.factor(date), group = date))

	# Plot forecasts over time for current period
	expectations_forecasts %>%
		filter(., date == floor_date(today(), 'months')) %>%
		ggplot(.) +
		geom_line(aes(x = vdate, y = value, color = varname))

	# Check spread history
	bind_rows(
		mutate(expectations_forecasts, type = 'forecast'),
		mutate(fred_data, vdate = date, type = 'hist')
		) %>%
		filter(., vdate >= min(expectations_forecasts$vdate)) %>%
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

	### 6. Add in LT term premium

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
			select(., c('YEAR', 'QUARTER', paste0(str_to_upper(var), 'D'))) %>%
			na.omit %>%
			transmute(
				.,
				reldate = from_pretty_date(paste0(YEAR, 'Q', QUARTER), 'q'),
				varname = {if (var == 'tbill') 't03m' else 't10y'},
				value = .[[paste0(str_to_upper(var), 'D')]]
			)
		})) %>%
		left_join(., spf_1 %>% group_by(., vdate) %>% summarize(., reldate = min(date)), by = 'reldate') %>%
		arrange(., vdate) %>%
		transmute(., vdate, varname, value, date = floor_date(vdate + years(3), 'quarter'))

	spf_2 %>%
		pivot_wider(., id_cols = c(date, vdate), names_from = varname)

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

	term_prems =
		expectations_forecasts %>%
		inner_join(bind_rows(spf_1, spf_2), by = c('date', 'varname', 'vdate')) %>%
		mutate(
			.,
			months_ahead = interval(floor_date(vdate, 'quarter'), date) %/% months(1) + 2,
			diff = value.x - value.y,
			cum_atp = 100 * ((1 + diff/100)^(12/months_ahead) - 1)
		) %>%
		arrange(., vdate, months_ahead) %>%
		group_by(., vdate, varname) %>%
		mutate(
			.,
			end = months_ahead,
			start = coalesce(lag(end, 1), 0) + 1,
			dur = end - start + 1,
			# Cumulative premium
			cum_prem = (1 + diff/100)^(12/months_ahead),
			chg_from_cum_prem = cum_prem/coalesce(dplyr::lag(cum_prem, 1), 1),
			monthly_tp = 100 * (chg_from_cum_prem^(1/dur) - 1)
			) %>%
		ungroup(.) %>%
		mutate(., time = paste0(str_pad(start, 2, pad = '0'), '-', str_pad(end, 2, pad = '0'))) %>%
		mutate(., monthly_tp_sm = predict(smooth.spline(monthly_tp, spar = .25))$y, .by = c('varname', 'time')) %>%
		na.omit(.)

	# Plot term premiuims by date
	term_prems %>%
		filter(., year(vdate) >= 2003) %>%
		# filter(., n() >= 4, .by = 'vdate') %>%
		ggplot() +
		geom_line(aes(x = vdate, y = monthly_tp, group = time, color = time)) +
		geom_point(aes(x = vdate, y = monthly_tp, group = time, color = time)) +
		facet_grid(rows = vars(time), cols = vars(varname)) +
		geom_hline(yintercept = 0) +
		labs(title = 'Term premiums by forward period', x = 'Vintage Date', y = 'Months Ahead', color = 'Term') +
		ggthemes::theme_fivethirtyeight()

	# Term premium needs to be @ each vdate, along forecasted months forward and (smoothed along) ttm
	tps =
		term_prems %>%
		left_join(., yield_curve_names_map, by = 'varname') %>%
		group_split(., vdate, varname, ttm) %>%
		# For each vdate x varname, get the monthly term premiums from 0:360
		lapply(., function(df) {

			existing_tps = tibble(months_out = 0:120) %>%
				expand_grid(., select(df, start, end, monthly_tp)) %>%
				filter(., months_out >= start & months_out <= end) %>%
				select(., months_out, monthly_tp)

			imputed_tps =
				tibble(months_out = c(0, (max(df$end) + 1):120)) %>%
				mutate(monthly_tp = ifelse(months_out == 0, 0, tail(existing_tps$monthly_tp)))

			bind_rows(existing_tps, imputed_tps) %>%
				arrange(., months_out) %>%
				mutate(., vdate = df$vdate[[1]], varname = df$varname[[1]], ttm = df$ttm[[1]])
		}) %>%
		list_rbind() %>%
		transmute(
			.,
			varname, months_out, vdate, ttm,
			date = floor_date(vdate, 'month') + months(months_out), monthly_tp
			) %>%
		arrange(., vdate, varname, date) %>%
		mutate(
			cum_tp = cumprod(1 + monthly_tp/100),
			ann_tp = 100 * (cum_tp^(12/1:n()) - 1),
			ann_tp_sm = predict(smooth.spline(ann_tp, spar = .25))$y,
			.by = c('vdate', 'varname')
		) %>%
		transmute(varname, months_out, vdate, ttm, date, ann_tp_sm)

	# Smooth also over durations
	tps %>%
		filter(., vdate == max(vdate)) %>%
		ggplot() +
		geom_line(aes(x = date, y = ann_tp_sm, color = varname)) +
		labs(title = 'Latest term premiums (smoothed)')

	tps %>%
		filter(., months_out == 36) %>%
		ggplot() +
		geom_line(aes(x = vdate, y = ann_tp_sm, color = varname)) +
		labs(title = '5Y forward term premiums over vdates')

	tps_sm_2 =
		tps %>%
		group_split(., vdate) %>%
		lapply(., function(df) {
			expand_grid(
				yield_curve_names_map,
				distinct(df, months_out, date)
				) %>%
				left_join(select(df, -vdate), by = c('varname', 'ttm', 'months_out', 'date')) %>%
				arrange(., date, ttm) %>%
				mutate(., ann_tp_locf = case_when(
					ttm == min(ttm) ~ zoo::na.locf(ann_tp_sm, na.rm = F, fromLast = T)/1,
					ttm == max(ttm) ~ zoo::na.locf(ann_tp_sm, na.rm = F, fromLast = F)/1,
					.default = ann_tp_sm/1
					), .by = 'date') %>%
				mutate(ann_tp_sm_2 = zoo::na.approx(ann_tp_locf, na.rm = F), .by = 'months_out') %>%
				mutate(., vdate = df$vdate[[1]])
		}) %>%
		list_rbind() %>%
		# Add small TP modifier
		mutate(., ann_tp_sm_3 = ann_tp_sm_2 + (1 + months_out/120 * log(ttm)/log(120) * .025)^12 - 1) %>%
		transmute(varname, ttm, date, months_out, vdate, value = ann_tp_sm_3)

	# Plot TP curve forecasts
	tps_sm_2 %>%
		filter(
			.,
			vdate %in% c(sample(unique(.$vdate), 9), max(.$vdate)),
			date <= vdate + years(5),
			month(date) %in% c(1, 6)
		) %>%
		ggplot() +
		geom_line(aes(x = ttm, y = value, color = as.factor(date), group = date)) +
		facet_wrap(vars(vdate))

	# Plot TP forecasts over time for current period
	tps_sm_2 %>%
		filter(., date == floor_date(today(), 'months')) %>%
		ggplot(.) +
		geom_line(aes(x = vdate, y = value, color = varname))

	adj_forecasts =
		expectations_forecasts %>%
		rename(norf = value_og, eh = value) %>%
		inner_join(
			transmute(tps_sm_2, date, varname, tp_vdate = vdate, tp = value),
			join_by(date, varname, closest(vdate >= tp_vdate))
			) %>%
		mutate(., value = eh + tp)

	adj_forecasts %>%
		filter(., vdate == max(vdate)) %>%
		ggplot(.) +
		geom_line(aes(x = date, y = value, color = varname)) +
		labs(title = 'Current forecast')

	# Plot curve forecasts
	adj_forecasts %>%
		filter(., vdate == max(vdate) & (date <= today() + years(5) & month(date) %in% c(1, 6))) %>%
		left_join(., yield_curve_names_map, by = 'varname') %>%
		ggplot() +
		geom_line(aes(x = ttm, y = value, color = as.factor(date), group = date))

	adj_forecasts %>%
		filter(vdate == max(vdate)) %>%
		pivot_longer(., cols = c(eh, tp, norf, value)) %>%
		ggplot() +
		geom_line(aes(x = date, y = value, color = name)) +
		facet_wrap(vars(varname)) +
		labs(title = 'Latest forecasts')

	adj_forecasts %>%
		filter(
			.,
			vdate %in% c(sample(unique(.$vdate), 9), max(.$vdate)),
			date <= vdate + years(5),
			month(date) %in% c(1, 6)
		) %>%
		select(., vdate, date, varname, tp, eh) %>%
		pivot_longer(., cols = -c(vdate, date, varname)) %>%
		left_join(yield_curve_names_map, by = 'varname') %>%

		ggplot() +
		geom_line(aes(x = ttm, y = value, color = as.factor(date), group = date)) +
		facet_grid(rows = vars(vdate), cols = vars(name))

	# # For 10-year forward forecast, use long-term spread forecast
	# # Exact interpolation at 10-3 10 years ahead
	# # Get historical term premium
	# term_prems_raw = collect(tbl(pg, sql(
	# 	"SELECT vdate, varname, date, d1 AS value
	# 	FROM forecast_values_v2_all
	# 	WHERE forecast = 'spf' AND varname IN ('t03m', 't10y')"
	# )))
	#
	# # Get long-term spreads forecast
	# forecast_spreads = lapply(c('tbill', 'tbond'), function(var) {
	#
	# 	request(paste0(
	# 		'https://www.philadelphiafed.org/-/media/frbp/assets/surveys-and-data/',
	# 		'survey-of-professional-forecasters/data-files/files/median_', var, '_level.xlsx?la=en'
	# 	)) %>%
	# 		req_perform(., path = file.path(tempdir(), paste0(var, '.xlsx')))
	#
	# 	readxl::read_excel(file.path(tempdir(), paste0(var, '.xlsx')), na = '#N/A', sheet = 'Median_Level') %>%
	# 		select(., c('YEAR', 'QUARTER', paste0(str_to_upper(var), 'D'))) %>%
	# 		na.omit %>%
	# 		transmute(
	# 			.,
	# 			reldate = from_pretty_date(paste0(YEAR, 'Q', QUARTER), 'q'),
	# 			varname = {if (var == 'tbill') 't03m_lt' else 't10y_lt'},
	# 			value = .[[paste0(str_to_upper(var), 'D')]]
	# 		)
	# 	}) %>%
	# 	list_rbind(.) %>%
	# 	# Guess release dates
	# 	left_join(., term_prems_raw %>% group_by(., vdate) %>% summarize(., reldate = min(date)), by = 'reldate') %>%
	# 	pivot_wider(., id_cols = vdate, names_from = varname, values_from = value) %>%
	# 	mutate(., spread = t10y_lt - t03m_lt) %>%
	# 	arrange(., vdate) %>%
	# 	mutate(., spread = zoo::rollmean(spread, 2, fill = NA, align = 'right')) %>%
	# 	tail(., -1)
	#
	# # 1. At each historical vdate, get the trailing 36-month historical spread between all Treasuries with the 3m yield
	# # 2. Get the relative ratio of these historical spreads relative to the 10-3 spread (%diff from 0)
	# # 3. Get the forecasted SPF long-term spread for the latest forecast available at each historical vdate
	# # 4. Compare the forecast SPF long-term spread to the hist_spread_ratio
	# # - Ex. Hist 10-3 spread = .5; hist 30-3 spread = 2 => 4x multiplier (spread_ratio)
	# # - Forecast 10-3 spread = 1; want to get forecast 30-3; so multiply historical spread_ratio * 1
	# # Get relative ratio of historical diffs to 10-3 year forecast to generate spread forecasts (from 3mo) for all variables
	# # Then reweight these back in time towards other
	# historical_ratios =
	# 	hist_df %>%
	# 	{left_join(
	# 		filter(., varname != 't03m') %>% transmute(., date, vdate, ttm, varname, value),
	# 		filter(., varname == 't03m') %>% transmute(., date, vdate, t03m = value),
	# 		by = c('vdate', 'date')
	# 	)} %>%
	# 	mutate(., spread = value - t03m) %>%
	# 	group_by(., vdate, varname, ttm) %>%
	# 	summarize(., mean_spread_above_3m = mean(spread), .groups = 'drop') %>%
	# 	arrange(., ttm) %>%
	# 	{left_join(
	# 		transmute(., vdate, varname, ttm, mean_spread_above_3m),
	# 		filter(., varname == 't10y') %>% transmute(., vdate, t10y_mean_spread_above_3m = mean_spread_above_3m),
	# 		by = c('vdate')
	# 	)} %>%
	# 	mutate(., hist_spread_ratio_to_10_3 = case_when(
	# 		t10y_mean_spread_above_3m <= 0 ~ max(0, mean_spread_above_3m),
	# 		mean_spread_above_3m <= 0 ~ 0,
	# 		TRUE ~ mean_spread_above_3m /t10y_mean_spread_above_3m
	# 	)) %>%
	# 	# Sanity check
	# 	mutate(., hist_spread_ratio_to_10_3 = ifelse(hist_spread_ratio_to_10_3 > 3, 3, hist_spread_ratio_to_10_3)) %>%
	# 	arrange(., vdate)
	#
	# if (!all(sort(unique(historical_ratios$vdate)) == sort(backtest_vdates))) {
	# 	stop ('Error: lost vintage dates!')
	# }
	#
	# # Watch for excessively discontinuous drops
	# forecast_lt_spreads =
	# 	historical_ratios %>%
	# 	select(., vdate, ttm, varname, hist_spread_ratio_to_10_3) %>%
	# 	left_join(
	# 		.,
	# 		transmute(forecast_spreads, hist_vdate = vdate, forecast_spread_10_3 = spread),
	# 		join_by(closest(vdate >= hist_vdate))
	# 	) %>%
	# 	mutate(., forecast_lt_spread = hist_spread_ratio_to_10_3 * forecast_spread_10_3) %>%
	# 	select(., vdate, varname, forecast_spread_10_3, forecast_lt_spread)
	#
	# forecast_lt_spreads %>%
	# 	ggplot(.) +
	# 	geom_line(aes(x = vdate, y = forecast_lt_spread, color = varname)) +
	# 	geom_point(aes(x = vdate, y = forecast_spread_10_3, color = varname))
	#
	#
	# # LT spread forecasts by date
	# adj_forecasts_raw =
	# 	expectations_forecasts %>%
	# 	filter(., varname != 't03m') %>%
	# 	left_join(
	# 		.,
	# 		transmute(filter(expectations_forecasts, varname == 't03m'), vdate, date, t03m = value),
	# 		by = c('vdate', 'date')
	# 	) %>%
	# 	mutate(
	# 		.,
	# 		months_ahead = interval(floor_date(vdate, 'months'), date) %/% months(1),
	# 		lt_weight = ifelse(months_ahead >= 120, 1/3, months_ahead/120 * 2/3),
	# 		spread = value - t03m
	# 	) %>%
	# 	left_join(., forecast_lt_spreads, by = c('varname', 'vdate')) %>%
	# 	mutate(., adj_spread = (1 - lt_weight) * spread + lt_weight * forecast_lt_spread) %>%
	# 	mutate(., adj_value = adj_spread + t03m)
	#
	# adj_forecasts_raw %>%
	# 	pivot_longer(., cols = c(value, adj_value)) %>%
	# 	filter(., vdate == max(vdate)) %>%
	# 	ggplot(.) +
	# 	geom_line(aes(x = date, y = value, color = name)) +
	# 	facet_wrap(vars(varname))
	#
	# adj_forecasts =
	# 	adj_forecasts_raw %>%
	# 	transmute(., vdate, varname, date, value = adj_value) %>%
	# 	bind_rows(., filter(expectations_forecasts, varname == 't03m')) %>%
	# 	arrange(., vdate, varname, date)


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
		adj_forecasts %>%
		mutate(., months_ahead = interval(floor_date(vdate, 'months'), date) %/% months(1)) %>%
		left_join(
			.,
			# Use hist proper mean of existing data
			hist_df_unagg %>%
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
			forecast_origin_y = logistic((1 - day(vdate)/days_in_month(vdate)), get_logistic_x0(1/3, k = 2), k = 2),
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

	hist_merged_df %>%
		filter(., date == floor_date(today('US/Eastern'), 'month'), varname == 't10y') %>%
		ggplot(.) +
		geom_line(aes(x = vdate, y = forecast_weight, color = format(vdate, '%Y%m'))) +
		labs(title = 'Forecast weights for this months forecast over time')

	hist_merged_df %>%
		filter(., vdate == max(vdate), varname == 't30y') %>%
		ggplot(.) +
		geom_line(aes(x = date, y = forecast_weight)) +
		labs(title = 'Forecast weights generated on this vdate for future forecasts')

	# Test plot 1
	hist_merged_df %>%
		filter(., vdate %in% head(unique(hist_merged_df$vdate), 10)) %>%
		group_by(., vdate) %>%
		filter(., date == min(date)) %>%
		inner_join(., yield_curve_names_map, by = 'varname') %>%
		arrange(., ttm) %>%
		ggplot(.) +
		geom_point(aes(x = ttm, y = final_value), color = 'green') +
		geom_point(aes(x = ttm, y = value), color = 'blue') +
		geom_point(aes(x = ttm, y = last_hist), color = 'black') +
		facet_wrap(vars(vdate)) +
		labs(title = 'Forecasts - last 10 vdates', subtitle = 'green = joined, blue = unjoined_forecast, black = hist')

	# Test plot 2
	hist_merged_df %>%
		filter(., vdate == max(vdate) & date <= today() + years(1)) %>%
		inner_join(., yield_curve_names_map, by = 'varname') %>%
		arrange(., ttm) %>%
		ggplot(.) +
		geom_point(aes(x = ttm, y = final_value), color = 'green') +
		geom_point(aes(x = ttm, y = value), color = 'blue') +
		geom_point(aes(x = ttm, y = last_hist), color = 'black') +
		facet_wrap(vars(date)) +
		labs(title = 'Forecasts - 1vdate', subtitle = 'green = joined, blue = unjoined_forecast, black = hist')

	## TBD: WEIGHT OF MONTH 0 SHOULD DEPEND ON HOW FAR IT IS INTO TE MONTH
	merged_forecasts =
		hist_merged_df %>%
		transmute(., vdate, varname, freq = 'm', date, value = final_value)

	merged_forecasts %>%
		filter(., vdate == max(vdate)) %>%
		ggplot(.) +
		geom_line(aes(x = date, y = value)) +
		facet_wrap(vars(varname))

	bind_rows(
		expectations_forecasts %>% filter(., vdate == max(vdate) - days(0)) %>% mutate(., type = 'expectations'),
		adj_forecasts %>% filter(., vdate == max(vdate) - days(0)) %>% mutate(., type = 'adj'),
		# merged_forecasts %>% filter(., vdate == max(vdate) - days(0)) %>% mutate(., type = 'merged')
		) %>%
		ggplot(.) +
		geom_line(aes(x = date, y = value, color = type), alpha = .9) +
		facet_wrap(vars(varname))

	# Take same-month t01m forecasts and merge with last historical period
	# at the short end during financial risk periods
	# Weight of last period is .5 at start of month and decreases to 0 at end of month
	final_forecasts =
		filter(merged_forecasts, floor_date(vdate, 'month') == date & varname == 't01m') %>%
		left_join(
			hist_df_unagg %>%
				group_by(., vdate, varname) %>%
				filter(., date == max(date)) %>%
				ungroup(.) %>%
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

	final_forecasts %>%
		filter(., vdate %in% sample(final_forecasts$vdate, size = 10)) %>%
		mutate(., months_ahead = interval(floor_date(vdate, 'month'), floor_date(date, 'month')) %/% months(1)) %>%
		filter(., months_ahead %in% c(0, 12, 60)) %>%
		left_join(., yield_curve_names_map, by = 'varname') %>%
		ggplot() +
		geom_line(aes(x = ttm, y = value, color = as.factor(months_ahead), group = months_ahead)) +
		facet_grid(rows = vars(vdate))

	tdns <<- list(
		dns_coefs_hist = dns_coefs_hist,
		dns_coefs_forecast = dns_coefs_forecast
	)

	# submodels$tdns <<- final_forecasts
})


## TFUT: Smoothed DNS + Futures ----------------------------------------------------------
#' Temporarily disabled - since currently all these are merged into a single INT model
# local({
#
# 	"
# 	Fit splines on both temporal dimension, ttm dimension, and their tensor product
# 	https://www.math3ma.com/blog/the-tensor-product-demystified
# 	"
#
# 	# Match each varname x forecast date from TDNS to the closest futures vdate (within 7 day range)
# 	futures_df =
# 		submodels$tdns %>%
# 		left_join(
# 			.,
# 			submodels$cme %>%
# 				filter(., str_detect(varname, '^t\\d\\d[y|m]$')) %>%
# 				transmute(., date, futures_vdate = vdate, varname, futures_value = value),
# 			join_by(date, closest(vdate >= futures_vdate), varname)
# 		) %>%
# 		# Replace future_values that are too far lagged (before TDNS vdate) with NAs
# 		mutate(., futures_value = ifelse(futures_vdate >= vdate - days(7), futures_value, NA)) %>%
# 		group_by(., vdate) %>%
# 		mutate(., count_hist = sum(ifelse(!is.na(futures_value), 1, 0))) %>%
# 		ungroup(.) %>%
# 		filter(., count_hist >= 1) %>%
# 		mutate(
# 			.,
# 			ttm = as.numeric(str_sub(varname, 2, 3)) * ifelse(str_sub(varname, 4, 4) == 'y', 12, 1),
# 			months_out = interval(floor_date(vdate, 'month'), floor_date(date, 'month')) %/% months(1)
# 		) %>%
# 		mutate(., mod_value = ifelse(!is.na(futures_value), futures_value, value))
#
# 	# Can use furrr for speed issues
# 	# plan(multisession, workers = parallel::detectCores() - 2, gc = TRUE)
#
# 	future_forecasts =
# 		futures_df %>%
# 		group_split(., vdate) %>%
# 		map(., .progress = T, function(df, i) {
#
# 			fit = gam(
# 				mod_value ~
# 					s(months_out, bs = 'tp', fx = F, k = -1) +
# 					s(ttm, bs = 'tp', fx = F, k = -1) +
# 					te(ttm, months_out, k = 10),
# 				data = df,
# 				method = 'ML'
# 				)
#
# 			final_df = mutate(df, gam_fit = fit$fitted.values, final_value = .5 * value + .5 * gam_fit)
#
# 			plot =
# 				final_df %>%
# 				filter(., varname == 't10y') %>%
# 				arrange(., ttm) %>%
# 				ggplot(.) +
# 				geom_line(aes(x = date, y = value), color = 'green') +
# 				geom_point(aes(x = date, y = futures_value), color = 'black', size = 5) +
# 				geom_line(aes(x = date, y = mod_value), color = 'red') +
# 				geom_line(aes(x = date, y = gam_fit), color = 'pink') +
# 				geom_line(aes(x = date, y = final_value), color = 'blue')
#
# 			list(
# 				final_df = final_df,
# 				plot = plot
# 				)
# 			})
#
# 	# future:::ClusterRegistry("stop")
#
# 	forecasts_df =
# 		map_dfr(future_forecasts, \(x) x$final_df) %>%
# 		transmute(., vdate, varname, freq = 'm', date, value)
#
# 	forecasts_df %>%
# 		count(., vdate) %>%
# 		arrange(., vdate)
#
# 	submodels$tfut <<- forecasts_df
# })


## CBOE: Futures ---------------------------------------------------------------------
local({

	# AMB1
	# AMT1 <->
	# Get FFR spread <-> compare historical change from AMERIBOR
	cboe_data =
		request('https://www.cboe.com/us/futures/market_statistics/settlement/') %>%
		req_perform %>%
		resp_body_html %>%
		html_elements(., 'ul.document-list > li > a') %>%
		map(., function(x)
			tibble(
				vdate = as_date(str_sub(html_attr(x, 'href'), -10)),
				url = paste0('https://cboe.com', html_attr(x, 'href'))
			)
		) %>%
		map(., function(x)
			read_csv(x$url, col_names = c('product', 'symbol', 'exp_date', 'price'), col_types = 'ccDn', skip = 1) %>%
				filter(., product %in% c('AMT1')) %>%
				transmute(
					.,
					product,
					vdate = as_date(x$vdate),
					date = floor_date(exp_date - months(1), 'months'),
					value = 100 - price/100
				)
			) %>%
		list_rbind %>%
		pivot_wider(., id_cols = c(vdate, date), names_from = 'product', values_from = 'value') %>%
		mutate(., value = ifelse(is.na(AMT1), coalesce(AMT1, 0), (AMT1))) %>%
		mutate(., ameribor = smooth.spline(1:length(value), value)$y) %>%
		group_split(., vdate) %>%
		lapply(., function(df) {

			hist_df = inner_join(
				hist$afx %>% filter(., varname == 'ameribor') %>% transmute(., vdate, date, ameribor = value),
				hist$fred %>% filter(., varname == 'ffr') %>% transmute(., vdate, date, ffr = value),
				by = c('vdate', 'date')
				) %>%
				filter(., date == max(date)) %>%
				filter(., vdate == max(vdate))

			forecast = left_join(
				transmute(df, vdate, date, ameribor = value),
				transmute(filter(submodels$cme, varname == 'ffr'), vdate, date, ffr = value),
				by = c('vdate', 'date')
				)

			forecast %>%
				mutate(., ameribor = ameribor - forecast$ameribor[[1]] + hist_df$ameribor[[1]])
		}) %>%
		list_rbind() %>%
		transmute(., vdate, date, value = ameribor, varname = 'ameribor')

	# Plot comparison against TDNS
	cboe_data %>%
		filter(., vdate == max(vdate)) %>%
		bind_rows(
			.,
			filter(submodels$cme, varname == 'ffr' & vdate == max(vdate)),
			filter(submodels$cme, varname == 'sofr' & vdate == max(vdate))
			) %>%
		ggplot(.) +
		geom_line(aes(x = date, y = value, color = varname, group = varname))

	ameribor_forecasts =
		filter(cboe_data, vdate == max(vdate)) %>%
		right_join(
			.,
			transmute(filter(submodels$cme, varname == 'sofr' & vdate == max(vdate)), date, sofr = value),
			by = 'date'
			) %>%
		mutate(., spread = value - sofr) %>%
		mutate(
			.,
			varname = 'ameribor',
			vdate = head(vdate, 1),
			spread = {c(
				na.omit(.$spread),
				forecast::forecast(Arima(.$spread, order = c(1, 1, 0)), length(.$spread[is.na(.$spread)]))$mean
				)},
			value = round(ifelse(!is.na(value), value, sofr + spread), 4)
			) %>%
		transmute(., varname, freq = 'm', vdate, date, value)

	submodels$cboe <<- ameribor_forecasts
})

## MOR: Mortgage Model ----------------------------------------------------------
local({

	# Calculate historical mortgage curve spreads
	input_df =
		# Get historical monthly averages
		bind_rows(hist$fred, hist$treasury) %>%
		filter(., varname %in% c('t10y', 't20y', 't30y', 'mort15y', 'mort30y')) %>%
		mutate(., date = floor_date(date, 'months')) %>%
		group_by(., varname, date) %>%
		summarize(., value = mean(value), .groups = 'drop') %>%
		# Pivot out and calculate spreads
		pivot_wider(., id_cols = 'date', names_from = 'varname', values_from = 'value') %>%
		mutate(
			.,
			t15y = (t10y + t20y)/2,
			spread15 = mort15y - t15y, spread30 = mort30y - t30y
			) %>%
		# Join on latest DNS coefs
		# inner_join(., filter(tdns$dns_coefs_hist, vdate == max(vdate)), by = 'date') %>%
		# select(date, spread15, spread30, tdns1, tdns2) %>%
		select(., date, spread15, spread30) %>%
		arrange(., date) %>%
		mutate(., spread15.l1 = lag(spread15, 1), spread30.l1 = lag(spread30, 1)) %>%
		na.omit(.)

	pred_df =
		tdns$dns_coefs_forecast %>%
		filter(., vdate == max(vdate)) %>%
		bind_rows(tail(filter(input_df, date < .$date[[1]]), 1), .)

	# Include average of historical month as "forecast" period
	coefs15 = matrix(lm(spread15 ~ spread15.l1, input_df)$coef, nrow = 1)
	coefs30 = matrix(lm(spread30 ~ spread30.l1, input_df)$coef, nrow = 1)

	spread_forecast = reduce(2:nrow(pred_df), function(accum, x) {
		accum[x, 'spread15'] =
			coefs15 %*% matrix(as.numeric(
				# c(1, accum[[x, 'tdns1']], ... if included tdns1 as a covariate
				c(1, accum[x - 1, 'spread15']),
				nrow = 1
				))
		accum[x, 'spread30'] =
			coefs15 %*% matrix(as.numeric(
				c(1, accum[x - 1, 'spread30']),
				nrow = 1
			))
		return(accum)
		},
		.init = pred_df
		) %>%
		.[2:nrow(.),] %>%
		select(., date, spread15, spread30)

	mor_data =
		inner_join(
			spread_forecast,
			submodels$tdns %>%
				filter(., varname %in% c('t10y', 't20y', 't30y')) %>%
				filter(., vdate == max(vdate)) %>%
				pivot_wider(., id_cols = 'date', names_from = 'varname', values_from = 'value'),
			by = 'date'
		) %>%
		mutate(., mort15y = (t10y + t30y)/2 + spread15, mort30y = t30y + spread30) %>%
		select(., date, mort15y, mort30y) %>%
		pivot_longer(., -date, names_to = 'varname', values_to = 'value') %>%
		transmute(
			.,
			varname,
			freq = 'm',
			vdate = today(),
			date,
			value
			)

	submodels$mor <<- mor_data
})


## ENG: BoE Bank Rate (DAILY) ----------------------------------------------------------
# local({
#
# 	# Use monthly data from ICE
#
# 	# Now extract calendar dates of meetings
# 	old_dates = as_date(c(
# 		'2021-02-04', '2021-03-18', '2021-05-06', '2021-06-24', '2021-09-23', '2021-11-04', '2021-12-16'
# 		))
#
# 	new_dates =
# 		GET('https://www.bankofengland.co.uk/monetary-policy/upcoming-mpc-dates') %>%
# 		content(.) %>%
# 		html_nodes(., 'div.page-content') %>%
# 		lapply(., function(x) {
#
# 			div_year = str_sub(html_text(html_node(x, 'h2')), 0, 4)
#
# 			month_dates =
# 				html_text(html_nodes(x, 'table tbody tr td:nth-child(1)')) %>%
# 				str_replace(., paste0(wday(now() + days(0:6), label = T, abbr = F), collapse = '|'), '') %>%
# 				str_squish(.)
#
# 			paste0(month_dates, ' ', div_year) %>%
# 				dmy(.)
# 			}) %>%
# 		unlist(.) %>%
# 		as_date(.)
#
# 	# Join except for anything in new_dates already in old_dates
# 	meeting_dates =
# 		tibble(dates = old_dates, year = year(old_dates), month = month(old_dates)) %>%
# 		bind_rows(
# 			.,
# 			anti_join(
# 				tibble(dates = new_dates, year = year(new_dates), month = month(new_dates)),
# 				.,
# 				by = c('year', 'month')
# 			)
# 		) %>%
# 		.$dates
#
#
#
# 	## Daily calculations
# 	this_vdate = max(monthly_df$vdate)
# 	this_monthly_df = monthly_df %>% filter(., vdate == this_vdate) %>% select(., -vdate, -freq)
#
# 	# Designate each period as a constant-rate time between meetings
# 	periods_df =
# 		meeting_dates %>%
# 		tibble(period_start = ., period_end = lead(period_start, 1)) %>%
# 		filter(., period_start >= min(this_monthly_df$date) & period_end <= max(this_monthly_df$date))
#
#
# 	# For each period, see which months fall fully within them; if multiple, take the average;
# 	# most periods will by none
# 	periods_df %>%
# 		purrr::transpose(.) %>%
# 		map_dfr(., function(x)
# 			this_monthly_df %>%
# 				filter(., date >= x$period_start & ceiling_date(date, 'months') - days(1) <= x$period_end) %>%
# 				summarize(., filled_value = mean(value, na.rm = T)) %>%
# 				bind_cols(x, .) %>%
# 				mutate(., filled_value = ifelse(is.nan(filled_value), NA, filled_value))
# 			) %>%
# 		mutate(., period_start = as_date(period_start), period_end = as_date(period_end))
#
# 	submodels$eng <<-
# })


# Finalize ----------------------------------------------------------

## Store in SQL ----------------------------------------------------------
local({

	# Store in SQL
	submodel_values =
		bind_rows(
			submodels$ice,
			submodels$cme %>% filter(., !str_detect(varname, 't\\d\\d[m|y]')),
			submodels$tdns,
			submodels$cboe,
			submodels$mor
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
