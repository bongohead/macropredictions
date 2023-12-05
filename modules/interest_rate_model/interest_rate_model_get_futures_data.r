#' Gets futures values needed for interest_rate_model_run.r
#'
#' Usage:
#' - Run this script before interest_rsate_model_run.r
#' - Runtime: with BACKFILL_MONTHS = 3: 15min

# Initialize ----------------------------------------------------------
BACKFILL_MONTHS = 3

validation_log <<- list()
data_dump <<- list()

## Load Libs ----------------------------------------------------------
library(macropredictions)
library(tidyverse)
library(httr2)
library(rvest)
load_env()

## Load Data ----------------------------------------------------------
pg = connect_pg()

scraped_data <- list()

# Scrape Data ----------------------------------------------------------

## Barchart ----------------------------------------------------------
#' Gets up to BACKFILL_MONTHS months of history for CME SOFR & FFR futures
local({

	desired_sources =
		tibble(date = seq(
			# Need to go 5 years ago to get all 5-year forward forecasts
			from = floor_date(today() - months(BACKFILL_MONTHS + 60), 'month'),
			to = floor_date(today() + months(5 * 12), 'month'),
			by = '1 month'
			)) %>%
		mutate(., year = year(date), month = month(date)) %>%
		left_join(., tibble(month = 1:12, code = c('F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z')), by = 'month') %>%
		expand_grid(
			.,
			# tibble(
			# 	varname = c('sofr', 'sofr', 'ffr', 't02y', 't05y', 't10y', 't30y'),
			# 	ticker = c('SL', 'SQ', 'ZQ', 'TU', 'TF', 'TO', 'TZ'),
			# 	max_months_out = c(5, 5, 5, 1, 1, 1, 1) * 12
			# )
			tibble(
				varname = c('sofr', 'sofr', 'ffr', 'bsby'),
				ticker = c('SL', 'SQ', 'ZQ', 'BR'),
				max_months_out = c(5, 5, 5, 5) * 12,
				tenor = c('1m', '3m', '30d', '3m')
			)
		) %>%
		mutate(
			.,
			code = paste0(ticker, code, str_sub(year, -2)),
			months_out = interval(floor_date(today(), 'month'), floor_date(date, 'month')) %/% months(1)
		) %>%
		filter(., months_out <= max_months_out) %>%
		transmute(., varname, code, date, tenor) %>%
		arrange(., date)

	available_sources = get_available_barchart_sources(unique(str_sub(desired_sources$code, 1, 2)))
	scrape_sources = filter(desired_sources, code %in% available_sources)
	raw_data = get_barchart_data(scrape_sources$code)

	cleaned_data = raw_data %>%
		left_join(., scrape_sources, by = 'code') %>%
		transmute(
			.,
			scrape_source = 'bc',
			varname,
			expdate = date,
			tenor,
			vdate = vdate - days(1),
			value = ifelse(str_detect(varname, '^t\\d\\dy$'), close, 100 - close)
		) %>%
		filter(., expdate >= floor_date(vdate, 'month')) # Get rid of forecasts for old observations

	scraped_data$bc <<- cleaned_data
})

## CME ---------------------------------------------------------------------
#' Gets most recent data for CME SOFR & FFR futures. Use barchart data preferrably.
local({

	# CME Group Data
	message('Starting CME data scrape...')
	cme_cookie =
		request('https://www.cmegroup.com/') %>%
		add_standard_headers %>%
		req_perform %>%
		get_cookies(., T) %>%
		.$ak_bmsc

	# Get CME Vintage Date
	last_trade_date =
		request('https://www.cmegroup.com/CmeWS/mvc/Quotes/Future/305/G?quoteCodes=null&_=') %>%
		add_standard_headers %>%
		list_merge(., headers = list('Host' = 'www.cmegroup.com', 'Cookie' = cme_cookie)) %>%
		req_perform %>%
		resp_body_json %>%
		.$tradeDate %>%
		parse_date_time(., 'd-b-Y') %>%
		as_date(.)

	# See https://www.federalreserve.gov/econres/feds/files/2019014pap.pdf for CME futures model
	# SOFR 3m tenor./
	cme_search_space = tribble(
		~ varname, ~ cme_id, ~ tenor,
		'ffr', '305', '30d',
		'sofr', '8462', '3m',
		'sofr', '8463', '1m',
		'bsby', '10038', '3m'
		# 't02y', '10048',
		# 't05y', '10049',
		# 't10y', '10050',
		# 't30y', '10051'
	)

	cme_raw_data = list_rbind(map(df_to_list(cme_search_space), function(var) {

		quotes = request(paste0(
			'https://www.cmegroup.com/CmeWS/mvc/Quotes/Future/',
			var$cme_id,
			'/G?quoteCodes=null&_='
			)) %>%
			add_standard_headers %>%
			list_merge(., headers = list('Host' = 'www.cmegroup.com', 'Cookie' = cme_cookie)) %>%
			req_perform %>%
			resp_body_json %>%
			.$quotes

		# Note: use last instead of last to get previous day's close
		res = list_rbind(map(quotes, function(x) {
			if (x$last %in% c('0.000', '0.00', '-')) return() # Related bug in CME website
			tibble(
				vdate = last_trade_date,
				date = ymd(x$expirationDate),
				value = {
					if (str_detect(var$varname, '^t\\d\\dy$')) as.numeric(x$last)
					else 100 - as.numeric(x$last)
				},
				varname = var$varname,
				cme_id = var$cme_id,
				tenor = var$tenor
			)
		}))

		return(res)
	}))

	cme_raw_data %>%
		arrange(., date) %>%
		ggplot(.) +
		geom_line(aes(x = date, y = value, color = cme_id))

	cme_data =
		cme_raw_data %>%
		# Now use only one-month SOFR futures if it's the only available data
		arrange(., date) %>%
		# Get rid of forecasts for old observations
		filter(., date >= floor_date(last_trade_date, 'month') & value != 100) %>%
		transmute(
			.,
			scrape_source = 'cme',
			varname,
			expdate = date,
			tenor,
			vdate = last_trade_date,
			value
			)

	# Most data starts in 88-89, except j=12 which starts at 1994-01-04. Misc missing obs until 2006.
	# 	df %>%
	#   	tidyr::pivot_wider(., names_from = j, values_from = settle) %>%
	#     	dplyr::arrange(., date) %>% na.omit(.) %>% dplyr::group_by(year(date)) %>% dplyr::summarize(., n = n()) %>%
	# 		View(.)
	scraped_data$cme <<- cme_data
})

## ICE ---------------------------------------------------------------------
#' SONIA and ESTR futures
local({

	ice_codes = tribble(
		~ varname, ~ product_id, ~ hub_id, ~ tenor,
		'sonia', '20343', '23428', '1m',
		'sonia', '20484', '23565', '3m',
		# 'euribor', '15275', '17455', '3m,
		'estr', '15274', '17454', '1m',
	)

	ice_raw_data = list_rbind(map(df_to_list(ice_codes), function(x) {

		# Get top-level expiration IDs
		expiration_ids = request(str_glue(
			'https://www.theice.com/marketdata/DelayedMarkets.shtml?',
			'getContractsAsJson=&productId={x$product_id}&hubId={x$hub_id}'
		)) %>%
			req_retry(max_tries = 5) %>%
			req_perform %>%
			resp_body_json %>%
			# Filter out packs & bundles
			keep(., ~ str_detect(.$marketStrip, 'Pack|Bundle', negate = T)) %>%
			map(., function(z) tibble(
				date = floor_date(parse_date(z$marketStrip, '%b%y'), 'months'),
				market_id = as.character(z$marketId)
			)) %>%
			list_rbind

		# Iterate through the expiration IDs
		list_rbind(map(df_to_list(expiration_ids), function(z)
			request(str_glue(
				'https://www.theice.com/marketdata/DelayedMarkets.shtml?',
				'getHistoricalChartDataAsJson=&marketId={z$market_id}&historicalSpan=2'
				)) %>%
				req_retry(max_tries = 5) %>%
				req_perform %>%
				resp_body_json %>%
				.$bars %>%
				{tibble(vdate = map_chr(., ~ .[[1]]), value = map_dbl(., ~ .[[2]]))} %>%
				transmute(
					.,
					scrape_source = 'ice',
					varname = x$varname,
					expdate = as_date(z$date),
					tenor = x$tenor,
					vdate = as_date(parse_date_time(vdate, orders = '%a %b %d %H:%M:%S %Y'), tz = 'ET'),
					value = 100 - value
				)
		))
	}))

	scraped_data$ice <<- ice_raw_data
})

## CBOE --------------------------------------------------------------------
#' AMERIBOR futures
local({

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
				filter(., product %in% c('AMT1', 'AMB1', 'AMB3')) %>%
				transmute(
					.,
					product,
					vdate = as_date(x$vdate),
					date = floor_date(exp_date - months(1), 'months'),
					value = 100 - price/100
				)
		) %>%
		list_rbind %>%
		mutate(., tenor = case_when(
			product == 'AMB1' ~ '1m',
			product == 'AMB3' ~ '3m',
			product == 'AMT1' ~ '30d'
		)) %>%
		transmute(
			.,
			scrape_source = 'cboe',
			varname = 'ameribor',
			expdate = date,
			tenor,
			vdate,
			value
		)

	scraped_data$cboe <<- cboe_data
})

# Finalize ----------------------------------------------------------
## Store in SQL ----------------------------------------------------------
local({

	# Store in SQL
	futures_values = bind_rows(scraped_data)

	rows_added = store_futures_values(pg, futures_values, .verbose = T)

	# Log
	validation_log$rows_added <<- rows_added
	validation_log$last_vdate <<- max(futures_values$vdate)

	disconnect_db(pg)

	futures_values <<- futures_values
})


# fred_data =
# 	get_fred_obs(c('T3MFF'), Sys.getenv('FRED_API_KEY'), .obs_start = '1980-01-01') %>%
# 	mutate(., value = zoo::rollmean(value, k = 1, align = 'right', fill = NA)) %>%
# 	transmute(., spread_vdate = date, spread = value) %>%
# 	na.omit()
#
# fred_data %>% ggplot() + geom_line(aes(x = spread_vdate, y = spread))
#
# spf_data = collect(tbl(pg, sql(
# 	"SELECT vdate, date, d1 AS spf
# 	FROM forecast_values_v2_all
# 	WHERE forecast = 'spf' AND varname IN ('t03m')"
# 	))) %>%
# 	left_join(fred_data, join_by(closest(vdate >= spread_vdate))) %>%
# 	mutate(., spf = spf - 0)
#
# futures_data = barchart_data %>% transmute(., vdate, date, fut = value)
# # collect(tbl(pg, sql(
# # 	"SELECT vdate, date, d1 AS fut
# # 	FROM forecast_values_v2_all
# # 	WHERE varname IN ('ffr') AND forecast = 'int'"
# # )))
#
# futures_data_q =
# 	futures_data %>%
# 	mutate(., date = floor_date(date, 'quarter')) %>%
# 	group_by(., vdate, date) %>%
# 	summarize(., fut = mean(fut), n = n(), .groups = 'drop') %>%
# 	filter(., n >= 3)
#
# annualized_term_premiums =
# 	spf_data %>%
# 	mutate(., vdate = vdate - days(7)) %>%
# 	# mutate(., vdate = floor_date(vdate, 'quarter')) %>%
# 	filter(., date >= vdate - months(3)) %>%
# 	inner_join(rename(futures_data_q, fut_vdate = vdate), join_by(date, closest(vdate >= fut_vdate))) %>%
# 	filter(., interval(vdate, fut_vdate)/days(1) >= -7) %>%
# 	mutate(
# 		.,
# 		months_ahead = interval(floor_date(vdate, 'quarter'), floor_date(date, 'quarter'))/months(1) + 2,
# 		diff = fut - spf,
# 		) %>%
# 	# {inner_join(
# 	# 	filter(., months_ahead != 2),
# 	# 	transmute(filter(., months_ahead == 2), vdate, diff_init = diff),
# 	# 	by = 'vdate'
# 	# 	)} %>%
# 	# mutate(., diff = diff - diff_init) %>%
#  	mutate(., cum_atp = 100 * ((1 + diff/100)^(12/months_ahead) - 1)) %>%
# 	filter(., months_ahead %in% c(5, 14)) %>%
# 	select(., -c(fut_vdate, n))
#
# annualized_term_premiums %>%
# 	filter(., year(vdate) >= 2005) %>%
# 	ggplot() +
# 	geom_line(aes(x = vdate, y = diff, group = months_ahead, color = as.factor(months_ahead)))
#
# atps =
# 	annualized_term_premiums %>%
# 	arrange(., vdate, months_ahead) %>%
# 	group_by(., vdate) %>%
# 	mutate(., end = months_ahead, start = coalesce(lag(end, 1), 0) + 1, dur = end - start + 1) %>%
# 	mutate(
# 		.,
# 		# Cumulative premium
# 		cum_prem = (1 + diff/100)^(12/months_ahead),
# 		chg_from_cum_prem = cum_prem/coalesce(dplyr::lag(cum_prem, 1), 1),
# 		atp = 100 * (chg_from_cum_prem^(1/dur) - 1)
# 		) %>%
# 	ungroup(.) %>%
# 	mutate(., time = paste0(start, '-', end)) %>%
# 	mutate(., time = case_when(
# 		time == '1-5' ~ 'Forecast date through end of 1Q ahead',
# 		# time == '3-5' ~ 'Start of 1Q',
# 		# time == '6-8' ~ 'Start of 2Q through end of 2Q',
# 		# time == '6-11' ~ 'Start of 3Q through end of 3Q',
# 		time == '6-14' ~ 'Start of 2Q ahead through end of 4Q ahead',
# 		.default = NA
# 	)) %>%
# 	na.omit(.)
#
# # atps %>%
# # 	nest(., .by = 'vdate') %>%
# # 	sample_n(., 30) %>%
# # 	unnest(., cols = c(data)) %>%
# # 	ggplot(.) +
# # 	geom_line(aes(x = months_ahead, y = atp)) +
# # 	geom_point(aes(x = months_ahead, y = atp)) +
# # 	facet_wrap(vars(vdate))
#
# atps %>%
# 	filter(., year(vdate) >= 2003) %>%
# 	# filter(., n() >= 4, .by = 'vdate') %>%
# 	mutate(., atp_sm = predict(smooth.spline(atp, spar = .01))$y, .by = c(time)) %>%
# 	ggplot() +
# 	geom_line(aes(x = vdate, y = atp_sm, group = time, color = time)) +
# 	geom_point(aes(x = vdate, y = atp_sm, group = time, color = time)) +
# 	# pivot_wider(., id_cols = vdate, names_from = months_ahead, values_from = diff, names_prefix = 'months_ahead_') %>%
# 	# ggplot() +
# 	# geom_line(aes(x = vdate, y = months_ahead_0)) +
# 	# geom_line(aes(x = vdate, y = months_ahead_6), color = 'blue') +
# 	# geom_line(aes(x = vdate, y = months_ahead_12), color = 'red') +
# 	facet_wrap(vars(time)) +
# 	geom_hline(yintercept = 0) +
# 	labs(x = 'Vintage Date', y = 'Months Ahead', color = 'Term') +
# 	ggthemes::theme_fivethirtyeight()
#
