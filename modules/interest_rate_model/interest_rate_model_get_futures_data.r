#' Gets futures values needed for interest_rate_model_run.r
#'
#' Usage:
#' - Run this script before interest_rsate_model_run.r. Assumes futures with trade dates of today are non-final.
#' - Runtime: with BACKFILL_MONTHS = 3: 15min
#'
#' CME futures reflect 3pm ET closing data.
#' https://www.cmegroup.com/confluence/display/EPICSANDBOX/Daily+Settlement+Time+Details

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
			from = floor_date(today('US/Eastern') %m-% months(BACKFILL_MONTHS + 60), 'month'),
			to = floor_date(today('US/Eastern') %m+% months(5 * 12), 'month'),
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
				varname = c('sofr', 'sofr', 'ffr', 'bsby', 'sonia', 'sonia', 'estr'),
				ticker = c('SL', 'SQ', 'ZQ', 'BR', 'J8', 'JU', 'EB'),
				max_months_out = c(5, 5, 5, 5, 6, 5, 6) * 12,
				tenor = c('1m', '3m', '30d', '3m', '3m', '1m', '3m')
			)
		) %>%
		mutate(
			.,
			code = paste0(ticker, code, str_sub(year, -2)),
			months_out = interval(floor_date(today('US/Eastern'), 'month'), floor_date(date, 'month')) %/% months(1)
		) %>%
		filter(., months_out <= max_months_out) %>%
		transmute(., varname, code, date, tenor) %>%
		arrange(., date)

	available_sources = get_available_barchart_sources(unique(str_sub(desired_sources$code, 1, 2)))
	scrape_sources = filter(desired_sources, code %in% available_sources)
	raw_data = get_barchart_data(scrape_sources$code)

	cleaned_data = raw_data %>%
		left_join(., scrape_sources, by = 'code') %>%
		# BSBY data is messed up, has old tradedates coming in - filter for product launch date
		filter(., varname != 'bsby' | tradedate >= '2021-07-01') %>%
		transmute(
			.,
			scrape_source = 'bc',
			varname,
			expdate = date,
			tenor,
			tradedate,
			is_final = ifelse(tradedate < today('US/Eastern'), T, F),
			value = ifelse(str_detect(varname, '^t\\d\\dy$'), close, 100 - close)
		) %>%
		filter(., expdate >= floor_date(tradedate, 'month')) # Get rid of forecasts for old observations

	scraped_data$bc <<- cleaned_data
})

## CME ---------------------------------------------------------------------
#' Gets most recent data for CME SOFR & FFR futures. Use barchart data preferrably.
# local({
#
# 	# CME Group Data
# 	message('Starting CME data scrape...')
# 	cme_cookie =
# 		request('https://www.cmegroup.com/') %>%
# 		add_standard_headers %>%
# 		req_perform %>%
# 		get_cookies(., T) %>%
# 		.$ak_bmsc
#
# 	# Get CME Vintage Date
# 	last_trade_date =
# 		request('https://www.cmegroup.com/CmeWS/mvc/Quotes/Future/305/G?quoteCodes=null&_=') %>%
# 		add_standard_headers %>%
# 		list_merge(., headers = list('Host' = 'www.cmegroup.com', 'Cookie' = cme_cookie)) %>%
# 		req_perform %>%
# 		resp_body_json %>%
# 		.$tradeDate %>%
# 		parse_date_time(., 'd-b-Y') %>%
# 		as_date(.)
#
# 	# See https://www.federalreserve.gov/econres/feds/files/2019014pap.pdf for CME futures model
# 	# SOFR 3m tenor./
# 	cme_search_space = tribble(
# 		~ varname, ~ cme_id, ~ tenor,
# 		'ffr', '305', '30d',
# 		'sofr', '8462', '3m',
# 		'sofr', '8463', '1m',
# 		'bsby', '10038', '3m'
# 		# 't02y', '10048',
# 		# 't05y', '10049',
# 		# 't10y', '10050',
# 		# 't30y', '10051'
# 	)
#
# 	cme_raw_data = list_rbind(map(df_to_list(cme_search_space), function(var) {
#
# 		quotes = request(paste0(
# 			'https://www.cmegroup.com/CmeWS/mvc/Quotes/Future/',
# 			var$cme_id,
# 			'/G?quoteCodes=null&_='
# 			)) %>%
# 			add_standard_headers %>%
# 			list_merge(., headers = list('Host' = 'www.cmegroup.com', 'Cookie' = cme_cookie)) %>%
# 			req_perform %>%
# 			resp_body_json %>%
# 			.$quotes
#
# 		# Note: use last instead of last to get previous day's close
# 		res = list_rbind(map(quotes, function(x) {
# 			if (x$last %in% c('0.000', '0.00', '-')) return() # Related bug in CME website
# 			tibble(
# 				vdate = last_trade_date,
# 				expdate = ymd(x$expirationDate),
# 				value = {
# 					if (str_detect(var$varname, '^t\\d\\dy$')) as.numeric(x$last)
# 					else 100 - as.numeric(x$last)
# 				},
# 				varname = var$varname,
# 				cme_id = var$cme_id,
# 				tenor = var$tenor
# 			)
# 		}))
#
# 		return(res)
# 	}))
#
# 	cme_raw_data %>%
# 		arrange(., expdate) %>%
# 		ggplot(.) +
# 		geom_line(aes(x = expdate, y = value, color = cme_id))
#
# 	cme_data =
# 		cme_raw_data %>%
# 		arrange(., expdate) %>%
# 		# Get rid of forecasts for old observations
# 		filter(., expdate >= floor_date(last_trade_date, 'month') & value != 100) %>%
# 		transmute(
# 			.,
# 			scrape_source = 'cme',
# 			varname,
# 			expdate,
# 			tenor,
# 			tradedate = last_trade_date,
# 			is_final = ifelse(tradedate < today('US/Eastern'), T, F),
# 			value
# 			)
#
# 	# Most data starts in 88-89, except j=12 which starts at 1994-01-04. Misc missing obs until 2006.
# 	# 	df %>%
# 	#   	tidyr::pivot_wider(., names_from = j, values_from = settle) %>%
# 	#     	dplyr::arrange(., date) %>% na.omit(.) %>% dplyr::group_by(year(date)) %>% dplyr::summarize(., n = n()) %>%
# 	# 		View(.)
# 	scraped_data$cme <<- cme_data
# })

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
					tradedate = as_date(parse_date_time(vdate, orders = '%a %b %d %H:%M:%S %Y'), tz = 'ET'),
					is_final = ifelse(tradedate < today('US/Eastern'), T, F),
					value = 100 - value
				)
		))
	}))

	scraped_data$ice <<- ice_raw_data
})

## CBOE --------------------------------------------------------------------
#' AMERIBOR futures
local({

	scrape_dates = seq(
		max(today('US/Eastern') %m-% months(BACKFILL_MONTHS), as_date('2019-08-16')), # Ameribor futures launch date
		to = today('US/Eastern'),
		by = '1 day'
		)

	scrape_reqs = map(scrape_dates, \(d)
		request(str_glue('https://www.cboe.com/us/futures/market_statistics/settlement/csv?dt={d}'))
		)

	responses = macropredictions::send_async_requests(scrape_reqs, .chunk_size = 10, .max_retries = 5)

	csv_data = list_rbind(compact(imap(responses, function(r, i) {
		raw_str = resp_body_string(r)
		if (str_length(raw_str) <= 50) return(NULL)
		read_csv(raw_str, col_names = c('prod', 'sym', 'expdate', 'price'), col_types = 'ccDn', skip = 1) %>%
			mutate(., tradedate = as_date(scrape_dates[[i]]))
	})))

	cboe_data =
		csv_data %>%
		filter(., prod %in% c('AMT1', 'AMB1', 'AMB3')) %>%
		mutate(., tenor = case_when(
			prod == 'AMB1' ~ '1m',
			prod == 'AMB3' ~ '3m',
			prod == 'AMT1' ~ '30d'
		)) %>%
		transmute(
			.,
			scrape_source = 'cboe',
			varname = 'ameribor',
			expdate = floor_date(expdate %m-% months(1), 'months'),
			tenor,
			tradedate,
			is_final = ifelse(tradedate < today('US/Eastern'), T, F),
			value = 100 - price/100
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
	validation_log$last_tradedate <<- max(futures_values$tradedate)

	disconnect_db(pg)

	futures_values <<- futures_values
})
