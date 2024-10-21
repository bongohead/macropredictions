#' Get data from Yahoo finance `r lifecycle::badge("superseded")`
#'
#' @param tickers Tickers to pass into Yahoo Finance
#' @param start_date A start date
#'
#' @import dplyr purrr
#' @importFrom lubridate as_date today days
#' @importFrom data.table fread
#'
#' @examples \dontrun{
#'  get_yahoo_data_old('^VIX')
#' }
#'
#' @export
get_yahoo_data_old = function(tickers, .obs_start = '2000-01-01') {

	stopifnot(
		is_character(tickers),
		is.Date(.obs_start) || is_scalar_character(.obs_start)
	)

	list_rbind(map(tickers, function(ticker) {

		url = paste0(
			'https://query1.finance.yahoo.com/v7/finance/download/', ticker,
			'?period1=', as.numeric(as.POSIXct(as_date(.obs_start))),
			'&period2=', as.numeric(as.POSIXct(today() + days(1))),
			'&interval=1d',
			'&events=history&includeAdjustedClose=true'
		)

		fread(url, showProgress = FALSE) %>%
			as_tibble(.) %>%
			.[, c('Date', 'Adj Close')]	%>%
			set_names(., c('date', 'value')) %>%
			as_tibble(.) %>%
			filter(., value != 'null') %>%
			mutate(., ticker = ticker, date = as_date(date), value = as.numeric(value))
	}))

}

#' Get data from Yahoo finance
#'
#' @param tickers Tickers to pass into Yahoo Finance
#' @param .obs_start The start date in YYYY-MM-DD format; 2000-01-01 by default
#' @param .verbose Echo URL if true
#'
#' @import dplyr purrr httr2
#' @importFrom lubridate as_date today days
#'
#' @examples \dontrun{
#'  get_yahoo_data('^VIX')
#' }
#'
#' @export
get_yahoo_data = function(tickers, .obs_start = '2000-01-01', .verbose = F) {

	stopifnot(
		is_character(tickers),
		is.Date(.obs_start) || is_scalar_character(.obs_start)
	)

	list_rbind(map(tickers, function(ticker) {

		url = paste0(
			'https://query1.finance.yahoo.com/v8/finance/chart/', ticker,
			'?formatted=true',
			'&includeAdjustedClose=true',
			'&period1=', format(as.numeric(as.POSIXct(as_date(.obs_start))), scientific = F),
			'&period2=', format(as.numeric(as.POSIXct(today() + days(1))), scientific = F),
			'&interval=1d',
			'&symbol=', ticker,
			'&userYfid=true&lang=en-US&region=US'
		)

		if (.verbose) print(url)

		response_parsed =
			request(url) %>%
			req_perform() %>%
			resp_body_json()

		tibble(
			date = unlist(response_parsed$chart$result[[1]]$timestamp),
			# Handle empty values in the JSON response
			value =
				response_parsed$chart$result[[1]]$indicators$adjclose[[1]]$adjclose %>%
				map(., \(x) if(length(x) == 1) x else 'null') %>%
				unlist()
			) %>%
			mutate(date = as_date(as_datetime(date))) %>%
			filter(., value != 'null') %>%
			arrange(date) %>%
			mutate(., ticker = ticker, value = as.numeric(value))
	}))

}
