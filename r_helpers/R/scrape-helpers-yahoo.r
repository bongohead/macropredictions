#' Get data from Yahoo finance
#'
#' @param tickers Tickers to pass into Yahoo Finance
#' @param start_date A start date
#'
#' @import dplyr purrr
#' @importFrom lubridate as_date today days
#' @importFrom data.table fread
#'
#' @examples \dontrun{
#'  get_yahoo_data('^VIX')
#' }
#'
#' @export
get_yahoo_data = function(tickers, .obs_start = '2000-01-01') {

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

