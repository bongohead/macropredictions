#' Get yields data from US Treasury
#'
#' @description
#' Gets yields data from the US Treasury website. Dates included are 2001 - today, subject to a possible 3:30PM lag.
#'
#' @import dplyr purrr
#' @importFrom lubridate as_date today year mdy
#' @importFrom readr read_csv
#' @importFrom httr2 request
#' @importFrom tidyr separate pivot_longer
#'
#' @examples \dontrun{
#'  get_treasury_yields()
#' }
#'
#' @export
get_treasury_yields = function() {

	urls = c(
		'https://home.treasury.gov/system/files/276/yield-curve-rates-2001-2010.csv',
		'https://home.treasury.gov/system/files/276/yield-curve-rates-2011-2020.csv',
		paste0(
			'https://home.treasury.gov/resource-center/data-chart-center/interest-rates/daily-treasury-rates.csv/',
			2021:year(today('US/Eastern')),
			'/all?type=daily_treasury_yield_curve&field_tdr_date_value=',year(today('US/Eastern')),'&page&_format=csv'
		)
	)

	responses = send_async_requests(map(urls, \(x) request(x)))
	csv = list_rbind(map(responses, \(r) read_csv(resp_body_string(r), col_types = 'c')))

	csv %>%
		pivot_longer(., cols = -c('Date'), names_to = 'varname', values_to = 'value') %>%
		separate(., col = 'varname', into = c('ttm_1', 'ttm_2'), sep = ' ') %>%
		transmute(
			.,
			varname = paste0('t', str_pad(ttm_1, 2, pad = '0'), ifelse(ttm_2 == 'Mo', 'm', 'y')),
			ttm = ifelse(ttm_2 == 'Mo', as.numeric(ttm_1), as.numeric(ttm_1) * 12),
			date = mdy(Date),
			value
		) %>%
		filter(., !is.na(value)) %>%
		arrange(., date, varname)

}
