#' Given a prefix, search for matching Barchart.com codes
#'
#' @param code_prefix A character vector of things to search for
#'
#' @examples \dontrun{
#' get_available_barchart_sources(c('ZQ', 'SQ'))
#' }
#'
#' @import dplyr purrr httr2
#'
#' @export
get_available_barchart_sources = function(code_prefix) {

	cookies =
		request('https://www.barchart.com/futures/quotes/ZQM22/historical-prices') %>%
		add_standard_headers %>%
		req_perform %>%
		get_cookies

	available_sources = unlist(lapply(code_prefix, function(x) {
		request(URLencode(paste0(
			'https://www.barchart.com/proxies/core-api/v1/quotes/get',
			'?fields=contractExpirationDate.format(Y),symbol,contractNameHistorical,lastPrice,tradeTime',
			'&list=futures.historical.byRoot(', x, ')&orderBy=contractExpirationDate&orderDir=asc',
			'&meta=field.shortName,field.type,field.description,lists.lastUpdate&hasOptions=true',
			'&raw=1'
		))) %>%
		add_standard_headers %>%
		list_merge(., headers = list(
			'X-XSRF-TOKEN' = URLdecode(str_extract(cookies$`XSRF-TOKEN`, '(?<==).*')),
			'Referer' = paste0('https://www.barchart.com/futures/quotes/', x, 'M20/historical-prices'),
			'Cookie' = URLdecode(paste0(
				'bcFreeUserPageView=0; webinar144WebinarClosed=true; ',
				paste0(cookies, collapse = '; ')
			))
		)) %>%
		req_retry(max_tries = 5) %>%
		req_perform %>%
		resp_body_json %>%
		.$data %>%
		map_chr(., \(x) x$symbol)

	}))

	return(available_sources)
}

#' Given a prefix, search for matching Barchart.com codes
#'
#' @param codes A character vector of codes
#' @param .verbose Whether to emit progress
#'
#' @examples \dontrun{
#' get_barchart_data(c('ZQF90', 'ZQF91'))
#' }
#'
#' @import dplyr purrr httr2
#'
#' @export
get_barchart_data = function(codes, .verbose = T) {

	cookies =
		request(paste0('https://www.barchart.com/futures/quotes/', codes[[1]], '/')) %>%
		add_standard_headers %>%
		req_perform %>%
		get_cookies(., T)

	barchart_data = list_rbind(map(codes, .progress = .verbose, function(code) {

		Sys.sleep(runif(1, 1, 4))

		http_response =
			request(paste0(
				'https://www.barchart.com/proxies/timeseries/queryeod.ashx?',
				'symbol=', code, '&data=daily&maxrecords=640&volume=contract&order=asc',
				'&dividends=false&backadjust=false&daystoexpiration=1&contractroll=expiration'
			)) %>%
			add_standard_headers %>%
			list_merge(., headers = list(
				'X-XSRF-TOKEN' = URLdecode(str_extract(cookies$`XSRF-TOKEN`, '(?<==).*')),
				'Referer' = 'https://www.barchart.com/futures/quotes/ZQZ20',
				'Cookie' = URLdecode(paste0(
					'bcFreeUserPageView=0; webinar113WebinarClosed=true; ',
					paste0(cookies, collapse = '; ')
				))
			)) %>%
			req_perform

		res =
			http_response %>%
			resp_body_string  %>%
			read_csv(
				.,
				# Verified columns are correct - compare CME official chart to barchart
				col_names = c('contract', 'tradedate', 'open', 'high', 'low', 'close', 'volume', 'oi'),
				col_types = 'cDdddddd'
			) %>%
			mutate(., code = code)

		if (.verbose) message('***** Rows: ', nrow(res))

		return(res)
	}))
}
