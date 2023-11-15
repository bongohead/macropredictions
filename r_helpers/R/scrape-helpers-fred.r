#' Returns data from St. Louis Federal Reserve Economic Database (FRED)
#'
#' @param series_id The FRED identifier of the time series to pull.
#' @param api_key A valid FRED API key.
#' @param .freq One of 'd', 'm', 'q'. If NULL, returns highest available frequency.
#' @param .return_vintages If TRUE, returns all historic recordings of an observation ('vintages').
#' @param .vintage_date If .return_vintages = TRUE, .vintage_date can be set to only return the vintage for a single date.
#' @param .obs_start The default start date of results to return.
#' @param .verbose If TRUE, echoes error messages.
#'
#' @return A data frame of data
#'
#' @import dplyr purrr httr2
#' @importFrom lubridate with_tz now as_date
#'
#' @export
get_fred_data = function(series_id, api_key, .freq = NULL, .return_vintages = F, .vintage_date = NULL, .obs_start = '2000-01-01', .verbose = F) {

	today = as_date(with_tz(now(), tz = 'America/Chicago'))

	url = paste0(
		'https://api.stlouisfed.org/fred/series/observations?',
		'series_id=', series_id,
		'&api_key=', api_key,
		'&file_type=json',
		'&realtime_start=', if (.return_vintages == T & is.null(.vintage_date)) .obs_start else if (.return_vintages == T & !is.null(.vintage_date)) .vintage_date else today,
		'&realtime_end=', if (.return_vintages == T & !is.null(.vintage_date)) .vintage_date else today,
		'&observation_start=', .obs_start,
		'&observation_end=', today,
		if(!is.null(.freq)) paste0('&frequency=', .freq) else '',
		'&aggregation_method=avg'
		)

	if (.verbose == TRUE) message(url)

	request(url) %>%
		req_perform %>%
		resp_body_json %>%
		.$observations %>%
		map(., as_tibble) %>%
		list_rbind %>%
		filter(., value != '.') %>%
		na.omit %>%
		transmute(
			.,
			date = as_date(date),
			vintage_date = as_date(realtime_start),
			varname = series_id,
			value = as.numeric(value)
		) %>%
		{if(.return_vintages == TRUE) . else select(., -vintage_date)} %>%
		return(.)
}

#' Returns last available observations from St. Louis Federal Reserve Economic Database (FRED)
#'
#' @param pull_ids A vector of FRED series IDs to pull from such as `c(id1, id2, ...)`;
#'  alternatively, a list of FRED series IDs and corresponding frequencies: `list(c(id1, d), (id2, w), ..)`.
#' @param api_key A valid FRED API key.
#' @param .obs_start The start date of observations dates to search for.
#' @param .verbose If `TRUE`, echoes error messages.
#'
#' @return A data frame of data with each observation corresponding to a unique date/series_id.
#'
#' @description
#' Get latest observations for multiple data series from FRED.
#'  Note that since FRED returns weekly data in the form of week-ending dates, we subtract 7 days off the date to
#'  convert them into week beginning dates.
#'
#' @import dplyr purrr httr2
#' @importFrom lubridate with_tz now as_date is.Date days
#'
#' @examples \dontrun{
#'  get_fred_obs(list(c('GDP', 'a'), c('PCE', 'a')), api_key)
#'  get_fred_obs(c('GDP', 'PCE'), api_key)
#' }
#'
#' @export
get_fred_obs = function(pull_ids, api_key, .obs_start = '2000-01-01', .verbose = F)  {

	stopifnot(
		(is_list(pull_ids) & every(pull_ids, \(x) length(x) == 2 && is_character(x))) || is_character(pull_ids),
		is.Date(.obs_start) || is_scalar_character(.obs_start),
		is_scalar_logical(.verbose)
	)

	today = as_date(with_tz(now(), tz = 'America/Chicago'))

	# Build hashmap of inputs per series
	reqs_map = {
		if (is_list(pull_ids)) map(pull_ids, \(x) list(series_id = x[1], freq = x[2]))
		else map(pull_ids, \(x) list(series_id = x, freq = NA))
	}

	# Use the series endpoint to check if each series exists & get frequencies if not explicitly passed
	reqs_map_filled = map(reqs_map, function(x) {
		res =
			paste0(
				'https://api.stlouisfed.org/fred/series?',
				'series_id=', x$series_id,
				'&api_key=',api_key,
				'&file_type=json'
			) %>%
			request() %>%
			req_error(., body = function(resp) {
				print(resp)
				return(resp_body_string(resp))
			}) %>%
			req_perform() %>%
			resp_body_json() %>%
			.$seriess %>%
			.[[1]]

		list(series_id = x$series_id, freq = if(is.na(x$freq)) tolower(res$frequency_short) else x$freq)
	})

	# Get data from obs endpoint
	obs_results = imap(reqs_map_filled, function(x, i) {
		req =
			request(paste0(
				'https://api.stlouisfed.org/fred/series/observations?',
				'series_id=', x$series_id,
				'&api_key=', api_key,
				'&file_type=json',
				'&realtime_start=', today, '&realtime_end=', today,
				'&observation_start=', .obs_start, '&observation_end=', today,
				'&frequency=', x$freq,
				'&aggregation_method=avg'
			)) %>%
			req_timeout(., 8000) %>%
			req_error(., body = function(resp) {
				print(resp)
				return(resp_body_string(resp))
			})

		resp = req_perform(req)

		resp %>%
			resp_body_json() %>%
			.$observations %>%
			map(., as_tibble) %>%
			list_rbind %>%
			filter(., value != '.') %>%
			na.omit %>%
			transmute(
				.,
				# Adjust week-ending periods to week-starting periods
				date = as_date(date) - days({if (x$freq == 'w') 6 else 0}),
				series_id = x$series_id,
				value = as.numeric(value),
				freq = x$freq
			)
	})

	return(list_rbind(obs_results))
}


#' Returns observations for current & historical vintages from St. Louis Federal Reserve Economic Database (FRED)
#'
#' @param pull_ids A vector of FRED series IDs to pull from such as `c(id1, id2, ...)`;
#'  alternatively, a list of FRED series IDs and corresponding frequencies: `list(c(id1, d), (id2, w), ..)`.
#' @param api_key A valid FRED API key.
#' @param .obs_start The start date of observations and vintage dates to search for.
#' @param .verbose If `TRUE`, echoes error messages.
#'
#' @return A data frame of data with each observation corresponding to a unique date/series_id/vintage_date.
#'
#' @description
#' Get observations and vintages for multiple data series from FRED.
#' It automatically breaks up vintage dates into chunks to avoid FRED errors.
#' Note that since FRED returns weekly data in the form of week-ending dates, we subtract 7 days off the date to
#' convert them into week beginning dates.
#'
#' @import dplyr purrr httr2
#' @importFrom lubridate with_tz now as_date days
#'
#' @examples \dontrun{
#'  get_fred_obs_with_vintage(list(c('GDP', 'a'), c('PCE', 'a')), api_key)
#'  get_fred_obs_with_vintage(c('GDP', 'PCE'), api_key)
#' }
#'
#' @export
get_fred_obs_with_vintage = function(pull_ids, api_key, .obs_start = '2000-01-01', .verbose = F)  {

	stopifnot(
		(is_list(pull_ids) & every(pull_ids, \(x) length(x) == 2 && is_character(x))) || is_character(pull_ids),
		is.Date(.obs_start) || is_scalar_character(.obs_start),
		is_scalar_logical(.verbose)
	)

	today = as_date(with_tz(now(), tz = 'America/Chicago'))
	max_vdates_per_fetch = 2000 # Limit imposed by FRED


	# Build hashmap of inputs per series
	reqs_map = {
		if (is_list(pull_ids)) map(pull_ids, \(x) list(series_id = x[1], freq = x[2]))
		else map(pull_ids, \(x) list(series_id = x, freq = NA))
	}

	# Use the series endpoint to check if each series exists & get frequencies if not explicitly passed
	# Use the vintages endpoint to check what the available vintage dates for a series is
	reqs_map_filled = map(reqs_map, function(x) {

		series_url = paste0(
			'https://api.stlouisfed.org/fred/series?',
			'series_id=', x$series_id,
			'&api_key=',api_key,
			'&file_type=json'
		)

		vintages_url = paste0(
			'https://api.stlouisfed.org/fred/series/vintagedates?',
			'series_id=', x$series_id,
			'&api_key=', api_key,
			'&file_type=json',
			'&realtime_start=', .obs_start,
			'&realtime_end=', today
		)

		if (.verbose == T) message(series_url)

		# Returns series metadata
		series_res =
			request(series_url) %>%
			req_retry(max_tries = 10, backoff = function(i) 30) %>%
			req_error(., body = function(resp) {
				print(resp)
				return(resp_body_string(resp))
			}) %>%
			req_perform() %>%
			resp_body_json() %>%
			.$seriess %>%
			.[[1]]

		# Returns list of vintage dates
		vintage_res =
			request(vintages_url) %>%
			req_retry(max_tries = 10, backoff = function(i) 30) %>%
			req_error(., body = function(resp) {
				print(resp)
				return(resp_body_string(resp))
			}) %>%
			req_perform() %>%
			resp_body_json() %>%
			.$vintage_dates %>%
			unlist(.)

		vintage_dates_split = split(vintage_res, ceiling(seq_along(vintage_res)/max_vdates_per_fetch))
		vintage_date_groups = map(vintage_dates_split, function(x) list(start = head(x, 1), end = tail(x, 1)))


		list(
			series_id = x$series_id,
			freq = if(is.na(x$freq)) tolower(series_res$frequency_short) else x$freq,
			vintage_date_groups = vintage_date_groups
			)
	})

	obs_results = list_rbind(map(reqs_map_filled, function(x) {

		results_by_vdate_group = list_rbind(map(x$vintage_date_groups, function(v) {

			obs_url = paste0(
				'https://api.stlouisfed.org/fred/series/observations?',
				'series_id=', x$series_id,
				'&api_key=', api_key,
				'&file_type=json',
				'&realtime_start=', v$start, '&realtime_end=', v$end,
				'&observation_start=', .obs_start, '&observation_end=', today,
				'&frequency=', x$freq,
				'&aggregation_method=avg'
				)

			if (.verbose == T) message(obs_url)

			request(obs_url) %>%
				req_retry(max_tries = 10, backoff = function(i) 30) %>%
				req_error(., body = function(resp) {
					print(resp)
					return(resp_body_string(resp))
				}) %>%
				req_perform() %>%
				resp_body_json() %>%
				.$observations %>%
				map(., as_tibble) %>%
				list_rbind %>%
				filter(., value != '.') %>%
				na.omit %>%
				transmute(
					.,
					# Adjust week-ending periods to week-starting periods
					date = as_date(date) - days({if (x$freq == 'w') 6 else 0}),
					vintage_date = as_date(realtime_start),
					series_id = x$series_id,
					value = as.numeric(value),
					freq = x$freq
				)
		}))

	}))

	return(obs_results)
}





