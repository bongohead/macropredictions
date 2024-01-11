#' Returns the top Reddit posts for a given subreddit
#'
#' @param subreddit The name of the subreddit, e.g. `dogs`.
#' @param freq The frequency of the aggregation - valid values are `day`, `week`, `month`, `year`, and `all`.
#' @param ua The `User-Agent` to pass as a header, should be of the form `<platform>:<app ID>:<version string>
#'  (by u/<Reddit username>`. See [https://praw.readthedocs.io/en/stable/getting_started/quick_start.html].
#' @param token The auth token fetched from [https://www.reddit.com/api/v1/access_token].
#' @param max_pages The maximum amount of pages to iterate through. There are 100 results per page.
#'  Defaults to 10.
#' @param min_upvotes The minimum number of upvotes to retain a post. Defaults to 0
#' @param .verbose Logging verbosity; defaults to FALSE.
#'
#' @return A data frame of data with each row representing a post
#'
#' @description
#' Uses the Reddit API to iterate through results to pull top posts for a given `subreddit` and `freq`
#'  (e.g., the top posts by week for the "dogs" subreddit).
#'
#' @examples
#' reddit_token = get_reddit_token(
#'  ua = Sys.getenv('REDDIT_UA'), id = Sys.getenv('REDDIT_ID'), secret = Sys.getenv('REDDIT_SECRET'),
#'  username = Sys.getenv('REDDIT_USERNAME'), password = Sys.getenv('REDDIT_PASSWORD')
#' )
#' get_reddit_data_by_board(
#'  'StockMarket',
#'  freq = 'day',
#'  ua = Sys.getenv('REDDIT_UA'),
#'  token = reddit_token,
#'  max_pages = 1
#' )
#'
#' @import dplyr purrr
#' @importFrom httr2 request req_headers req_perform resp_headers resp_body_json
#'
#' @export
get_reddit_data_by_board = function(subreddit, freq, ua, token, max_pages = 10, min_upvotes = 0, .verbose = F) {

	stopifnot(
		is_scalar_character(subreddit),
		is_scalar_character(freq) & freq %in% c('day', 'week', 'month', 'year', 'all'),
		is_scalar_character(ua),
		is_scalar_character(token),
		is_scalar_double(min_upvotes) | is_scalar_integer(min_upvotes),
		is_scalar_double(min_upvotes) | is_scalar_integer(min_upvotes),
		is_scalar_logical(.verbose),
		all(map_lgl(c(subreddit, freq, ua, token), \(x) nchar(x) > 0))
	)

	# Only top possible for top
	reduce(1:max_pages, function(accum, i) {

		if (.verbose) message('***** Pull ', i)

		query =
			list(t = freq, limit = 100, show = 'all', after = {if (i == 1) NULL else tail(accum, 1)$after}) %>%
			compact(.) %>%
			paste0(names(.), '=', .) %>%
			paste0(collapse = '&')

		# message(query)
		http_result =
			request(paste0('https://oauth.reddit.com/r/', subreddit, '/top?', query)) %>%
			req_headers('User-Agent' = ua, 'Authorization' = paste0('bearer ', token)) %>%
			req_perform(.)

		if (as.integer(resp_headers(http_result)$`x-ratelimit-remaining`) == 0) {
			Sys.sleep(as.integer(resp_headers(http_result)$`x-ratelimit-reset`))
		}

		result = resp_body_json(http_result)

		parsed =
			map(result$data$children, \(post) as_tibble(compact(keep(post$data, \(z) !is.list(z))))) %>%
			list_rbind() %>%
			select(., any_of(c(
				'name', 'subreddit', 'title', 'created',
				'selftext', 'upvote_ratio', 'ups', 'is_self', 'domain', 'url_overridden_by_dest'
			))) %>%
			mutate(., i = i, after = coalesce(result$data$after, NA_character_))

		if (is.null(result$data$after)) {
			if (.verbose) message('----- Break, missing AFTER')
			return(done(bind_rows(list(accum, parsed))))
		} else {
			return(bind_rows(list(accum, parsed)))
		}

	}, .init = tibble()) %>%
	filter(., ups >= min_upvotes)

}

#' Returns the auth token for Reddit
#'
#' @param ua The `User-Agent` to pass as a header, should be of the form `<platform>:<app ID>:<version string>
#'  (by u/<Reddit username>`. See [https://praw.readthedocs.io/en/stable/getting_started/quick_start.html].
#' @param id The Reddit secret ID (received when creating a new API token).
#' @param secret The Reddit secret key (received when creating a new API token).
#' @param username The Reddit username used to create the API token.
#' @param password The Reddit password used to create the API token.
#' @param .verbose Logging verbosity; defaults to FALSE.
#'
#' @return An auth token
#'
#' @description
#' Get a session auth token used for running Reddit API calls
#'
#' @examples
#' reddit_token = get_reddit_token(
#'  ua = Sys.getenv('REDDIT_UA'), id = Sys.getenv('REDDIT_ID'), secret = Sys.getenv('REDDIT_SECRET'),
#'  username = Sys.getenv('REDDIT_USERNAME'), password = Sys.getenv('REDDIT_PASSWORD')
#' )
#' get_reddit_data_by_board(
#'  'StockMarket',
#'  freq = 'day',
#'  ua = Sys.getenv('REDDIT_UA'),
#'  token = reddit_token,
#'  max_pages = 1
#' )
#'
#' @import dplyr purrr
#' @importFrom httr2 request req_headers req_perform resp_headers resp_body_json
#' @importFrom RCurl base64
#'
#' @export
get_reddit_token = function(ua, id, secret, username, password) {
	stopifnot(
		is_scalar_character(ua),
		is_scalar_character(id),
		is_scalar_character(secret),
		is_scalar_character(username),
		is_scalar_character(password),
		all(map_lgl(c(ua, id, secret, username, password), \(x) nchar(x) > 0))
	)
	reddit_token =
		request('https://www.reddit.com/api/v1/access_token') %>%
		req_headers(
			'User-Agent' = ua,
			'Authorization' = paste0(
				'Basic ',
				base64(
					txt = paste0(id, ':', secret),
					mode = 'character'
				)
			)
		) %>%
		req_body_form(
			grant_type = 'client_credentials',
			username = username,
			password = password
			) %>%
		req_perform() %>%
		resp_body_json() %>%
		.$access_token

	return(reddit_token)

}
