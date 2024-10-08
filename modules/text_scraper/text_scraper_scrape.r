#' Scrapes text from different sources

# Initialize ----------------------------------------------------------
validation_log <<- list()

## Load Libs ----------------------------------------------------------
library(macropredictions)
library(tidyverse)
library(httr2)
library(rvest)
library(RCurl, include.only = 'base64')

## Load Connection Info ----------------------------------------------------------
load_env()
pg = connect_pg()

scrape_data <- list()

# Data 1 --------------------------------------------------------

## Reddit --------------------------------------------------------
local({

	message(str_glue('*** Pulling Reddit Data: {format(now(), "%H:%M")}'))

	reddit_boards = get_query(
		pg,
		"SELECT scrape_board, scrape_ups_floor
		FROM text_scraper_reddit_boards
		WHERE scrape_active = true"
		)

	reddit_token = get_reddit_token(
		ua = Sys.getenv('REDDIT_UA'),
		id = Sys.getenv('REDDIT_ID'),
		secret = Sys.getenv('REDDIT_SECRET'),
		username = Sys.getenv('REDDIT_USERNAME'),
		password = Sys.getenv('REDDIT_PASSWORD')
	)

	# Run monthly year and all
	search_combinations = expand_grid(
		reddit_boards,
		tribble(
			~freq, ~ scrape_method, ~ max_pages,
			'day', 'top_1000_today', 9,
			'week', 'top_1000_week', 9,
			'month', 'top_1000_month', 9,
			'year', 'top_1000_year', 9
			)
		) %>%
		# Only get top monthly on Sundays, top annually on the first day of month
		{if(wday(today('US/Eastern')) == 1) . else filter(., freq != 'month')} %>%
		{if(day(today('US/Eastern')) == 1) . else filter(., freq != 'year')}

	scrape_results = map(df_to_list(search_combinations), .progress = T, function(s) {
		get_reddit_data_by_board(
		 	s$scrape_board,
		 	freq = s$freq,
		 	ua = Sys.getenv('REDDIT_UA'),
		 	token = reddit_token,
		 	max_pages = s$max_pages,
		 	min_upvotes = s$scrape_ups_floor ,
		 	.verbose = F
		 	) %>%
			mutate(., scrape_method = s$scrape_method, scrape_board = s$scrape_board)
	})

	cleaned_results =
		scrape_results %>%
		list_rbind() %>%
		transmute(
			.,
			scrape_method,
			post_id = name,
			scrape_board,
			source_board = subreddit,
			title, selftext, upvote_ratio, ups, is_self, domain, url = url_overridden_by_dest,
			created_dttm = with_tz(as_datetime(created, tz = 'UTC'), 'US/Eastern'),
			scraped_dttm = now('US/Eastern')
		) %>%
		# Filter out duplicated post_ids x scrape_method. This can occur rarely due to page bumping.
		slice_sample(., n = 1, by = c(post_id, scrape_method, scrape_board))


	# Check dupes
	cleaned_results %>%
		group_by(., post_id, scrape_method) %>%
		summarize(., n = n(), boards = paste0(scrape_board, collapse = ', '), .groups = 'drop') %>%
		arrange(., desc(n)) %>%
		filter(., n > 1) %>%
		print(.)


	scrape_data$reddit <<- cleaned_results
})


## Store --------------------------------------------------------
local({

	message(str_glue('*** Sending Reddit Data to SQL: {format(now(), "%H:%M")}'))

	initial_count = get_rowcount(pg, 'text_scraper_reddit_scrapes')
	message('***** Initial Count: ', initial_count)

	insert_data =
		bind_rows(scrape_data$reddit) %>%
		filter(., !is.na(post_id) & !is.na(title) & !is.na(scrape_board) & !is.na(source_board))

	insert_result = write_df_to_sql(
		pg,
		insert_data,
		'text_scraper_reddit_scrapes',
		'ON CONFLICT (scrape_method, post_id, scrape_board) DO UPDATE SET
			source_board=EXCLUDED.source_board,
			title=EXCLUDED.title,
			selftext=EXCLUDED.selftext,
			upvote_ratio=EXCLUDED.upvote_ratio,
			ups=EXCLUDED.ups,
			is_self=EXCLUDED.is_self,
			domain=EXCLUDED.domain,
			url=EXCLUDED.url,
			created_dttm=EXCLUDED.created_dttm,
			scraped_dttm=EXCLUDED.scraped_dttm'
	)


	final_count = get_rowcount(pg, 'text_scraper_reddit_scrapes')
	rows_added = final_count - initial_count


	message('***** Rows Added: ', rows_added)

	validation_log$reddit_rows_added <<- rows_added
})


# Data 2 --------------------------------------------------------

## Pull Reuters --------------------------------------------------------
local({

	message(str_glue('*** Pulling Reuters Data: {format(now(), "%H:%M")}'))

	pages = 1:10 #10 normally, 300 for backfill

	requests = map(pages, function(p) {
		request(paste0(
			'https://www.reuters.com/pf/api/v3/content/fetch/articles-by-search-v2',
			'?query=',
			URLencode(paste0(
				'{"keyword":"business","offset":', (p - 1) * 100,',"orderby":"display_date:desc",',
				'"sections":"/business","size":100,"website":"reuters"}'
				)),
			'&d=168&_website=reuters'
			)) %>%
			add_standard_headers()
	})

	http_responses = send_async_requests(requests, .chunk_size = 20, .max_retries = 5, .verbose = T)

	cleaned_responses = imap(http_responses, .progress = F, function(x, i) {
		x %>%
			resp_body_json() %>%
			.$result %>%
			.$articles %>%
			map(., \(a) tibble(
				page = pages[i],
				title = a$title,
				description = a$description,
				link = a$canonical_url,
				created = as_date(with_tz(as_datetime(a$published_time), 'US/Eastern'))
			)) %>%
			list_rbind()
	})

	reuters_data =
		bind_rows(cleaned_responses) %>%
		transmute(
			.,
			scrape_source = 'reuters',
			scrape_subsource = 'business',
			title,
			link,
			description,
			created_dt = created,
			scraped_dttm = now('US/Eastern')
		) %>%
		# Duplicates can be caused by shifting pages
		distinct(., title, created_dt, .keep_all = T)

	scrape_data$reuters <<- reuters_data
})


## Pull FT --------------------------------------------------------
local({

	message(str_glue('*** Pulling FT Data: {format(now(), "%H:%M")}'))

	method_map = tribble(
		~ scrape_subsource, ~ ft_key,
		'economics', 'ec4ffdac-4f55-4b7a-b529-7d1e3e9f150c',
		'US economy', '6aa143a2-7a0c-4a20-ae90-ca0a46f36f92'
	)

	existing_pulls = as_tibble(get_query(
		pg,
		"SELECT created_dt, scrape_subsource, COUNT(*) as count
		FROM text_scraper_media_scrapes
		WHERE scrape_source = 'ft'
		GROUP BY created_dt, scrape_subsource"
		))

	possible_pulls = expand_grid(
		created_dt = seq(from = as_date('2020-01-01'), to = today() + days(1), by = '1 day'),
		scrape_subsource = method_map$scrape_subsource
	)

	new_pulls =
		anti_join(
			possible_pulls,
			# Always pull last week articles
			existing_pulls %>% filter(., created_dt <= today() - days(7)),
			by = c('created_dt', 'scrape_subsource')
		) %>%
		left_join(., method_map, by = 'scrape_subsource')

	message('***** New Pulls')
	print(new_pulls)


	ft_data_raw = imap(df_to_list(new_pulls), .progress = T, function(x, i) {

		# message(str_glue('***** Pulling data for {i} of {nrow(new_pulls)}'))
		url = str_glue(
			'https://www.ft.com/search?',
			'&q=the',
			'&dateFrom={as_date(x$created_dt)}&dateTo={as_date(x$created_dt) + days(1)}',
			'&sort=date&expandRefinements=true&contentType=article',
			'&concept={x$ft_key}'
		)
		# message(url)

		page1 =
			request(url) %>%
			add_standard_headers() %>%
			req_retry(max_tries = 10, backoff = \(j) 2 * 2^j) %>%
			req_perform() %>%
			resp_body_html()

		pages =
			page1 %>%
			html_node(., 'div.search-results__heading-title > h2') %>%
			html_text(.) %>%
			str_replace_all(., coll('Powered By Algolia'), '') %>%
			str_extract(., '(?<=of ).*') %>%
			as.numeric(.) %>%
			{(. - 1) %/% 25 + 1}

		if (is.na(pages)) return(NULL)

		list_rbind(map(1:pages, function(page) {

			this_page = {
				if (page == 1) page1
				else
					request(paste0(url, '&page=',page)) %>%
					req_retry(max_tries = 10, backoff = \(j) 2 * 2^j) %>%
					req_perform() %>%
					resp_body_html()
			}

			search_results =
				this_page %>%
				html_nodes(., '.search-results__list-item .o-teaser__content') %>%
				map(., function(z) tibble(
					title = z %>% html_nodes('.o-teaser__heading') %>% html_text(.),
					description = z %>% html_nodes('.o-teaser__standfirst') %>% html_text,
					link = z %>% html_nodes('.o-teaser__standfirst > a') %>% html_attr(., 'href')
				)) %>%
				list_rbind()
			})) %>%
			mutate(., scrape_subsource = x$scrape_subsource, created_dt = as_date(x$created_dt))
	})


	ft_data =
		ft_data_raw %>%
		compact(.) %>%
		keep(., is_tibble) %>%
		{
			if (length(.) >= 1)
				bind_rows(.) %>%
				transmute(
					.,
					scrape_source = 'ft',
					scrape_subsource,
					title,
					link,
					description,
					created_dt,
					scraped_dttm = now('US/Eastern')
				)
			else tibble()
		} %>%
		distinct(., title, created_dt, .keep_all = T)

	scrape_data$ft <<- ft_data
})

## Store --------------------------------------------------------
local({0

	message(str_glue('*** Sending Media Data to SQL: {format(now(), "%H:%M")}'))

	initial_count = get_rowcount(pg, 'text_scraper_media_scrapes')
	message('***** Initial Count: ', initial_count)

	insert_data = bind_rows(scrape_data$ft, scrape_data$reuters)

	insert_result = write_df_to_sql(
		pg,
		insert_data,
		'text_scraper_media_scrapes',
		'ON CONFLICT (scrape_source, scrape_subsource, title, created_dt) DO UPDATE SET
			link=EXCLUDED.link,
			description=EXCLUDED.description,
			scraped_dttm=EXCLUDED.scraped_dttm'
	)

	final_count = get_rowcount(pg, 'text_scraper_media_scrapes')
	rows_added = final_count - initial_count
	message('***** Rows Added: ', rows_added)

	disconnect_db(pg)

	validation_log$media_rows_added <<- rows_added
})

