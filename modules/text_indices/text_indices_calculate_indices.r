#' Backfill specific boards

# Initialize ----------------------------------------------------------
validation_log <<- list()

## Load Libs ----------------------------------------------------------
library(econforecasting)
library(tidyverse)
library(httr2)

## Load Connection Info ----------------------------------------------------------
load_env(Sys.getenv('EF_DIR'))
pg = connect_pg()

# Construct Scorer-Level Indices --------------------------------------------------------

## Get Benchmarks ----------------------------------------------------------
local({

	api_key = Sys.getenv('FRED_API_KEY')

	input_sources = tribble(
		~varname, ~ hist_source_freq, ~ hist_source_key,
		'unemp', 'm', 'UNRATE',
		'ics', 'w', 'ICSA',
		'ccs', 'w', 'CCSA',
		'kclf', 'm', 'FRBKCLMCILA',
		'sent', 'm', 'UMCSENT'
	)

	fred_data =
		input_sources %>%
		df_to_list	%>%
		map(., \(x) {
				get_fred_obs_with_vintage(x$hist_source_key, api_key, x$hist_source_freq) %>%
				transmute(date, vdate = vintage_date, varname = x$varname, value)
			}) %>%
		list_rbind() %>%
		filter(., date >= '2017-01-01')

	benchmarks <<- fred_data
})

## LLM/BERT Scores --------------------------------------------------------
local({

	input_data = get_query(pg, str_glue(
	    "SELECT
	        a.post_id, a.label_key, a.label_value, a.label_rationale AS rationale,
	        b.title, b.selftext, b.source_board,
	        b.ups, DATE(created_dttm AT TIME ZONE 'US/Eastern') AS created_dt
	    FROM text_scraper_reddit_llm_scores_v2 a
	    INNER JOIN text_scraper_reddit_scrapes b
	        ON a.scrape_id = b.scrape_id
	    WHERE a.prompt_id = 'financial_health_v1'"
	))

	# Check balance is correct
	wide_values %>%
		count(., source_board, created_dt) %>%
		arrange(., created_dt) %>%
		mutate(., z = zoo::rollmean(n, 14, align = 'right', fill = NA), .by = 'source_board') %>%
		filter(., created_dt >= as_date('2023-01-01')) %>%
		ggplot() + geom_line(aes(x  = created_dt, y = z, color = source_board))

	input_data %>%
		group_by(., created_dt, label_key, label_value) %>%
		summarize(., count = n(), .groups = 'drop') %>%
		mutate(., key_count = sum(count), .by = c('created_dt', 'label_key'))

	wide_values =
		input_data %>%
		pivot_wider(
			.,
			id_cols = c('post_id', 'created_dt', 'source_board'),
			names_from = 'label_key',
			values_from = 'label_value'
		)

	is_res =
		wide_values %>%
		filter(., source_board == 'jobs') %>%
		mutate(
			.,
			is_unemp = ifelse(employment_status == 'unemployed', 1, 0),
			is_emp = ifelse(employment_status == 'employed', 1, 0),
			is_pos = ifelse(financial_sentiment == 'strong', 1, 0),
			is_neg = ifelse(financial_sentiment == 'weak', 1, 0),
			is_neutral = ifelse(financial_sentiment == 'neutral', 1, 0),
			is_fired = ifelse(!is.na(unemployment_reason) & unemployment_reason == 'fired_or_laid_off', 1, 0),
			is_resign = ifelse(!is.na(unemployment_reason) & unemployment_reason == 'resigned', 1, 0),
			is_unsat = ifelse(!is.na(employment_satisfaction) & employment_satisfaction == 'unsatisfied', 1, 0),
			is_sat = ifelse(!is.na(employment_satisfaction) & employment_satisfaction == 'satisfied', 1, 0)
		) %>%
		group_by(., created_dt, source_board) %>%
		summarize(., across(starts_with('is_'), sum), count = n(), .groups = 'drop') %>%
		arrange(., created_dt)

	smoothed =
		is_res %>%
		mutate(across(c(starts_with('is_'), count), function(x)
			zoo::rollapply(
				x, width = 180, FUN = \(x) sum(.98^((length(x) - 1):0) * x),
				fill = NA, align = 'right'
			)
		)) %>%
		mutate(
			.,
			# unemp_ratio = is_unemp/(is_emp),
			pos_ratio = (is_pos )/(is_neg),
			unsat_ratio =  (is_unsat)/(is_emp),
			# res_ratio = is_resign/is_emp,
			) %>%
		select(., created_dt, contains('_ratio'))	%>%
		pivot_longer(., -c(created_dt), names_to = 'index', values_to = 'value', values_drop_na = T) %>%
		transmute(., date = created_dt, index, value)

	smoothed %>%
		ggplot() +
		geom_line(aes(x = date, y = value, color = index, group = index))


		mutate(., score = case_when(
			recently_seperated %in% c('fired/laid off') & job_search_status != 'received offer/started new job' ~ -2,
			recently_received_pay_increase == 'yes - significant off' ~ 2,
			job_search_status == 'received offer/started new job ' ~ 2,
			recently_seperated %in% c('resigned') ~ 2,
			employment_status %in% c('employed') ~ .5,
			employment_status %in% c('unemployed') ~ -.5,
			TRUE ~ 0
		)) %>%
		group_by(., created_dt) %>%
		summarize(., score = sum(score), count = n()) %>%
		arrange(., created_dt) %>%
		# Cast into rolling sums
		mutate(across(c(count, score), function(col) {
			zoo::rollapply(col, width = 180, FUN = \(x) sum(.95^(179:0) * x), fill = NA, align = 'right')
		})) %>%
		mutate(., score_weight = score/count) %>%
		ggplot() +
		geom_line(aes(x = created_dt, y = score/count))

})

## Regex Scores --------------------------------------------------------
local({

		samples = get_query(pg, str_glue(
			"WITH t0 AS (
				SELECT
					a.scrape_id, a.post_id,
					a.source_board, a.scrape_method,
					a.title, a.selftext, a.ups,
					DATE(a.created_dttm) AS date,
					ARRAY_LENGTH(REGEXP_SPLIT_TO_ARRAY(TRIM(a.selftext), '\\s+'), 1) * 1.5 + 100 AS n_tokens,
					ROW_NUMBER() OVER (PARTITION BY a.post_id ORDER BY a.created_dttm DESC) AS rn
				FROM text_scraper_reddit_scrapes a
				WHERE
					a.source_board IN ('jobs', 'careerguidance', 'personalfinance')
					AND a.selftext IS NOT NULL
					-- 10 - 1000 words
					AND ARRAY_LENGTH(REGEXP_SPLIT_TO_ARRAY(TRIM(a.selftext), '\\s+'), 1) BETWEEN 10 AND 1000
					AND (
						(DATE(a.created_dttm) BETWEEN '2016-01-01' AND '2022-12-31' AND scrape_method = 'pushshift_backfill')
						OR (DATE(a.created_dttm) BETWEEN '2023-01-01' AND '2023-08-20' AND scrape_method = 'pullpush_backfill')
						-- Allow low-upvoted posts from top 1k to be included to avoid skew of negative posts being most popular
						OR (
							DATE(a.created_dttm) BETWEEN '2023-01-01' AND '2023-11-01'
							AND scrape_method = 'top_1000_year'
							AND ups <= 100
						)
						OR (
							DATE(a.created_dttm) > '2023-08-20'
							AND scrape_method IN ('top_1000_today', 'top_1000_week', 'top_1000_month')
						)
					)
			)
			SELECT * FROM t0 WHERE rn = 1 ORDER BY random()"
		))

		samples %>% count(., date) %>% ggplot + geom_line(aes(x = date, y = n))

		raw_counts =
			samples %>%
			# filter(., source_board == 'personalfinance') %>%
			mutate(
				.,
				text = paste0('TITLE: ', title, '\nPOST: ', str_replace_all(selftext, "\\t|\\n", " ")),
				is_layoff = ifelse(str_detect(text, 'layoff|laid off|fired|unemployed|lost( my|) job|laying off'), 1, 0),
				is_resign = ifelse(str_detect(text, 'quit|resign|weeks notice|(leave|leaving)( a| my|)( job)'), 1, 0),
				is_new_job = ifelse(str_detect(text, 'hired|new job|background check|job offer|(found|got|landed|accepted|starting)( the| a|)( new|)( job| offer)'), 1, 0),
				is_searching = ifelse(str_detect(text, 'job search|application|apply|reject|interview|hunt|resume'), 1, 0),
				is_inflation = ifelse(str_detect(text, 'inflation|cost-of-living|cost of living|expensive'), 1, 0),
				is_struggling = ifelse(str_detect(text, 'struggling|struggle|unemployed|fired|sad|angry|poor|poverty'), 1, 0),
				is_new_job_or_resign = ifelse(is_resign == 1 | is_new_job == 1, 1, 0),
				# is_debt = ifelse(str_detect(text, 'debt|credit card debt|loan'), 1, 0),
				# is_housing = ifelse(str_detect(text, 'housing|home|rent'), 1, 0)
				) %>%
			group_by(., date) %>%
			summarize(
				.,
				across(starts_with('is_'), sum),
				count = n(),
				.groups = 'drop'
				) %>%
			arrange(., date)

		smoothed_ratios =
			raw_counts %>%
			mutate(across(c(starts_with('is_'), count), function(x)
				zoo::rollapply(
					x, width = 4 * 7,
					FUN = function(x) sum(.985^((length(x) - 1):0) * x),
					fill = NA, align = 'right'
					)
				)) %>%
			pivot_longer(., -c(date, count), names_to = 'varname', values_to = 'value', values_drop_na = T) %>%
			mutate(., value = value/count) %>%
			transmute(., date, varname, value)

		smoothed_ratios %>%
			ggplot() +
			geom_line(aes(x = date, y = value, color = varname, group = varname))

		# Create total sentiment index
		employment_index =
			smoothed_ratios %>%
			pivot_wider(., names_from = 'varname', values_from = 'value') %>%
			mutate(., value = is_new_job_or_resign - is_layoff) %>% # domain is [-1, 1]
			# Scale to ~(0, 1)
			mutate(., value =
			 	(value - mean(filter(., date >= '2016-01-01' & date <= '2016-12-31')$value))/
				mean(filter(., date >= '2016-01-31' & date <= '2016-12-31')$value) * 100 + 100
			) %>%
			transmute(., date, vdate = date + days(1), value, varname = 'employment_index')

		employment_index %>%
			ggplot() +
			geom_line(aes(x = date, y = value)) +
			geom_hline(yintercept = 100)

		# sigmoid = function(x, L, k, x0) {
		# 	L/(1 + exp(-1 * k * (x - x0)))
		# }

		regex_subindices <<- bind_rows(total_sent_index)
})

## Create Indices ----------------------------------------------------------
local({

	total_sent_index =
		regex_smoothed %>%
		pivot_wider(., names_from = 'index', values_from = 'value') %>%
		mutate(., value = is_layoff - is_new_job - is_resign) %>%
		transmute(., date, vdate = date + days(1), value, varname = 'employment_index') %>%
		# Filter so that 2018-2019 is mean 0
		# and 2019-2023 is mean 0
		mutate(., value = (value + .25) * 3)

	total_sent_index %>%
		ggplot() +
		geom_line(aes(x = date, y = value))


	total_sent_index

	benchmarks

	# bind_rows(
	# 	regex_smoothed,
	# 	transmute(benchmarks, date, index = varname, value)
	# ) %>%
	# 	filter(., date >= as_date('2018-01-01')) %>%
	# 	filter(., !index %in% c('kclf', 'is_searching',  'sp500', 'sent', 'is_inflation')) %>%
	# 	mutate(., value = ifelse(index %in% c('unemp', 'ccs', 'ics'), log(value), value)) %>%
	# 	group_split(., index) %>%
	# 	map(., function(df) {
	# 		df %>%
	# 			mutate(., value = scale(.$value))
	# 		# mutate(., baseline = head(filter(., date > '2020-01-01'), 1)$value)
	# 	}) %>%
	# 	list_rbind() %>%
	# 	mutate(., change = value) %>%
	# 	ggplot() +
	# 	geom_line(aes(x = date, y = change, color = index, group = index), linewidth = .8)


})

# Construct Top-Level Indices ----------------------------------------------------

## Predictive Modeling ----------------------------------------------------
local({

	# Benchmarks:
	# 1. unemp_t(v=v*) = f(unemp_{t-1m})
	# 2. unemp_t(v=v*) = f(unemp_{t-1m}, ics_{t-1w}, ics_{t-2w}, ics_{t-3w}, ics_{t-4w})

	# Compare
	# 1. unemp_t(v=v*) = f(unemp_{t-1m}, ics_{t-1w}, ics_{t-2w}, ics_{t-3w}, ics_{t-4w}, employment_index_{t - 1d})


	fdate = '2023-11-01'

	unemp_final_values =
		filter(benchmarks, varname == 'unemp') %>%
		slice_max(., order_by = vdate, n = 1, by = date) %>%
		mutate(., value = diff(value, 1)) %>%
		select(., date, value)

	train_fdates = tibble(
		fdate = seq(from = as_date('2018-01-01'), to = as_date('2023-10-01'), by = '1 month'),
		fc0date = fdate, # Nowcast
		fc1date = add_with_rollback(fc0date, months(1)) # T+1 forecast
		) %>%
		left_join(., transmute(unemp_final_values, fc0date = date, unemp.fc0 = value), by = 'fc0date') %>%
		left_join(., transmute(unemp_final_values, fc1date = date, unemp.fc1 = value), by = 'fc1date')

	# available_data =
	# 	bind_rows(benchmarks, regex_subindices) %>%
	# 	filter(., vdate <= as_date(fdate)) %>%
	# 	group_by(., date, varname) %>%
	# 	slice_max(., order_by = vdate, n = 1) %>%
	# 	ungroup(.) %>%
	# 	pivot_wider(., id_cols = date, names_from = varname, values_from = value)

	# Unemp lags refer to the months before the actual date
	#### TBD: Test alternative specifications of employer sentiment
	employment_index =
		smoothed_ratios %>%
		pivot_wider(., names_from = 'varname', values_from = 'value') %>%
		# mutate(., value = is_layoff) %>%
		mutate(., value = is_new_job_or_resign - is_layoff) %>% # domain is [-1, 1]
		# mutate(., value = is_layoff) %>%
		# Scale to ~(0, 1)
		mutate(., value =
					 	(value - mean(filter(., date >= '2018-06-01' & date <= '2018-12-31')$value))/
					 	mean(filter(., date >= '2020-01-01' & date <= '2020-06-01')$value) * 100
		) %>%
		transmute(., date, vdate = date + days(1), value, varname = 'employment_index')

	employment_index %>%
		ggplot() +
		geom_line(aes(x = date, y = value))
	####
	available_data_by_fdate_varname =
		train_fdates %>%
		select(., fdate) %>%
		cross_join(., bind_rows(benchmarks, employment_index)) %>%
		filter(., vdate <= fdate & fdate - years(1) <= vdate) %>%
		slice_max(., order_by = vdate, n = 1, by = c(fdate, date, varname)) %>% # Get latest vdate for each date
		split(., f = as.factor(.$varname)) %>%
		map(., \(x) arrange(select(x, -varname), date))

	# diff(unemp_t)
	data_by_fdate_varname = list()

	data_by_fdate_varname$unemp_d =
		available_data_by_fdate_varname$unemp %>%
		mutate(., lag_index = interval(date, fdate) %/% months(1)) %>%
		filter(., lag_index <= 6) %>%
		mutate(., value = diff(value, 1), .by = fdate) %>%
		na.omit(.) %>%
		pivot_wider(., id_cols = fdate, names_from = lag_index, values_from = value, names_prefix = 'unemp_d.l', names_sort = T)

	# diff(ics_t)
	data_by_fdate_varname$ics_d =
		available_data_by_fdate_varname$ics %>%
		# Order lag index by # before occurance
		mutate(., lag_index = n():1, .by = fdate) %>%
		mutate(., value = diff(log(value), 1), .by = fdate) %>%
		filter(., lag_index <= 8) %>%
		pivot_wider(., id_cols = fdate, names_from = lag_index, values_from = value, names_prefix = 'ics_d.l', names_sort = T)  %>%
		mutate(., sum_ics.l1 = ics_d.l1 + ics_d.l2 + ics_d.l3)

	data_by_fdate_varname$ics_b =
		available_data_by_fdate_varname$ics %>%
		mutate(., lag_index = n():1, .by = fdate) %>%
		filter(., lag_index <= 8) %>%
		pivot_wider(., id_cols = fdate, names_from = lag_index, values_from = value, names_prefix = 'ics_b.l', names_sort = T)

	data_by_fdate_varname$empsent_b =
		available_data_by_fdate_varname$employment_index %>%
		mutate(., lag_index = n():1, .by = fdate) %>%
		# mutate(., value = diff(value, 7), .by = fdate) %>%
		filter(., lag_index <= 14) %>%
		pivot_wider(., id_cols = fdate, names_from = lag_index, values_from = value, names_prefix = 'empsent_b.l', names_sort = T)

	data_by_fdate_varname$empsent_d =
		available_data_by_fdate_varname$employment_index %>%
		mutate(., lag_index = n():1, .by = fdate) %>%
		mutate(., value = diff(value, 7), .by = fdate) %>%
		filter(., lag_index <= 14) %>%
		pivot_wider(., id_cols = fdate, names_from = lag_index, values_from = value, names_prefix = 'empsent_d.l', names_sort = T)

	c(list(train_fdates), data_by_fdate_varname) %>%
		reduce(., \(a, b) left_join(a, b, by = 'fdate')) %>%
		lm(unemp.fc1 ~ ics_d.l1 + ics_d.l2 + ics_d.l3 + empsent_d.l1 - 1, data = .) %>%
		summary(.)

	## Alternate: predict CLAIMS using empsent

	fdate = '2023-11-01'

	ics_final_values =
		filter(benchmarks, varname == 'ics') %>%
		slice_min(., order_by = vdate, n = 1, by = date) %>%
		mutate(., ics = dlog(value)) %>%
		select(., date, vdate, ics)

	train_fdates = tibble(
		fdate = seq(from = as_date('2017-12-31'), to = as_date('2023-10-01'), by = '1 week')
		) %>%
		cross_join(., ics_final_values) %>%
		filter(., vdate > fdate) %>%
		arrange(., fdate, date) %>%
		mutate(., lead_ix = 1:n(), datediff = as.numeric(interval(fdate, date), 'days'), .by = 'fdate') %>%
		filter(., datediff <= 14) %>%
		pivot_wider(
			.,
			id_cols = fdate, names_from = lead_ix, values_from = c(ics, datediff),
			names_glue = '{.value}.f{lead_ix}'
			)

	# available_data =
	# 	bind_rows(benchmarks, regex_subindices) %>%
	# 	filter(., vdate <= as_date(fdate)) %>%
	# 	group_by(., date, varname) %>%
	# 	slice_max(., order_by = vdate, n = 1) %>%
	# 	ungroup(.) %>%
	# 	pivot_wider(., id_cols = date, names_from = varname, values_from = value)

	# Unemp lags refer to the months before the actual date
	#### TBD: Test alternative specifications of employer sentiment
	smoothed_ratios =
		raw_counts %>%
		mutate(across(c(starts_with('is_'), count), function(x)
			zoo::rollapply(
				x, width = 4*8,
				FUN = function(x) sum(.9^((length(x) - 1):0) * x),
				fill = NA, align = 'right'
			)
		)) %>%
		pivot_longer(., -c(date, count), names_to = 'varname', values_to = 'value', values_drop_na = T) %>%
		mutate(., value = value/count) %>%
		transmute(., date, varname, value)

	smoothed_ratios %>%
		ggplot() +
		geom_line(aes(x = date, y = value, color = varname, group = varname))

	employment_index =
		smoothed_ratios %>%
		pivot_wider(., names_from = 'varname', values_from = 'value') %>%
		# mutate(., value = is_layoff) %>%
		mutate(., value = is_new_job_or_resign - is_layoff) %>% # domain is [-1, 1]
		# mutate(., value = is_layoff) %>%
		# Scale to ~(0, 1)
		mutate(., value =
					 	(value - mean(filter(., date >= '2018-06-01' & date <= '2018-12-31')$value))/
					 	mean(filter(., date >= '2020-01-01' & date <= '2020-06-01')$value) * 100
		) %>%
		transmute(., date, vdate = date + days(1), value, varname = 'employment_index')

	employment_index %>%
		ggplot() +
		geom_line(aes(x = date, y = value))
	####


	## Consider dropping COVID data jump
	available_data_by_fdate_varname =
		train_fdates %>%
		select(., fdate) %>%
		cross_join(., bind_rows(benchmarks, employment_index)) %>%
		filter(., vdate <= fdate & fdate - years(1) <= vdate) %>%
		slice_max(., order_by = vdate, n = 1, by = c(fdate, date, varname)) %>% # Get latest vdate for each date
		split(., f = as.factor(.$varname)) %>%
		map(., \(x) arrange(select(x, -varname), date))

	# diff(unemp_t)
	data_by_fdate_varname = list()

	# diff(ics_t)
	data_by_fdate_varname$ics_dlog =
		available_data_by_fdate_varname$ics %>%
		# Order lag index by # before occurance
		mutate(., lag_index = n():1, .by = fdate) %>%
		mutate(., value = dlog(value), .by = fdate) %>%
		filter(., lag_index <= 8) %>%
		pivot_wider(., id_cols = fdate, names_from = lag_index, values_from = value, names_prefix = 'ics_dlog.l', names_sort = T)

	data_by_fdate_varname$ics_b =
		available_data_by_fdate_varname$ics %>%
		mutate(., lag_index = n():1, .by = fdate) %>%
		filter(., lag_index <= 8) %>%
		pivot_wider(., id_cols = fdate, names_from = lag_index, values_from = value, names_prefix = 'ics_b.l', names_sort = T)

	data_by_fdate_varname$empsent_diff =
		available_data_by_fdate_varname$employment_index %>%
		mutate(., lag_index = n():1, .by = fdate) %>%
		mutate(., value = diff(value, 7), .by = fdate) %>%
		filter(., lag_index <= 14) %>%
		pivot_wider(., id_cols = fdate, names_from = lag_index, values_from = value, names_prefix = 'empsent_diff.l', names_sort = T)

	data_by_fdate_varname$empsent_diff1 =
		available_data_by_fdate_varname$employment_index %>%
		mutate(., lag_index = n():1, .by = fdate) %>%
		mutate(., value = diff(value, 1), .by = fdate) %>%
		filter(., lag_index <= 14) %>%
		pivot_wider(., id_cols = fdate, names_from = lag_index, values_from = value, names_prefix = 'empsent_diff1.l', names_sort = T)

	data_by_fdate_varname$empsent_diff30 =
		available_data_by_fdate_varname$employment_index %>%
		mutate(., lag_index = n():1, .by = fdate) %>%
		mutate(., value = diff(value, 30), .by = fdate) %>%
		filter(., lag_index <= 14) %>%
		pivot_wider(., id_cols = fdate, names_from = lag_index, values_from = value, names_prefix = 'empsent_diff30.l', names_sort = T)

	c(list(train_fdates), data_by_fdate_varname) %>%
		reduce(., \(a, b) left_join(a, b, by = 'fdate')) %>%
		# filter(., floor_date(fdate, 'month') != '2022-03-01') %>%
		lm(ics.f1 ~ ics_dlog.l1 + ics_dlog.l2 + empsent_diff.l1, data = .) %>%
		summary(.)

	c(list(train_fdates), data_by_fdate_varname) %>%
		reduce(., \(a, b) left_join(a, b, by = 'fdate')) %>%
		# filter(., floor_date(fdate, 'month') != '2022-03-01') %>%
		lm(ics.f2 ~ ics_dlog.l1 + empsent_diff.l1, data = .) %>%
		summary(.)

	c(list(train_fdates), data_by_fdate_varname) %>%
		reduce(., \(a, b) left_join(a, b, by = 'fdate')) %>%
		# filter(., floor_date(fdate, 'month') != '2022-03-01') %>%
		lm(ics.f3 ~ ics_dlog.l1 + empsent_diff.l1, data = .) %>%
		summary(.)

})
