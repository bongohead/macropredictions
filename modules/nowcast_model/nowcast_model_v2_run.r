#'  Run this script on scheduler after close of business each day

# Initialize ----------------------------------------------------------
BACKTEST_MONTHS = 12
IMPORT_DATE_START = '2007-01-01'  # spdw, usd, metals, moo start in Q1-Q2 2007, can start at 2008-01-01
STORE_NEW_ONLY = T # Generally T; only F if there's a reason to believe that past vdates are incorrect

validation_log <<- list() # Logging

## Load Libs ----------------------------------------------------------'
library(macropredictions)
library(tidyverse)
library(httr2)
load_env()

## Load Connection Info ----------------------------------------------------------
pg = connect_pg()

releases <- list()
hist <- list()
model <- list()
models <- list()

## Load Variable Defs ----------------------------------------------------------
variable_params = get_query(pg, 'SELECT * FROM nowcast_model_variables')
release_params = get_query(pg, 'SELECT * FROM nowcast_model_input_releases')

## Set Backtest Dates  ----------------------------------------------------------
local({

	contiguous = seq(
		from = today('US/Eastern') %m-% months(BACKTEST_MONTHS),
		to = today('US/Eastern'),
		by = '1 day'
	)

	backtest_vdates <<- contiguous
})

# Release Data ----------------------------------------------------------

## 1. Get Data Releases ----------------------------------------------------------
local({

	message(str_glue('*** Getting Releases History | {format(now(), "%H:%M")}'))
	api_key = Sys.getenv('FRED_API_KEY')

	pull_data = df_to_list(
		filter(release_params, id %in% filter(variable_params, nc_dfm_input == 1)$release & source == 'fred')
		)

	requests = map(pull_data, \(x) request(str_glue(
		'https://api.stlouisfed.org/fred/release/dates?',
		'release_id={x$source_key}&realtime_start=2010-01-01',
		'&include_release_dates_with_no_data=true&api_key={api_key}&file_type=json'
	)))

	responses = send_async_requests(requests)

	fred_releases = list_rbind(imap(responses, function(resp, i) {
		tibble(
			release = pull_data[[i]]$id,
			date = as_date(map_chr(resp_body_json(resp)$release_dates, \(x) x$date))
		)
	}))

	releases$raw$fred <<- fred_releases
})

## 2. Combine & Store ----------------------------------------------------------
local({

	releases_all = bind_rows(releases$raw)

	sql_result = write_df_to_sql(
		pg,
		releases_all,
		'nowcast_model_input_release_dates',
		'ON CONFLICT (release, date) DO NOTHING'
		)
	message('***** Rows Modified: ', sql_result)

	releases$all <<- releases_all
})

# Historical Data ----------------------------------------------------------

## 1. FRED ----------------------------------------------------------
local({

	message('*** Importing FRED Data')
	api_key = Sys.getenv('FRED_API_KEY')

	pull_data = keep(df_to_list(variable_params), \(x) x$hist_source == 'fred')

	fred_data =
		# pull_data %>%
		# map(., \(x) c(x$hist_source_key, x$hist_source_freq)) %>%
		# get_fred_obs_with_vintage(., api_key, .obs_start = IMPORT_DATE_START, .verbose = F) %>%
		list_rbind(imap(pull_data, function(x, i) {
			message(str_glue('**** Pull {i}: {x$varname} {x$hist_source_key}'))
			res =
				get_fred_obs_with_vintage(list(c(x$hist_source_key, x$hist_source_freq)), api_key) %>%
				transmute(., varname = x$varname, freq = x$hist_source_freq, date, vdate = vintage_date, value) %>%
				filter(., date >= as_date(IMPORT_DATE_START), vdate >= as_date(IMPORT_DATE_START))
			message(str_glue('**** Count: {nrow(res)}'))
			return(res)
		})) %>%
		inner_join(
			.,
			transmute(variable_params, varname, series_id = hist_source_key),
			by = 'series_id',
			relationship = 'many-to-one'
		) %>%
		select(., -series_id) %>%
		rename(., vdate = vintage_date) %>%
		filter(., date >= as_date(IMPORT_DATE_START), vdate >= as_date(IMPORT_DATE_START))

	hist$raw$fred <<- fred_data
})

## 2. Yahoo Finance ----------------------------------------------------------
local({

	message(str_glue('*** Importing Yahoo Finance Data | {format(now(), "%H:%M")}'))

	pull_data = keep(df_to_list(variable_params), \(x) x$hist_source == 'yahoo')

	yahoo_data = list_rbind(map(pull_data, function(x) {
		get_yahoo_data(x$hist_source_key, .obs_start = IMPORT_DATE_START) %>%
			transmute(
				.,
				varname = x$varname,
				freq = x$hist_source_freq,
				date,
				vdate = date,
				value
			)
		}))

	hist$raw$yahoo <<- yahoo_data
})

## 3. Calculated Variables ----------------------------------------------------------
local({

	message('*** Adding Calculated Variables')

	# For each vintage date, pull all CPI values from the latest available vintage for each date
	# This intelligently handles revisions and prevents
	# duplications of CPI values by vintage dates/obs date
	cpi_df =
		hist$raw$fred %>%
		filter(., varname == 'cpi' & freq == 'm') %>%
		select(., date, vdate, value)

	cpi_data =
		cpi_df %>%
		group_split(., vdate) %>%
		map(., function(x)
			filter(cpi_df, vdate <= x$vdate[[1]]) %>%
				group_by(., date) %>%
				filter(., vdate == max(vdate)) %>%
				ungroup(.) %>%
				arrange(., date) %>%
				transmute(date, vdate = roll::roll_max(vdate, 13), value = 100 * (value/lag(value, 12) - 1))
		) %>%
		list_rbind %>%
		na.omit %>%
		distinct(., date, vdate, value) %>%
		transmute(
			.,
			varname = 'cpi',
			freq = 'm',
			date,
			vdate,
			value
		)

	pcepi_df =
		hist$raw$fred %>%
		filter(., varname == 'pcepi' & freq == 'm') %>%
		select(., date, vdate, value)

	pcepi_data =
		pcepi_df %>%
		group_split(., vdate) %>%
		map(., function(x)
			filter(pcepi_df, vdate <= x$vdate[[1]]) %>%
				group_by(., date) %>%
				filter(., vdate == max(vdate)) %>%
				ungroup(.) %>%
				arrange(., date) %>%
				transmute(date, vdate = roll::roll_max(vdate, 13), value = 100 * (value/lag(value, 12) - 1))
		) %>%
		list_rbind %>%
		na.omit %>%
		distinct(., date, vdate, value) %>%
		transmute(
			.,
			varname = 'pcepi',
			freq = 'm',
			date,
			vdate,
			value
		)

	hist_calc = bind_rows(cpi_data, pcepi_data)

	hist$raw$calc <<- hist_calc
})

## 4. Aggregate Frequencies ----------------------------------------------------------
local({

	message(str_glue('*** Aggregating Monthly & Quarterly Data | {format(now(), "%H:%M")}'))

	hist_agg_0 =
		hist$raw$calc %>%
		bind_rows(
			.,
			filter(hist$raw$fred, !varname %in% unique(.$varname)),
			filter(hist$raw$yahoo, !varname %in% unique(.$varname)),
			)

	# # 3/17/24 - Migrated to match get_historical_data.r
	# # See get_historical_data.r for a similar version
	# # (but doesn't return values for each vdate, only ones with updates)
	#
	# # For each backtest vdate, get monthly-aggregated data (using up to 12 months of history)
	# # 1. Get unique combinations of varname x hist dates
	# # 2. Combine to get each backtest vdate x varname x hist date
	# # 3. For each backtest vdate x varname x hist release date, get the most recent data release of that varname
	# # for that hist date (this handles revisions of older daily/weekly data)
	# # 4. Then aggregate to monthly
	# dw_data_to_agg = filter(hist_agg_0, freq %in% c('d', 'w'))
	# dw_data_uniq_varname_dates = distinct(dw_data_to_agg, varname, hist_date = date)
	#
	# monthly_agg2 =
	# 	expand_grid(vdate = backtest_vdates, dw_data_uniq_varname_dates) %>%
	# 	inner_join(
	# 		dw_data_to_agg %>% transmute(., varname, value, hist_date = date, hist_vdate = vdate),
	# 		join_by(varname, hist_date, closest(vdate >= hist_vdate))
	# 	) %>%
	# 	# Step 4
	# 	group_by(., vdate, varname, date = floor_date(hist_date, 'month')) %>%
	# 	summarize(., value = mean(value), .groups = 'drop') %>%
	# 	transmute(., varname, freq = 'm', date, vdate, value)
	#
	# # For quarterly data, same process but aggregate iff all 3 monthly data points are available
	# m_data_to_agg = bind_rows(filter(hist_agg_0, freq == 'm'), monthly_agg)
	# m_data_uniq_varname_dates = distinct(m_data_to_agg, varname, hist_date = date)
	#
	# quarterly_agg =
	# 	expand_grid(vdate = backtest_vdates, m_data_uniq_varname_dates) %>%
	# 	inner_join(
	# 		.,
	# 		m_data_to_agg %>% transmute(., varname, value, hist_date = date, hist_vdate = vdate),
	# 		join_by(varname, hist_date, closest(vdate >= hist_vdate))
	# 	) %>%
	# 	group_by(., vdate, varname, date = floor_date(hist_date, 'quarter')) %>%
	# 	summarize(., value = mean(value), n = n(), .groups = 'drop') %>%
	# 	filter(., n == 3) %>%
	# 	transmute(., varname, freq = 'm', date, vdate, value)

	monthly_inputs =
		hist_agg_0 %>%
		filter(., freq %in% c('d', 'w')) %>%
		mutate(., target_month = floor_date(date, 'month'))

	# Get all dates where any of the subcomponents are updated
	monthly_component_dates = distinct(monthly_inputs, varname, freq, vdate, target_month)

	monthly_agg =
		# For each data update days, join all component updates available by the data update day
		monthly_component_dates %>%
		left_join(
			.,
			monthly_inputs %>%
				transmute(., varname, target_month, component_date = date, component_vdate = vdate, component_value = value),
			join_by(varname, target_month, vdate >= component_vdate)
		) %>%
		# Now to account for revisions, get most recent vdate only for each component_date
		group_by(., varname, target_month, vdate, component_date) %>%
		filter(., component_vdate == max(component_vdate)) %>%
		ungroup(.) %>%
		# Now for each vdate, get mean of existing values within each month
		group_by(., varname, target_month, vdate) %>%
		summarize(., value = mean(component_value, na.rm = T), count = n(), .groups = 'drop') %>%
		transmute(., varname, freq = 'm', date = target_month, vdate, value)

	# Works similarly as monthly aggregation but does not create new quarterly data unless all
	# 3 monthly data points are available (this can still result in premature quarterly calcs if the last
	# month value has been agrgegated before that last month was finished)
	quarterly_inputs =
		monthly_agg %>%
		bind_rows(., filter(hist_agg_0, freq == 'm')) %>%
		mutate(., target_quarter = floor_date(date, 'quarter'))

	# Get all dates where any of the subcomponents are updated
	quarterly_component_dates = distinct(quarterly_inputs, varname, freq, vdate, target_quarter)

	quarterly_agg =
		# For each data update days, join all component updates available by the data update day
		quarterly_component_dates %>%
		left_join(
			.,
			quarterly_inputs %>%
				transmute(., varname, target_quarter, component_date = date, component_vdate = vdate, component_value = value),
			join_by(varname, target_quarter, vdate >= component_vdate)
		) %>%
		# Now to account for revisions, get most recent vdate only for each component_date
		group_by(., varname, target_quarter, vdate, component_date) %>%
		filter(., component_vdate == max(component_vdate)) %>%
		ungroup(.) %>%
		# Now for each vdate, get mean of existing values within each month
		group_by(., varname, target_quarter, vdate) %>%
		summarize(., value = mean(component_value, na.rm = T), count = n(), .groups = 'drop') %>%
		filter(., count == 3) %>%
		transmute(., varname, freq = 'q', date = target_quarter, vdate, value)

	hist_agg =
		bind_rows(hist_agg_0, monthly_agg, quarterly_agg) %>%
		filter(., freq %in% c('m', 'q')) %>%
		# Throw out data earlier than 24 years before the vdate
		filter(., date >= vdate - months(240))

	# Check min/max dates
	# The latest min date for PCA inputs should be no earlier than the last PCA input variable (3/17/24 - what?)
	message('***** Variables Dates:')
	hist_agg %>%
		group_by(., varname) %>%
		summarize(., min_dt = min(date), max_dt = max(date)) %>%
		arrange(., desc(min_dt)) %>%
		print(., n = 100)

	hist$agg <<- hist_agg
})

## 5. Add Stationary Transformations ----------------------------------------------------------
local({

	message(str_glue('*** Adding Stationary Transforms | {format(now(), "%H:%M")}'))

	# Synced with `get_historical_data.r`
	# Get each target date in the hist dataset, get vdates where its value or its LAGGED value was revised
	# dates = date x vdate x varname x freq

	# Get all unique combinations of date-couplets (a date and its lag), and vdates where either the date
	# and its lag was updated, but where the date itself was available
	date_couplets =
		hist$agg %>%
		distinct(., varname, freq, date_0 = date) %>%
		mutate(., date_l = date_0 %m+% months(ifelse(freq == 'm', -1, -3))) %>%
		mutate(., t_date_0 = date_0, t_date_l = date_l) %>%
		pivot_longer(., cols = c(t_date_0, t_date_l), names_to = 'source', values_to = 'date') %>%
		# Join all update dates onto target date couplets
		inner_join(., hist$agg, by = c('varname', 'freq', 'date'), relationship = 'many-to-many') %>%
		# Condense down into unique update dates for each couplet!
		distinct(., varname, freq, date_0, date_l, vdate)

	# Now for each date_0 x date_l x vdate, get the latest available values at those dates, then use it to calculate transforms
	date_couplet_values =
		date_couplets %>%
		left_join(
			.,
			hist$agg %>% transmute(., varname, freq, date_0 = date, vdate_0 = vdate, value_0 = value),
			join_by(varname, freq, date_0, closest(vdate >= vdate_0))
		) %>%
		left_join(
			.,
			hist$agg %>% transmute(., varname, freq, date_l = date, vdate_l = vdate, value_l = value),
			join_by(varname, freq, date_l, closest(vdate >= vdate_l))
		) %>%
		filter(., !is.na(value_0))

	# Now for each date_0 x date_l x vdate, get the latest available values at those dates, then use it to calculate transforms
	hist_flat =
		variable_params %>%
		transmute(., varname, base = 'base', st, d1, d2) %>%
		pivot_longer(., cols = c(base, st, d1, d2), names_to = 'form', values_to = 'transform') %>%
		filter(., transform != 'none') %>%
		left_join(., date_couplet_values, by = 'varname', relationship = 'many-to-many') %>%
		group_split(., transform) %>%
		map(., function(x) {
			if (x$transform[[1]] == 'base') x %>% mutate(., value = value_0)
			else if (x$transform[[1]] == 'log') x %>% mutate(., value = log(value_0))
			else if (x$transform[[1]] == 'dlog') x %>% mutate(., value = log(value_0/value_l))
			else if (x$transform[[1]] == 'diff1') x %>% mutate(., value = value_0 - value_l)
			else if (x$transform[[1]] == 'pchg') x %>% mutate(., value = (value_0/value_l - 1) * 100)
			else if (x$transform[[1]] == 'apchg') x %>% mutate(., value = ((value_0/value_l)^ifelse(freq == 'q', 4, 12) - 1) * 100)
			else stop('Error')
		}) %>%
		list_rbind() %>%
		# Remove NAs - should only be dates at the very start
		filter(., !is.na(value)) %>%
		transmute(
			.,
			varname,
			form,
			freq,
			vdate = as_date(ifelse(vdate_0 >= vdate_l | is.na(vdate_l), vdate_0, vdate_l)),
			date = date_0,
			value
		)

	hist$flat <<- hist_flat
})

## 6. Create Monthly/Quarterly Matrices ----------------------------------------------------------
# local({
#
# 	message(str_glue('*** Creating Wide Matrices | {format(now(), "%H:%M")}'))
#
# 	wide =
# 		hist$flat %>%
# 		split(., by = 'freq', keep.by = F) %>%
# 		lapply(., function(x)
# 			split(x, by = 'form', keep.by = F) %>%
# 				lapply(., function(y)
# 					split(y, by = 'bdate', keep.by = T) %>%
# 					lapply(., function(z) {
# 						# message(z$bdate[[1]])
# 						dcast(select(z, -bdate), date ~ varname, value.var = 'value') %>%
# 							.[, date := as_date(date)] %>%
# 							.[order(date)] %>%
# 							as_tibble(.)
# 						})
# 				)
# 			)
#
# 	wide_last <<- lapply(wide, function(x) lapply(x, function(y) tail(y, 1)[[1]]))
#
# 	hist$wide <<- wide
# 	hist$wide_last <<- wide_last
# })

## 7. SQL ---------------------------------------------------------------------
# local({
#
# 	if (STORE_HIST) {
#
# 		message(str_glue('*** Sending Historical Data to SQL: {format(now(), "%H:%M")}'))
#
# 		initial_count = get_rowcount(pg, 'nowcast_model_input_values')
# 		message('***** Initial Count: ', initial_count)
#
# 		sql_result =
# 			hist$flat %>%
# 			unique(., by = c('vdate', 'form', 'freq', 'varname', 'date', 'value')) %>%
# 			select(., vdate, form, freq, varname, date, value) %>%
# 			as_tibble(.) %>%
# 			mutate(., split = ceiling((1:nrow(.))/10000)) %>%
# 			group_split(., split, .keep = FALSE) %>%
# 			sapply(., function(x)
# 				dbExecute(pg, create_insert_query(
# 					x,
# 					'nowcast_model_input_values',
# 					'ON CONFLICT (vdate, form, freq, varname, date) DO UPDATE SET value=EXCLUDED.value'
# 					))
# 			) %>%
# 			{if (any(is.null(.))) stop('SQL Error!') else sum(.)}
#
# 		final_count = get_rowcount(pg, 'nowcast_model_input_values')
# 		message('***** Rows Added: ', final_count - initial_count)
#
# 	}
# })

# State-Space Model -----------------------------------------------------------------

## 1. Dates -----------------------------------------------------------------
local({

	message(str_glue('*** Getting State-Space Sources | {format(now(), "%H:%M")}'))

	quarters_forward = 12
	pca_varnames = filter(variable_params, nc_dfm_input == T)$varname

	results = map(backtest_vdates, .progress = T, function(this_vdate) {

		# Get long df of all PCA variables
		pca_variables_0 =
			hist$flat %>%
			filter(., form == 'st' & freq == 'm' & varname %in% pca_varnames & vdate <= this_vdate) %>%
			group_by(., varname, date) %>%
			filter(., vdate == max(vdate)) %>%
			ungroup(.) %>%
			select(., -form, -freq, -vdate)

		# Calculate valid dates
		count_rles =
			pca_variables_0 %>%
			group_by(., date) %>%
			summarize(., count = n_distinct(varname)) %>%
			arrange(., date) %>%
			mutate(
				.,
				contains_all_varnames = count == length(pca_varnames),
				rle = consecutive_id(contains_all_varnames)
				) %>%
			group_by(., rle, contains_all_varnames) %>%
			summarize(., min_date = min(date), max_date = max(date), .groups = 'drop')

		# Throw error if unexpected NAs in the middle of the dataset
		if (nrow(filter(count_rles, contains_all_varnames == T)) > 1) {
			stop('Error: unexpected missing data')
		}

		t0 = count_rles %>% filter(., contains_all_varnames == T) %>% .$min_date
		big_t_date = count_rles %>% filter(., contains_all_varnames == T) %>% .$max_date
		big_tau_date = count_rles %>% filter(., rle == max(rle)) %>% .$max_date
		big_tstar_date = floor_date(big_tau_date, 'quarter') %m+% months(3 * (1 + quarters_forward) - 1)

		pca_variables_df =
			pca_variables_0 %>%
			filter(., date >= t0) %>%
			pivot_wider(., id_cols = date, names_from = varname, values_from = value) %>%
			arrange(., date)

		big_t_dates = seq(t0, big_t_date, by = '1 month')
		big_tau_dates = seq(t0, big_tau_date, by = '1 month')
		big_tstar_dates = seq(big_tau_date %m+% months(1), big_tstar_date, by = '1 month')

		big_t = length(big_t_dates)
		big_tau = big_t + length(big_tau_dates)
		big_tstar = big_tau + length(big_tstar_dates)

		time_df = tibble(
			date = as_date(c(pca_variables_df$date[[1]], big_t_date, big_tau_date, big_tstar_date)),
			time = c('1', 'T', 'Tau', 'T*')
			)

		list(
			vdate = this_vdate,
			pca_variables_df = pca_variables_df,
			big_t_dates = big_t_dates,
			big_tau_dates = big_tau_dates,
			big_tstar_dates = big_tstar_dates,
			big_t_date = big_t_date,
			big_tau_date = big_tau_date,
			big_tstar_date = big_tstar_date,
			big_t = big_t,
			big_tau = big_tau,
			big_tstar = big_tstar,
			time_df = time_df
			)
		})


	for (x in results) {
		models[[as.character(x$vdate)]] <<- x
	}
	model$quarters_forward <<- quarters_forward
	model$pca_varnames <<- pca_varnames
})


## 2. Extract PCA Factors -----------------------------------------------------------------
local({

	message(str_glue('*** Extracting PCA Factors | {format(now(), "%H:%M")}'))

	results = map(backtest_vdates, .progress = T, function(this_vdate) {

		m = models[[as.character(this_vdate)]]

		xDf = filter(m$pca_variables_df, date %in% m$big_t_dates)

		xMat = scale(as.matrix(select(xDf, -date)))

		lambdaHat = eigen(t(xMat) %*% xMat)$vectors
		fHat = (xMat) %*% lambdaHat
		bigN = ncol(xMat)
		bigT = nrow(xMat)
		bigCSquared = min(bigN, bigT)

		# Total variance of data
		totalVar = xMat %>% cov(.) %>% diag(.) %>% sum(.)

		# Calculate ICs from Bai and Ng (2002)
		# Total SSE should be approx 0 due to normalization above;
		# sapply(1:ncol(xMat), function(i)
		# 	sapply(1:nrow(xMat), function(t)
		# 		(xMat[i, 1] - matrix(lambdaHat[i, ], nrow = 1) %*% matrix(fHat[t, ], ncol = 1))^2
		# 		) %>% sum(.)
		# 	) %>%
		# 	sum(.) %>%
		# 	{./(ncol(xMat) %*% nrow(xMat))}
		(xMat - (fHat %*% t(lambdaHat)))^1

		# Now test by R
		mseByR =
			sapply(1:bigN, function(r)
				sum((xMat - (fHat[, 1:r, drop = FALSE] %*% t(lambdaHat)[1:r, , drop = FALSE]))^2)/(bigT * bigN)
			)


		# Explained variance of data
		screeDf =
			fHat %>% cov(.) %>% diag(.) %>%
			{lapply(1:length(.), function(i)
				tibble(
					factors = i,
					var_explained_by_factor = .[i],
					pct_of_total = .[i]/totalVar,
					cum_pct_of_total = sum(.[1:i])/totalVar
				)
			)} %>%
			bind_rows(.) %>%
			mutate(., mse = mseByR) %>%
			mutate(
				.,
				ic1 = (mse) + factors * (bigN + bigT)/(bigN * bigT) * log((bigN * bigT)/(bigN + bigT)),
				ic2 = (mse) + factors * (bigN + bigT)/(bigN * bigT) * log(bigCSquared),
				ic3 = (mse) + factors * (log(bigCSquared)/bigCSquared)
			)

		screePlot =
			screeDf %>%
			ggplot(.) +
			geom_col(aes(x = factors, y = cum_pct_of_total, fill = factors)) +
			# geom_col(aes(x = factors, y = pct_of_total)) +
			labs(
				title = 'Percent of Variance Explained',
				x = 'Factors (R)', y = 'Cumulative % of Total Variance Explained', fill = NULL
				)

		# 2 factors needed since 1 now represents COVID shock
		big_r = 1
			# screeDf %>%
			# filter(., ic1 == min(ic1)) %>%
			# .$factors #+ 2
			# ((
			# 	{screeDf %>% dplyr::filter(cum_pct_of_total >= .80) %>% head(., 1) %>%.$factors} +
			#   	{screeDf %>% dplyr::filter(., ic1 == min(ic1)) %>% .$factors}
			# 	)/2) %>%
			# round(., digits = 0)

		zDf =
			xDf[, 'date'] %>%
			bind_cols(., fHat[, 1:big_r] %>% as.data.frame(.) %>% setNames(., paste0('f', 1:big_r)))


		z_plots =
			imap(colnames(zDf) %>% .[. != 'date'], function(x, i)
				select(zDf, all_of(c('date', x))) %>%
					setNames(., c('date', 'value')) %>%
					ggplot() +
					geom_line(
						aes(x = date, y = value),
						color = hcl(h = seq(15, 375, length = big_r + 1), l = 65, c = 100)[i]
					) +
					labs(x = NULL, y = NULL, title = paste0('Estimated PCA Factor ', str_sub(x, -1), ' Plot')) +
					ggthemes::theme_fivethirtyeight() +
					scale_x_date(date_breaks = '1 year', date_labels = '%Y')
			)

		factor_weights_df =
			lambdaHat %>%
			as.data.frame %>%
			as_tibble %>%
			setNames(., paste0('f', str_pad(1:ncol(.), pad = '0', 1))) %>%
			bind_cols(weight = colnames(xMat), .) %>%
			pivot_longer(., -weight, names_to = 'varname') %>%
			group_by(varname) %>%
			arrange(., varname, desc(abs(value))) %>%
			mutate(., order = 1:n(), valFormat = paste0(weight, ' (', round(value, 2), ')')) %>%
			ungroup(.) %>%
			select(., -value, -weight) %>%
			pivot_wider(., names_from = varname, values_from = valFormat) %>%
			arrange(., order) %>%
			select(., -order) %>%
			select(., paste0('f', 1:big_r))

		list(
			vdate = this_vdate,
			factor_weights_df = factor_weights_df,
			scree_df = screeDf,
			scree_plot = screePlot,
			big_r = big_r,
			pca_input_df = xDf,
			z_df = zDf,
			z_plots = z_plots
		)
	})

	for (x in results) {
		models[[as.character(x$vdate)]]$factor_weights_df <<- x$factor_weights_df
		models[[as.character(x$vdate)]]$scree_df <<- x$scree_df
		models[[as.character(x$vdate)]]$scree_plot <<- x$scree_plot
		models[[as.character(x$vdate)]]$big_r <<- x$big_r
		models[[as.character(x$vdate)]]$pca_input_df <<- x$pca_input_df
		models[[as.character(x$vdate)]]$z_df <<- x$z_df
		models[[as.character(x$vdate)]]$z_plots <<- x$z_plots
	}
})

## 3. Run as VAR(1) -----------------------------------------------------------------
local({

	message('*** Running VAR(1)')

	results = map(backtest_vdates, function(this_vdate) {

		m = models[[as.character(this_vdate)]]

		input_df = na.omit(inner_join(m$z_df, add_lagged_columns(m$z_df, max_lag = 1), by = 'date'))

		y_mat = input_df %>% select(., -contains('.l'), -date) %>% as.matrix(.)
		x_df = input_df %>% select(., contains('.l')) %>% bind_cols(constant = 1, .)

		coef_df =
			lm(y_mat ~ . - 1, data = x_df) %>%
			coef(.) %>%
			as.data.frame(.) %>%
			rownames_to_column(., 'coefname') %>%
			as_tibble(.) %>%
			set_names(., c('coefname', paste0('f', 1:m$big_r)))

		gof_df =
			lm(y_mat ~ . - 1, x_df) %>%
			resid(.) %>%
			as.data.frame(.) %>%
			as_tibble(.) %>%
			pivot_longer(., everything(), names_to = 'varname') %>%
			group_by(., varname) %>%
			summarize(., MAE = mean(abs(value)), MSE = mean(value^2))

		resid_plot =
			lm(y_mat ~ . - 1, x_df) %>%
			resid(.) %>%
			as.data.frame(.) %>%
			as_tibble(.) %>%
			bind_cols(date = input_df$date, .) %>%
			pivot_longer(., -date) %>%
			ggplot(.) +
			geom_line(aes(x = date, y = value, group = name, color = name), linewidth = 1) +
			labs(title = 'Residuals plot for PCA factors', x = NULL, y = NULL, color = NULL)

		fitted_plots =
			lm(y_mat ~ . - 1, data = x_df) %>%
			fitted(.) %>%
			as_tibble(.) %>%
			bind_cols(date = input_df$date, ., type = 'Fitted Values') %>%
			bind_rows(., m$z_df %>% dplyr::mutate(., type = 'Data')) %>%
			pivot_longer(., -c('type', 'date')) %>%
			as.data.table(.) %>%
			split(., by = 'name') %>%
			imap(., function(x, i)
				ggplot(x) +
					geom_line(
						aes(x = date, y = value, group = type, linetype = type, color = type),
						linewidth = 1, alpha = 1.0
					) +
					labs(
						x = NULL, y = NULL, color = NULL, linetype = NULL,
						title = paste0('Fitted values vs actual for factor ', i)
					) +
					scale_x_date(date_breaks = '1 year', date_labels = '%Y') +
					ggthemes::theme_fivethirtyeight()
				)

			b_mat = coef_df %>% filter(., coefname != 'constant') %>% select(., -coefname) %>% t(.)
			c_mat = coef_df %>% filter(., coefname == 'constant') %>% select(., -coefname) %>% t(.)
			q_mat =
				lm(y_mat ~ . - 1, data = x_df) %>%
				residuals(.) %>%
				as_tibble(.) %>%
				purrr::transpose(.) %>%
				lapply(., function(x) as.numeric(x)^2 %>% diag(., nrow = length(.), ncol = length(.))) %>%
				{reduce(., function(x, y) x + y)/length(.)}

			list(
				vdate = this_vdate,
				var_fitted_plots = fitted_plots,
				var_resid_plots = resid_plot,
				var_gof_df = gof_df,
				var_coef_df = coef_df,
				q_mat = q_mat,
				b_mat = b_mat,
				c_mat = c_mat
			)
		})

	for (x in results) {
		models[[as.character(x$vdate)]]$var_fitted_plots <<- x$var_fitted_plots
		models[[as.character(x$vdate)]]$var_resid_plots <<- x$var_resid_plots
		models[[as.character(x$vdate)]]$var_gof_df <<- x$var_gof_df
		models[[as.character(x$vdate)]]$var_coef_df <<- x$var_coef_df
		models[[as.character(x$vdate)]]$q_mat <<- x$q_mat
		models[[as.character(x$vdate)]]$b_mat <<- x$b_mat
		models[[as.character(x$vdate)]]$c_mat <<- x$c_mat
	}
})

## 4. Run DFM on PCA Monthly Vars -----------------------------------------------------------------
local({

	message('*** Running DFM')

	results = map(backtest_vdates, .progress = T, function(this_vdate) {

		m = models[[as.character(this_vdate)]]

		y_mat = as.matrix(select(m$pca_input_df, -date))
		x_df = bind_cols(constant = 1, select(m$z_df, -date))

		coef_df =
			lm(y_mat ~ . - 1, x_df) %>%
			coef(.) %>%
			as.data.frame(.) %>%
			rownames_to_column(., 'coefname') %>%
			as_tibble(.)

		fitted_plots =
			lm(y_mat ~ . - 1, x_df) %>%
			fitted(.) %>%
			as_tibble(.) %>%
			bind_cols(date = m$z_df$date, ., type = 'Fitted Values') %>%
			bind_rows(., mutate(m$pca_input_df, type = 'Data')) %>%
			pivot_longer(., -c('type', 'date')) %>%
			as.data.table(.) %>%
			split(., by = 'name') %>%
			purrr::imap(., function(x, i)
				ggplot(x) +
					geom_line(
						aes(x = date, y = value, group = type, linetype = type, color = type),
						size = 1, alpha = 1.0
					) +
					labs(
						x = NULL, y = NULL, color = NULL, linetype = NULL,
						title = paste0('Fitted values vs actual for factor ', i)
					) +
					scale_x_date(date_breaks = '1 year', date_labels = '%Y')
				)

		gof_df =
			lm(y_mat ~ . - 1, x_df) %>%
			resid(.) %>%
			as.data.frame(.) %>%
			as_tibble(.) %>%
			pivot_longer(., everything(), names_to = 'varname') %>%
			group_by(., varname) %>%
			summarize(., MAE = mean(abs(value)), MSE = mean(value^2))

		a_mat = coef_df %>% filter(., coefname != 'constant') %>% select(., -coefname) %>% t(.)
		d_mat = coef_df %>% filter(., coefname == 'constant') %>% select(., -coefname) %>% t(.)

		r_mat_0 =
			lm(y_mat ~ . - 1, data = x_df) %>%
			residuals(.) %>%
			as_tibble(.) %>%
			purrr::transpose(.) %>%
			lapply(., function(x) as.numeric(x)^2 %>% diag(.)) %>%
			{purrr::reduce(., function(x, y) x + y)/length(.)}

		r_mat_diag = data.table(varname = model$pca_varnames, variance = diag(r_mat_0))

		r_mats =
			lapply(m$big_tau_dates, function(d) {
				d_hist = as.data.table(hist$wide$m$st[[as.character(this_bdate)]])[date == d]
				sapply(model$pca_varnames, function(v)
					d_hist[[v]] %>% {if (is.na(.)) 1e20 else (r_mat_diag[varname == v])$variance}
					) %>%
					diag(.)
			}) %>%
			c(lapply(1:length(m$big_t_dates), function(x) r_mat_0), .) %>%
			# As of 2/19/22: Only store diagonal r_mat elements!
			# Call diag() to restructure them as diag matrices
			lapply(., function(x) diag(x))

		list(
			vdate = this_vdate,
			dfm_gof_df = gof_df,
			dfm_coef_df = coef_df,
			dfm_fitted_plots = fitted_plots,
			r_mats = r_mats,
			a_mat = a_mat,
			d_mat = d_mat
		)
	})

	for (x in results) {
		models[[as.character(x$bdate)]]$dfm_gof_df <<- x$dfm_gof_df
		models[[as.character(x$bdate)]]$dfm_coef_df <<- x$dfm_coef_df
		models[[as.character(x$bdate)]]$dfm_fitted_plots <<- x$dfm_fitted_plots
		models[[as.character(x$bdate)]]$r_mats <<- x$r_mats
		models[[as.character(x$bdate)]]$a_mat <<- x$a_mat
		models[[as.character(x$bdate)]]$d_mat <<- x$d_mat
	}
})

## 5. Kalman Filter on State Space Obs -----------------------------------------------------------------
local({

	message('*** Running Kalman Filter')
	gc()

	results = lapply(backtest_vdates, function(this_bdate) {

		# message(str_glue('***** Running KF {this_bdate}'))
		m = models[[as.character(this_bdate)]]

		b_mat = m$b_mat
		c_mat = m$c_mat
		a_mat = m$a_mat
		d_mat = m$d_mat
		r_mats = map(m$r_mats, diag)
		q_mat = m$q_mat
		y_mats =
			bind_rows(
				m$pca_input_df,
				hist$wide$m$st[[as.character(this_bdate)]] %>%
					filter(., date %in% m$big_tau_dates) %>%
					select(., date, model$pca_varnames)
			) %>%
			mutate(., across(-date, function(x) ifelse(is.na(x), 0, x))) %>%
			select(., -date) %>%
			purrr::transpose(.) %>%
			lapply(., function(x) matrix(unlist(x), ncol = 1))

		z0Cond0 = matrix(rep(0, m$big_r), ncol = 1)
		sigmaZ0Cond0 = matrix(rep(0, m$big_r^2), ncol = m$big_r)

		zTCondTMinusOne = list()
		zTCondT = list()

		sigmaZTCondTMinusOne = list()
		sigmaZTCondT = list()

		yTCondTMinusOne = list()
		sigmaYTCondTMinusOne = list()

		pT = list()

		## Filter Step
		for (t in 1:length(c(m$big_t_dates, m$big_tau_dates))) {
			# message(t)
			# Prediction Step
			zTCondTMinusOne[[t]] = b_mat %*% {if (t == 1) z0Cond0 else zTCondT[[t-1]]} + c_mat
			sigmaZTCondTMinusOne[[t]] = b_mat %*% {if (t == 1) sigmaZ0Cond0 else sigmaZTCondT[[t-1]]} + q_mat
			yTCondTMinusOne[[t]] = a_mat %*% zTCondTMinusOne[[t]] + d_mat
			sigmaYTCondTMinusOne[[t]] = a_mat %*% sigmaZTCondTMinusOne[[t]] %*% t(a_mat) + r_mats[[t]]

			# Correction Step
			pT[[t]] = sigmaZTCondTMinusOne[[t]] %*% t(a_mat) %*%
				{
					if (t %in% 1:length(m$big_t_dates)) solve(sigmaYTCondTMinusOne[[t]])
					else chol2inv(chol(sigmaYTCondTMinusOne[[t]]))
				}
			zTCondT[[t]] = zTCondTMinusOne[[t]] + pT[[t]] %*% (y_mats[[t]] - yTCondTMinusOne[[t]])
			sigmaZTCondT[[t]] = sigmaZTCondTMinusOne[[t]] - (pT[[t]] %*% sigmaYTCondTMinusOne[[t]] %*% t(pT[[t]]))
		}

		kFitted =
			zTCondT %>%
			purrr::map_dfr(., function(x)
				as.data.frame(x) %>% t(.) %>% as_tibble(.)
			) %>%
			bind_cols(date = c(m$big_t_dates, m$big_tau_dates), .)


		## Smoothing step
		zTCondBigTSmooth = list()
		sigmaZTCondBigTSmooth = list()
		sT = list()

		for (t in (length(zTCondT) - 1): 1) {
			# message(t)
			sT[[t]] = sigmaZTCondT[[t]] %*% t(b_mat) %*% solve(sigmaZTCondTMinusOne[[t + 1]])
			zTCondBigTSmooth[[t]] = zTCondT[[t]] + sT[[t]] %*%
				({if (t == length(zTCondT) - 1) zTCondT[[t + 1]] else zTCondBigTSmooth[[t + 1]]} - zTCondTMinusOne[[t + 1]])
			sigmaZTCondBigTSmooth[[t]] = sigmaZTCondT[[t]] - sT[[t]] %*%
				(sigmaZTCondTMinusOne[[t + 1]] -
				 	{if (t == length(zTCondT) - 1) sigmaZTCondT[[t + 1]] else sigmaZTCondBigTSmooth[[t + 1]]}
				) %*% t(sT[[t]])
		}

		kSmooth =
			zTCondBigTSmooth %>%
			map_dfr(., function(x)
				as.data.frame(x) %>% t(.) %>% as_tibble(.)
			) %>%
			bind_cols(date = c(m$big_t_dates, m$big_tau_dates) %>% .[1:(length(.) - 1)], .)



		## Forecasting step
		zTCondBigT = list()
		sigmaZTCondBigT = list()
		yTCondBigT = list()
		sigmaYTCondBigT = list()

		for (j in 1:length(m$big_tstar_dates)) {
			zTCondBigT[[j]] = b_mat %*% {if (j == 1) zTCondT[[length(zTCondT)]] else zTCondBigT[[j - 1]]} + c_mat
			sigmaZTCondBigT[[j]] = b_mat %*%
				{if (j == 1) sigmaZTCondT[[length(sigmaZTCondT)]] else sigmaZTCondBigT[[j - 1]]} + q_mat
			yTCondBigT[[j]] = a_mat %*% zTCondBigT[[j]] + d_mat
			sigmaYTCondBigT[[j]] = a_mat %*% sigmaZTCondBigT[[j]] %*% t(a_mat) + r_mats[[1]]
		}

		kForecast =
			zTCondBigT %>%
			map_dfr(., function(x)
				as.data.frame(x) %>% t(.) %>% as_tibble(.)
			) %>%
			bind_cols(date = m$big_tstar_dates, .)

		## Plot and Cleaning
		kf_plots =
			lapply(colnames(m$z_df) %>% .[. != 'date'], function(.varname)
				bind_rows(
					mutate(m$z_df, type = 'Data'),
					mutate(kFitted, type = 'Kalman Filtered'),
					mutate(kForecast, type = 'Forecast'),
					mutate(kSmooth, type = 'Kalman Smoothed'),
				) %>%
					pivot_longer(., -c('date', 'type'), names_to = 'varname') %>%
					filter(., varname == .varname) %>%
					ggplot(.) +
					geom_line(aes(x = date, y = value, color = type), size = 1) +
					labs(x = NULL, y = NULL, color = NULL, title = paste0('Kalman smoothed values for ', .varname)) +
					scale_x_date(date_breaks = '1 year', date_labels = '%Y')
			)

		f_df = bind_rows(
			kSmooth,
			tail(kFitted, 1),
			kForecast # %>% mutate(., f1 = mean(filter(m$z_df, date < '2020-03-01')$f1))
			)

		y_df =
			yTCondBigT %>%
			map_dfr(., function(x) as_tibble(t(as.data.frame(x)))) %>%
			bind_cols(date = m$big_tstar_dates, .)

		kf_df = y_df

		list(
			k_smooth = kSmooth,
			k_fitted = tail(kFitted, 1),
			k_forecast = kForecast,
			bdate = this_bdate,
			f_df = f_df,
			kf_plots = kf_plots,
			y_df = y_df,
			kf_df = kf_df
			)
	})

	# Test factor forecasts by backtest date
	map_dfr(results, function(x)
		x$f_df %>%
			filter(., date == '2022-01-01') %>%
			mutate(., bdate = as_date(x$bdate))
		) %>%
		ggplot(.) +
		geom_line(aes(x = bdate, y = f1))

	for (x in results) {
		models[[as.character(x$bdate)]]$f_df <<- x$f_df
		models[[as.character(x$bdate)]]$kf_plots <<- x$kf_plots
		models[[as.character(x$bdate)]]$y_df <<- x$y_df
		models[[as.character(x$bdate)]]$kf_df <<- x$kf_df
	}
})


# Nowcast  -----------------------------------------------------------------

## 1. Monthly Variables (DFM-AR)  -----------------------------------------------------------------
#' This forecasts an autoregressive DFM-AR process by rewriting it to a VAR(1)
#' y_{t+s} = A^s*Y_t + sum^{s-1}_{j=0} (A^j (BF_{t+j} + C))
local({

	message('*** Forecasting Monthly Variables')
	gc()

	dfm_varnames = filter(variable_params, nc_method == 'dfm.m')$varname
	ar_lags = 1 # Lag can be included from 1-4

	# As of 2/6/22
	# Single core takes about 1s per date for ar_lags = 2
	results = lapply(bdates, function(this_bdate) {

		# message(str_glue('**** Forecasting {this_bdate}'))
		m = models[[as.character(this_bdate)]]

		# Only include variables which are available at that date
		stat_df =
			hist$wide$m$st[[as.character(this_bdate)]] %>%
			select(., all_of(c('date', dfm_varnames[dfm_varnames %in% colnames(.)]))) %>%
			# Add lags & inner join F
			{if (ar_lags > 0) bind_cols(., select(add_lagged_columns(., max_lag = ar_lags), -date)) else .} %>%
			inner_join(., m$f_df, by = 'date') %>%
			as.data.table(.)

		vars_to_forecast = colnames(stat_df) %>% keep(., ~ . %in% dfm_varnames)
		factor_vars = paste0('f', 1:m$big_r)

		dfm_df = lapply(vars_to_forecast, function(.varname) {

			# message(.varname)
			lag_vars = {if (ar_lags == 0) NULL else paste0(.varname, '.l', 1:ar_lags)}

			# Get inputs - include all historical data
			input_df =
				stat_df[, c('date', .varname, factor_vars, lag_vars), with = F] %>%
				na.omit(.)

			y_mat = input_df[, c(.varname), with = F] %>% as.matrix(.)
			x_mat = input_df[, c(factor_vars, lag_vars), with = F][, constant := 1] %>% as.matrix(.)

			coef_df =
				(solve(t(x_mat) %*% x_mat) %*% (t(x_mat) %*% y_mat)) %>%
				as.data.table(.) %>%
				.[, coefname := c(factor_vars, lag_vars, 'constant')] %>%
				set_names(., c('value', 'coefname'))

			# Initialize lag Y_0 matrix
			y_0 =
				stat_df[order(-date)][date <= tail(input_df$date, 1), .varname, with = F] %>%
				head(., ar_lags) %>%
				as.matrix(., ncol = 1)

			# Initialize factor matrix
			f_mats =
				as.data.table(m$f_df)[date > tail(input_df$date, 1), !'date'] %>%
				split(., 1:nrow(.)) %>%
				lapply(., function(x) t(x))

			# Initialize factor loadings matrix
			b_mat =
				coef_df[coefname %chin% factor_vars]$value %>%
				matrix(., nrow = 1) %>%
				{
					if (ar_lags == 0) .
					else rbind(., matrix(rep(0, (ar_lags - 1) * length(factor_vars)), ncol = length(factor_vars)))
					}

			# Initialize constant matrix
			c_mat =
				c(coef_df[coefname == 'constant']$value, {if (ar_lags == 0) NULL else rep(0, ar_lags - 1)}) %>%
				matrix(., ncol = 1)

			# Lag weighted matrix
			a_mat =
				matrix(coef_df[coefname %chin% lag_vars][order(coefname)]$value, nrow = 1) %>%
				{
					if (ar_lags == 0) .
					else rbind(., cbind(diag(1, ar_lags - 1), matrix(rep(0, ar_lags - 1), ncol = 1)))
				}

			forecast_dates = tail(seq(tail(input_df$date, 1), tail(m$big_tstar_dates, 1), by = '1 month'), -1)

			accumulate(1:length(forecast_dates), function(accum, i)
				c_mat + {if (ar_lags == 0) 0 else a_mat %*% accum} + b_mat %*% f_mats[[i]],
				.init = y_0
				) %>%
				.[2:length(.)] %>%
				sapply(., function(x) matrix(x, ncol = 1)[[1, 1]]) %>%
				tibble(date = forecast_dates, varname = .varname, value = .)

			}) %>%
			bind_rows(.) %>%
			pivot_wider(., id_cols = 'date', names_from = varname, values_from = value) %>%
			arrange(., date)

		list(
			bdate = this_bdate,
			dfm_m_df = dfm_df
		)
	})

	for (x in results) {
		models[[as.character(x$bdate)]]$dfm_m_df <<- x$dfm_m_df
	}
})

## 2. Quarterly Variables (DFM-LASSO)  -----------------------------------------------------------------
local({

	message('*** Forecasting Quarterly Variables')

	dfm_varnames = filter(variable_params, nc_method == 'dfm.q')$varname
	ar_lags = 1 # Number of AR lag terms to include.
	use_net = T # Use elastic net?
	use_intercept = T # Include an intercept? Works with either elastic net or ols
	alpha_search_grid = c(0, .5, 1) # L1 penalty, only for elastic net

	results = lapply(bdates, function(this_bdate) {

		message(str_glue('***** Forecasting {this_bdate}'))

		m = models[[as.character(this_bdate)]]

		q_f_df =
			m$f_df %>%
			pivot_longer(., -date, names_to = 'varname') %>%
			mutate(., q = floor_date(date, 'quarters')) %>%
			select(., -date) %>%
			group_by(., varname, q) %>%
			summarize(., value = mean(value), .groups = 'drop') %>%
			pivot_wider(., names_from = varname, values_from = value) %>%
			rename(., date = q)

		# Only include variables which are available at that date
		stat_df =
			hist$wide$q$st[[as.character(this_bdate)]] %>%
			select(., all_of(c('date', dfm_varnames[dfm_varnames %in% colnames(.)]))) %>%
			# Add lags & inner join F
			{if (ar_lags > 0) bind_cols(., select(add_lagged_columns(., max_lag = ar_lags), -date)) else .} %>%
			inner_join(., q_f_df, by = 'date') %>%
			as.data.table(.)

		vars_to_forecast = colnames(stat_df) %>% keep(., ~ . %in% dfm_varnames)
		factor_vars = paste0('f', 1:m$big_r)

		dfm_results = lapply(dfm_varnames %>% set_names(., .), function(.varname) {

			lag_vars = {if (ar_lags == 0) NULL else paste0(.varname, '.l', 1:ar_lags)}

			# Get inputs - include all historical data
			input_df_0 =
				stat_df[, c('date', .varname, factor_vars, lag_vars), with = F] %>%
				na.omit(.)

			forecast_dates = tail(seq(tail(input_df_0$date, 1), tail(m$big_tstar_dates, 1), by = '3 months'), -1)

			input_df =
				input_df_0 %>%
				# Add date filters here if needed
				.[! date %in% from_pretty_date(c('2020Q2', '2020Q3'), 'q')]

			y_mat =
				input_df %>%
				.[, c(.varname), with = F] %>%
				as.matrix(.)

			# Don't need to create constant here since glmnet will handle
			x_mat =
				input_df %>%
				.[, c(factor_vars, lag_vars), with = F] %>%
				as.matrix(.)

			#  Calculate OLS values for weights
			ols_yhat = x_mat %*% {solve(t(x_mat) %*% x_mat) %*% (t(x_mat) %*% y_mat)}
			ols_weights = 1/(lm(abs(y_mat - ols_yhat) ~ y_mat)$fitted.values^2)
			# ols = tibble(
			# 	date = input_df$date,
			# 	value = as.numeric(y_mat) * 400,
			# 	yhat = as.numeric(ols_yhat) * 400,
			# 	resids = abs(yhat - value) ,
			# 	weights = ols_weights
			# )

			if (use_net == T) {

				glm_result = lapply(alpha_search_grid, function(.alpha) {
					cv = glmnet::cv.glmnet(
						x = x_mat,
						y = y_mat,
						deviance = 'mae',
						alpha = .alpha,
						intercept = use_intercept,
						weights = ols_weights
						)
					tibble(alpha = .alpha, lambda = cv$lambda, mae = cv$cvm) %>%
						mutate(., min_lambda_for_given_alpha = (mae == min(mae))) %>%
						return(.)
					}) %>%
					bind_rows(.) %>%
					mutate(., min_overall = (mae == min(mae)))

				glm_optim = filter(glm_result, min_overall == TRUE)

				cv_plot =
					glm_result %>%
					ggplot(.) +
					geom_line(aes(x = log(lambda), y = mae, group = alpha, color = alpha)) +
					geom_point(
						data = glm_result %>% filter(., min_lambda_for_given_alpha == TRUE),
						aes(x = log(lambda), y = mae), color = 'red'
					) +
					geom_point(
						data = glm_result %>% filter(., min_overall == TRUE),
						aes(x = log(lambda), y = mae), color = 'green'
					) +
					labs(
						x = 'log-Lambda', y = 'MAE', color = 'alpha',
						title = paste0(.varname, ' DFM-AR(1) Model, Elastic Net Hyperparameter Fit'),
						subtitle =
						'Red = MAE Minimizing Lambda for Given Alpha, Green = MAE Minimizing (Lambda, Alpha) Pair'
					)

				glm_obj = glmnet::glmnet(
						x = x_mat,
						y = y_mat,
						alpha = glm_optim$alpha,
						lambda = glm_optim$lambda,
						intercept = use_intercept,
						weights = ols_weights
					)

				coef_mat = glm_obj %>% coef(.) %>% as.matrix(.)

				coef_df =
					coef_mat %>%
					as.data.frame(.) %>%
					rownames_to_column(., var = 'Covariate') %>%
					setNames(., c('coefname', 'value')) %>%
					as_tibble(.) %>%
					mutate(., coefname = ifelse(coefname == '(Intercept)', 'constant', coefname)) %>%
					as.data.table(.)

			} else {

				# Use OLS with weighted coefficients
				w_mat = diag(ols_weights)
				x_mat2 =
					x_mat %>%
					as.data.frame(.) %>%
					{if (use_intercept) mutate(., constant = 1) else .} %>%
					as.matrix(.)

				coef_df =
					(solve(t(x_mat2) %*% w_mat %*% x_mat2) %*% (t(x_mat2) %*% w_mat %*% y_mat)) %>%
					t(.) %>%
					as_tibble(.) %>%
					pivot_longer(., everything(), names_to = 'coefname', values_to = 'value') %>%
					as.data.table(.)
			}

			# Initialize lag Y_0 matrix
			y_0 =
				stat_df[order(-date)][date <= tail(input_df$date, 1), .varname, with = F] %>%
				head(., ar_lags) %>%
				as.matrix(., ncol = 1)

			# Initialize factor matrix
			f_mats =
				as.data.table(q_f_df)[date > tail(input_df$date, 1), !'date'] %>%
				split(., 1:nrow(.)) %>%
				lapply(., function(x) t(x))

			# Initialize factor loadings matrix
			b_mat =
				coef_df[coefname %chin% factor_vars]$value %>%
				matrix(., nrow = 1) %>%
				{
					if (ar_lags == 0) .
					else rbind(., matrix(rep(0, (ar_lags - 1) * length(factor_vars)), ncol = length(factor_vars)))
				}

			# Initialize constant matrix
			c_mat =
				c(coef_df[coefname == 'constant']$value, {if (ar_lags == 0) NULL else rep(0, ar_lags - 1)}) %>%
				matrix(., ncol = 1)

			# Lag weighted matrix
			a_mat =
				matrix(coef_df[coefname %chin% lag_vars][order(coefname)]$value, nrow = 1) %>%
				{
					if (ar_lags == 0) .
					else rbind(., cbind(diag(1, ar_lags - 1), matrix(rep(0, ar_lags - 1), ncol = 1)))
				}

			forecast_df =
				purrr::accumulate(1:length(forecast_dates), function(accum, i)
					c_mat + {if (ar_lags == 0) 0 else a_mat %*% accum} + b_mat %*% f_mats[[i]],
					.init = y_0
				) %>%
				.[2:length(.)] %>%
				sapply(., function(x) matrix(x, ncol = 1)[[1, 1]]) %>%
				tibble(date = forecast_dates, varname = .varname, value = .)

			list(
				glm_optim = {if (use_net == T) glm_optim else NA},
				cv_plot = {if (use_net == T) cv_plot else NA},
				forecast_df = forecast_df,
				coef_df = coef_df
				)
			})

		dfm_df =
			dfm_results %>%
			map(., ~ .$forecast_df) %>%
			bind_rows(.) %>%
			pivot_wider(., id_cols = 'date', names_from = varname, values_from = value) %>%
			arrange(., date)

		glm_coef_list = map(dfm_results, ~ .$coef_df)

		glm_optim_df = imap_dfr(dfm_results, function(x, i) bind_cols(varname = i, x$glm_optim))

		glm_cv_plots = map(dfm_results, ~ .$cv_plot)

		list(
			bdate = this_bdate,
			glm_coef_list = glm_coef_list,
			glm_optim_df = glm_optim_df,
			glm_cv_plots = glm_cv_plots,
			dfm_q_df = dfm_df
			)
		})

	for (x in results) {
		models[[as.character(x$bdate)]]$dfm_q_df <<- x$dfm_q_df
		models[[as.character(x$bdate)]]$glm_coef_list <<- x$glm_coef_list
		models[[as.character(x$bdate)]]$glm_optim_df <<- x$glm_optim_df
		models[[as.character(x$bdate)]]$glm_cv_plots <<- x$glm_cv_plots
	}
})

## 3. Detransform ---------------------------------------------------------------
local({

	message('*** Reversing Stationary Transformations')

	# Detransform monthly and quarterly forecasts
	results = lapply(bdates, function(this_bdate) {

		# message(str_glue('... Detransforming {this_bdate}'))

		m = models[[as.character(this_bdate)]]

		this_m_hist = hist$wide$m$base[[as.character(this_bdate)]]
		this_q_hist = hist$wide$q$base[[as.character(this_bdate)]]

		# Keep amount same as the last difference between pdi and pdir+pdin
		cbi_val =
			na.omit(select(m$dfm_q_df, all_of(c('date', 'pdinstruct', 'pdinequip', 'pdinip', 'pdir')))) %>%
			.$date %>%
			.[[1]] %>%
			{tail(as.data.table(this_q_hist)[date < .], 1)} %>%
			.[, zzz := pdi - pdinstruct - pdinequip - pdinip - pdir] %>%
			.$zzz

		m_df =
			m$dfm_m_df %>%
			as.data.table(.) %>%
			melt(., id.vars = 'date', value.name = 'value', variable.name = 'varname', na.rm = T) %>%
			merge(., variable_params[, c('varname', 'st')], by = 'varname', all.x = T) %>%
			split(., by = 'varname') %>%
			lapply(., function(x)
				 x[order(date)] %>%
				 	.[, value := {
				 		if (.$st[[1]] == 'none') NA
				 		else if (.$st[[1]] == 'base') value
				 		else if (.$st[[1]] == 'log') exp(value)
				 		else if (.$st[[1]] == 'dlog')
				 			undlog(value, tail(filter(this_m_hist, date < x$date[[1]]), 1)[[.$varname[[1]]]])
				 		else if (.$st[[1]] == 'diff1')
				 			undiff(value, 1, tail(filter(this_m_hist, date < x$date[[1]]), 1)[[.$varname[[1]]]])
				 		else if (.$st[[1]] == 'pchg')
				 			unpchg(value, tail(filter(this_m_hist, date < x$date[[1]]), 1)[[.$varname[[1]]]])
				 		else if (.$st[[1]] == 'apchg')
				 			unapchg(value, 12, tail(filter(this_m_hist, date < x$date[[1]]), 1)[[.$varname[[1]]]])
				 		else stop('Error')
				 		}]
				) %>%
			rbindlist(.) %>%
			.[, st := NULL] %>%
			dcast(., date ~ varname, value.var = 'value', fill = NA) %>%
			as_tibble(.)

		q_df =
			m$dfm_q_df %>%
			as.data.table(.) %>%
			melt(., id.vars = 'date', value.name = 'value', variable.name = 'varname', na.rm = T) %>%
			merge(., variable_params[, c('varname', 'st')], by = 'varname', all.x = T) %>%
			split(., by = 'varname') %>%
			lapply(., function(x)
				x[order(date)] %>%
					.[, value := {
						if (.$st[[1]] == 'none') NA
						else if (.$st[[1]] == 'base') value
						else if (.$st[[1]] == 'log') exp(value)
						else if (.$st[[1]] == 'dlog')
							undlog(value, tail(filter(this_q_hist, date < x$date[[1]]), 1)[[.$varname[[1]]]])
						else if (.$st[[1]] == 'diff1')
							undiff(value, 1, tail(filter(this_q_hist, date < x$date[[1]]), 1)[[.$varname[[1]]]])
						else if (.$st[[1]] == 'pchg')
							unpchg(value, tail(filter(this_q_hist, date < x$date[[1]]), 1)[[.$varname[[1]]]])
						else if (.$st[[1]] == 'apchg')
							unapchg(value, 4, tail(filter(this_q_hist, date < x$date[[1]]), 1)[[.$varname[[1]]]])
						else stop('Error')
					}]
				) %>%
			rbindlist(.) %>%
			.[, st := NULL] %>%
			dcast(., date ~ varname, value.var = 'value', fill = NA) %>%
			as_tibble(.) %>%
			# Adjust CBI val
			mutate(., cbi = ifelse(!is.na(cbi), cbi_val, cbi))

		list(
			bdate = this_bdate,
			m_ut = m_df,
			q_ut = q_df
			)
		})

	for (x in results) {
		models[[as.character(x$bdate)]]$pred_m_ut0 <<- x$m_ut
		models[[as.character(x$bdate)]]$pred_q_ut0 <<- x$q_ut
	}
})


## 4. Calculate GDP Nowcast  -----------------------------------------------------------------
local({

	message('*** Adding Calculated Variables')

	results = lapply(bdates, function(this_bdate) {

		# message(str_glue('... Forecasting {this_bdate}'))
		m = models[[as.character(this_bdate)]]

		pred_q_ut1 =
			m$pred_q_ut0 %>%
			transmute(
				.,
				date,
				govt = govtf + govts,
				ex = exg + exs,
				im = img + ims,
				nx = ex - im,
				pdin = pdinstruct + pdinequip + pdinip,
				# PDI = PDI_FIXED + CBI
				# ~= PDI_FIXED + DLOG.PDI_FIXED * (-1/100)
				pdi = pdin + pdir + cbi,
				# pdi = pdin + pdir + pceschange,
				pces = pceshousing + pceshealth + pcestransport + pcesrec + pcesfood + pcesfinal + pcesother + pcesnonprofit,
				pcegn = pcegnfood + pcegnclothing + pcegngas + pcegnother,
				pcegd = pcegdmotor + pcegdfurnish + pcegdrec + pcegdother,
				pceg = pcegn + pcegd,
				pce = pceg + pces,
				gdp = pce + pdi + nx + govt
			)

		pred_m_ut1 =
			m$pred_m_ut0 %>%
			transmute(
				.,
				date,
				psr = ps/pid
			)

		list(
			bdate = this_bdate,
			pred_q_ut1 = pred_q_ut1,
			pred_m_ut1 = pred_m_ut1
			)
	})

	for (x in results) {
		models[[as.character(x$bdate)]]$pred_m_ut1 <<- x$pred_m_ut1
		models[[as.character(x$bdate)]]$pred_q_ut1 <<- x$pred_q_ut1
	}
})


## 5. Aggregate & Create Display Formats  -----------------------------------------------------------------
#' Take 7-day MA to account for weird seasonality issue
local({

	message('*** Aggregating & Creating Display Formats')

	results = lapply(bdates, function(this_bdate) {

		# message(str_glue('... Aggregating {this_bdate}'))
		m = models[[as.character(this_bdate)]]

		pred_ut = lapply(c('m', 'q') %>% set_names(., .), function(freq)
			{if (freq == 'm') list(m$pred_m_ut0, m$pred_m_ut1) else list(m$pred_q_ut0, m$pred_q_ut1)} %>%
				reduce(., function(x, y) full_join(x, y, by = 'date')) %>%
				arrange(., date)
			)

		this_hist = lapply(c('m', 'q') %>% set_names(., .), function(freq)
			hist$wide[[freq]]$base[[as.character(this_bdate)]] %>%
				as.data.table(.) %>%
				melt(., id.vars = 'date', variable.name = 'varname', na.rm = T)
			)

		pred_flat =
			# Map each frequency x display form combination
			expand_grid(form = c('d1', 'd2'), freq = c('m', 'q')) %>%
			df_to_list %>%
			lapply(., function(z) {
				# message(z)
				df =
					pred_ut[[z$freq]] %>%
						as.data.table(.) %>%
						melt(., id.vars = 'date', value.name = 'value', variable.name = 'varname', na.rm = T) %>%
						merge(
							.,
							rename(variable_params[, c('varname', z$form)], 'transform' = z$form),
							by = 'varname', all.x = T
							) %>%
						split(., by = 'varname') %>%
						lapply(., function(x) {
							# message(x$varname[[1]])
							x[order(date)] %>%
								# Bind historical date
								rbind(
									this_hist[[z$freq]][date < .$date[[1]] & varname == .$varname[[1]]] %>%
										.[, transform := x$transform[[1]]] %>%
										last(.),
										.
									) %>%
								.[, value := {
									if (.$transform[[1]] == 'none') NA
									else if (.$transform[[1]] == 'base') value
									else if (.$transform[[1]] == 'log') log(value)
									else if (.$transform[[1]] == 'dlog') dlog(value)
									else if (.$transform[[1]] == 'diff1') diff1(value)
									else if (.$transform[[1]] == 'pchg') pchg(value)
									else if (.$transform[[1]] == 'apchg' & z$freq == 'q') apchg(value, 4)
									else if (.$transform[[1]] == 'apchg' & z$freq == 'm') apchg(value, 12)
									else stop('Error')
								}] %>%
								tail(., -1)
						}) %>%
						rbindlist(.) %>%
						.[, transform := NULL] %>%
						na.omit(.)

				list(
					df = df,
					form = z$form,
					freq = z$freq
					)
			}) %>%
			map_dfr(., function(z) z$df[, c('form', 'freq') := list(z$form, z$freq)])

		list(
			bdate = this_bdate,
			pred_flat = pred_flat
			)
		})

	pred_flat =
		map_dfr(results, function(z) as_tibble(mutate(z$pred_flat, bdate = z$bdate))) %>%
		# Varname gets casted as a factor
		mutate(., varname = as.character(varname))

	pred_wide =
		lapply(split(as.data.table(pred_flat), by = 'bdate', keep.by = F), function(x)
			lapply(split(x, by = 'freq', keep.by = F), function(y)
				lapply(split(y, by = 'form', keep.by = F), function(z)
					dcast(z, date ~ varname, value.var = 'value')[order(date)] %>%
						as_tibble(.)
					)
				)
			)

	# pred_flat_with_form =
	# 	pred_flat %>%
	# 	inner_join(
	# 		.,
	# 		pivot_longer(
	# 			variable_params[, c('varname', 'd1', 'd2')],
	# 			-varname,
	# 			names_to = 'form',
	# 			values_to = 'transform'
	# 			),
	# 		by = c('varname', 'form')
	# 	) %>%
	# 	select(., -form)


	model$pred_flat <<- pred_flat
	model$pred_wide <<- pred_wide
})

## 6. Stabilize ------------------------------------------------------------
local({

	message(str_glue('*** Stabilizing Results | {format(now(), "%H:%M")}'))

	pred_flat_stab =
		model$pred_flat %>%
		group_by(., varname, date, form, freq) %>%
		arrange(., bdate, .by_group = T) %>%
		mutate(
			.,
			last_is_contiguous = dplyr::lag(bdate, 1) == bdate - days(1),
			rle = rowid(rleid(last_is_contiguous)),
			value = ifelse(
				!is.na(last_is_contiguous) & last_is_contiguous == T & rle >= 7,
				zoo::rollmean(value, 7, align = 'right', fill = NA),
				NA
				)
			) %>%
		ungroup %>%
		filter(., !is.na(value)) %>%
		select(., date, varname, value, form, freq, bdate)

	model$pred_flat_stab <<- pred_flat_stab
})

## 7. Final Checks ------------------------------------------------------------
local({

	message(str_glue('*** Getting Final Checks | {format(now(), "%H:%M")}'))

	plots =
		model$pred_flat_stab %>%
		filter(
			.,
			varname %in% c('gdp', 'pceg', 'pces', 'govt', 'ex', 'im', 'pdi', 'pdin', 'pdir'),
			form == 'd1'
		) %>%
		filter(., bdate >= date - days(90)) %>%
		group_split(., date) %>%
		set_names(., map_vec(., \(x) x$date[[1]])) %>%
		lapply(., function(x)
			ggplot(x) + geom_line(aes(x = bdate, y = value, color = varname), size = 1) +
				geom_point(aes(x = bdate, y = value, color = varname), size = 2) +
				labs(x = 'Vintage Date', y = 'Annualized % Change', title = x$date[[1]]) +
				scale_x_date(date_breaks = '1 week', date_labels =  '%m/%d/%y')
		)

	for (p in tail(plots, 4)) {
		print(p)
	}

	# imap_dfr(models, function(m, this_bdate)
	# 	mutate(m$f_df, bdate = as_date(this_bdate))
	# 	) %>% filter(., bdate >= today() - days(30) & date == '2022-01-01') %>%
	# 	arrange(., date) %>%
	# 	ggplot(.) +
	# 	geom_line(aes(x = bdate, y = f1))

	model$pred_plots <<- plots
})


# Finalize ----------------------------------------------------------------

## 1. SQL ----------------------------------------------------------------
local({

	message(str_glue('*** Sending Values to SQL: {format(now(), "%H:%M")}'))

	model_values =
		model$pred_flat_stab %>%
		transmute(., forecast = 'now', vdate = bdate, form, freq, varname, date, value) %>%
		filter(., varname %in% get_query(pg, 'SELECT * FROM forecast_variables')$varname)

	rows_added = store_forecast_values_v2(pg, model_values, .store_new_only = STORE_NEW_ONLY, .verbose = T)

	# Log
	validation_log$store_new_only <<- STORE_NEW_ONLY
	validation_log$rows_added <<- rows_added
	validation_log$last_bdate <<- model$pred_flat_stab$bdate

	disconnect_db(pg)
})

## 2. Create Docs  ----------------------------------------------------------
local({

	message('*** Creating Docs')

	temp = tempdir()

	knitr::knit2pdf(
		input = file.path(Sys.getenv('EF_DIR'), 'modules', 'nowcast-model', 'nowcast-model-documentation-template.rnw'),
		output = file.path(temp, 'nowcast-model-documentation.tex'),
		clean = T
		)

	fs::file_move(
		file.path(temp, 'nowcast-model-documentation.pdf'),
		file.path(Sys.getenv('EF_DIR'), 'nowcast-model-documentation.pdf')
		)

	system(str_glue(
		"scp {filepath} {remote_user}@{remote_host}:{remote_path}",
		filepath = file.path(Sys.getenv('EF_DIR'), 'nowcast-model-documentation.pdf'),
		remote_user = Sys.getenv('SCP_USER'),
		remote_host = Sys.getenv('SCP_SERVER'),
		remote_path = Sys.getenv('SCP_DIR')
		), wait = T)

})
