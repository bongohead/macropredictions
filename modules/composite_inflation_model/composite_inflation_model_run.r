#' Historical data lags used for vintage assignments
#'
#' Assuming run at <=8:00am ET:
#' - Treasury data: 1 day lag after actual release & value day (publishes same-day of value, at 3pm ET)
#' - CPI data: 1 day lag after actual release (8:30am, ~2 week lag after month end)
#' - Cleveland Fed: 1 day lag after actual release (publishes usually on CPI release dates)

# Initialize ----------------------------------------------------------
# If T, adds data to the database that belongs to data vintages already existing in the database.
# Set to F only when there are model updates, new variable pulls, or old vintages are unreliable.
STORE_NEW_ONLY = T
BACKTEST_MONTHS = 160
validation_log <<- list()

## Load Libs ----------------------------------------------------------
library(macropredictions)
library(tidyverse)
library(httr2)
library(readxl, include.only = 'read_excel')

## Load Connection Info ----------------------------------------------------------
load_env()
pg = connect_pg()

# Get Data ----------------------------------------------------------

## TIPS ----------------------------------------------------------
# One-date vdate lag after actual date
local({

	nominal = get_treasury_yields()
	real = get_real_treasury_yields()

	hist =
		inner_join(nominal, transmute(real, date, ttm, real = value), join_by(date, ttm)) %>%
		transmute(., date, vdate = date + days(1), varname, ttm, value = value - real)

	hist %>%
		filter(., varname %in% c('t05y', 't10y', 't30y')) %>%
		ggplot() +
		geom_line(aes(x = vdate, y = value, color = varname))

	tips_inf <<- hist
})

## Cleveland Fed Forecasts ----------------------------------------------------------
# One-date vdate lag after release
# These are usually synced with BLS release dates
local({

	api_key = Sys.getenv('FRED_API_KEY')

	# Note: Cleveland Fed gives one-year forward rates
	download.file(
		'https://www.clevelandfed.org/-/media/files/webcharts/inflationexpectations/inflation-expectations.xlsx',
		file.path(tempdir(), 'inf.xlsx'),
		mode = 'wb'
	)

	## Latest vdates
	vintage_dates = request(paste0(
		'https://api.stlouisfed.org/fred/release/dates?release_id=10&api_key=', api_key, '&file_type=json'
		)) %>%
		req_perform %>%
		resp_body_json %>%
		.$release_dates %>%
		map(., \(x) x$date) %>%
		unlist(., recursive = F) %>%
		as_date %>%
		keep(., \(x) x > as_date('2010-01-01'))

	vdate_map =
		tibble(vdate = vintage_dates) %>%
		mutate(., vdate0 = floor_date(vdate, 'months')) %>%
		group_by(., vdate0) %>%
		summarize(., vdate = max(vdate), .groups = 'drop') %>%
		arrange(., vdate0)

	# Now parse data and get inflation expectations
	einf_raw =
		read_excel(file.path(tempdir(), 'inf.xlsx'), sheet = 'Expected Inflation') %>%
		rename(., vdate0 = 'Model Output Date') %>%
		pivot_longer(., -vdate0, names_to = 'ttm', values_to = 'value') %>%
		mutate(
			.,
			vdate0 = as_date(vdate0),
			ttm = as.numeric(str_replace(str_sub(ttm, 1, 2), ' ', '')) * 12,
			value = value * 100
			) %>%
		inner_join(., vdate_map, by = 'vdate0') %>%
		select(., -vdate0) %>%
		filter(., vdate >= as_date('2010-01-01')) %>%
		mutate(., vdate = vdate + days(1))

	einf <<- einf_raw
})


## CPI ---------------------------------------------------------------------
# Available to model 1 day after release
local({

	api_key = Sys.getenv('FRED_API_KEY')

	# Get raw CPI history
	cpi = transmute(
		get_fred_obs_with_vintage('CPIAUCSL', api_key),
		date,
		vdate = vintage_date + days(1),
		value
	)

	# Test for equal release dates
	inner_join(
		tibble(cpi_vdate = unique(cpi$vdate), floor = floor_date(unique(cpi$vdate), 'month')),
		tibble(einf_vdate = unique(einf$vdate), floor = floor_date(unique(einf$vdate), 'month')),
		by = 'floor'
		) %>%
		mutate(., is_eq = einf_vdate == cpi_vdate) %>%
		arrange(desc(floor)) %>%
		count(., is_eq) %>%
		print(.)

	cpi <<- cpi
})


# Model ----------------------------------------------------------

## Vintage Dates ------------------------------------------------------------------
local({

	backtest_vdates = seq(
		from = today('US/Eastern') - months(BACKTEST_MONTHS),
		to = today('US/Eastern'),
		by = '1 day'
	)

	backtest_vdates <<- backtest_vdates
})

## Get TIPS-derived curve --------------------------------------------------------------------
# The final TIPS curve will be ttm-level for every forecast vdate, not just dates
# with actual TIPS data published (this is unlike the einf curve)
local({

	# For time "1" ttm (1-month forward inf), substitute in the trailing 3-month annualized % change +
	# some AR component
	last_values_cpi =
		cpi %>%
		slice_max(., date, by = vdate) %>%
		mutate(., date_l3m = add_with_rollback(date, months(-3))) %>%
		left_join(
			.,
			transmute(cpi, date_l3m = date, vdate_l3m = vdate, value_l3m = value),
			join_by(date_l3m, closest(vdate >= vdate_l3m))
		) %>%
		na.omit() %>%
		mutate(., value = 100 * ((value/value_l3m)^4 - 1)) %>%
		transmute(., vdate, date, value) %>%
		arrange(., date)

	# Now get last values of TIPS & last value of CPI available for each forecast vdate
	tips_available_data =
		expand_grid(vdate = backtest_vdates, ttm = c(1, unique(tips_inf$ttm))) %>%
		left_join(
			.,
			bind_rows(
				last_values_cpi %>% transmute(., h_vdate = vdate, h_date = date, ttm = 1, value),
				tips_inf %>% transmute(., h_vdate = vdate, h_date = date, ttm, value)
			),
			join_by(ttm, closest(vdate >= h_vdate))
		) %>%
		# Add date columns to indicate vdates of source
		left_join(
			.,
			transmute(filter(., ttm == 1), vdate, cpi_date = h_date, cpi_vdate = h_vdate),
			'vdate'
			) %>%
		left_join(
			.,
			transmute(filter(., ttm == 60), vdate, tips_date = h_date, tips_vdate = h_vdate),
			'vdate'
			) %>%
		select(., -h_vdate, -h_date)

	tips_available_data %>% arrange(desc(vdate))

	# Diebold-Li fits
	fitted_einfs =
		tips_available_data %>%
		rename(., date = vdate) %>%
		estimate_diebold_li_fit(., .0609, return_all = T) %>%
		distinct(date, b1, b2, b3, lambda) %>%
		expand_grid(., ttm = 1:360) %>%
		transmute(vdate = date, ttm, value = get_dns_fit(b1, b2, b3, lambda, ttm))

	tips_curve =
		fitted_einfs %>%
		left_join(
			.,
			distinct(tips_available_data, vdate, cpi_date, cpi_vdate, tips_date, tips_vdate),
			by = 'vdate'
		)

	ggplot() +
		geom_line(
			aes(x = ttm, y = value), color = 'black',
			data = filter(tips_curve, vdate == max(vdate))
			) +
		geom_point(
			aes(x = ttm, y = value), color = 'black',
			data = filter(tips_available_data, ttm != 1 & vdate == max(vdate))
			) +
		geom_point(
			aes(x = ttm, y = value), color = 'red',
			data = filter(einf, ttm != 1 & vdate == max(vdate))
			)

	last_values_cpi <<- last_values_cpi
	tips_available_data <<- tips_available_data
	tips_curve <<- tips_curve
})

## Validate TIPS curve ----------------------------------------------------------------
local({

	# Get realized inflation numbers for plotting
	actual_cpis = expand_grid(
		date = seq(from = floor_date(backtest_vdates[[1]], 'month'), to = today(), by = '1 month'),
		months_ahead = 0:360
		) %>%
		mutate(., fdate = date %m+% months(months_ahead)) %>%
		left_join(
			.,
			cpi %>%
				slice_max(., vdate, by = date) %>%
				transmute(., fdate = date, months_ahead_value = value),
			join_by(fdate)
		) %>%
		left_join(
			.,
			filter(., months_ahead == 0) %>%
				transmute(., date, initial_value = months_ahead_value),
			join_by(date)
		) %>%
		mutate(., value = 100*((months_ahead_value/initial_value)^(12/months_ahead) - 1)) %>%
		transmute(., ttm = months_ahead, date, value) %>%
		filter(., ttm %in% c(12, 24, 36, 60, 120, 240, 360)) %>%
		na.omit(.)

	inf_plot =
		bind_rows(
			tips_available_data %>% mutate(., type = 'tips_actual'),
			tips_curve %>% mutate(., type = 'fitted') %>% filter(., ttm %% 5 == 0)
		) %>%
		filter(., vdate %in% c(sample(unique(.$vdate), 10), max(.$vdate))) %>%
		bind_rows(
			.,
			left_join(
				distinct(., vdate),
				transmute(einf, ttm, value, vdate_last = vdate),
				join_by(closest(vdate >= vdate_last))
				) %>%
				mutate(., type = 'einf'),
			left_join(
				distinct(., vdate),
				transmute(actual_cpis, ttm, value, vdate_last = date),
				join_by(closest(vdate >= vdate_last))
				) %>%
				mutate(., type = 'realized')
			) %>%
		filter(., ttm <= 120) %>%
		ggplot() +
		geom_point(aes(x = ttm, y = value, color = type, size = type), alpha = .8) +
		scale_size_manual(values = c(tips_actual = 2, fitted = .5, einf = 1, realized = 1.5)) +
		facet_wrap(vars(vdate))

	print(inf_plot)
})

## Convert TTM Curves into % Change Forecasts ----------------------------------------------------------
# Convert einf and TIPS curves into monthly % change forecasts
# Note that TIPS curves exist for every vdate, whereas einf curves do not - this uses the last
# vdate available relative to each TIPS curve
local({

	tips_change_forecast =
		tips_curve %>%
		mutate(
			.,
			vdate,
			date = add_with_rollback(floor_date(vdate, 'month'), months(ttm - 1)),
			cum_return = (1 + value/100)^((ttm)/12),
			chg_from_last_period = cum_return/coalesce(lag(cum_return, 1), 1) - 1,
			.by = vdate
		) %>%
		select(vdate, date, chg_from_last_period, cpi_date, cpi_vdate, tips_date, tips_vdate)

	# Add smoothing + interp for einf
	einf_change_forecast =
		einf %>%
		# Force months 6:11 to have the same inflation
		right_join(
			.,
			expand_grid(vdate = unique(.$vdate), ttm = 1:360),
			by = c('ttm', 'vdate')
			) %>%
		arrange(., vdate, ttm) %>%
		mutate(
			.,
			value = zoo::na.spline(value),
			vdate,
			date = add_with_rollback(floor_date(vdate, 'month'), months(ttm - 1)),
			cum_return = (1 + value/100)^((ttm)/12),
			chg_from_last_period = cum_return/coalesce(lag(cum_return, 1), 1) - 1,
			.by = vdate
		) %>%
		select(vdate, date, chg_from_last_period)

	# After joining each TIPS forecast to the most recently available einf forecast,
	# there may be a "misalignment" issue. If the TIPS vdate is 12/2 and the EINF vdate joined to it
	# is 11/12, the first TIPS forecast month is Dec. while the first EINF forecast month is Nov.
	forecasts =
		tips_change_forecast %>%
		rename(tips_chg = chg_from_last_period) %>%
		left_join(
			.,
			einf_change_forecast %>%
				transmute(., date, einf_chg = chg_from_last_period, einf_vdate = vdate),
			join_by(date, closest(vdate >= einf_vdate))
		) %>%
		mutate(., chg = ifelse(
			!is.na(tips_chg) & !is.na(einf_chg),
			.5 * tips_chg + .5 * einf_chg,
			tips_chg
		))

	last_forecast_chart =
		forecasts %>%
		pivot_longer(., cols = contains('chg')) %>%
		filter(., vdate == max(vdate)) %>%
		ggplot() +
		geom_line(aes(x = date, y = value, color = name, group = name))

	print(last_forecast_chart)

	f_plot =
		forecasts %>%
		filter(., vdate >= '2021-01-01')  %>%
		transmute(., vdate, date, tips_chg = tips_chg*1200, einf_chg = einf_chg*1200)  %>%
		filter(., date == '2022-06-01') %>%
		ggplot() +
		geom_line(aes(x = vdate, y = tips_chg), color = 'forestgreen') +
		geom_line(aes(x = vdate, y = einf_chg), color = 'skyblue') +
		labs(title = 'Forecast over time for June 2022 (9%) - green = TIPS, blue = Fed')

	print(f_plot)

	g_plot =
		forecasts %>%
		filter(., vdate >= '2022-01-01')  %>%
		transmute(., vdate, date, tips_chg = tips_chg*1200, einf_chg = einf_chg*1200)  %>%
		filter(., date == '2023-06-01') %>%
		ggplot() +
		geom_line(aes(x = vdate, y = tips_chg), color = 'forestgreen') +
		geom_line(aes(x = vdate, y = einf_chg), color = 'skyblue') +
		labs(title = 'Forecast over time for June 2023 (3%) - green = TIPS, blue = Fed')

	print(g_plot)

	composite_chg <<- forecasts
})

## Calculate YOY CPI growth ----------------------------------------------------------
local({

	# For each vdate x forecast month (-12:120),
	# get the historical data if available, o.w. use latest available growth rate data
	# This way, if today is 12/15 and the last CPI data is for Oct., we use the
	# 11/30 vintage forecast for Nov growth.
	desired_values = expand_grid(
		tibble(vdate = backtest_vdates, date = floor_date(backtest_vdates, 'month')),
		months_out = -13:120
		) %>%
		mutate(., date = add_with_rollback(date, months(months_out))) %>%
		left_join(
			cpi %>% transmute(., date, cpi_vdate = vdate, cpi = value),
			join_by(date, closest(vdate >= cpi_vdate))
		) %>%
		left_join(
			composite_chg %>% transmute(., date, chg_vdate = vdate, growth = chg),
			join_by(date, closest(vdate >= chg_vdate))
		) %>%
		mutate(., has_actual = !is.na(cpi))

	fill_inputs = inner_join(
		desired_values %>%
			filter(., has_actual) %>%
			group_by(., vdate) %>%
			summarize(., cpi_values = list(cpi), last_cpi = tail(na.omit(cpi), 1)),
		desired_values %>%
			filter(., !has_actual) %>%
			group_by(., vdate) %>%
			summarize(., growth_rates = list(growth), forecast_dates = list(date)),
		by = 'vdate'
		)

	fill_outputs =
		fill_inputs %>%
		df_to_list() %>%
		map(., function(x) {

			new_cpi = tail(
				accumulate(x$growth_rates, \(accum, g) accum * (1 + g), .init = x$last_cpi),
				-1
				)

			all_cpi = c(x$cpi_values, new_cpi)
			inf = tail((all_cpi/lag(all_cpi, 12) - 1) * 100, length(x$forecast_dates))

			bind_rows(
				tibble(vdate = x$vdate, date = x$forecast_dates, form = 'd1', value = inf),
				tibble(vdate = x$vdate, date = x$forecast_dates, form = 'd2', value = x$growth_rates * 100),
			)
		}) %>%
		list_rbind() %>%
		filter(., !is.na(value))

	# Forecasts over time
	forecasts_by_vdate_plot =
		fill_outputs %>%
		filter(., form == 'd1' & date %in% floor_date(today() - months(0:6), 'month') & vdate >= today() - months(24)) %>%
		mutate(., date = as.character(date)) %>%
		ggplot() +
		geom_line(aes(x = vdate, y = value, color = date, group = date)) +
		ggthemes::theme_igray() +
		labs(title = 'Forecasts over time, for fixed dates')

	# Forward forecasts
	forward_plot =
		fill_outputs %>%
		filter(., form == 'd1' & vdate %in% c(sample(unique(.$vdate), 12), max(vdate))) %>%
		mutate(., vdate = as.factor(vdate)) %>%
		ggplot() +
		geom_line(aes(x = date, y = value, color = vdate, group = vdate)) +
		geom_line(aes(x = date, y = value), data = last_values_cpi) +
		ggthemes::theme_igray()

	print(forecasts_by_vdate_plot)
	print(forward_plot)

	final_forecasts =
		fill_outputs %>%
		transmute(
			.,
			forecast = 'compinf',
			form,
			freq = 'm',
			varname = 'cpi',
			vdate,
			date,
			value
		)

	final_forecasts <<- final_forecasts
})


# Finalize ----------------------------------------------------------------

## Store TIPS Curve -------------------------------------------------------------------
local({

	curve_to_store = bind_rows(
		transmute(tips_inf, vdate, ttm, value, curve_source = 'tips_spread'),
		transmute(tips_curve, vdate, ttm, value, curve_source = 'tips_spread_smoothed')
	)
	rows_added = store_composite_inflation_yield_curve_values(pg, curve_to_store, .verbose = T)

	# Log
	validation_log$curve_rows_added <<- rows_added
})

## Store Final Forecasts -------------------------------------------------------------------
local({

	# Store in SQL
	rows_added = store_forecast_values_v2(pg, final_forecasts, .store_new_only = STORE_NEW_ONLY, .verbose = T)

	# Log
	validation_log$store_new_only <<- STORE_NEW_ONLY
	validation_log$rows_added <<- rows_added

	disconnect_db(pg)
})
