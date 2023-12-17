
#' Get the next Fed bank holidays ahead of today
#'
#' @description See https://www.frbservices.org/about/holiday-schedules
#'
#' @import dplyr purrr
#' @importFrom lubridate wday as_date month ceiling_date floor_date days
#' @export
get_fed_holidays = function(year) {
	get_nth_wday_of_month = function(ith, wd, month, year) {
		start = as_date(paste0(year, '-', month, '-1'))
		dates = head(seq(from = start, to = ceiling_date(start, 'month'), by = '1 day'), -1)
		wdays = wday(dates)
		return(dates[wdays == wd][ith])
	}

	holidays_1 = list(
		c(3, 2, 1), # MLK
		c(3, 2, 2), # Washington's Birthday
		c(1, 2, 9), # Labor Day
		c(2, 2, 10), # Columbus Day
		c(4, 5, 11) # Thanksgiving
	) %>% map_vec(., \(x) get_nth_wday_of_month(x[1], x[2], x[3], year))

	last_day_of_may = as_date(paste(year, '05', '31', sep = '-'))
	holidays_2 = floor_date(last_day_of_may, "week", week_start = 1)

	holidays_3 = list(
		c(1, 1), # NY
		c(7, 4), # Independence Day
		c(11, 11), # Veterans Day
		c(12, 25) # Christmas
	) %>%
		{if (year >= 2021) c(., list(c(6, 19))) else .} %>% # Juneteenth
		map_vec(., \(x) {
			holiday_dt = as_date(paste0(year, '-', x[1], '-', x[2]))
			if (wday(holiday_dt) == 1) holiday_dt + days(1) else holiday_dt
		})

	holidays = sort(c(holidays_1, holidays_2, holidays_3))
	return(holidays)
}

#' Get the next Fed bank business day ahead of today
#'
#' @import dplyr purrr
#' @importFrom lubridate wday as_date
#' @export
get_next_fed_business_day = function(days) {

	days = as_date(days)
	holidays = as_date(unlist(map(unique(year(days)), get_fed_holidays), recursive = F))

	bdays =
		tibble(search_date = seq(
			from = min(days),
			to = max(days) + days(7),
			by = '1 day'
		)) %>%
		filter(., !wday(search_date) %in% c(1, 7)) %>%
		filter(., !search_date %in% holidays)

	tibble(days = days + days(1)) %>%
		left_join(., bdays, join_by(closest(days <= search_date))) %>%
		.$search_date
}
