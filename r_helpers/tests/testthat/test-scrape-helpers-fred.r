test_that('Get obs from FRED', {

	if (Sys.getenv('FRED_API_KEY') == '') {
		skip('Skipping FRED test due to missing env variables')
	}

	api_key = Sys.getenv('FRED_API_KEY')

	attempt = get_fred_obs(c('GDP'), api_key)

	expect_s3_class(attempt, 'data.frame')
	expect_equal(round(filter(attempt, date == '2010-01-01')$value), 14765)
})


test_that('Get vintages from FRED', {

	if (Sys.getenv('FRED_API_KEY') == '') {
		skip('Skipping FRED test due to missing env variables')
	}

	api_key = Sys.getenv('FRED_API_KEY')

	attempt = get_fred_obs_with_vintage(list(c('GDP', 'q')), api_key)

	expect_s3_class(attempt, 'data.frame')
	expect_equal(
		round(filter(filter(attempt, date == '2010-01-01'), vintage_date == max(vintage_date))$value),
		14765
		)
})

test_that('Get long weekly vintages from FRED', {

	if (Sys.getenv('FRED_API_KEY') == '') {
		skip('Skipping FRED test due to missing env variables')
	}

	api_key = Sys.getenv('FRED_API_KEY')

	attempt = get_fred_obs_with_vintage(c('ICSA'), api_key)

	expect_s3_class(attempt, 'data.frame')
	# Series starts on Saturdays, should be adjusted to Sundays
	expect_equal(weekdays(attempt$date[[length(attempt$date)]]), "Sunday")
})
