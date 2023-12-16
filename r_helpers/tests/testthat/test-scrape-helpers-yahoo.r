test_that('Get Yahoo data', {

	attempt = get_yahoo_data('VIX')

	expect_s3_class(attempt, 'data.frame')
	expect_equal(round(filter(attempt, date == '2014-12-04')$value), 28448)
})
