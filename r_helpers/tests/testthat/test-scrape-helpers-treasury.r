test_that('Get Treasury data', {

	attempt = get_treasury_yields()

	expect_s3_class(attempt, 'data.frame')
	expect_equal(filter(attempt, date == '2001-01-02' & varname == 't01y')$value, 5.11)
})
