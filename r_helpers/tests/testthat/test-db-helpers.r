test_that('Connection can be established', {

	if (Sys.getenv('MP_DIR') == '' || any(!c('DB_USERNAME', 'DB_SERVER', 'DB_PASSWORD', 'DB_DATABASE', 'DB_PORT') %in% names(Sys.getenv()))) {
		skip('Skipping DB test due to missing env variables')
	}

	conn = tryCatch(connect_pg(), error = function(e) conditionMessage(e))
	on.exit(dbDisconnect(conn))
	expect_s4_class(conn, 'PqConnection')
})

test_that('Test reads and write to SQL database', {

	if (Sys.getenv('MP_DIR') == '' || any(!c('DB_USERNAME', 'DB_SERVER', 'DB_PASSWORD', 'DB_DATABASE', 'DB_PORT') %in% names(Sys.getenv()))) {
		skip('Skipping DB test due to missing env variables')
	}

	conn = tryCatch(connect_pg(), error = function(e) conditionMessage(e))
	on.exit(dbDisconnect(conn))
	if (!'PqConnection' %in% class(conn)) skip('Skipping write test due to conn error')

	test_dataset = data.frame(
		char_col = c('char1', 'char2', 'char3'),
		num_col = c(1, 2, 3),
		dt_col = c(Sys.Date(), Sys.Date() + 1, Sys.Date() + 2),
		dttmtz_col = c(Sys.time(), as.POSIXct(Sys.time() + 1 * 60 * 60, tz = 'GMT'), Sys.time() + 1 * 60 * 60)
	)

	dbExecute(conn, 'CREATE TABLE IF NOT EXISTS testthat_this (char_col VARCHAR(255), num_col INT, dt_col DATE, dttmtz_col TIMESTAMPTZ)')
	dbExecute(conn, 'TRUNCATE TABLE testthat_this')

	write_df_to_sql(conn, test_dataset, 'testthat_this')
	res = get_query(conn, 'SELECT * FROM testthat_this')

	expect_equal(nrow(res), 3)
	expect_equal(unname(unlist(lapply(res, class))), c('character', 'integer', 'Date', 'POSIXct', 'POSIXt'))

	on.exit(conn, dbExecute(conn, 'DROP TABLE IF EXISTS testthat_this'))
})
