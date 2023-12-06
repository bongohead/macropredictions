#' Connect to a Postgres database
#'
#' @importFrom RPostgres Postgres
#' @importFrom DBI dbConnect
#'
#' @export
connect_pg = function() {

	check_env_variables(c('DB_DATABASE', 'DB_SERVER', 'DB_USERNAME', 'DB_PASSWORD', 'DB_PORT'))

	db = dbConnect(
		Postgres(),
		dbname = Sys.getenv('DB_DATABASE'),
		host = Sys.getenv('DB_SERVER'),
		port = Sys.getenv('DB_PORT'),
		user = Sys.getenv('DB_USERNAME'),
		password = Sys.getenv('DB_PASSWORD')
	)

	return(db)
}

#' Execute a select query
#'
#' @param db The DBI connector object
#' @param query The query
#'
#' @importFrom DBI dbGetQuery
#' @importFrom purrr is_scalar_character
#'
#' @export
get_query = function(db, query) {

	if (!inherits(db, 'PqConnection')) stop('Object "db" must be of class PgConnection')
	if (!is_scalar_character(query)) stop('Parameter "query" must be a character!')

	return(as_tibble(dbGetQuery(db, query)))
}

#' Disconnect from DB
#'
#' @param db The DBI connector object
#'
#' @importFrom DBI dbDisconnect
#'
#' @export
disconnect_db = function(db) {

	if (!inherits(db, 'PqConnection')) stop('Object "db" must be of class PgConnection')

	dbDisconnect(db)
}

#' Check largest table sizes in Postgres database.
#'
#' @param db A PostgreSQL DBI connection.
#'
#' @return A data frame of table sizes.
#'
#' @import dplyr
#'
#' @export
get_pg_table_sizes = function(db) {

	if (!inherits(db, 'PqConnection')) stop('Object "db" must be of class PgConnection')

	res = get_query(db, sql(
		"SELECT
			schema_name,
			relname,
			pg_size_pretty(table_size) AS size,
			table_size
		FROM (
			SELECT
				pg_catalog.pg_namespace.nspname AS schema_name,
				relname,
				pg_relation_size(pg_catalog.pg_class.oid) AS table_size
			FROM pg_catalog.pg_class
			JOIN pg_catalog.pg_namespace ON relnamespace = pg_catalog.pg_namespace.oid
		) t
		WHERE schema_name NOT LIKE 'pg_%'
		ORDER BY table_size DESC;"
	))
}

#' Helper function to check row count of a table in Postgres
#'
#' @description
#' Returns the number of rows in a table
#'
#' @param db The database connection object.
#' @param tablename The name of the table.
#'
#' @importFrom DBI dbGetQuery
#' @importFrom purrr is_scalar_character
#'
#' @export
get_rowcount = function(db, tablename) {

	if (!inherits(db, 'PqConnection')) stop('Object "db" must be of class PgConnection')
	if (!is_scalar_character(tablename)) stop('Parameter "tablename" must be a character!')

	count = as.numeric(dbGetQuery(db, paste0('SELECT COUNT(*) AS count FROM ', tablename, ''))$count)

	return(count)
}

#' Create a SQL INSERT query string.
#'
#' @param df A data frame to insert into SQL.
#' @param tblname The name of the SQL table.
#' @param .append Any string to append at the end of the query; useful for ON DUPLICATE statements.
#'
#' @return A character string representing a SQL query.
#'
#' @import dplyr
#' @importFrom tidyr unite
#' @importFrom lubridate is.POSIXct
#' @importFrom DBI dbQuoteString ANSI
#' @importFrom purrr is_scalar_character
#'
#' @export
create_insert_query = function(df, tblname, .append = '') {

	if (!is.data.frame(df)) stop('Parameter "df" must be a data.frame.')
	if (!is_scalar_character(.append)) stop('Parameter ".append" must be a character.')

	paste0(
		'INSERT INTO ', tblname, ' (', paste0(colnames(df), collapse = ','), ')\n',
		'VALUES\n',
		df %>%
			mutate(across(where(is.Date), as.character)) %>%
			mutate(across(where(is.POSIXct), function(x) format(x, '%Y-%m-%d %H:%M:%S %Z'))) %>%
			mutate(across(where(is.character), function(x) dbQuoteString(ANSI(), x))) %>%
			mutate(across(where(is.numeric), function(x) dbQuoteString(ANSI(), as.character(x)))) %>%
			unite(., 'x', sep = ',') %>%
			mutate(., x = paste0('(', x, ')')) %>%
			.$x %>%
			paste0(., collapse = ', '), '\n',
		.append, ';'
		) %>%
		return(.)
}

#' Inserts a dataframe into SQL
#'
#' @description
#' Writes a dataframe into SQL using an INSERT query.
#'
#' @param df A data frame to insert into SQL.
#' @param tblname The name of the SQL table.
#' @param .append Any additional characters to append to the end of the query.
#' @param .chunk_size The number of rows to send at a time.
#' @param .verbose Whether to log progress of the SQL write.
#'
#' @return A count of the number of rows added.
#'
#' @import dplyr
#' @importFrom purrr map_dbl is_scalar_logical is_scalar_double is_scalar_integer is_scalar_character
#' @importFrom DBI dbExecute
#'
#' @export
write_df_to_sql = function(db, df, tblname, .append = '', .chunk_size = 1000, .verbose = F) {

	if (!inherits(db, 'PqConnection')) stop('Object "db" must be of class PgConnection')
	if (!is.data.frame(df)) stop('Parameter "df" must be a data.frame.')
	if (!is_scalar_character(tblname)) stop('Parameter "tblname" must be a character.')
	if (!is_scalar_character(.append)) stop('Parameter ".append" must be a character.')
	if (!is_scalar_double(.chunk_size) && !is_scalar_integer(.chunk_size)) stop('Parameter ".chunk_size" must be an integer.')
	if (!is_scalar_logical(.verbose)) stop('Parameter ".verbose" must be a logical.')

	rows_modified_by_chunk =
		df %>%
		mutate(., split = ceiling((1:nrow(df))/.chunk_size)) %>%
		group_split(., split, .keep = FALSE) %>%
		map_dbl(., .progress = .verbose, function(x) {
			dbExecute(db, create_insert_query(x, tblname = tblname, .append = .append))
			})

	if (any(is.null(rows_modified_by_chunk))) {
		stop('SQL Error!')
	}

	return(sum(rows_modified_by_chunk))
}


#' Store forecast values in forecast_values_v2 table
#'
#' This breaks up the dataframe into chunks of size 10000 and sends them into the database.
#'
#' @param db The database conenction object.
#' @param df The dataframe of forecast values. Must include columns forecast, vdate, form, freq, varname, and date.
#' @param .store_new_only Boolean; whether to *only* store rows where the combination of (forecast, vdate) doesn't already exist in the database. Defaults to F.
#' @param .verbose Echo error messages.
#'
#' @return The number of rows added
#'
#' @importFrom dplyr mutate group_split
#' @importFrom lubridate today
#' @importFrom DBI dbExecute
#' @importFrom purrr is_scalar_logical
#'
#' @export
store_forecast_values_v2 = function(db, df, .store_new_only = F, .verbose = F) {

	if (!inherits(db, 'PqConnection')) stop('Object "db" must be of class PgConnection')
	if (!is.data.frame(df)) stop('Parameter "df" must be a data.frame.')
	if (
		length(colnames(df)) != 7 ||
		!all(sort(colnames(df)) == sort(c('forecast', 'vdate', 'freq', 'form', 'varname', 'date', 'value')))
		) {
		stop('Incorrect columns')
	}
	if (!is_scalar_logical(.store_new_only)) stop('Parameter ".store_new_only" must be a logical.')
	if (!is_scalar_logical(.verbose)) stop('Parameter ".verbose" must be a logical.')

	initial_count = get_rowcount(db, 'forecast_values_v2')
	if (.verbose == T) message('***** Initial Count: ', initial_count)

	today_string = today('US/Eastern')

	# Time misalignment issue - bneed to use forecast x vdate x varname combination instead of forecast x vdate,
	# due to some variables coming in later for the same forecast
	existing_forecast_combinations = get_query(db, sql("SELECT forecast, varname, vdate FROM forecast_values_v2 GROUP BY 1, 2, 3"))
	store_df = anti_join(df, existing_forecast_combinations, by = c('forecast', 'varname', 'vdate'))

	if (nrow(store_df) > 0) {

		insert_result = write_df_to_sql(
			db,
			mutate(store_df, mdate = today_string),
			'forecast_values_v2',
			'ON CONFLICT (mdate, forecast, vdate, form, freq, varname, date) DO UPDATE SET value=EXCLUDED.value',
			.chunk_size = 10000,
			.verbose = .verbose
			)
	}


	final_count = get_rowcount(db, 'forecast_values_v2')
	rows_added = final_count - initial_count


	if (.verbose == T) message('***** Rows Added: ', rows_added)

	if (rows_added != 0) {

		if (.verbose == T) message('***** Refreshing Views')
		initial_count_all = get_rowcount(db, 'forecast_values_v2_all')
		initial_count_latest = get_rowcount(db, 'forecast_values_v2_latest')
		dbExecute(db, 'REFRESH MATERIALIZED VIEW CONCURRENTLY forecast_values_v2_all;')
		dbExecute(db, 'REFRESH MATERIALIZED VIEW CONCURRENTLY forecast_values_v2_latest;')
		rows_added_all = get_rowcount(db, 'forecast_values_v2_all') - initial_count_all
		rows_added_latest = get_rowcount(db, 'forecast_values_v2_latest') - initial_count_latest

	} else {
		rows_added_all = 0
		rows_added_latest = 0

	}

	return(list(forecast_values = rows_added, forecast_values_all = rows_added_all, forecast_values_latest = rows_added_latest))
}



#' Store forecast values in forecast_hist_values_v2 table
#'
#' This breaks up the dataframe into chunks of size 10000 and sends them into the database.
#'
#' @param db The database conenction object.
#' @param df The dataframe of forecast values. Must include columns forecast, vdate, form, freq, varname, and date.
#' @param .verbose Echo error messages.
#'
#' @return The number of rows added
#'
#' @import dplyr
#' @importFrom DBI dbExecute
#' @importFrom purrr is_scalar_logical
#'
#' @export
store_forecast_hist_values_v2 = function(db, df, .verbose = F) {

	if (!inherits(db, 'PqConnection')) stop('Object "db" must be of class PgConnection')
	if (!is.data.frame(df)) stop('Parameter "df" must be a data.frame.')
	if (
		length(colnames(df)) != 6 ||
		!all(sort(colnames(df)) == sort(c('vdate', 'freq', 'form', 'varname', 'date', 'value')))
	) {
		stop('Incorrect columns')
	}
	if (!is_scalar_logical(.verbose)) stop('Parameter ".verbose" must be a logical.')

	initial_count = get_rowcount(db, 'forecast_hist_values_v2')
	if (.verbose == T) message('***** Initial Count: ', initial_count)

	insert_result = write_df_to_sql(
		db,
		df,
		'forecast_hist_values_v2',
		'ON CONFLICT (vdate, form, freq, varname, date) DO UPDATE SET value=EXCLUDED.value',
		.chunk_size = 10000,
		.verbose = .verbose
	)

	final_count = get_rowcount(db, 'forecast_hist_values_v2')
	rows_added = final_count - initial_count
	if (.verbose == T) message('***** Rows Added: ', rows_added)

	if (rows_added != 0) {
		if (.verbose == T) message('***** Refreshing Views')
		initial_count_latest = get_rowcount(db, 'forecast_hist_values_v2_latest')
		dbExecute(db, 'REFRESH MATERIALIZED VIEW CONCURRENTLY forecast_hist_values_v2_latest;')
		rows_added_latest = get_rowcount(db, 'forecast_hist_values_v2_latest') - initial_count_latest
	} else {
		rows_added_latest = 0
	}

	return(list(forecast_hist_values = rows_added, forecast_hist_values_latest = rows_added_latest))
}


#' Store futures values in interest_rate_model_futures_values table
#'
#' This breaks up the dataframe into chunks of size 10000 and sends them into the database.
#'
#' @param db The database conenction object.
#' @param df The dataframe of forecast values. Must include columns scrape_source, varname, expdate, tenor, vdate, and value.
#' @param .verbose Echo error messages.
#'
#' @return The number of rows added
#'
#' @import dplyr
#' @importFrom DBI dbExecute
#' @importFrom purrr is_scalar_logical
#'
#' @export
store_futures_values = function(db, df, .verbose = F) {

	if (!inherits(db, 'PqConnection')) stop('Object "db" must be of class PgConnection')
	if (!is.data.frame(df)) stop('Parameter "df" must be a data.frame.')
	if (
		length(colnames(df)) != 6 ||
		!all(sort(colnames(df)) == sort(c('scrape_source', 'varname', 'expdate', 'tenor', 'vdate', 'value')))
	) {
		stop('Incorrect columns')
	}
	if (!is_scalar_logical(.verbose)) stop('Parameter ".verbose" must be a logical.')

	initial_count = get_rowcount(db, 'interest_rate_model_futures_values')
	if (.verbose == T) message('***** Initial Count: ', initial_count)

	insert_result = write_df_to_sql(
		db,
		df,
		'interest_rate_model_futures_values',
		'ON CONFLICT (scrape_source, varname, expdate, tenor, vdate) DO UPDATE SET value=EXCLUDED.value',
		.chunk_size = 10000,
		.verbose = .verbose
	)

	final_count = get_rowcount(db, 'interest_rate_model_futures_values')
	rows_added = final_count - initial_count
	if (.verbose == T) message('***** Rows Added: ', rows_added)

	return(list(interest_rate_model_futures_values = rows_added))
}

