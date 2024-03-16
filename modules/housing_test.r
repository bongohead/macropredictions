
# Initialize --------------------------------------------------------------------


## Load Libs ---------------------------------------------------------------
library(macropredictions)
library(tidyverse)
library(httr2)


## Load Data ---------------------------------------------------------------
local({

	import_df = read_csv('https://econdata.s3-us-west-2.amazonaws.com/Reports/Core/RDC_Inventory_Core_Metrics_County_History.csv')

	cleaned_df =
		import_df %>%
		head(., -1) %>%
		rename(., date = month_date_yyyymm, fips = county_fips, county = county_name) %>%
		mutate(
			.,
			date = ymd(paste0(date, '01')),
			fips = str_pad(fips, 5, pad = '0')
			) %>%
		pivot_longer(., cols = -c(date, fips, county), names_to = 'varname', values_to = 'value', values_drop_na = T) %>%
		filter(., !str_detect(varname, 'mm|yy'))

})


muscogee =
	cleaned_df %>%
	filter(., county == 'muscogee, ga')

muscogee %>%
	ggplot() +
	geom_line(aes(x = date, y = value)) +
	facet_wrap(vars(varname), scales = 'free')
