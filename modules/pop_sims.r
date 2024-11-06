library(dplyr)
library(ggplot2)

# Population simulation function with initial age distribution and age-specific fertility rates
simulate_population_growth <- function(initial_population, asfr, years) {
	
	# Initialize population distribution by age
	age_distribution <- data.frame(
		age = 0:100,
		population = ifelse(0:100 <= 80, initial_population * dnorm(0:100, mean = 30, sd = 15), 0)
	) %>%
		mutate(population = round(population))
	
	# Define age-specific fertility rates for ages 15 to 49 (assuming zero outside these ages)
	fertility_rates <- c(rep(0, 15), asfr, rep(0, 100 - 49))
	
	# Simulation loop
	population_data <- list()
	total_population_over_time <- numeric(years + 1)  # To store total population each year
	population_data[[1]] <- age_distribution
	total_population_over_time[1] <- sum(age_distribution$population)
	
	for (year in 1:years) {
		# Create a new age distribution data frame for the year
		new_population <- data.frame(age = 0:100, population = rep(0, 101))
		
		# Calculate births
		reproductive_pop <- sum(population_data[[year]]$population[16:50] * fertility_rates[16:50])
		new_population$population[1] <- round(reproductive_pop)
		
		# Age the population and apply "mortality" at age 80
		for (i in 2:101) {
			if (i <= 80) {
				new_population$population[i] <- population_data[[year]]$population[i - 1]
			} else {
				new_population$population[i] <- 0  # Mortality after age 80
			}
		}
		
		# Store the year’s population distribution
		population_data[[year + 1]] <- new_population
		total_population_over_time[year + 1] <- sum(new_population$population)
	}
	
	# Combine results into a single data frame for visualization
	population_data_df <- do.call(rbind, lapply(1:(years + 1), function(year) {
		data.frame(year = year - 1, age = 0:100, population = population_data[[year]]$population)
	}))
	
	# Plot 1: Population structure over time (age distribution)
	plot1 <- ggplot(population_data_df, aes(x = age, y = population, color = as.factor(year))) +
		geom_line() +
		labs(title = "Population Age Structure Over Time",
				 x = "Age", y = "Population", color = "Year") +
		theme_minimal()
	
	# Plot 2: Total population over time
	total_population_df <- data.frame(year = 0:years, total_population = total_population_over_time)
	plot2 <- ggplot(total_population_df, aes(x = year, y = total_population)) +
		geom_line() +
		labs(title = "Total Population Over Time",
				 x = "Year", y = "Total Population") +
		theme_minimal()
	
	# Display both plots
	print(plot1)
	print(plot2)
}

# Example usage:

# Initial population size
initial_population <- 1e6  # 1 million people

# Age-specific fertility rates (example rates for ages 15-49)
asfr <- c(0.02, 0.08, 0.1, 0.12, 0.11, 0.09, 0.07, 0.04, 0.02, 0.01, 0.005)  # Fertility rates for ages 15-49

# Number of years to project
years <- 20

# Run the population simulation
simulate_population_growth(initial_population, asfr, years)

