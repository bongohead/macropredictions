#' Calculate Diebold-Li fit of a yield curve
#'
#' @description Calculates the Diebold-Li (2007) fit by date. A linear regression will be fitted for each `date`.
#'
#' @param df: A data frame continuing columns with names `date`,
#'  `value` (containing curve values), and `ttm` (containing maturity durations).
#' @param return_all: Boolean; if F, will return only the MSE (useful for optimization).
#'  Otherwise, will return a tibble containing fitted values, residuals, and the beta coefficients.
#'
#' @details
#'  12/21/22: data.table refactor, significant speed improvements (600ms optim -> 100ms)
#'  12/27/23: Replaced lm with lm.fit, 100ms -> 70ms
#'
#' @examples \dontrun{
#' ## Example: suppose we want to fit the yield curve on 12/26/23
#'
#' # Get data
#' raw_data = select(get_treasury_yields(), date, ttm, value)
#' test_data = filter(raw_data, date == '2023-12-26')
#'
#' # Estimate coefficients, assuming lambda = .0609 as in the Diebold Li (2007) paper
#' coefs = estimate_diebold_li_fit(test_data, .0609, return_all = T)
#'
#' # Fill in the rest of the curve using the fitted coefficients
#' fit =
#'  distinct(coefs, date, b1, b2, b3, lambda) %>%
#'  expand_grid(ttm = 1:360) %>%
#'  mutate(., value = get_dns_fit(b1, b2, b3, lambda, ttm)) %>%
#'  select(ttm, value)
#'
#' # Compare fitted to actual
#' bind_rows(mutate(test_data, type = 'actual'), mutate(fit, type = 'fitted')) %>%
#'  ggplot() +
#'  geom_point(aes(x = ttm, y = value, group = type, color = type))
#'
#' ## Example: suppose now we want to optimize lambda instead of always using .0609
#'
#' # Get prior training data
#' train_data = filter(raw_data, date >= '2023-09-26' & date <= '2023-12-26')
#'
#' # Use MLE
#' opt_lambda = optimize(
#'  estimate_diebold_li_fit, df = test_data, return_all = F,
#'  interval = c(-.5, .5),
#'  maximum = F
#'  )$minimum
#'
#'  # Now we can again get the coefficients using this new lambda
#' coefs_opt = estimate_diebold_li_fit(test_data, opt_lambda, return_all = T)
#'
#' fit_opt =
#'  distinct(coefs_opt, date, b1, b2, b3, lambda) %>%
#'  expand_grid(ttm = 1:360) %>%
#'  mutate(., value = get_dns_fit(b1, b2, b3, lambda, ttm)) %>%
#'  select(ttm, value)
#'
#' bind_rows(
#'  mutate(test_data, type = 'actual'),
#'  mutate(fit, type = 'fitted'),
#'  mutate(fit_opt, type = 'fitted_optimized')
#'  ) %>%
#'  ggplot() +
#'  geom_point(aes(x = ttm, y = value, group = type, color = type))
#'
#' # Speed benchmarking
#' microbenchmark::microbenchmark(
#'  estimate_diebold_li_fit(train_data, lambda = .0609),
#'  times = 10,
#'  unit = 'ms'
#'  )
#' }
#'
#' @name curve-helpers
NULL


#' @rdname curve-helpers
#' @import dplyr
#' @importFrom data.table := rbindlist as.data.table
#' @export
estimate_diebold_li_fit = function(df, lambda, return_all = FALSE, regularization = 1e-5) {
	df %>%
		as.data.table(.) %>%
		.[, c('f1', 'f2') := list(1, (1 - exp(-1 * lambda * ttm))/(lambda * ttm))] %>%
		.[, f3 := f2 - exp(-1*lambda*ttm)] %>%
		split(., by = 'date') %>%
		lapply(., function(x) {
			reg = lm.fit(as.matrix(x[, c('f1', 'f2', 'f3')]), x$value)
			x %>%
				.[, fitted := fitted(reg)] %>%
				.[, c('b1', 'b2', 'b3') := list(coef(reg)[['f1']], coef(reg)[['f2']], coef(reg)[['f3']])] %>%
				.[, resid := value - fitted] %>%
				.[, lambda := lambda]
		}) %>%
		rbindlist(.) %>%
		{
			if (return_all == FALSE) mean(.$resid^2) + regularization*(mean(.$f1^2) + mean(.$f2^2) + mean(.$f3^2))
			else as_tibble(.)
		}
}

#' @rdname curve-helpers
#' @export
get_dns_fit = function(b1, b2, b3, lambda, ttm) {
	b1 +
		b2 * (1-exp(-1 * lambda * ttm))/(lambda * ttm) +
		b3 *((1-exp(-1 * lambda * ttm))/(lambda * ttm) - exp(-1 * lambda * ttm))
}
