#' Send multiple prompts to the OpenAI Responses API
#'
#' @param prompts_list A list of prompts. Each prompt should itself be a list of message objects, for example:
#'  list(
#'    list(role = 'system', content = 'You are a helpful assistant.'),
#'    list(role = 'user', content = 'Hello!')
#'  )
#' @param params A named list of additional request-body parameters, such as model, text, reasoning, max_output_tokens, etc.
#' @param .chunk_size The number of requests to send in a single asynchronous batch.
#' @param .max_conn The maximum number of connections to establish; equal to .chunk_size by default.
#' @param .max_retries The maximum number of retries to attempt before erroring out.
#' @param .api_key The OpenAI API key. Defaults to Sys.getenv('OPENAI_API_KEY').
#' @param .verbose If TRUE, outputs error statuses and progress.
#'
#' @return A list of parsed JSON responses in the same order as prompts_list.
#'
#' @description Sends multiple prompts to the OpenAI Responses API using the existing async request helper.
#'
#' @examples \dontrun{
#' prompts_list = list(
#'  list(
#'    list(role = 'system', content = 'You are a helpful assistant.'),
#'    list(role = 'user', content = 'Return your output as JSON. What is 1+1?')
#'  ),
#'  list(
#'    list(role = 'system', content = 'You are a helpful assistant'),
#'    list(role = 'user', content = 'What is 1+3? Return response as JSON.')
#'  )
#' )
#'
#' res = get_llm_responses_openai(
#'  prompts_list,
#'  params = list(
#'    model = 'gpt-5.2',
#'    text = list(format = list(type = 'json_object')),
#'    reasoning = list(effort = 'high')
#'  )
#' )
#' }
#'
#' @import purrr httr2
#'
#' @export
get_llm_responses_openai = function(
		prompts_list,
		params = list(model = 'gpt-5.3-codex', reasoning = list('effort': 'low')),
		.chunk_size = 3,
		.max_conn = .chunk_size,
		.max_retries = 3,
		.api_key = Sys.getenv('OPENAI_API_KEY'),
		.verbose = T
) {
	
	stopifnot(
		is.list(prompts_list),
		is.list(params),
		is.numeric(.chunk_size), length(.chunk_size) == 1,
		is.numeric(.max_conn), length(.max_conn) == 1,
		is.numeric(.max_retries), length(.max_retries) == 1,
		is.character(.api_key), length(.api_key) == 1,
		is.logical(.verbose), length(.verbose) == 1
	)
	
	if (.api_key == '') stop('Argument ".api_key" is empty!')
	
	url = 'https://api.openai.com/v1/responses'
	
	reqs_list = map(prompts_list, function(prompt) {
		
		payload = c(list(input = prompt), params)
		
		req = request(url) |>
			req_headers(
				'Authorization' = paste0('Bearer ', .api_key),
				'Content-Type' = 'application/json'
			) |>
			req_body_json(payload)
		
		return(req)
	})
	
	responses = send_async_requests(
		reqs_list = reqs_list,
		.chunk_size = .chunk_size,
		.max_conn = .max_conn,
		.max_retries = .max_retries,
		.verbose = .verbose
	)
	
	parsed_responses = map(responses, function(response) {
		return(resp_body_json(response, simplifyVector = F))
	})
	
	return(parsed_responses)
}
