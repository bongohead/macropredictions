test_that('Async function', {

	skip_if_offline('catfact.ninja')

	req_list = map(1:5, function(x) request('https://catfact.ninja/fact'))

	resp_list = send_async_requests(req_list)

	expect_length(resp_list, 5)
	expect_length(map_chr(resp_list, function(x) resp_body_json(x)$fact), 5)
})
