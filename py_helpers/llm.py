import os
import asyncio
import aiohttp
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

async def _send_async_batch_llm_requests(prompts: list[list], url: str, headers: dict, params: dict, batch_size: int, max_retries: int, verbose: bool | None = None):
    """
    Core internal function to handle batching, retries, and making requests. Don't call this function directly, use the functions below instead.

    Params:
        @prompts: A lists of prompts, where each prompt is a list of messages to send in the request.
        @url: The endpoint.
        @headers: Any headers to send to the endpoint.
        @params: Any other POST parameters to send to the endpoint.
        @batch_size: Max number of prompts to group in a single batch. Prompts in a batch are sent concurrently.
        @max_retries: Max number of retries on failed prompt calls.
        @verbose: Whether to print the progress.
        
    Example:
        prompts_list = [
            [{'role': 'system', 'content': 'You are a math assistant. Answer all questions with a thorough explanation.'}, {'role': 'user', 'content': 'What is 1+1?'}],
            [{'role': 'system', 'content': 'You are a math assistant. Answer all questions with a thorough explanation.'}, {'role': 'user', 'content': 'What is 1+2?'}]
        ]
        await send_batch_llm_requests(
            prompts_list,
            url = 'https://api.openai.com/v1/chat/completions'
            headers = {"Authorization": f"Bearer {api_key or os.environ.get('OPENAI_API_KEY')}",},
            batch_size = 2,
            max_retries = 1,
            verbose = True
        )
    """
    async def make_request(session, prompt):
        async with session.post(url, headers=headers, json={**{'messages': prompt}, **params}) as response:
            return await response.json()

    async def retry_requests(req_prompts, total_retries=0):
        if total_retries > max_retries:
            raise Exception('Requests failed')

        if total_retries > 0:
            print(f'Retry {total_retries} for {len(req_prompts)} failed requests!')
            await asyncio.sleep(2 * 2 ** total_retries)  # Backoff rate

        async with aiohttp.ClientSession() as session:
            results = await asyncio.gather(
                *(make_request(session, prompt) for prompt in req_prompts),
                return_exceptions=True
            )

        successful_responses = [result for result in results if not isinstance(result, Exception)]
        failed_requests = [request for request, result in zip(req_prompts, results) if isinstance(result, Exception)]

        if failed_requests:
            print([result for result in results if isinstance(result, Exception)])
            retry_responses = await retry_requests(failed_requests, total_retries + 1)
            successful_responses.extend(retry_responses)

        return successful_responses

    chunks = [prompts[i:i + batch_size] for i in range(0, len(prompts), batch_size)]
    verbose = True if verbose is True or (verbose is None and len(chunks) > 1) else False

    responses = [await retry_requests(chunk) for chunk in tqdm(chunks, disable=not verbose)]
    parsed_responses = [item for sublist in responses for item in sublist]  # Flatten the list

    if len(parsed_responses) != len(prompts):
        raise Exception('Error: length of output not the same as input!')

    return parsed_responses



async def get_llm_responses_openai(prompts: list[list], params: dict = {'model': 'gpt-4o-mini'}, batch_size: int = 3, max_retries: int = 3, api_key: str = os.environ.get('OPENAI_API_KEY'), verbose: bool | None = None):
    """
    Asynchronously send a list of LLM prompts to the OpenAI API endpoint directly

    Params:
        @prompts: A lists of prompts, where each prompt is a list of messages to send in the request.
        @params: Anything other than the messages to pass into the request body, such as model or temperature. See https://platform.openai.com/docs/api-reference/chat/create.
        @batch_size: Max number of prompts to group in a single batch. Prompts in a batch are sent concurrently.
        @max_retries: Max number of retries on failed prompt calls.
        @api_key: The OpenAI API key.
        
    Example:
        prompts_list = [
            [{'role': 'system', 'content': 'You are a math assistant. Answer all questions with a thorough explanation.'}, {'role': 'user', 'content': 'What is 1+1?'}],
            [{'role': 'system', 'content': 'You are a math assistant. Answer all questions with a thorough explanation.'}, {'role': 'user', 'content': 'What is 1+2?'}]
        ]
        
        await get_llm_responses_openai(prompts_list, {'model': 'gpt-4o-mini', 'temperature': 0.5})
    """

    url = 'https://api.openai.com/v1/chat/completions'
    headers = {
        "Authorization": f"Bearer {api_key}",
    }

    return await _send_async_batch_llm_requests(prompts, url, headers, params, batch_size, max_retries, verbose)
