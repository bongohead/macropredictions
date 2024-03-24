from dotenv import load_dfrom typing import Callable
import asyncio
import aiohttp
from tqdm import tqdm

async def send_async_requests(inputs: list[dict], request_generator: Callable, batch_size: int = 10, max_retries: int = 3):
    """
    Params:
        @inputs: A list of dicts, where each element corresponds to a single request.
        @request_generator: A function with two arguments (session, inputs), which takes an input dictionary and returns a asyncio session response.
        @batch_size: Max number of prompts to group in a single batch. Prompts in a batch are sent concurrently.
        @max_retries: Max number of retries on failed prompt calls.
        
    Example:
        # Example 1: GET request returning JSON
        async def req_gen(session, input):
            url = 'https://jmdv.sandbox.indeed.net/api/jobs/' + input['jobkey'] + '/'
            headers = {'Content-Type': 'application/json'}
            async with session.get(url, headers = headers) as response:
                return await response.json(content_type='text/html')
                
        res = await send_async_requests(
            inputs = [{"jobkey": "8866999381"}],
            request_generator = req_gen
        )

        # Example 2: POST request returning JSON
        async def req_gen(session, input):
            url = 'https://llm-proxy.sandbox.indeed.net/openai/v1/chat/completions'
            headers = {
                "Authorization": "Bearer " + api_key,
                "OpenAI-Organization": 'org-SFf9IpnK1hAwQf3Aq1oa7k90',   
            }
            async with session.post(url, headers = headers, json = {'messages': input['prompt']}, 'model': 'gpt-3.5-turbo'}) as response:
                return await response.json()
                
        res = await send_async_requests(
            inputs = [{"prompt": [...]}, {"prompt": [...]}] 
            request_generator = req_gen
        )

    """
    async def retry_requests(req_prompts, total_retries = 0):
        if total_retries > max_retries:
            raise Exception('Requests failed')
        if total_retries > 0:
            print(f'Retry {total_retries} for {len(req_prompts)} failed requests')
            await asyncio.sleep(2 * 2 ** total_retries) # Backoff rate 
    
        async with aiohttp.ClientSession() as session:
            results = await asyncio.gather(*(request_generator(session, prompt) for prompt in req_prompts), return_exceptions = True)
        
        successful_responses = [result for result in results if not isinstance(result, Exception)]
        failed_requests = [request for request, result in zip(req_prompts, results) if isinstance(result, Exception)]
    
        if failed_requests:
            print([result for result in results if isinstance(result, Exception)])
            retry_responses = await retry_requests(failed_requests, total_retries + 1)
            successful_responses.extend(retry_responses)
    
        return successful_responses

    # Split into batches
    chunks = [inputs[i:i + batch_size] for i in range(0, len(inputs), batch_size)]
    
    # For each chunk, send requests and retry any failed elements
    responses = [await retry_requests(chunk) for chunk in tqdm(chunks)]

    parsed_responses = [item for sublist in responses for item in sublist]  # Flatten the list

    if len(parsed_responses) != len(inputs):
        raise Exception('Error: output length does not match your input length.')

    return parsed_responses