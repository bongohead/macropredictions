import asyncio

def async_run(coro):
    """
    This allows async runs from both Jupyter/VSCode chunks as well as calling the script from CLI.
    
    Description:
        This is equivalent to:
        - In a script/CLI (no active loop) -> asyncio.run()
        - In Jupyter/VS Code (loop already running) -> await via run_until_complete()
    
    Params:
        @coro: The coroutine to pass.

    Returns:
        The coroutine's result.
    """
    try:
        loop = asyncio.get_running_loop() # ⇐ raises if no loop
    except RuntimeError: # No loop -> plain script
        return asyncio.run(coro)

    # Running loop detected (Jupyter/VSCode)
    # run_until_complete normally forbids nesting, so we patch it once:
    if not getattr(loop, "_nest_asyncio_patched", False):
        try:
            import nest_asyncio
            nest_asyncio.apply(loop)
            loop._nest_asyncio_patched = True
        except ImportError: # fall back: schedule + wait
            return asyncio.create_task(coro)   # user can `await` this Task
    return loop.run_until_complete(coro)