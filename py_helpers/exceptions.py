class SkipRun(Exception):
    """
    Signal that the rest of the script should be skipped

    Description:
        Raise this when a precondition (e.g. "no new data") makes further script execution unnecessary, but
         does **not** constitute an error. The controller catches ``SkipRun`` and records the job as a success,
         with no error message.

        Don't pass any arguments into ``SkipRun``, since it won't be logged anyways.
    """
    pass
