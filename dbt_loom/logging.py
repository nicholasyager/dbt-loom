try:
    import dbt_common.events.functions as dbt_event_function
    from dbt_common.events.types import Note
except ModuleNotFoundError:
    import dbt.events.functions as dbt_event_function  # type: ignore
    from dbt.events.types import Note  # type: ignore


def fire_event(*args, **kwargs) -> None:
    """Fire a dbt-core event."""
    dbt_event_function.fire_event(Note(*args, **kwargs))
