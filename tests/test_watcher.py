from src.jobs.rules import WatcherState


def test_watcher_state_initializes_user():
    state = WatcherState()

    u = state.get_user("user_1")

    assert u.deposit_times is not None
    assert u.breach_times is not None