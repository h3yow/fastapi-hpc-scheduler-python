def test_skeleton_imports():
    import app.main  # noqa: F401
    import app.service  # noqa: F401
    import app.scheduler  # noqa: F401
    import app.executor  # noqa: F401
    import app.storage  # noqa: F401
    import app.errors  # noqa: F401
    import app.models  # noqa: F401
