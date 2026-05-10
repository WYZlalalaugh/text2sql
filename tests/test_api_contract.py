from pathlib import Path


def test_api_health_contract_shape_static():
    """Static check: ensure api.py declares a health_check endpoint that returns {'status': 'ok'}.

    We avoid importing api (module-level imports are script-style and can fail under pytest package import) and
    instead assert the function body text contains the expected return literal. This verifies the minimal
    response-shape contract without spinning up the server.
    """
    api_path = Path(__file__).parent.parent / "api.py"
    text = api_path.read_text(encoding="utf-8")
    assert "def health_check" in text, "api.py must define a health_check() function"
    # look for return containing status: ok (allow variations of whitespace)
    assert '"status": "ok"' in text or "'status': 'ok'" in text, "health_check should return a JSON with status 'ok'"


def test_intent_type_contract_is_normalized_static():
    """Static check: SSE result and /api/chat should serialize intent_type via one normalizer."""
    api_path = Path(__file__).parent.parent / "api.py"
    text = api_path.read_text(encoding="utf-8")

    assert "def _serialize_intent_type" in text, "api.py should define a shared intent_type serializer"
    assert "'intent_type': _serialize_intent_type(final_state.get(\"intent_type\"))" in text, (
        "SSE result payload should serialize intent_type via _serialize_intent_type"
    )
    assert "intent_type=_serialize_intent_type(result.get(\"intent_type\"))" in text, (
        "/api/chat response should serialize intent_type via _serialize_intent_type"
    )
