from producer.generate_events import create_event


def test_normal_event_has_required_identifier():
    event = create_event(False)
    assert event["event_id"]
    assert event["user_id"].endswith("@example.com")
    assert event["label"] == "normal"


def test_anomaly_has_high_risk_indicators():
    event = create_event(True)
    assert event["bytes_transferred"] >= 750_000_000
    assert event["country"] in {"KP", "RU", "CN"}
    assert event["privileged_account"] is True
    assert event["data_classification"] == "restricted"

