from app.analytics.rules import abnormal_spike, quality_flag, safe_divide, suspicious_inviter


def test_safe_divide() -> None:
    assert safe_divide(1, 2) == 0.5
    assert safe_divide(1, 0) is None


def test_suspicious_inviter() -> None:
    assert suspicious_inviter(25, 0.05, 0, 0)
    assert suspicious_inviter(5, 0.4, 10, 0)
    assert not suspicious_inviter(5, 0.4, 0, 0)


def test_quality_flag() -> None:
    assert quality_flag(2, 1.0) == "insufficient_data"
    assert quality_flag(10, 0.7) == "high_quality"
    assert quality_flag(10, 0.4) == "normal"
    assert quality_flag(10, 0.1) == "low_quality"


def test_abnormal_spike() -> None:
    assert abnormal_spike(20, 9.9)
    assert not abnormal_spike(10, 10)
