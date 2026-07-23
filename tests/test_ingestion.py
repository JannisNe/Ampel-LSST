from itertools import cycle
from pathlib import Path

import fastavro
import pytest
import yaml

from ampel.abstract.AbsAlertFilter import AbsAlertFilter, AmpelAlertProtocol
from ampel.abstract.AbsAlertLoader import AbsAlertLoader
from ampel.alert.AlertConsumer import AlertConsumer
from ampel.dev.DevAmpelContext import DevAmpelContext
from ampel.model.UnitModel import UnitModel


class MockAlertLoader(AbsAlertLoader):
    alerts: list[dict]

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._it = iter(self.alerts)

    def _add_metadata(self, alert: dict) -> dict:
        alert["__kafka"] = {"alertId": alert["alertId"]}
        return alert

    def __next__(self):
        return self._add_metadata(next(self._it))


class MockFilter(AbsAlertFilter):
    pattern: list[bool]

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._cycle = cycle(self.pattern)

    def process(self, alert: AmpelAlertProtocol) -> None | bool | int:  # noqa: ARG002
        return next(self._cycle)


@pytest.fixture
def alert_consumer(mock_context: DevAmpelContext) -> AlertConsumer:
    with (Path(__file__).parent / "test-data" / "elasticc-consumer.yml").open() as f:
        model = UnitModel(**yaml.safe_load(f))
    # alerts from a single diaObject
    with (Path(__file__).parent / "test-data" / "11290844.avro").open("rb") as f:
        alerts = list(fastavro.reader(f))[:2]
    model.config["supplier"]["config"]["alert_identifier"] = "alertId"
    model.config["supplier"]["config"]["loader"] = UnitModel(
        unit="MockAlertLoader", config={"alerts": alerts}
    ).dict()
    # accept first alert in one channel only
    model.config["directives"][0]["filter"] = UnitModel(
        unit="MockFilter", config={"pattern": [True, True]}
    ).dict()
    model.config["directives"][1]["filter"] = UnitModel(
        unit="MockFilter", config={"pattern": [False, True]}
    ).dict()

    for c in "ElasticcLong", "ElasticcShort":
        mock_context.add_channel(c)
    mock_context.register_unit(MockAlertLoader)
    mock_context.register_unit(MockFilter)

    return mock_context.loader.new_context_unit(
        model=model,
        context=mock_context,
        sub_type=AlertConsumer,
    )


def test_muxer(mock_context: DevAmpelContext, alert_consumer: AlertConsumer):
    """
    A point T2 bound to a specific datapoint appears for both channels, even
    when the alert where the target datapoint first appeared was accepted by
    only one channel.
    """

    object.__setattr__(alert_consumer, "iter_max", 1)

    # insert datapoints from first alert into the database
    assert alert_consumer.run() == 1
    # inserts datapoints unique to second alert
    assert alert_consumer.run() == 1

    assert len(mock_context.db.get_collection("stock").find_one()["channel"]) == 2, (
        "stock in both channels"
    )
    assert (
        len(
            docs := list(
                mock_context.db.get_collection("t2").find({"unit": "T2GetDiaObject"})
            )
        )
        == 1
    ), "single point T2 doc found"
    assert len(set(docs[0]["channel"])) == 2, "t2 doc in both channels"


def test_message_ack(alert_consumer: AlertConsumer, mocker):
    """
    Alerts are explicitly acknowledged back to the loader
    """
    ack = mocker.patch.object(
        type(alert_consumer.alert_supplier.alert_loader), "acknowledge"
    )

    assert alert_consumer.run() == 2

    assert ack.call_count == 1, "exactly one batch of acks"
    # NB: alerts may be acked in arbitrary order
    alerts = sorted(ack.call_args[0][0], key=lambda d: d["__kafka"]["alertId"])
    assert len(alerts) == 2
    assert alerts == [
        {"__kafka": {"alertId": alert["alertId"]}}
        for alert in alert_consumer.alert_supplier.alert_loader.alerts
    ], "alerts acked"
