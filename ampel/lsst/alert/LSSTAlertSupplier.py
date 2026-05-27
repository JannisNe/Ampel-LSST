#!/usr/bin/env python
# File              : Ampel-LSST/ampel/lsst/alert/LSSTAlertSupplier.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 20.04.2021
# Last Modified Date: 21.03.2022
# Last Modified By  : Marcus Fenner <mf@physik.hu-berlin.de>

from collections.abc import Generator, Iterator
from itertools import chain
from typing import Literal

from ampel.alert.AmpelAlert import AmpelAlert
from ampel.alert.BaseAlertSupplier import BaseAlertSupplier
from ampel.protocol.AmpelAlertProtocol import AmpelAlertProtocol
from ampel.view.ReadOnlyDict import ReadOnlyDict


class DIAObjectMissingError(Exception):
    """
    Raised when there is no DIAObject in the alert
    """

    ...


# Translate 2022-era field names to schema 7.x
_field_upgrades: dict[str, str] = {
    "midPointTai": "midpointMjdTai",
    "psFlux": "psfFlux",
    "psFluxErr": "psfFluxErr",
    "filterName": "band",
    "decl": "dec",
    "ccdVisitId": "visit",
}


class LSSTAlertSupplier(BaseAlertSupplier):
    """
    Iterable class that, for each alert payload provided by the underlying alert_loader,
    returns an AmpelAlert instance.
    """

    # Override default
    deserialize: None | Literal["avro", "json"] = "avro"

    max_history: float = float("inf")

    alert_identifier: Literal["diaSourceId", "alertId"] = "diaSourceId"

    forced_source_overwrite: bool = True

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._emitted_any = False

    @staticmethod
    def _shape_dp(d: dict) -> ReadOnlyDict:
        return ReadOnlyDict({_field_upgrades.get(k, k): v for k, v in d.items()})

    @classmethod
    def _get_sources(
        cls, alert: dict, max_history: float, forced_source_overwrite: bool = True
    ) -> Generator[dict, None, None]:
        """
        yield one photometric point per visit, preferring forced photometry to
        difference imaging and taking upper limits as a last resort
        """
        diaSource = cls._shape_dp(alert["diaSource"])
        yield diaSource

        visits: set[int] = set()
        t0 = diaSource["midpointMjdTai"] - max_history
        for dp in map(
            cls._shape_dp,
            chain(
                alert.get("prvDiaForcedSources") or (),
                alert.get("prvDiaSources") or (),
                alert.get("diaNondetectionLimit") or (),
            ),
        ):
            if ((visit := dp["visit"]) not in visits or not forced_source_overwrite) and dp["midpointMjdTai"] >= t0:
                yield dp
                visits.add(visit)

    @classmethod
    def _shape(
        cls,
        d: dict,
        max_history: float = float("inf"),
        alert_identifier: Literal["diaSourceId", "alertId"] = "diaSourceId",
        forced_source_overwrite: bool = True,
    ) -> AmpelAlertProtocol:
        if diaObject := d.get("diaObject"):
            dps = (
                *cls._get_sources(d, max_history=max_history, forced_source_overwrite=forced_source_overwrite),
                cls._shape_dp(diaObject),
            )
            # Add base alert information to extras field
            extras = {}
            for alertprop in ["observation_reason", "target_name"]:
                if val := d.get(alertprop):
                    extras[alertprop] = val
            if kafka := d.get("__kafka"):
                extras["kafka"] = kafka
            return AmpelAlert(
                id=d[
                    alert_identifier
                ],  # ID of the triggering DiaSource - use as alert id?
                stock=diaObject["diaObjectId"],  # internal ampel id
                datapoints=dps,
                extra=extras if len(extras) > 0 else None,
            )
        raise DIAObjectMissingError

    def acknowledge(self, alerts: Iterator[AmpelAlertProtocol]) -> None:
        # invert transformation applied in _shape()
        self.alert_loader.acknowledge(
            {"__kafka": alert.extra["kafka"]}  # type: ignore[misc]
            for alert in alerts
            if alert.extra and "kafka" in alert.extra
        )

    def __next__(self) -> AmpelAlertProtocol:
        """
        :returns: a dict with a structure that AlertConsumer understands
        :raises StopIteration: when alert_loader dries out.
        :raises AttributeError: if alert_loader was not set properly before this method is called
        """
        while True:
            d = self._deserialize(next(self.alert_loader))

            try:
                alert = self._shape(d, self.max_history, self.alert_identifier, self.forced_source_overwrite)
                self._emitted_any = True
                return alert
            except DIAObjectMissingError:
                # silently skip over SSObjects
                if not self._emitted_any:
                    # if we have not yet emitted any alerts, we can be sure that
                    # all previous messages were handled, and acknowledge
                    # immediately. this avoids an edge case where the source
                    # partition only contains SSObject messages that will never
                    # be acknowledged, and so consumed over and over until a
                    # different message arrives
                    self.alert_loader.acknowledge(iter([d]))  # type: ignore[list-item]
                continue
