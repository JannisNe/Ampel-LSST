#!/usr/bin/env python
# File              : Ampel-LSST/ampel/lsst/ingest/LSSTDataPointShaper.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 20.04.2021
# Last Modified Date: 21.03.2022
# Last Modified By  : Marcus Fenner <mf@physik.hu-berlin.de>

from collections.abc import Iterable
from typing import Any

from ampel.model.operator.AnyOf import AnyOf
from ampel.model.operator.AllOf import AllOf
from bson import encode

from ampel.abstract.AbsT0Unit import AbsT0Unit
from ampel.content.DataPoint import DataPoint
from ampel.types import StockId
from ampel.util.hash import hash_payload

from .FieldSelection import FieldSelection, StringFilter


class LSSTDataPointShaper(AbsT0Unit):
    """
    This class 'shapes' datapoints in a format suitable
    to be saved into the ampel database
    """

    digest_size: int = 8  # Byte width of datapoint ids

    fields: FieldSelection = FieldSelection.default()
    # Mandatory implementation

    combine_flags: dict[str, list[str]] | None = None

    def process(  # type: ignore[override]
        self, arg: Iterable[dict[str, Any]], stock: StockId
    ) -> list[DataPoint]:
        """
        :param arg: sequence of unshaped dps
        """

        ret_list: list[DataPoint] = []
        # Record forced photometry for comp.
        sourceid_list: set[int | str] = set()
        setitem = dict.__setitem__

        for photo_dict in arg:
            tags = ["LSST"]
            if "band" in photo_dict:
                setitem(photo_dict, "band", photo_dict["band"].lower())
                tags.append("LSST_" + photo_dict["band"].upper())
            """
            Non detection limit don't have an identifier.
            """

            selection: None | StringFilter = None

            if "diaSourceId" in photo_dict:
                tags.append("LSST_DP")
                sourceid_list.add(photo_dict["diaSourceId"])
                selection = self.fields.diaSource
            elif "diaForcedSourceId" in photo_dict:
                tags.append("LSST_FP")
                selection = self.fields.diaForcedSource
            elif "diaObjectId" in photo_dict:  # DiaObject
                # diaObjectId is also used in (prv)diaSource and diaForcedPhotometry
                # if other fields are added, check if they contain diaObjectId
                tags.append("LSST_OBJ")
                selection = self.fields.diaObject
            else:
                # Nondetection Limit
                tags.append("LSST_ND")
                selection = self.fields.diaNondetectionLimit

            if self.combine_flags:
                photo_dict = dict(photo_dict)
                for combined_flag, flag_combination in self.combine_flags.items():
                    combination_values = [photo_dict.pop(flag) for flag in flag_combination if flag in photo_dict]
                    if len(combination_values) > 0:
                        photo_dict[combined_flag] = any(combination_values)

            if selection:
                body = {k: photo_dict[k] for k in selection.filter(photo_dict)}
            else:
                body = photo_dict

            id = hash_payload(
                encode(dict(sorted(body.items()))),
                size=-self.digest_size * 8,
            )
            ret_list.append(
                {"id": id, "stock": stock, "tag": tags, "body": body}  # type: ignore[typeddict-item]
            )

        # Current alert format allows for the same dp to be provided as
        # both source and forcedsource. If so, we here choose FP.
        # (flux values should consistently be the same)
        return [
            dp
            for dp in ret_list
            if not (
                dp["body"].get("diaForcedSourceId") in sourceid_list
                and "LSST_FP" in dp["tag"]
            )
        ]
