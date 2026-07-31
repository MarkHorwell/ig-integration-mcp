from __future__ import annotations

from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator


def to_camel(name: str) -> str:
    first, *rest = name.split("_")
    return first + "".join(word.title() for word in rest)


class Direction(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


class OrderType(StrEnum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"
    QUOTE = "QUOTE"


class TimeInForce(StrEnum):
    EXECUTE_AND_ELIMINATE = "EXECUTE_AND_ELIMINATE"
    FILL_OR_KILL = "FILL_OR_KILL"


class WorkingOrderType(StrEnum):
    LIMIT = "LIMIT"
    STOP = "STOP"


class WorkingTimeInForce(StrEnum):
    GOOD_TILL_CANCELLED = "GOOD_TILL_CANCELLED"
    GOOD_TILL_DATE = "GOOD_TILL_DATE"


class IGModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid", alias_generator=to_camel, populate_by_name=True
    )

    def payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", by_alias=True, exclude_none=True)


class CreatePosition(IGModel):
    currency_code: str = Field(min_length=3, max_length=3, pattern=r"^[A-Z]{3}$")
    direction: Direction
    epic: str = Field(min_length=6, max_length=30, pattern=r"^[A-Za-z0-9._]+$")
    expiry: str
    force_open: bool
    guaranteed_stop: bool
    size: float = Field(gt=0)
    order_type: OrderType = OrderType.MARKET
    deal_reference: str | None = Field(default=None, pattern=r"^[A-Za-z0-9_-]{1,30}$")
    level: float | None = None
    quote_id: str | None = None
    limit_distance: float | None = Field(default=None, gt=0)
    limit_level: float | None = None
    stop_distance: float | None = Field(default=None, gt=0)
    stop_level: float | None = None
    time_in_force: TimeInForce | None = None
    trailing_stop: bool | None = None
    trailing_stop_increment: float | None = Field(default=None, gt=0)

    @model_validator(mode="after")
    def validate_order(self) -> CreatePosition:
        if self.limit_distance is not None and self.limit_level is not None:
            raise ValueError("Specify only one of limit_distance and limit_level")
        if self.stop_distance is not None and self.stop_level is not None:
            raise ValueError("Specify only one of stop_distance and stop_level")
        if (
            self.limit_distance is not None
            or self.limit_level is not None
            or self.stop_distance is not None
            or self.stop_level is not None
        ) and not self.force_open:
            raise ValueError("force_open must be true when a stop or limit is supplied")
        if self.guaranteed_stop and (self.stop_distance is not None) == (
            self.stop_level is not None
        ):
            raise ValueError(
                "guaranteed_stop requires exactly one of stop_distance or stop_level"
            )
        if self.order_type == OrderType.MARKET and (
            self.level is not None or self.quote_id is not None
        ):
            raise ValueError("MARKET orders cannot include level or quote_id")
        if self.order_type == OrderType.LIMIT and (
            self.level is None or self.quote_id is not None
        ):
            raise ValueError("LIMIT orders require level and cannot include quote_id")
        if self.order_type == OrderType.QUOTE and (
            self.level is None or not self.quote_id
        ):
            raise ValueError("QUOTE orders require level and quote_id")
        if self.trailing_stop:
            if (
                self.guaranteed_stop
                or self.stop_level is not None
                or self.stop_distance is None
                or self.trailing_stop_increment is None
            ):
                raise ValueError(
                    "trailing_stop requires stop_distance and trailing_stop_increment, "
                    "without guaranteed_stop or stop_level"
                )
        elif self.trailing_stop_increment is not None:
            raise ValueError("trailing_stop_increment requires trailing_stop=true")
        return self


class UpdatePosition(IGModel):
    limit_level: float | None = None
    stop_level: float | None = None
    guaranteed_stop: bool | None = None
    trailing_stop: bool | None = None
    trailing_stop_distance: float | None = Field(default=None, gt=0)
    trailing_stop_increment: float | None = Field(default=None, gt=0)

    @model_validator(mode="after")
    def has_change(self) -> UpdatePosition:
        if not self.payload():
            raise ValueError("At least one position field must be supplied")
        if self.guaranteed_stop and self.stop_level is None:
            raise ValueError("guaranteed_stop requires stop_level")
        if self.trailing_stop:
            if (
                self.guaranteed_stop
                or self.stop_level is None
                or self.trailing_stop_distance is None
                or self.trailing_stop_increment is None
            ):
                raise ValueError(
                    "trailing_stop requires stop_level, trailing_stop_distance, "
                    "and trailing_stop_increment"
                )
        elif self.trailing_stop is False and (
            self.trailing_stop_distance is not None
            or self.trailing_stop_increment is not None
        ):
            raise ValueError("trailing stop fields require trailing_stop=true")
        return self


class ClosePosition(IGModel):
    direction: Direction
    size: float = Field(gt=0)
    order_type: OrderType = OrderType.MARKET
    level: float | None = None
    quote_id: str | None = None
    time_in_force: TimeInForce | None = None

    @model_validator(mode="after")
    def validate_order(self) -> ClosePosition:
        if self.order_type == OrderType.MARKET and (
            self.level is not None or self.quote_id is not None
        ):
            raise ValueError("MARKET orders cannot include level or quote_id")
        if self.order_type == OrderType.LIMIT and (
            self.level is None or self.quote_id is not None
        ):
            raise ValueError("LIMIT orders require level and cannot include quote_id")
        if self.order_type == OrderType.QUOTE and (
            self.level is None or not self.quote_id
        ):
            raise ValueError("QUOTE orders require level and quote_id")
        return self


class CreateWorkingOrder(IGModel):
    currency_code: str = Field(min_length=3, max_length=3, pattern=r"^[A-Z]{3}$")
    direction: Direction
    epic: str = Field(min_length=1)
    expiry: str
    guaranteed_stop: bool
    level: float
    size: float = Field(gt=0)
    type: WorkingOrderType
    time_in_force: WorkingTimeInForce
    force_open: bool | None = None
    good_till_date: str | None = None
    deal_reference: str | None = Field(default=None, pattern=r"^[A-Za-z0-9_-]{1,30}$")
    limit_distance: float | None = Field(default=None, gt=0)
    limit_level: float | None = None
    stop_distance: float | None = Field(default=None, gt=0)
    stop_level: float | None = None

    @model_validator(mode="after")
    def validate_order(self) -> CreateWorkingOrder:
        if self.limit_distance is not None and self.limit_level is not None:
            raise ValueError("Specify only one of limit_distance and limit_level")
        if self.stop_distance is not None and self.stop_level is not None:
            raise ValueError("Specify only one of stop_distance and stop_level")
        if self.guaranteed_stop and self.stop_distance is None:
            raise ValueError("guaranteed_stop requires stop_distance")
        if (
            self.time_in_force == WorkingTimeInForce.GOOD_TILL_DATE
            and not self.good_till_date
        ):
            raise ValueError("GOOD_TILL_DATE requires good_till_date")
        return self


class UpdateWorkingOrder(IGModel):
    level: float
    type: WorkingOrderType
    time_in_force: WorkingTimeInForce
    good_till_date: str | None = None
    guaranteed_stop: bool | None = None
    limit_distance: float | None = Field(default=None, gt=0)
    limit_level: float | None = None
    stop_distance: float | None = Field(default=None, gt=0)
    stop_level: float | None = None

    @model_validator(mode="after")
    def validate_order(self) -> UpdateWorkingOrder:
        if self.limit_distance is not None and self.limit_level is not None:
            raise ValueError("Specify only one of limit_distance and limit_level")
        if self.stop_distance is not None and self.stop_level is not None:
            raise ValueError("Specify only one of stop_distance and stop_level")
        if self.guaranteed_stop and self.stop_level is None:
            raise ValueError("guaranteed_stop requires stop_level")
        if (
            self.time_in_force == WorkingTimeInForce.GOOD_TILL_DATE
            and not self.good_till_date
        ):
            raise ValueError("GOOD_TILL_DATE requires good_till_date")
        return self
