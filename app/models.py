"""SQLAlchemy models for the finance database.

These models represent the tables in the 'finance' schema of the postgres database.
IMPORTANT: These models must match the production DB schema exactly for Alembic onboarding.
"""
from datetime import date, datetime
from decimal import Decimal
from typing import Optional

from sqlalchemy import (
    Boolean, Date, DateTime, Integer, Numeric, String,
    UniqueConstraint, PrimaryKeyConstraint
)
from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from config import DB_SCHEMA


class Base(DeclarativeBase):
    """Base class for all models."""
    pass


class EntsoeImbalancePrices(Base):
    """ENTSO-E imbalance prices and volumes data (15-minute intervals)."""
    __tablename__ = 'entsoe_imbalance_prices'
    __table_args__ = (
        PrimaryKeyConstraint('id', name='entsoe_imbalance_prices_pkey'),
        UniqueConstraint('trade_date', 'time_interval', name='entsoe_imbalance_prices_trade_date_time_interval_key'),
        UniqueConstraint('trade_date', 'period', name='entsoe_imbalance_prices_trade_date_period_key'),
        {'schema': DB_SCHEMA}
    )

    id: Mapped[int] = mapped_column(Integer, autoincrement=True)
    trade_date: Mapped[date] = mapped_column(Date, nullable=False)
    period: Mapped[int] = mapped_column(Integer, nullable=False)
    time_interval: Mapped[str] = mapped_column(String(11), nullable=False)
    pos_imb_price_czk_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    pos_imb_scarcity_czk_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    pos_imb_incentive_czk_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    pos_imb_financial_neutrality_czk_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    neg_imb_price_czk_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    neg_imb_scarcity_czk_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    neg_imb_incentive_czk_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    neg_imb_financial_neutrality_czk_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    imbalance_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 5))
    difference_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 5))
    situation: Mapped[Optional[str]] = mapped_column(String)
    status: Mapped[Optional[str]] = mapped_column(String)
    created_at: Mapped[Optional[datetime]] = mapped_column(DateTime, server_default='CURRENT_TIMESTAMP')
    delivery_datetime: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP(timezone=True))


class OteDailyPayments(Base):
    """OTE daily settlement payments data."""
    __tablename__ = 'ote_daily_payments'
    __table_args__ = (
        PrimaryKeyConstraint('id', name='ote_daily_payments_pkey'),
        {'schema': DB_SCHEMA}
    )

    id: Mapped[int] = mapped_column(Integer, autoincrement=True)
    delivery_day: Mapped[date] = mapped_column(Date, nullable=False)
    settlement_version: Mapped[Optional[str]] = mapped_column(String(100))
    settlement_item: Mapped[Optional[str]] = mapped_column(String(100))
    type_of_payment: Mapped[Optional[str]] = mapped_column(String(50))
    volume_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 5))
    amount_excl_vat: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 2))
    currency_of_payment: Mapped[Optional[str]] = mapped_column(String(10))
    currency_rate: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 6))
    system: Mapped[Optional[str]] = mapped_column(String(50))
    message: Mapped[Optional[str]] = mapped_column(String(100))
    created_at: Mapped[Optional[datetime]] = mapped_column(DateTime, server_default='CURRENT_TIMESTAMP')
    updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime, server_default='CURRENT_TIMESTAMP')


class OtePricesDayAhead(Base):
    """OTE day-ahead electricity market prices (15/60-minute intervals)."""
    __tablename__ = 'ote_prices_day_ahead'
    __table_args__ = (
        PrimaryKeyConstraint('id', name='ote_prices_day_ahead_pkey'),
        UniqueConstraint('trade_date', 'period', name='ote_prices_day_ahead_trade_date_period_key'),
        UniqueConstraint('trade_date', 'time_interval', name='ote_prices_day_ahead_trade_date_time_interval_key'),
        {'schema': DB_SCHEMA}
    )

    id: Mapped[int] = mapped_column(Integer, autoincrement=True)
    trade_date: Mapped[date] = mapped_column(Date, nullable=False)
    period: Mapped[int] = mapped_column(Integer, nullable=False)
    time_interval: Mapped[str] = mapped_column(String(11), nullable=False)
    price_15min_eur_mwh: Mapped[Decimal] = mapped_column(Numeric(10, 2), nullable=False)
    volume_mwh: Mapped[Decimal] = mapped_column(Numeric(12, 3), nullable=False)
    purchase_15min_products_mwh: Mapped[Decimal] = mapped_column(Numeric(12, 3), nullable=False)
    purchase_60min_products_mwh: Mapped[Decimal] = mapped_column(Numeric(12, 3), nullable=False)
    sale_15min_products_mwh: Mapped[Decimal] = mapped_column(Numeric(12, 3), nullable=False)
    sale_60min_products_mwh: Mapped[Decimal] = mapped_column(Numeric(12, 3), nullable=False)
    saldo_dm_mwh: Mapped[Decimal] = mapped_column(Numeric(12, 3), nullable=False)
    export_mwh: Mapped[Decimal] = mapped_column(Numeric(12, 3), nullable=False)
    import_mwh: Mapped[Decimal] = mapped_column(Numeric(12, 3), nullable=False)
    price_60min_ref_eur_mwh: Mapped[Decimal] = mapped_column(Numeric(10, 2), nullable=False)
    created_at: Mapped[Optional[datetime]] = mapped_column(DateTime, server_default='CURRENT_TIMESTAMP')
    is_15min: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default='true')


class OtePricesImbalance(Base):
    """OTE imbalance prices and costs (15-minute intervals)."""
    __tablename__ = 'ote_prices_imbalance'
    __table_args__ = (
        PrimaryKeyConstraint('id', name='ote_prices_imbalance_pkey'),
        UniqueConstraint('trade_date', 'period', name='ote_prices_imbalance_trade_date_period_key'),
        UniqueConstraint('trade_date', 'time_interval', name='ote_prices_imbalance_trade_date_time_interval_key'),
        {'schema': DB_SCHEMA}
    )

    id: Mapped[int] = mapped_column(Integer, autoincrement=True)
    trade_date: Mapped[date] = mapped_column(Date, nullable=False)
    period: Mapped[int] = mapped_column(Integer, nullable=False)
    time_interval: Mapped[str] = mapped_column(String(11), nullable=False)
    system_imbalance_mwh: Mapped[Decimal] = mapped_column(Numeric(12, 5), nullable=False)
    absolute_imbalance_sum_mwh: Mapped[Decimal] = mapped_column(Numeric(12, 5), nullable=False)
    positive_imbalance_mwh: Mapped[Decimal] = mapped_column(Numeric(12, 5), nullable=False)
    negative_imbalance_mwh: Mapped[Decimal] = mapped_column(Numeric(12, 5), nullable=False)
    rounded_imbalance_mwh: Mapped[Decimal] = mapped_column(Numeric(12, 5), nullable=False)
    cost_of_be_czk: Mapped[Decimal] = mapped_column(Numeric(15, 3), nullable=False)
    cost_of_imbalance_czk: Mapped[Decimal] = mapped_column(Numeric(15, 3), nullable=False)
    settlement_price_imbalance_czk_mwh: Mapped[Decimal] = mapped_column(Numeric(15, 3), nullable=False)
    settlement_price_counter_imbalance_czk_mwh: Mapped[Decimal] = mapped_column(Numeric(15, 3), nullable=False)
    price_protective_be_component_czk_mwh: Mapped[Decimal] = mapped_column(Numeric(15, 3), nullable=False)
    price_be_component_czk_mwh: Mapped[Decimal] = mapped_column(Numeric(15, 3), nullable=False)
    price_im_component_czk_mwh: Mapped[Decimal] = mapped_column(Numeric(15, 3), nullable=False)
    price_si_component_czk_mwh: Mapped[Decimal] = mapped_column(Numeric(15, 3), nullable=False)
    price_not_performed_activation_czk_mwh: Mapped[Decimal] = mapped_column(Numeric(15, 3), nullable=False)
    created_at: Mapped[Optional[datetime]] = mapped_column(DateTime, server_default='CURRENT_TIMESTAMP')


class OtePricesIntradayMarket(Base):
    """OTE intraday market trading data (15-minute intervals).

    Note: Legacy constraint names use 'prices_intraday_market_' prefix instead of 'ote_prices_intraday_market_'.
    """
    __tablename__ = 'ote_prices_intraday_market'
    __table_args__ = (
        PrimaryKeyConstraint('id', name='prices_intraday_market_pkey'),
        UniqueConstraint('trade_date', 'period', name='prices_intraday_market_trade_date_period_key'),
        UniqueConstraint('trade_date', 'time_interval', name='prices_intraday_market_trade_date_time_interval_key'),
        {'schema': DB_SCHEMA}
    )

    id: Mapped[int] = mapped_column(Integer, autoincrement=True)
    trade_date: Mapped[date] = mapped_column(Date, nullable=False)
    period: Mapped[int] = mapped_column(Integer, nullable=False)
    time_interval: Mapped[str] = mapped_column(String(11), nullable=False)
    traded_volume_mwh: Mapped[Decimal] = mapped_column(Numeric(12, 4), nullable=False)
    traded_volume_purchased_mwh: Mapped[Decimal] = mapped_column(Numeric(12, 4), nullable=False)
    traded_volume_sold_mwh: Mapped[Decimal] = mapped_column(Numeric(12, 4), nullable=False)
    weighted_avg_price_eur_mwh: Mapped[Decimal] = mapped_column(Numeric(15, 3), nullable=False)
    min_price_eur_mwh: Mapped[Decimal] = mapped_column(Numeric(15, 3), nullable=False)
    max_price_eur_mwh: Mapped[Decimal] = mapped_column(Numeric(15, 3), nullable=False)
    last_price_eur_mwh: Mapped[Decimal] = mapped_column(Numeric(15, 3), nullable=False)
    created_at: Mapped[Optional[datetime]] = mapped_column(DateTime, server_default='CURRENT_TIMESTAMP')


class OteTradeBalance(Base):
    """OTE trade balance data (15-minute intervals)."""
    __tablename__ = 'ote_trade_balance'
    __table_args__ = (
        PrimaryKeyConstraint('id', name='ote_trade_balance_pkey'),
        UniqueConstraint('delivery_date', 'time_interval', name='ote_trade_balance_delivery_date_time_interval_key'),
        {'schema': DB_SCHEMA}
    )

    id: Mapped[int] = mapped_column(Integer, autoincrement=True)
    delivery_date: Mapped[date] = mapped_column(Date, nullable=False)
    time_interval: Mapped[str] = mapped_column(String(11), nullable=False)
    period: Mapped[int] = mapped_column(Integer, nullable=False)
    total_buy_mw: Mapped[Decimal] = mapped_column(Numeric(12, 5), nullable=False)
    total_sell_mw: Mapped[Decimal] = mapped_column(Numeric(12, 5), nullable=False)
    daily_market_buy_mw: Mapped[Decimal] = mapped_column(Numeric(12, 5), nullable=False)
    daily_market_sell_mw: Mapped[Decimal] = mapped_column(Numeric(12, 5), nullable=False)
    intraday_auction_buy_mw: Mapped[Decimal] = mapped_column(Numeric(12, 5), nullable=False)
    intraday_auction_sell_mw: Mapped[Decimal] = mapped_column(Numeric(12, 5), nullable=False)
    intraday_market_buy_mw: Mapped[Decimal] = mapped_column(Numeric(12, 5), nullable=False)
    intraday_market_sell_mw: Mapped[Decimal] = mapped_column(Numeric(12, 5), nullable=False)
    realization_diagrams_buy_mw: Mapped[Decimal] = mapped_column(Numeric(12, 5), nullable=False)
    realization_diagrams_sell_mw: Mapped[Decimal] = mapped_column(Numeric(12, 5), nullable=False)
    total_buy_mwh: Mapped[Decimal] = mapped_column(Numeric(12, 5), nullable=False)
    total_sell_mwh: Mapped[Decimal] = mapped_column(Numeric(12, 5), nullable=False)
    daily_market_buy_mwh: Mapped[Decimal] = mapped_column(Numeric(12, 5), nullable=False)
    daily_market_sell_mwh: Mapped[Decimal] = mapped_column(Numeric(12, 5), nullable=False)
    intraday_auction_buy_mwh: Mapped[Decimal] = mapped_column(Numeric(12, 5), nullable=False)
    intraday_auction_sell_mwh: Mapped[Decimal] = mapped_column(Numeric(12, 5), nullable=False)
    intraday_market_buy_mwh: Mapped[Decimal] = mapped_column(Numeric(12, 5), nullable=False)
    intraday_market_sell_mwh: Mapped[Decimal] = mapped_column(Numeric(12, 5), nullable=False)
    realization_diagrams_buy_mwh: Mapped[Decimal] = mapped_column(Numeric(12, 5), nullable=False)
    realization_diagrams_sell_mwh: Mapped[Decimal] = mapped_column(Numeric(12, 5), nullable=False)
    uploaded_at: Mapped[Optional[datetime]] = mapped_column(DateTime, server_default='CURRENT_TIMESTAMP')
