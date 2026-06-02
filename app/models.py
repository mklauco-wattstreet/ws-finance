"""SQLAlchemy models for the finance database.

These models represent the tables in the 'finance' schema of the postgres database.
IMPORTANT: These models must match the production DB schema exactly for Alembic onboarding.
"""
from datetime import date, datetime
from decimal import Decimal
from typing import Optional

from sqlalchemy import (
    Boolean, CheckConstraint, Date, DateTime, Integer, Numeric, SmallInteger, String,
    UniqueConstraint, PrimaryKeyConstraint
)
from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from config import DB_SCHEMA


class Base(DeclarativeBase):
    """Base class for all models."""
    pass


class EntsoeAreas(Base):
    """ENTSO-E delivery area lookup table.

    Central source of truth for delivery area metadata (EIC codes).
    Used by partitioned tables to reference areas.

    Pre-populated areas:
    - id=1: CZ (Czech Republic) - 10YCZ-CEPS-----N
    - id=2: DE (Germany TenneT) - 10YDE-EON------1
    - id=3: AT (Austria) - 10YAT-APG------L
    - id=4: PL (Poland) - 10YPL-AREA-----S
    - id=5: SK (Slovakia) - 10YSK-SEPS-----K
    - id=9: HU (Hungary) - 10YHU-MAVIR----U
    """
    __tablename__ = 'entsoe_areas'
    __table_args__ = (
        PrimaryKeyConstraint('id', name='entsoe_areas_pkey'),
        UniqueConstraint('code', name='entsoe_areas_code_key'),
        {'schema': DB_SCHEMA}
    )

    id: Mapped[int] = mapped_column(Integer, autoincrement=True)
    code: Mapped[str] = mapped_column(String(20), nullable=False)
    country_name: Mapped[str] = mapped_column(String(100), nullable=False)
    country_code: Mapped[str] = mapped_column(String(5), nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default='true')


class EntsoeImbalancePrices(Base):
    """ENTSO-E imbalance prices and volumes data (15-minute intervals).

    Partitioned by country_code for multi-area storage with partition pruning.
    Partitions: CZ, DE, AT, PL, SK, HU (by country_code string).

    Currency field indicates the price currency:
    - CZ: CZK (Czech Koruna)
    - HU, DE, AT, PL, SK: EUR (Euro)
    """
    __tablename__ = 'entsoe_imbalance_prices'
    __table_args__ = (
        PrimaryKeyConstraint('trade_date', 'period', 'area_id', 'country_code'),
        {'schema': DB_SCHEMA}
    )

    id: Mapped[int] = mapped_column(Integer, autoincrement=True)
    trade_date: Mapped[date] = mapped_column(Date, nullable=False)
    period: Mapped[int] = mapped_column(Integer, nullable=False)
    area_id: Mapped[int] = mapped_column(Integer, nullable=False)
    country_code: Mapped[str] = mapped_column(String(5), nullable=False)
    time_interval: Mapped[str] = mapped_column(String(11), nullable=False)
    pos_imb_price_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    pos_imb_scarcity_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    pos_imb_incentive_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    pos_imb_financial_neutrality_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    neg_imb_price_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    neg_imb_scarcity_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    neg_imb_incentive_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    neg_imb_financial_neutrality_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    imbalance_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 5))
    difference_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 5))
    situation: Mapped[Optional[str]] = mapped_column(String)
    status: Mapped[Optional[str]] = mapped_column(String)
    currency: Mapped[str] = mapped_column(String(3), nullable=False)
    created_at: Mapped[Optional[datetime]] = mapped_column(DateTime, server_default='CURRENT_TIMESTAMP')
    updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime, server_default='CURRENT_TIMESTAMP')
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
    updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime, server_default='CURRENT_TIMESTAMP')
    is_15min: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default='true')


class OtePricesDayAhead60min(Base):
    """OTE day-ahead electricity market prices (60-minute contracts)."""
    __tablename__ = 'ote_prices_day_ahead_60min'
    __table_args__ = (
        PrimaryKeyConstraint('id', name='ote_prices_day_ahead_60min_pkey'),
        UniqueConstraint('trade_date', 'period_60', name='ote_prices_day_ahead_60min_trade_date_period_60_key'),
        UniqueConstraint('trade_date', 'time_interval', name='ote_prices_day_ahead_60min_trade_date_time_interval_key'),
        {'schema': DB_SCHEMA}
    )

    id: Mapped[int] = mapped_column(Integer, autoincrement=True)
    trade_date: Mapped[date] = mapped_column(Date, nullable=False)
    period_60: Mapped[int] = mapped_column(Integer, nullable=False)
    time_interval: Mapped[str] = mapped_column(String(11), nullable=False)
    price_60min_eur_mwh: Mapped[Decimal] = mapped_column(Numeric(10, 2), nullable=False)
    volume_mwh: Mapped[Decimal] = mapped_column(Numeric(12, 3), nullable=False)
    purchase_15min_products_mwh: Mapped[Decimal] = mapped_column(Numeric(12, 3), nullable=False)
    purchase_60min_products_mwh: Mapped[Decimal] = mapped_column(Numeric(12, 3), nullable=False)
    sale_15min_products_mwh: Mapped[Decimal] = mapped_column(Numeric(12, 3), nullable=False)
    sale_60min_products_mwh: Mapped[Decimal] = mapped_column(Numeric(12, 3), nullable=False)
    saldo_dm_mwh: Mapped[Decimal] = mapped_column(Numeric(12, 3), nullable=False)
    export_mwh: Mapped[Decimal] = mapped_column(Numeric(12, 3), nullable=False)
    import_mwh: Mapped[Decimal] = mapped_column(Numeric(12, 3), nullable=False)
    created_at: Mapped[Optional[datetime]] = mapped_column(DateTime, server_default='CURRENT_TIMESTAMP')
    updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime, server_default='CURRENT_TIMESTAMP')


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
    updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime, server_default='CURRENT_TIMESTAMP')


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
    updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime, server_default='CURRENT_TIMESTAMP')


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


class EntsoeLoad(Base):
    """ENTSO-E load data (actual and forecast, 15-minute intervals).

    Partitioned by country_code for multi-area storage with partition pruning.
    Partitions: CZ, DE, AT, PL, SK (by country_code string).
    """
    __tablename__ = 'entsoe_load'
    __table_args__ = (
        PrimaryKeyConstraint('trade_date', 'period', 'area_id', 'country_code'),
        {'schema': DB_SCHEMA}
    )

    id: Mapped[int] = mapped_column(Integer, autoincrement=True)
    trade_date: Mapped[date] = mapped_column(Date, nullable=False)
    period: Mapped[int] = mapped_column(Integer, nullable=False)
    area_id: Mapped[int] = mapped_column(Integer, nullable=False)
    country_code: Mapped[str] = mapped_column(String(5), nullable=False)
    time_interval: Mapped[str] = mapped_column(String(11), nullable=False)
    actual_load_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    forecast_load_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    created_at: Mapped[Optional[datetime]] = mapped_column(DateTime, server_default='CURRENT_TIMESTAMP')
    updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime, server_default='CURRENT_TIMESTAMP')


class EntsoeGenerationActual(Base):
    """ENTSO-E actual generation data in wide format (15-minute intervals).

    Partitioned by country_code for multi-area storage with partition pruning.
    Partitions: CZ, DE, AT, PL, SK (by country_code string).

    This structure allows new TSOs/bidding zones to be added without modifying
    partition DDL - new TSOs automatically route to their country partition.

    Wide-format columns with aggregated PSR types:
    - gen_nuclear_mw: B14 (Nuclear)
    - gen_coal_mw: B02 (Brown coal/Lignite) + B05 (Hard coal)
    - gen_gas_mw: B04 (Fossil Gas)
    - gen_solar_mw: B16 (Solar)
    - gen_wind_mw: B19 (Wind Onshore)
    - gen_wind_offshore_mw: B18 (Wind Offshore) - primarily for DE
    - gen_hydro_pumped_mw: B10 (Hydro Pumped Storage)
    - gen_biomass_mw: B01 (Biomass)
    - gen_hydro_other_mw: B11 (Run-of-river) + B12 (Water Reservoir)

    Note: This is a partitioned table. The composite PK includes country_code.
    """
    __tablename__ = 'entsoe_generation_actual'
    __table_args__ = (
        # Partitioned table: composite PK includes partition key (country_code)
        PrimaryKeyConstraint('trade_date', 'period', 'area_id', 'country_code'),
        {'schema': DB_SCHEMA}
    )

    id: Mapped[int] = mapped_column(Integer, autoincrement=True)
    trade_date: Mapped[date] = mapped_column(Date, nullable=False)
    period: Mapped[int] = mapped_column(Integer, nullable=False)
    area_id: Mapped[int] = mapped_column(Integer, nullable=False)
    country_code: Mapped[str] = mapped_column(String(5), nullable=False)
    time_interval: Mapped[str] = mapped_column(String(11), nullable=False)
    gen_nuclear_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    gen_coal_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    gen_gas_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    gen_solar_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    gen_wind_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    gen_wind_offshore_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    gen_hydro_pumped_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    gen_biomass_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    gen_hydro_other_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    created_at: Mapped[Optional[datetime]] = mapped_column(DateTime, server_default='CURRENT_TIMESTAMP')
    updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime, server_default='CURRENT_TIMESTAMP')


class EntsoeCrossBorderFlows(Base):
    """ENTSO-E cross-border physical flows in wide format (15-minute intervals).

    Wide-format columns for each neighboring border:
    - flow_de_mw: Physical flow to/from Germany (positive = import, negative = export)
    - flow_at_mw: Physical flow to/from Austria
    - flow_pl_mw: Physical flow to/from Poland
    - flow_sk_mw: Physical flow to/from Slovakia
    - flow_total_net_mw: Sum of all border flows

    Note: trade_date/period columns added in migration 010 for ML feature alignment.
    """
    __tablename__ = 'entsoe_cross_border_flows'
    __table_args__ = (
        PrimaryKeyConstraint('id', name='entsoe_cross_border_flows_pkey'),
        UniqueConstraint('delivery_datetime', 'area_id', name='entsoe_cross_border_flows_datetime_area_key'),
        UniqueConstraint('trade_date', 'period', 'area_id', name='entsoe_cross_border_flows_trade_date_period_area_key'),
        {'schema': DB_SCHEMA}
    )

    id: Mapped[int] = mapped_column(Integer, autoincrement=True)
    trade_date: Mapped[date] = mapped_column(Date, nullable=False)
    period: Mapped[int] = mapped_column(Integer, nullable=False)
    time_interval: Mapped[str] = mapped_column(String(11), nullable=False)
    delivery_datetime: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    area_id: Mapped[str] = mapped_column(String(20), nullable=False)
    flow_de_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    flow_at_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    flow_pl_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    flow_sk_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    flow_total_net_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    created_at: Mapped[Optional[datetime]] = mapped_column(DateTime, server_default='CURRENT_TIMESTAMP')
    updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime, server_default='CURRENT_TIMESTAMP')


class EntsoeGenerationForecast(Base):
    """ENTSO-E day-ahead generation forecasts (A69) - renewable sources.

    Partitioned by country_code for multi-area storage with partition pruning.
    Partitions: CZ, DE, AT, PL, SK (by country_code string).

    Captures day-ahead forecasts for calculating forecast errors:
    - forecast_solar_mw: B16 (Solar) day-ahead forecast
    - forecast_wind_mw: B19 (Wind Onshore) day-ahead forecast
    - forecast_wind_offshore_mw: B18 (Wind Offshore) day-ahead forecast
    """
    __tablename__ = 'entsoe_generation_forecast'
    __table_args__ = (
        PrimaryKeyConstraint('trade_date', 'period', 'area_id', 'country_code'),
        {'schema': DB_SCHEMA}
    )

    id: Mapped[int] = mapped_column(Integer, autoincrement=True)
    trade_date: Mapped[date] = mapped_column(Date, nullable=False)
    period: Mapped[int] = mapped_column(Integer, nullable=False)
    area_id: Mapped[int] = mapped_column(Integer, nullable=False)
    country_code: Mapped[str] = mapped_column(String(5), nullable=False)
    time_interval: Mapped[str] = mapped_column(String(11), nullable=False)
    forecast_solar_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    forecast_wind_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    forecast_wind_offshore_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    created_at: Mapped[Optional[datetime]] = mapped_column(DateTime, server_default='CURRENT_TIMESTAMP')
    updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime, server_default='CURRENT_TIMESTAMP')


class EntsoeGenerationForecastIntraday(Base):
    """ENTSO-E intraday generation forecasts (A69/A40) - renewable sources.

    Partitioned by country_code. Same schema as EntsoeGenerationForecast.
    """
    __tablename__ = 'entsoe_generation_forecast_intraday'
    __table_args__ = (
        PrimaryKeyConstraint('trade_date', 'period', 'area_id', 'country_code'),
        {'schema': DB_SCHEMA}
    )

    id: Mapped[int] = mapped_column(Integer, autoincrement=True)
    trade_date: Mapped[date] = mapped_column(Date, nullable=False)
    period: Mapped[int] = mapped_column(Integer, nullable=False)
    area_id: Mapped[int] = mapped_column(Integer, nullable=False)
    country_code: Mapped[str] = mapped_column(String(5), nullable=False)
    time_interval: Mapped[str] = mapped_column(String(11), nullable=False)
    forecast_solar_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    forecast_wind_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    forecast_wind_offshore_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    created_at: Mapped[Optional[datetime]] = mapped_column(DateTime, server_default='CURRENT_TIMESTAMP')
    updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime, server_default='CURRENT_TIMESTAMP')


class EntsoeGenerationForecastCurrent(Base):
    """ENTSO-E current generation forecasts (A69/A18) - renewable sources.

    Partitioned by country_code. Same schema as EntsoeGenerationForecast.
    """
    __tablename__ = 'entsoe_generation_forecast_current'
    __table_args__ = (
        PrimaryKeyConstraint('trade_date', 'period', 'area_id', 'country_code'),
        {'schema': DB_SCHEMA}
    )

    id: Mapped[int] = mapped_column(Integer, autoincrement=True)
    trade_date: Mapped[date] = mapped_column(Date, nullable=False)
    period: Mapped[int] = mapped_column(Integer, nullable=False)
    area_id: Mapped[int] = mapped_column(Integer, nullable=False)
    country_code: Mapped[str] = mapped_column(String(5), nullable=False)
    time_interval: Mapped[str] = mapped_column(String(11), nullable=False)
    forecast_solar_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    forecast_wind_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    forecast_wind_offshore_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    created_at: Mapped[Optional[datetime]] = mapped_column(DateTime, server_default='CURRENT_TIMESTAMP')
    updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime, server_default='CURRENT_TIMESTAMP')


class EntsoeBalancingEnergy(Base):
    """ENTSO-E activated balancing energy prices (A84) - TSO intervention.

    Captures activation prices for balancing reserves (EUR/MWh):
    - afrr_up/down: aFRR (A95) activation prices
    - mfrr_up/down: mFRR (A96) activation prices
    - rr_up/down: Replacement Reserve (A97) activation prices

    BusinessTypes: A95 (aFRR), A96 (mFRR), A97 (RR)
    """
    __tablename__ = 'entsoe_balancing_energy'
    __table_args__ = (
        PrimaryKeyConstraint('id', name='entsoe_balancing_energy_pkey'),
        UniqueConstraint('trade_date', 'period', name='entsoe_balancing_energy_trade_date_period_key'),
        UniqueConstraint('trade_date', 'time_interval', name='entsoe_balancing_energy_trade_date_time_interval_key'),
        {'schema': DB_SCHEMA}
    )

    id: Mapped[int] = mapped_column(Integer, autoincrement=True)
    trade_date: Mapped[date] = mapped_column(Date, nullable=False)
    period: Mapped[int] = mapped_column(Integer, nullable=False)
    time_interval: Mapped[str] = mapped_column(String(11), nullable=False)
    afrr_up_price_eur: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    afrr_down_price_eur: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    mfrr_up_price_eur: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    mfrr_down_price_eur: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    rr_up_price_eur: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    rr_down_price_eur: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    created_at: Mapped[Optional[datetime]] = mapped_column(DateTime, server_default='CURRENT_TIMESTAMP')
    updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime, server_default='CURRENT_TIMESTAMP')


class EntsoeGenerationScheduled(Base):
    """ENTSO-E scheduled generation (A71) - day-ahead scheduled.

    Captures scheduled generation for comparing with actual:
    - scheduled_total_mw: Total scheduled generation
    """
    __tablename__ = 'entsoe_generation_scheduled'
    __table_args__ = (
        PrimaryKeyConstraint('id', name='entsoe_generation_scheduled_pkey'),
        UniqueConstraint('trade_date', 'period', name='entsoe_generation_scheduled_trade_date_period_key'),
        UniqueConstraint('trade_date', 'time_interval', name='entsoe_generation_scheduled_trade_date_time_interval_key'),
        {'schema': DB_SCHEMA}
    )

    id: Mapped[int] = mapped_column(Integer, autoincrement=True)
    trade_date: Mapped[date] = mapped_column(Date, nullable=False)
    period: Mapped[int] = mapped_column(Integer, nullable=False)
    time_interval: Mapped[str] = mapped_column(String(11), nullable=False)
    scheduled_total_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    created_at: Mapped[Optional[datetime]] = mapped_column(DateTime, server_default='CURRENT_TIMESTAMP')
    updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime, server_default='CURRENT_TIMESTAMP')


class EntsoeScheduledCrossBorderFlows(Base):
    """ENTSO-E scheduled cross-border exchanges (A09) - day-ahead scheduled.

    Captures scheduled commercial exchanges for CZ borders:
    - scheduled_de_mw: Scheduled exchange with Germany (positive = import)
    - scheduled_at_mw: Scheduled exchange with Austria
    - scheduled_pl_mw: Scheduled exchange with Poland
    - scheduled_sk_mw: Scheduled exchange with Slovakia
    - scheduled_total_net_mw: Sum of all scheduled exchanges

    Compare with entsoe_cross_border_flows (physical A11) to calculate schedule deviation.
    """
    __tablename__ = 'entsoe_scheduled_cross_border_flows'
    __table_args__ = (
        PrimaryKeyConstraint('id', name='entsoe_sched_xborder_flows_pkey'),
        UniqueConstraint('trade_date', 'period', name='entsoe_sched_xborder_flows_date_period_key'),
        UniqueConstraint('trade_date', 'time_interval', name='entsoe_sched_xborder_flows_date_interval_key'),
        {'schema': DB_SCHEMA}
    )

    id: Mapped[int] = mapped_column(Integer, autoincrement=True)
    trade_date: Mapped[date] = mapped_column(Date, nullable=False)
    period: Mapped[int] = mapped_column(Integer, nullable=False)
    time_interval: Mapped[str] = mapped_column(String(11), nullable=False)
    scheduled_de_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    scheduled_at_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    scheduled_pl_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    scheduled_sk_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    scheduled_total_net_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    created_at: Mapped[Optional[datetime]] = mapped_column(DateTime, server_default='CURRENT_TIMESTAMP')
    updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime, server_default='CURRENT_TIMESTAMP')


class EntsoeDayAheadPrices(Base):
    """ENTSO-E day-ahead prices (A44) - market clearing prices.

    Partitioned by country_code for multi-area storage with partition pruning.
    Currently supports: HU (Hungary), DE (Germany-Luxembourg BZ), AT (Austria).

    Columns:
    - price_eur_mwh: Day-ahead market clearing price in EUR/MWh

    Note: This is a partitioned table. The composite PK includes country_code.
    """
    __tablename__ = 'entsoe_day_ahead_prices'
    __table_args__ = (
        # Partitioned table: composite PK includes partition key (country_code)
        PrimaryKeyConstraint('trade_date', 'period', 'area_id', 'country_code'),
        {'schema': DB_SCHEMA}
    )

    id: Mapped[int] = mapped_column(Integer, autoincrement=True)
    trade_date: Mapped[date] = mapped_column(Date, nullable=False)
    period: Mapped[int] = mapped_column(Integer, nullable=False)
    area_id: Mapped[int] = mapped_column(Integer, nullable=False)
    country_code: Mapped[str] = mapped_column(String(5), nullable=False)
    time_interval: Mapped[str] = mapped_column(String(11), nullable=False)
    price_eur_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    created_at: Mapped[Optional[datetime]] = mapped_column(DateTime, server_default='CURRENT_TIMESTAMP')
    updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime, server_default='CURRENT_TIMESTAMP')


class CepsActualRePrice1Min(Base):
    """CEPS actual reserve energy (RE) prices at 1-minute granularity.

    Stores minute-level pricing data for automatic frequency restoration reserve (aFRR)
    and manual frequency restoration reserve (mFRR) in the Czech grid.

    Note: Partitioned by year based on delivery_timestamp for efficient data management.
    """
    __tablename__ = 'ceps_actual_re_price_1min'
    __table_args__ = (
        PrimaryKeyConstraint('delivery_timestamp', 'id', name='ceps_actual_re_price_1min_pkey'),
        UniqueConstraint('delivery_timestamp', name='uq_ceps_re_price_1min_delivery_timestamp'),
        {'schema': DB_SCHEMA}
    )

    id: Mapped[int] = mapped_column(Integer, autoincrement=True)
    delivery_timestamp: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    price_afrr_plus_eur_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    price_afrr_minus_eur_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    price_mfrr_plus_eur_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    price_mfrr_minus_eur_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    price_mfrr_5_eur_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    created_at: Mapped[Optional[datetime]] = mapped_column(DateTime, server_default='CURRENT_TIMESTAMP')
    updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime, server_default='CURRENT_TIMESTAMP')


class CepsActualRePrice15Min(Base):
    """CEPS actual reserve energy (RE) prices aggregated to 15-minute intervals.

    Provides mean, median, and last-in-interval pricing statistics for aFRR and mFRR.
    This aggregated view supports correlation analysis with 15-minute imbalance data.

    Note: Partitioned by year based on trade_date for efficient data management.
    """
    __tablename__ = 'ceps_actual_re_price_15min'
    __table_args__ = (
        PrimaryKeyConstraint('trade_date', 'time_interval', 'id', name='ceps_actual_re_price_15min_pkey'),
        UniqueConstraint('trade_date', 'time_interval', name='uq_ceps_re_price_15min_trade_date_interval'),
        {'schema': DB_SCHEMA}
    )

    id: Mapped[int] = mapped_column(Integer, autoincrement=True)
    trade_date: Mapped[date] = mapped_column(Date, nullable=False)
    time_interval: Mapped[str] = mapped_column(String(11), nullable=False)
    price_afrr_plus_mean_eur_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    price_afrr_minus_mean_eur_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    price_mfrr_plus_mean_eur_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    price_mfrr_minus_mean_eur_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    price_mfrr_5_mean_eur_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    price_afrr_plus_median_eur_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    price_afrr_minus_median_eur_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    price_mfrr_plus_median_eur_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    price_mfrr_minus_median_eur_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    price_mfrr_5_median_eur_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    price_afrr_plus_last_at_interval_eur_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    price_afrr_minus_last_at_interval_eur_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    price_mfrr_plus_last_at_interval_eur_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    price_mfrr_minus_last_at_interval_eur_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    price_mfrr_5_last_at_interval_eur_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    created_at: Mapped[Optional[datetime]] = mapped_column(DateTime, server_default='CURRENT_TIMESTAMP')
    updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime, server_default='CURRENT_TIMESTAMP')


class Ceps1MinFeatures15Min(Base):
    """CEPS 1-minute distributional/volatility features aggregated to 15-minute intervals.

    Computed from ceps_actual_re_price_1min, ceps_actual_imbalance_1min, and
    ceps_export_import_svr_1min. Includes price distribution stats (min, max, std, skew)
    for aFRR+/-, mFRR+/-, imbalance range/std/slope, and threshold counts.

    Partitioned by year on trade_date.
    """
    __tablename__ = 'ceps_1min_features_15min'
    __table_args__ = (
        PrimaryKeyConstraint('trade_date', 'time_interval', 'id', name='ceps_1min_features_15min_pkey'),
        UniqueConstraint('trade_date', 'time_interval', name='uq_ceps_1min_features_15min'),
        {'schema': DB_SCHEMA}
    )

    id: Mapped[int] = mapped_column(Integer, autoincrement=True)
    trade_date: Mapped[date] = mapped_column(Date, nullable=False)
    time_interval: Mapped[str] = mapped_column(String(11), nullable=False)
    minute_count: Mapped[Optional[int]] = mapped_column(SmallInteger)
    # aFRR+ price distribution
    afrr_plus_min_eur: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    afrr_plus_max_eur: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    afrr_plus_std_eur: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 5))
    afrr_plus_skew: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 5))
    # aFRR- price distribution
    afrr_minus_min_eur: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    afrr_minus_max_eur: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    afrr_minus_std_eur: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 5))
    afrr_minus_skew: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 5))
    # mFRR+ price distribution
    mfrr_plus_min_eur: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    mfrr_plus_max_eur: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    mfrr_plus_std_eur: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 5))
    mfrr_plus_skew: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 5))
    # mFRR- price distribution
    mfrr_minus_min_eur: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    mfrr_minus_max_eur: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    mfrr_minus_std_eur: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 5))
    mfrr_minus_skew: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 5))
    # Imbalance distribution
    imbalance_range_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 5))
    imbalance_std_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 5))
    imbalance_slope: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 8))
    # Threshold counts
    minutes_at_floor: Mapped[Optional[int]] = mapped_column(SmallInteger)
    minutes_near_peak: Mapped[Optional[int]] = mapped_column(SmallInteger)
    saturation_count: Mapped[Optional[int]] = mapped_column(SmallInteger)
    # Golden Trio: total activation, platform saturation, marginal slope
    total_active_mean_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    total_active_std_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 5))
    platform_active_count: Mapped[Optional[int]] = mapped_column(SmallInteger)
    afrr_mfrr_plus_spread_mean_eur: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    afrr_mfrr_plus_spread_std_eur: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 5))
    afrr_mfrr_minus_spread_mean_eur: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    afrr_mfrr_minus_spread_std_eur: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 5))
    created_at: Mapped[Optional[datetime]] = mapped_column(DateTime, server_default='CURRENT_TIMESTAMP')
    updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime, server_default='CURRENT_TIMESTAMP')


class CepsDerivedFeatures15Min(Base):
    """CEPS derived cross-table features at 15-minute resolution.

    Rolling memory: trailing 2h/4h imbalance statistics from ceps_actual_imbalance_15min.
    Forecast surprise: actual vs forecast/plan generation errors.
    - solar_error = pvpp_mw (actual) - solar_mean_mw (RES forecast)
    - wind_error = wpp_mw (actual) - wind_mean_mw (RES forecast)
    - gen_total_error = actual total - planned total_mw

    Partitioned by year on trade_date.
    """
    __tablename__ = 'ceps_derived_features_15min'
    __table_args__ = (
        PrimaryKeyConstraint('trade_date', 'time_interval', 'id', name='ceps_derived_features_15min_pkey'),
        UniqueConstraint('trade_date', 'time_interval', name='uq_ceps_derived_features_15min'),
        {'schema': DB_SCHEMA}
    )

    id: Mapped[int] = mapped_column(Integer, autoincrement=True)
    trade_date: Mapped[date] = mapped_column(Date, nullable=False)
    time_interval: Mapped[str] = mapped_column(String(11), nullable=False)
    # Rolling memory
    imb_roll_2h: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 5))
    imb_roll_4h: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 5))
    imb_integral_4h: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 5))
    # Forecast surprise
    solar_error_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    wind_error_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    gen_total_error_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    created_at: Mapped[Optional[datetime]] = mapped_column(DateTime, server_default='CURRENT_TIMESTAMP')
    updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime, server_default='CURRENT_TIMESTAMP')


class CnbExchangeRate(Base):
    """CNB daily CZK/EUR exchange rate (Czech National Bank fixing).

    One rate per business day. CNB quotes as "CZK per 1 EUR" (e.g., 24.415).
    No weekend/holiday rows — CNB only publishes on business days.
    """
    __tablename__ = 'cnb_exchange_rate'
    __table_args__ = (
        PrimaryKeyConstraint('rate_date'),
        {'schema': DB_SCHEMA}
    )

    id: Mapped[int] = mapped_column(Integer, autoincrement=True)
    rate_date: Mapped[date] = mapped_column(Date, nullable=False)
    czk_eur: Mapped[Decimal] = mapped_column(Numeric(10, 6), nullable=False)
    created_at: Mapped[Optional[datetime]] = mapped_column(DateTime, server_default='CURRENT_TIMESTAMP')
    updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime, server_default='CURRENT_TIMESTAMP')


class DaBid(Base):
    """OTE Day-Ahead Market matching curve bid stacks.

    Stores aggregated supply/demand bids from the DAM auction.
    Period is 1-96 (15-minute intervals), converted from XML per-hour periods.
    """
    __tablename__ = 'da_bid'
    __table_args__ = (
        PrimaryKeyConstraint('delivery_date', 'period', 'side', 'price', 'order_resolution', name='da_bid_pkey'),
        {'schema': DB_SCHEMA}
    )

    delivery_date: Mapped[date] = mapped_column(Date, nullable=False)
    period: Mapped[int] = mapped_column(Integer, nullable=False)
    time_interval: Mapped[str] = mapped_column(String(11), nullable=False)
    side: Mapped[str] = mapped_column(String(4), nullable=False)
    price: Mapped[Decimal] = mapped_column(Numeric(10, 2), nullable=False)
    volume_bid: Mapped[Decimal] = mapped_column(Numeric(12, 3), nullable=False)
    volume_matched: Mapped[Decimal] = mapped_column(Numeric(12, 3), nullable=False)
    order_resolution: Mapped[str] = mapped_column(String(5), nullable=False)
    created_at: Mapped[Optional[datetime]] = mapped_column(DateTime, server_default='CURRENT_TIMESTAMP')


class DaPeriodSummary(Base):
    """OTE Day-Ahead Market per-period depth analytics.

    Computed from da_bid after each fetch: clearing price/volume,
    nearest unmatched supply/demand steps, and price/volume gaps.
    """
    __tablename__ = 'da_period_summary'
    __table_args__ = (
        PrimaryKeyConstraint('delivery_date', 'period', name='da_period_summary_pkey'),
        {'schema': DB_SCHEMA}
    )

    delivery_date: Mapped[date] = mapped_column(Date, nullable=False)
    period: Mapped[int] = mapped_column(Integer, nullable=False)
    time_interval: Mapped[str] = mapped_column(String(11), nullable=False)
    clearing_price: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))
    clearing_volume: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    supply_next_price: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))
    supply_next_volume: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    supply_price_gap: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))
    supply_volume_gap: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    demand_next_price: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))
    demand_next_volume: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    demand_price_gap: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))
    demand_volume_gap: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    created_at: Mapped[Optional[datetime]] = mapped_column(DateTime, server_default='CURRENT_TIMESTAMP')


class DaCurveDepth(Base):
    """OTE Day-Ahead Market curve walls anchored to clearing.

    For each period, stores the largest price jump (wall) found by walking
    the DAM curve outward from clearing in four directions: supply,
    supply_matched, demand, demand_matched. price_from_clearing is signed
    (negative for supply_matched and demand). NULL across a direction's
    three fields when that side has < 2 bids in that range.
    """
    __tablename__ = 'da_curve_depth'
    __table_args__ = (
        PrimaryKeyConstraint('delivery_date', 'period', name='da_curve_depth_pkey'),
        {'schema': DB_SCHEMA}
    )

    delivery_date: Mapped[date] = mapped_column(Date, nullable=False)
    period: Mapped[int] = mapped_column(Integer, nullable=False)
    time_interval: Mapped[str] = mapped_column(String(11), nullable=False)
    clearing_price: Mapped[Decimal] = mapped_column(Numeric(10, 2), nullable=False)

    supply_mw_from_clearing: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    supply_price_from_clearing: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))
    supply_slope: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4))

    supply_matched_mw_from_clearing: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    supply_matched_price_from_clearing: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))
    supply_matched_slope: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4))

    demand_mw_from_clearing: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    demand_price_from_clearing: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))
    demand_slope: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4))

    demand_matched_mw_from_clearing: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    demand_matched_price_from_clearing: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))
    demand_matched_slope: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4))

    created_at: Mapped[Optional[datetime]] = mapped_column(DateTime, server_default='CURRENT_TIMESTAMP')


class OtePricesIda(Base):
    """OTE Intraday Auction (IDA1/IDA2/IDA3) prices (15-minute intervals)."""
    __tablename__ = 'ote_prices_ida'
    __table_args__ = (
        PrimaryKeyConstraint('id', name='ote_prices_ida_pkey'),
        UniqueConstraint('trade_date', 'period', 'ida_idx', name='ote_prices_ida_trade_date_period_ida_idx_key'),
        {'schema': DB_SCHEMA}
    )

    id: Mapped[int] = mapped_column(Integer, autoincrement=True)
    trade_date: Mapped[date] = mapped_column(Date, nullable=False)
    period: Mapped[int] = mapped_column(Integer, nullable=False)
    ida_idx: Mapped[int] = mapped_column(Integer, nullable=False)
    time_interval: Mapped[str] = mapped_column(String(11), nullable=False)
    price_eur_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))
    volume_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    saldo_dm_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    export_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    import_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    created_at: Mapped[Optional[datetime]] = mapped_column(DateTime, server_default='CURRENT_TIMESTAMP')
    updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime, server_default='CURRENT_TIMESTAMP')


class WeatherForecast(Base):
    """Open-Meteo weather forecast data (15-min live, hourly backfill).

    D+1 forecasts for central Czechia (lat=49.80, lon=15.47).
    Variables: temperature_2m, shortwave/direct radiation, cloud_cover, wind_speed_10m.
    """
    __tablename__ = 'weather_forecast'
    __table_args__ = (
        PrimaryKeyConstraint('trade_date', 'time_interval', 'forecast_made_at'),
        {'schema': DB_SCHEMA}
    )

    trade_date: Mapped[date] = mapped_column(Date, nullable=False)
    time_interval: Mapped[str] = mapped_column(String(11), nullable=False)
    forecast_made_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    temperature_2m_degc: Mapped[Optional[Decimal]] = mapped_column(Numeric(6, 2))
    shortwave_radiation_wm2: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 2))
    direct_radiation_wm2: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 2))
    cloud_cover_pct: Mapped[Optional[Decimal]] = mapped_column(Numeric(5, 2))
    wind_speed_10m_kmh: Mapped[Optional[Decimal]] = mapped_column(Numeric(6, 2))
    created_at: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP(timezone=True), server_default='CURRENT_TIMESTAMP')
    updated_at: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP(timezone=True), server_default='CURRENT_TIMESTAMP')


class WeatherCurrent(Base):
    """Open-Meteo current weather conditions (15-min snapshots).

    Observed conditions at central Czechia (lat=49.80, lon=15.47).
    Variables: temperature, shortwave/direct radiation, cloud cover, wind speed.
    """
    __tablename__ = 'weather_current'
    __table_args__ = (
        PrimaryKeyConstraint('trade_date', 'time_interval'),
        {'schema': DB_SCHEMA}
    )

    trade_date: Mapped[date] = mapped_column(Date, nullable=False)
    time_interval: Mapped[str] = mapped_column(String(11), nullable=False)
    temperature_2m_degc: Mapped[Optional[Decimal]] = mapped_column(Numeric(6, 2))
    shortwave_radiation_wm2: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 2))
    direct_radiation_wm2: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 2))
    cloud_cover_pct: Mapped[Optional[Decimal]] = mapped_column(Numeric(5, 2))
    wind_speed_10m_kmh: Mapped[Optional[Decimal]] = mapped_column(Numeric(6, 2))
    created_at: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP(timezone=True), server_default='CURRENT_TIMESTAMP')
    updated_at: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP(timezone=True), server_default='CURRENT_TIMESTAMP')


# =============================================================================
# 60-minute aggregations (see docs/60min_tables_plan.md, migrations 057-061)
#
# Every 60-min table keys solely on time_interval (no `period` column).
# Column types and rules mirror the 15-min source 1:1.
# =============================================================================


class DaPeriodSummary60Min(Base):
    """Day-ahead per-period summary aggregated to 60-min hours."""
    __tablename__ = 'da_period_summary_60min'
    __table_args__ = (
        PrimaryKeyConstraint('delivery_date', 'time_interval', name='da_period_summary_60min_pkey'),
        {'schema': DB_SCHEMA}
    )

    delivery_date: Mapped[date] = mapped_column(Date, nullable=False)
    time_interval: Mapped[str] = mapped_column(String(11), nullable=False)
    clearing_price: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))
    clearing_volume: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    supply_next_price: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))
    supply_next_volume: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    supply_price_gap: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))
    supply_volume_gap: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    demand_next_price: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))
    demand_next_volume: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    demand_price_gap: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))
    demand_volume_gap: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    created_at: Mapped[Optional[datetime]] = mapped_column(DateTime, server_default='CURRENT_TIMESTAMP')


class DaCurveDepth60Min(Base):
    """Day-ahead curve depth aggregated to 60-min hours."""
    __tablename__ = 'da_curve_depth_60min'
    __table_args__ = (
        PrimaryKeyConstraint('delivery_date', 'time_interval', name='da_curve_depth_60min_pkey'),
        {'schema': DB_SCHEMA}
    )

    delivery_date: Mapped[date] = mapped_column(Date, nullable=False)
    time_interval: Mapped[str] = mapped_column(String(11), nullable=False)
    clearing_price: Mapped[Decimal] = mapped_column(Numeric(10, 2), nullable=False)
    supply_mw_from_clearing: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    supply_price_from_clearing: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))
    supply_slope: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4))
    supply_matched_mw_from_clearing: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    supply_matched_price_from_clearing: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))
    supply_matched_slope: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4))
    demand_mw_from_clearing: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    demand_price_from_clearing: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))
    demand_slope: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4))
    demand_matched_mw_from_clearing: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    demand_matched_price_from_clearing: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))
    demand_matched_slope: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4))
    created_at: Mapped[Optional[datetime]] = mapped_column(DateTime, server_default='CURRENT_TIMESTAMP')


class OtePricesImbalance60Min(Base):
    """OTE-CR domestic imbalance settlement aggregated to 60-min hours.

    CZ-specific CZK/MWh table — distinct from EntsoeImbalancePrices60Min
    which holds the ENTSO-E per-country EUR/MWh series. See
    docs/60min_tables_plan.md §4.7 for the column-by-column rules:
    volumes & costs SUM; settlement and component prices MEAN.
    """
    __tablename__ = 'ote_prices_imbalance_60min'
    __table_args__ = (
        PrimaryKeyConstraint('id', name='ote_prices_imbalance_60min_pkey'),
        UniqueConstraint('trade_date', 'time_interval', name='ote_prices_imbalance_60min_trade_date_interval_key'),
        {'schema': DB_SCHEMA}
    )

    id: Mapped[int] = mapped_column(Integer, autoincrement=True)
    trade_date: Mapped[date] = mapped_column(Date, nullable=False)
    time_interval: Mapped[str] = mapped_column(String(11), nullable=False)
    system_imbalance_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 5))
    absolute_imbalance_sum_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 5))
    positive_imbalance_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 5))
    negative_imbalance_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 5))
    rounded_imbalance_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 5))
    cost_of_be_czk: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    cost_of_imbalance_czk: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    settlement_price_imbalance_czk_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    settlement_price_counter_imbalance_czk_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    price_protective_be_component_czk_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    price_be_component_czk_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    price_im_component_czk_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    price_si_component_czk_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    price_not_performed_activation_czk_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    created_at: Mapped[Optional[datetime]] = mapped_column(DateTime, server_default='CURRENT_TIMESTAMP')
    updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime, server_default='CURRENT_TIMESTAMP')


class OtePricesIda60Min(Base):
    """OTE intraday auction prices aggregated to 60-min hours."""
    __tablename__ = 'ote_prices_ida_60min'
    __table_args__ = (
        PrimaryKeyConstraint('id', name='ote_prices_ida_60min_pkey'),
        UniqueConstraint('trade_date', 'time_interval', 'ida_idx', name='ote_prices_ida_60min_trade_date_interval_ida_idx_key'),
        {'schema': DB_SCHEMA}
    )

    id: Mapped[int] = mapped_column(Integer, autoincrement=True)
    trade_date: Mapped[date] = mapped_column(Date, nullable=False)
    time_interval: Mapped[str] = mapped_column(String(11), nullable=False)
    ida_idx: Mapped[int] = mapped_column(Integer, nullable=False)
    price_eur_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))
    volume_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    saldo_dm_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    export_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    import_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    created_at: Mapped[Optional[datetime]] = mapped_column(DateTime, server_default='CURRENT_TIMESTAMP')
    updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime, server_default='CURRENT_TIMESTAMP')


class WeatherCurrent60Min(Base):
    """Open-Meteo current weather aggregated to 60-min hours."""
    __tablename__ = 'weather_current_60min'
    __table_args__ = (
        PrimaryKeyConstraint('trade_date', 'time_interval'),
        {'schema': DB_SCHEMA}
    )

    trade_date: Mapped[date] = mapped_column(Date, nullable=False)
    time_interval: Mapped[str] = mapped_column(String(11), nullable=False)
    temperature_2m_degc: Mapped[Optional[Decimal]] = mapped_column(Numeric(6, 2))
    shortwave_radiation_wm2: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 2))
    direct_radiation_wm2: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 2))
    cloud_cover_pct: Mapped[Optional[Decimal]] = mapped_column(Numeric(5, 2))
    wind_speed_10m_kmh: Mapped[Optional[Decimal]] = mapped_column(Numeric(6, 2))
    created_at: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP(timezone=True), server_default='CURRENT_TIMESTAMP')
    updated_at: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP(timezone=True), server_default='CURRENT_TIMESTAMP')


class WeatherForecast60Min(Base):
    """Open-Meteo weather forecast aggregated to 60-min hours."""
    __tablename__ = 'weather_forecast_60min'
    __table_args__ = (
        PrimaryKeyConstraint('trade_date', 'time_interval', 'forecast_made_at'),
        {'schema': DB_SCHEMA}
    )

    trade_date: Mapped[date] = mapped_column(Date, nullable=False)
    time_interval: Mapped[str] = mapped_column(String(11), nullable=False)
    forecast_made_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    temperature_2m_degc: Mapped[Optional[Decimal]] = mapped_column(Numeric(6, 2))
    shortwave_radiation_wm2: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 2))
    direct_radiation_wm2: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 2))
    cloud_cover_pct: Mapped[Optional[Decimal]] = mapped_column(Numeric(5, 2))
    wind_speed_10m_kmh: Mapped[Optional[Decimal]] = mapped_column(Numeric(6, 2))
    created_at: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP(timezone=True), server_default='CURRENT_TIMESTAMP')
    updated_at: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP(timezone=True), server_default='CURRENT_TIMESTAMP')


class CepsActualImbalance60Min(Base):
    """CEPS actual imbalance aggregated to 60-min hours. Partitioned by year."""
    __tablename__ = 'ceps_actual_imbalance_60min'
    __table_args__ = (
        PrimaryKeyConstraint('trade_date', 'time_interval', 'id', name='ceps_actual_imbalance_60min_pkey'),
        UniqueConstraint('trade_date', 'time_interval', name='uq_ceps_actual_imbalance_60min_trade_date_interval'),
        {'schema': DB_SCHEMA}
    )

    id: Mapped[int] = mapped_column(Integer, autoincrement=True)
    trade_date: Mapped[date] = mapped_column(Date, nullable=False)
    time_interval: Mapped[str] = mapped_column(String(11), nullable=False)
    load_mean_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 5))
    load_median_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 5))
    created_at: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP(timezone=True), server_default='CURRENT_TIMESTAMP')
    updated_at: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP(timezone=True), server_default='CURRENT_TIMESTAMP')


class CepsEstimatedImbalancePrice60Min(Base):
    """CEPS estimated imbalance price aggregated to 60-min hours. Partitioned by year."""
    __tablename__ = 'ceps_estimated_imbalance_price_60min'
    __table_args__ = (
        PrimaryKeyConstraint('trade_date', 'time_interval', 'id', name='ceps_estimated_imbalance_price_60min_pkey'),
        UniqueConstraint('trade_date', 'time_interval', name='uq_ceps_estimated_imbalance_price_60min_trade_date_interval'),
        {'schema': DB_SCHEMA}
    )

    id: Mapped[int] = mapped_column(Integer, autoincrement=True)
    trade_date: Mapped[date] = mapped_column(Date, nullable=False)
    time_interval: Mapped[str] = mapped_column(String(11), nullable=False)
    estimated_price_czk_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    created_at: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP(timezone=True), server_default='CURRENT_TIMESTAMP')
    updated_at: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP(timezone=True), server_default='CURRENT_TIMESTAMP')


class CepsActualRePrice60Min(Base):
    """CEPS reserve energy prices aggregated to 60-min hours. Partitioned by year."""
    __tablename__ = 'ceps_actual_re_price_60min'
    __table_args__ = (
        PrimaryKeyConstraint('trade_date', 'time_interval', 'id', name='ceps_actual_re_price_60min_pkey'),
        UniqueConstraint('trade_date', 'time_interval', name='uq_ceps_actual_re_price_60min_trade_date_interval'),
        {'schema': DB_SCHEMA}
    )

    id: Mapped[int] = mapped_column(Integer, autoincrement=True)
    trade_date: Mapped[date] = mapped_column(Date, nullable=False)
    time_interval: Mapped[str] = mapped_column(String(11), nullable=False)
    price_afrr_plus_mean_eur_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    price_afrr_plus_median_eur_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    price_afrr_plus_last_at_interval_eur_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    price_afrr_minus_mean_eur_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    price_afrr_minus_median_eur_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    price_afrr_minus_last_at_interval_eur_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    price_mfrr_plus_mean_eur_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    price_mfrr_plus_median_eur_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    price_mfrr_plus_last_at_interval_eur_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    price_mfrr_minus_mean_eur_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    price_mfrr_minus_median_eur_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    price_mfrr_minus_last_at_interval_eur_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    price_mfrr_5_mean_eur_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    price_mfrr_5_median_eur_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    price_mfrr_5_last_at_interval_eur_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    created_at: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP(timezone=True), server_default='CURRENT_TIMESTAMP')
    updated_at: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP(timezone=True), server_default='CURRENT_TIMESTAMP')


class CepsSvrActivation60Min(Base):
    """CEPS SVR activation aggregated to 60-min hours. Partitioned by year."""
    __tablename__ = 'ceps_svr_activation_60min'
    __table_args__ = (
        PrimaryKeyConstraint('trade_date', 'time_interval', 'id', name='ceps_svr_activation_60min_pkey'),
        UniqueConstraint('trade_date', 'time_interval', name='uq_ceps_svr_activation_60min_trade_date_interval'),
        {'schema': DB_SCHEMA}
    )

    id: Mapped[int] = mapped_column(Integer, autoincrement=True)
    trade_date: Mapped[date] = mapped_column(Date, nullable=False)
    time_interval: Mapped[str] = mapped_column(String(11), nullable=False)
    afrr_plus_mean_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    afrr_plus_median_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    afrr_plus_last_at_interval_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    afrr_minus_mean_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    afrr_minus_median_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    afrr_minus_last_at_interval_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    mfrr_plus_mean_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    mfrr_plus_median_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    mfrr_plus_last_at_interval_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    mfrr_minus_mean_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    mfrr_minus_median_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    mfrr_minus_last_at_interval_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    created_at: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP(timezone=True), server_default='CURRENT_TIMESTAMP')
    updated_at: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP(timezone=True), server_default='CURRENT_TIMESTAMP')


class CepsExportImportSvr60Min(Base):
    """CEPS export/import SVR aggregated to 60-min hours. Partitioned by year."""
    __tablename__ = 'ceps_export_import_svr_60min'
    __table_args__ = (
        PrimaryKeyConstraint('trade_date', 'time_interval', 'id', name='ceps_export_import_svr_60min_pkey'),
        UniqueConstraint('trade_date', 'time_interval', name='uq_ceps_export_import_svr_60min_trade_date_interval'),
        {'schema': DB_SCHEMA}
    )

    id: Mapped[int] = mapped_column(Integer, autoincrement=True)
    trade_date: Mapped[date] = mapped_column(Date, nullable=False)
    time_interval: Mapped[str] = mapped_column(String(11), nullable=False)
    imbalance_netting_mean_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 5))
    imbalance_netting_median_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 5))
    imbalance_netting_last_at_interval_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 5))
    mari_mfrr_mean_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 5))
    mari_mfrr_median_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 5))
    mari_mfrr_last_at_interval_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 5))
    picasso_afrr_mean_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 5))
    picasso_afrr_median_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 5))
    picasso_afrr_last_at_interval_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 5))
    sum_exchange_mean_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 5))
    sum_exchange_median_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 5))
    sum_exchange_last_at_interval_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 5))
    created_at: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP(timezone=True), server_default='CURRENT_TIMESTAMP')
    updated_at: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP(timezone=True), server_default='CURRENT_TIMESTAMP')


class CepsGeneration60Min(Base):
    """CEPS generation by plant type aggregated to 60-min hours. Partitioned by year."""
    __tablename__ = 'ceps_generation_60min'
    __table_args__ = (
        PrimaryKeyConstraint('trade_date', 'time_interval', 'id', name='ceps_generation_60min_pkey'),
        UniqueConstraint('trade_date', 'time_interval', name='uq_ceps_generation_60min_trade_date_interval'),
        {'schema': DB_SCHEMA}
    )

    id: Mapped[int] = mapped_column(Integer, autoincrement=True)
    trade_date: Mapped[date] = mapped_column(Date, nullable=False)
    time_interval: Mapped[str] = mapped_column(String(11), nullable=False)
    tpp_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    ccgt_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    npp_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    hpp_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    pspp_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    altpp_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    appp_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    wpp_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    pvpp_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    created_at: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP(timezone=True), server_default='CURRENT_TIMESTAMP')
    updated_at: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP(timezone=True), server_default='CURRENT_TIMESTAMP')


class CepsGenerationPlan60Min(Base):
    """CEPS planned generation aggregated to 60-min hours. Partitioned by year."""
    __tablename__ = 'ceps_generation_plan_60min'
    __table_args__ = (
        PrimaryKeyConstraint('trade_date', 'time_interval', 'id', name='ceps_generation_plan_60min_pkey'),
        UniqueConstraint('trade_date', 'time_interval', name='uq_ceps_generation_plan_60min_trade_date_interval'),
        {'schema': DB_SCHEMA}
    )

    id: Mapped[int] = mapped_column(Integer, autoincrement=True)
    trade_date: Mapped[date] = mapped_column(Date, nullable=False)
    time_interval: Mapped[str] = mapped_column(String(11), nullable=False)
    total_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    created_at: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP(timezone=True), server_default='CURRENT_TIMESTAMP')
    updated_at: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP(timezone=True), server_default='CURRENT_TIMESTAMP')


class CepsGenerationRes60Min(Base):
    """CEPS renewable generation aggregated to 60-min hours. Partitioned by year."""
    __tablename__ = 'ceps_generation_res_60min'
    __table_args__ = (
        PrimaryKeyConstraint('trade_date', 'time_interval', 'id', name='ceps_generation_res_60min_pkey'),
        UniqueConstraint('trade_date', 'time_interval', name='uq_ceps_generation_res_60min_trade_date_interval'),
        {'schema': DB_SCHEMA}
    )

    id: Mapped[int] = mapped_column(Integer, autoincrement=True)
    trade_date: Mapped[date] = mapped_column(Date, nullable=False)
    time_interval: Mapped[str] = mapped_column(String(11), nullable=False)
    wind_mean_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    wind_median_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    wind_last_at_interval_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    solar_mean_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    solar_median_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    solar_last_at_interval_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    created_at: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP(timezone=True), server_default='CURRENT_TIMESTAMP')
    updated_at: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP(timezone=True), server_default='CURRENT_TIMESTAMP')


class Ceps1MinFeatures60Min(Base):
    """CEPS 1-min distributional features re-aggregated natively to 60-min hours.

    Note: populated by reading the underlying 1-min source over a 60-min
    window, NOT by aggregating ceps_1min_features_15min rows (aggregating
    stats-of-stats is mathematically wrong). Partitioned by year.
    """
    __tablename__ = 'ceps_1min_features_60min'
    __table_args__ = (
        PrimaryKeyConstraint('trade_date', 'time_interval', 'id', name='ceps_1min_features_60min_pkey'),
        UniqueConstraint('trade_date', 'time_interval', name='uq_ceps_1min_features_60min_trade_date_interval'),
        {'schema': DB_SCHEMA}
    )

    id: Mapped[int] = mapped_column(Integer, autoincrement=True)
    trade_date: Mapped[date] = mapped_column(Date, nullable=False)
    time_interval: Mapped[str] = mapped_column(String(11), nullable=False)
    minute_count: Mapped[Optional[int]] = mapped_column(SmallInteger)
    afrr_plus_min_eur: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    afrr_plus_max_eur: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    afrr_plus_std_eur: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 5))
    afrr_plus_skew: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 5))
    afrr_minus_min_eur: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    afrr_minus_max_eur: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    afrr_minus_std_eur: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 5))
    afrr_minus_skew: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 5))
    mfrr_plus_min_eur: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    mfrr_plus_max_eur: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    mfrr_plus_std_eur: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 5))
    mfrr_plus_skew: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 5))
    mfrr_minus_min_eur: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    mfrr_minus_max_eur: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    mfrr_minus_std_eur: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 5))
    mfrr_minus_skew: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 5))
    imbalance_range_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 5))
    imbalance_std_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 5))
    imbalance_slope: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 8))
    minutes_at_floor: Mapped[Optional[int]] = mapped_column(SmallInteger)
    minutes_near_peak: Mapped[Optional[int]] = mapped_column(SmallInteger)
    saturation_count: Mapped[Optional[int]] = mapped_column(SmallInteger)
    total_active_mean_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    total_active_std_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 5))
    platform_active_count: Mapped[Optional[int]] = mapped_column(SmallInteger)
    afrr_mfrr_plus_spread_mean_eur: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    afrr_mfrr_plus_spread_std_eur: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 5))
    afrr_mfrr_minus_spread_mean_eur: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    afrr_mfrr_minus_spread_std_eur: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 5))
    created_at: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP(timezone=True), server_default='CURRENT_TIMESTAMP')
    updated_at: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP(timezone=True), server_default='CURRENT_TIMESTAMP')


class CepsDerivedFeatures60Min(Base):
    """CEPS derived rolling/surprise features at 60-min hours. Partitioned by year.

    Rule for every column is `last` — the value of the last 15-min quarter
    of the hour. Rolling fields already span >= 2h windows; surprise
    fields are differences of means already taken at quarter resolution.
    """
    __tablename__ = 'ceps_derived_features_60min'
    __table_args__ = (
        PrimaryKeyConstraint('trade_date', 'time_interval', 'id', name='ceps_derived_features_60min_pkey'),
        UniqueConstraint('trade_date', 'time_interval', name='uq_ceps_derived_features_60min_trade_date_interval'),
        {'schema': DB_SCHEMA}
    )

    id: Mapped[int] = mapped_column(Integer, autoincrement=True)
    trade_date: Mapped[date] = mapped_column(Date, nullable=False)
    time_interval: Mapped[str] = mapped_column(String(11), nullable=False)
    imb_roll_2h: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 5))
    imb_roll_4h: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 5))
    imb_integral_4h: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 5))
    solar_error_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    wind_error_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    gen_total_error_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    created_at: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP(timezone=True), server_default='CURRENT_TIMESTAMP')
    updated_at: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP(timezone=True), server_default='CURRENT_TIMESTAMP')


class EntsoeLoad60Min(Base):
    """ENTSO-E load aggregated to 60-min hours. Partitioned by country_code."""
    __tablename__ = 'entsoe_load_60min'
    __table_args__ = (
        PrimaryKeyConstraint('trade_date', 'time_interval', 'area_id', 'country_code'),
        {'schema': DB_SCHEMA}
    )

    id: Mapped[int] = mapped_column(Integer, autoincrement=True)
    trade_date: Mapped[date] = mapped_column(Date, nullable=False)
    time_interval: Mapped[str] = mapped_column(String(11), nullable=False)
    area_id: Mapped[int] = mapped_column(Integer, nullable=False)
    country_code: Mapped[str] = mapped_column(String(5), nullable=False)
    actual_load_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    forecast_load_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    created_at: Mapped[Optional[datetime]] = mapped_column(DateTime, server_default='CURRENT_TIMESTAMP')
    updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime, server_default='CURRENT_TIMESTAMP')


class EntsoeGenerationForecast60Min(Base):
    """ENTSO-E day-ahead generation forecast aggregated to 60-min hours. Partitioned by country_code."""
    __tablename__ = 'entsoe_generation_forecast_60min'
    __table_args__ = (
        PrimaryKeyConstraint('trade_date', 'time_interval', 'area_id', 'country_code'),
        {'schema': DB_SCHEMA}
    )

    id: Mapped[int] = mapped_column(Integer, autoincrement=True)
    trade_date: Mapped[date] = mapped_column(Date, nullable=False)
    time_interval: Mapped[str] = mapped_column(String(11), nullable=False)
    area_id: Mapped[int] = mapped_column(Integer, nullable=False)
    country_code: Mapped[str] = mapped_column(String(5), nullable=False)
    forecast_solar_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    forecast_wind_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    forecast_wind_offshore_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    created_at: Mapped[Optional[datetime]] = mapped_column(DateTime, server_default='CURRENT_TIMESTAMP')
    updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime, server_default='CURRENT_TIMESTAMP')


class EntsoeGenerationActual60Min(Base):
    """ENTSO-E actual generation aggregated to 60-min hours. Partitioned by country_code."""
    __tablename__ = 'entsoe_generation_actual_60min'
    __table_args__ = (
        PrimaryKeyConstraint('trade_date', 'time_interval', 'area_id', 'country_code'),
        {'schema': DB_SCHEMA}
    )

    id: Mapped[int] = mapped_column(Integer, autoincrement=True)
    trade_date: Mapped[date] = mapped_column(Date, nullable=False)
    time_interval: Mapped[str] = mapped_column(String(11), nullable=False)
    area_id: Mapped[int] = mapped_column(Integer, nullable=False)
    country_code: Mapped[str] = mapped_column(String(5), nullable=False)
    gen_nuclear_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    gen_coal_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    gen_gas_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    gen_solar_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    gen_wind_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    gen_wind_offshore_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    gen_hydro_pumped_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    gen_biomass_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    gen_hydro_other_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    created_at: Mapped[Optional[datetime]] = mapped_column(DateTime, server_default='CURRENT_TIMESTAMP')
    updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime, server_default='CURRENT_TIMESTAMP')


class EntsoeCrossBorderFlows60Min(Base):
    """ENTSO-E cross-border physical flows aggregated to 60-min hours."""
    __tablename__ = 'entsoe_cross_border_flows_60min'
    __table_args__ = (
        PrimaryKeyConstraint('id', name='entsoe_cross_border_flows_60min_pkey'),
        UniqueConstraint('delivery_datetime', 'area_id', name='entsoe_cross_border_flows_60min_datetime_area_key'),
        UniqueConstraint('trade_date', 'time_interval', 'area_id', name='entsoe_cross_border_flows_60min_trade_date_interval_area_key'),
        {'schema': DB_SCHEMA}
    )

    id: Mapped[int] = mapped_column(Integer, autoincrement=True)
    trade_date: Mapped[date] = mapped_column(Date, nullable=False)
    time_interval: Mapped[str] = mapped_column(String(11), nullable=False)
    delivery_datetime: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    area_id: Mapped[str] = mapped_column(String(20), nullable=False)
    flow_de_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    flow_at_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    flow_pl_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    flow_sk_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    flow_total_net_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    created_at: Mapped[Optional[datetime]] = mapped_column(DateTime, server_default='CURRENT_TIMESTAMP')
    updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime, server_default='CURRENT_TIMESTAMP')


class EntsoeScheduledCrossBorderFlows60Min(Base):
    """ENTSO-E scheduled cross-border exchanges aggregated to 60-min hours."""
    __tablename__ = 'entsoe_scheduled_cross_border_flows_60min'
    __table_args__ = (
        PrimaryKeyConstraint('id', name='entsoe_sched_xborder_flows_60min_pkey'),
        UniqueConstraint('trade_date', 'time_interval', name='entsoe_sched_xborder_flows_60min_date_interval_key'),
        {'schema': DB_SCHEMA}
    )

    id: Mapped[int] = mapped_column(Integer, autoincrement=True)
    trade_date: Mapped[date] = mapped_column(Date, nullable=False)
    time_interval: Mapped[str] = mapped_column(String(11), nullable=False)
    scheduled_de_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    scheduled_at_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    scheduled_pl_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    scheduled_sk_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    scheduled_total_net_mw: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    created_at: Mapped[Optional[datetime]] = mapped_column(DateTime, server_default='CURRENT_TIMESTAMP')
    updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime, server_default='CURRENT_TIMESTAMP')


class EntsoeDayAheadPrices60Min(Base):
    """ENTSO-E day-ahead prices aggregated to 60-min hours. Partitioned by country_code."""
    __tablename__ = 'entsoe_day_ahead_prices_60min'
    __table_args__ = (
        PrimaryKeyConstraint('trade_date', 'time_interval', 'area_id', 'country_code'),
        {'schema': DB_SCHEMA}
    )

    id: Mapped[int] = mapped_column(Integer, autoincrement=True)
    trade_date: Mapped[date] = mapped_column(Date, nullable=False)
    time_interval: Mapped[str] = mapped_column(String(11), nullable=False)
    area_id: Mapped[int] = mapped_column(Integer, nullable=False)
    country_code: Mapped[str] = mapped_column(String(5), nullable=False)
    price_eur_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 3))
    created_at: Mapped[Optional[datetime]] = mapped_column(DateTime, server_default='CURRENT_TIMESTAMP')
    updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime, server_default='CURRENT_TIMESTAMP')


class EntsoeImbalancePrices60Min(Base):
    """ENTSO-E imbalance prices aggregated to 60-min hours. Partitioned by country_code.

    Added beyond the upstream ta-feature-api spec — see
    docs/60min_tables_plan.md §5 default #5.
    """
    __tablename__ = 'entsoe_imbalance_prices_60min'
    __table_args__ = (
        PrimaryKeyConstraint('trade_date', 'time_interval', 'area_id', 'country_code'),
        {'schema': DB_SCHEMA}
    )

    id: Mapped[int] = mapped_column(Integer, autoincrement=True)
    trade_date: Mapped[date] = mapped_column(Date, nullable=False)
    time_interval: Mapped[str] = mapped_column(String(11), nullable=False)
    area_id: Mapped[int] = mapped_column(Integer, nullable=False)
    country_code: Mapped[str] = mapped_column(String(5), nullable=False)
    pos_imb_price_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    pos_imb_scarcity_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    pos_imb_incentive_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    pos_imb_financial_neutrality_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    neg_imb_price_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    neg_imb_scarcity_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    neg_imb_incentive_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    neg_imb_financial_neutrality_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 3))
    imbalance_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 5))
    difference_mwh: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 5))
    situation: Mapped[Optional[str]] = mapped_column(String)
    status: Mapped[Optional[str]] = mapped_column(String)
    currency: Mapped[str] = mapped_column(String(3), nullable=False)
    delivery_datetime: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP(timezone=True))
    created_at: Mapped[Optional[datetime]] = mapped_column(DateTime, server_default='CURRENT_TIMESTAMP')
    updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime, server_default='CURRENT_TIMESTAMP')
