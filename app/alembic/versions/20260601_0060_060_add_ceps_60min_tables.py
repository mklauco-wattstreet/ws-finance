"""Add CEPS 60-min tables (10 tables, year-partitioned).

Revision ID: 060
Revises: 059
Create Date: 2026-06-01

Materialized 60-minute aggregations for all 10 CEPS 15-min tables:
  - ceps_actual_imbalance_60min
  - ceps_estimated_imbalance_price_60min
  - ceps_actual_re_price_60min
  - ceps_svr_activation_60min
  - ceps_export_import_svr_60min
  - ceps_generation_60min
  - ceps_generation_plan_60min
  - ceps_generation_res_60min
  - ceps_1min_features_60min
  - ceps_derived_features_60min

See docs/60min_tables_plan.md §4.4 for column-by-column rules.

Common shape per table:
  - BIGSERIAL id
  - PK (trade_date, time_interval, id)
  - UNIQUE (trade_date, time_interval)
  - PARTITION BY RANGE (trade_date) — 2024..2028 partitions
  - Index on trade_date
"""

from alembic import op


revision = '060'
down_revision = '059'
branch_labels = None
depends_on = None


PARTITION_YEARS = [2024, 2025, 2026, 2027, 2028]


def _create_partitioned_table(table: str, columns_sql: str) -> None:
    """Create a CEPS 60-min table partitioned BY RANGE (trade_date)."""
    op.execute(f"""
        CREATE TABLE finance.{table} (
            id BIGSERIAL,
            trade_date DATE NOT NULL,
            time_interval VARCHAR(11) NOT NULL,
            {columns_sql}
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (trade_date, time_interval, id),
            CONSTRAINT uq_{table}_trade_date_interval UNIQUE (trade_date, time_interval)
        ) PARTITION BY RANGE (trade_date);
    """)
    for year in PARTITION_YEARS:
        op.execute(f"""
            CREATE TABLE finance.{table}_{year}
            PARTITION OF finance.{table}
            FOR VALUES FROM ('{year}-01-01') TO ('{year + 1}-01-01');
        """)
    op.execute(f"""
        CREATE INDEX ix_{table}_trade_date
        ON finance.{table} (trade_date);
    """)
    op.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE finance.{table} TO user_finance;")
    op.execute(f"GRANT USAGE, SELECT ON SEQUENCE finance.{table}_id_seq TO user_finance;")


def upgrade() -> None:
    # ceps_actual_imbalance_60min ----------------------------------------
    _create_partitioned_table(
        "ceps_actual_imbalance_60min",
        """
            load_mean_mw NUMERIC(12, 5),
            load_median_mw NUMERIC(12, 5),
        """,
    )

    # ceps_estimated_imbalance_price_60min -------------------------------
    _create_partitioned_table(
        "ceps_estimated_imbalance_price_60min",
        """
            estimated_price_czk_mwh NUMERIC(12, 3),
        """,
    )

    # ceps_actual_re_price_60min -----------------------------------------
    _create_partitioned_table(
        "ceps_actual_re_price_60min",
        """
            price_afrr_plus_mean_eur_mwh NUMERIC(15, 3),
            price_afrr_plus_median_eur_mwh NUMERIC(15, 3),
            price_afrr_plus_last_at_interval_eur_mwh NUMERIC(15, 3),
            price_afrr_minus_mean_eur_mwh NUMERIC(15, 3),
            price_afrr_minus_median_eur_mwh NUMERIC(15, 3),
            price_afrr_minus_last_at_interval_eur_mwh NUMERIC(15, 3),
            price_mfrr_plus_mean_eur_mwh NUMERIC(15, 3),
            price_mfrr_plus_median_eur_mwh NUMERIC(15, 3),
            price_mfrr_plus_last_at_interval_eur_mwh NUMERIC(15, 3),
            price_mfrr_minus_mean_eur_mwh NUMERIC(15, 3),
            price_mfrr_minus_median_eur_mwh NUMERIC(15, 3),
            price_mfrr_minus_last_at_interval_eur_mwh NUMERIC(15, 3),
            price_mfrr_5_mean_eur_mwh NUMERIC(15, 3),
            price_mfrr_5_median_eur_mwh NUMERIC(15, 3),
            price_mfrr_5_last_at_interval_eur_mwh NUMERIC(15, 3),
        """,
    )

    # ceps_svr_activation_60min ------------------------------------------
    _create_partitioned_table(
        "ceps_svr_activation_60min",
        """
            afrr_plus_mean_mw NUMERIC(15, 3),
            afrr_plus_median_mw NUMERIC(15, 3),
            afrr_plus_last_at_interval_mw NUMERIC(15, 3),
            afrr_minus_mean_mw NUMERIC(15, 3),
            afrr_minus_median_mw NUMERIC(15, 3),
            afrr_minus_last_at_interval_mw NUMERIC(15, 3),
            mfrr_plus_mean_mw NUMERIC(15, 3),
            mfrr_plus_median_mw NUMERIC(15, 3),
            mfrr_plus_last_at_interval_mw NUMERIC(15, 3),
            mfrr_minus_mean_mw NUMERIC(15, 3),
            mfrr_minus_median_mw NUMERIC(15, 3),
            mfrr_minus_last_at_interval_mw NUMERIC(15, 3),
        """,
    )

    # ceps_export_import_svr_60min ---------------------------------------
    _create_partitioned_table(
        "ceps_export_import_svr_60min",
        """
            imbalance_netting_mean_mw NUMERIC(15, 5),
            imbalance_netting_median_mw NUMERIC(15, 5),
            imbalance_netting_last_at_interval_mw NUMERIC(15, 5),
            mari_mfrr_mean_mw NUMERIC(15, 5),
            mari_mfrr_median_mw NUMERIC(15, 5),
            mari_mfrr_last_at_interval_mw NUMERIC(15, 5),
            picasso_afrr_mean_mw NUMERIC(15, 5),
            picasso_afrr_median_mw NUMERIC(15, 5),
            picasso_afrr_last_at_interval_mw NUMERIC(15, 5),
            sum_exchange_mean_mw NUMERIC(15, 5),
            sum_exchange_median_mw NUMERIC(15, 5),
            sum_exchange_last_at_interval_mw NUMERIC(15, 5),
        """,
    )

    # ceps_generation_60min ----------------------------------------------
    _create_partitioned_table(
        "ceps_generation_60min",
        """
            tpp_mw NUMERIC(12, 3),
            ccgt_mw NUMERIC(12, 3),
            npp_mw NUMERIC(12, 3),
            hpp_mw NUMERIC(12, 3),
            pspp_mw NUMERIC(12, 3),
            altpp_mw NUMERIC(12, 3),
            appp_mw NUMERIC(12, 3),
            wpp_mw NUMERIC(12, 3),
            pvpp_mw NUMERIC(12, 3),
        """,
    )

    # ceps_generation_plan_60min -----------------------------------------
    _create_partitioned_table(
        "ceps_generation_plan_60min",
        """
            total_mw NUMERIC(12, 3),
        """,
    )

    # ceps_generation_res_60min ------------------------------------------
    _create_partitioned_table(
        "ceps_generation_res_60min",
        """
            wind_mean_mw NUMERIC(12, 3),
            wind_median_mw NUMERIC(12, 3),
            wind_last_at_interval_mw NUMERIC(12, 3),
            solar_mean_mw NUMERIC(12, 3),
            solar_median_mw NUMERIC(12, 3),
            solar_last_at_interval_mw NUMERIC(12, 3),
        """,
    )

    # ceps_1min_features_60min — populated by native re-aggregation -----
    _create_partitioned_table(
        "ceps_1min_features_60min",
        """
            minute_count SMALLINT,
            afrr_plus_min_eur NUMERIC(15, 3),
            afrr_plus_max_eur NUMERIC(15, 3),
            afrr_plus_std_eur NUMERIC(15, 5),
            afrr_plus_skew NUMERIC(10, 5),
            afrr_minus_min_eur NUMERIC(15, 3),
            afrr_minus_max_eur NUMERIC(15, 3),
            afrr_minus_std_eur NUMERIC(15, 5),
            afrr_minus_skew NUMERIC(10, 5),
            mfrr_plus_min_eur NUMERIC(15, 3),
            mfrr_plus_max_eur NUMERIC(15, 3),
            mfrr_plus_std_eur NUMERIC(15, 5),
            mfrr_plus_skew NUMERIC(10, 5),
            mfrr_minus_min_eur NUMERIC(15, 3),
            mfrr_minus_max_eur NUMERIC(15, 3),
            mfrr_minus_std_eur NUMERIC(15, 5),
            mfrr_minus_skew NUMERIC(10, 5),
            imbalance_range_mw NUMERIC(12, 5),
            imbalance_std_mw NUMERIC(12, 5),
            imbalance_slope NUMERIC(12, 8),
            minutes_at_floor SMALLINT,
            minutes_near_peak SMALLINT,
            saturation_count SMALLINT,
            total_active_mean_mw NUMERIC(12, 3),
            total_active_std_mw NUMERIC(12, 5),
            platform_active_count SMALLINT,
            afrr_mfrr_plus_spread_mean_eur NUMERIC(15, 3),
            afrr_mfrr_plus_spread_std_eur NUMERIC(15, 5),
            afrr_mfrr_minus_spread_mean_eur NUMERIC(15, 3),
            afrr_mfrr_minus_spread_std_eur NUMERIC(15, 5),
        """,
    )

    # ceps_derived_features_60min — rule `last` for every column --------
    _create_partitioned_table(
        "ceps_derived_features_60min",
        """
            imb_roll_2h NUMERIC(12, 5),
            imb_roll_4h NUMERIC(12, 5),
            imb_integral_4h NUMERIC(15, 5),
            solar_error_mw NUMERIC(12, 3),
            wind_error_mw NUMERIC(12, 3),
            gen_total_error_mw NUMERIC(12, 3),
        """,
    )


def downgrade() -> None:
    # Drop parent tables in reverse — CASCADE removes their partitions and indexes
    for table in [
        "ceps_derived_features_60min",
        "ceps_1min_features_60min",
        "ceps_generation_res_60min",
        "ceps_generation_plan_60min",
        "ceps_generation_60min",
        "ceps_export_import_svr_60min",
        "ceps_svr_activation_60min",
        "ceps_actual_re_price_60min",
        "ceps_estimated_imbalance_price_60min",
        "ceps_actual_imbalance_60min",
    ]:
        op.execute(f"DROP TABLE IF EXISTS finance.{table} CASCADE;")
