"""Border register access.

Loads borders from ``entsoe_borders`` (migration 090) joined to
``entsoe_areas`` for EIC codes and country codes, so runners iterate over
whatever is active in the database instead of a hard-coded list.
"""

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class Border:
    """One border (an unordered pair of areas) from ``entsoe_borders``."""

    border_id: int
    border_code: str
    kind: str                 # 'bidding_zone' | 'control_area'
    level: int                # 0 = CZ borders, 1 = borders of CZ neighbours
    area_a_id: int
    area_a_eic: str
    area_a_country: str
    area_b_id: int
    area_b_eic: str
    area_b_country: str


_SELECT_BORDERS = """
    SELECT b.border_id, b.border_code, b.kind, b.level,
           b.area_a_id, a.code, a.country_code,
           b.area_b_id, c.code, c.country_code
    FROM entsoe_borders b
    JOIN entsoe_areas a ON a.id = b.area_a_id
    JOIN entsoe_areas c ON c.id = b.area_b_id
    WHERE b.is_active
      AND (%(level)s IS NULL OR b.level = %(level)s)
      AND (%(kinds)s IS NULL OR b.kind = ANY(%(kinds)s))
    ORDER BY b.border_id
"""


def load_active_borders(
    conn,
    level: Optional[int] = None,
    kinds: Optional[tuple[str, ...]] = None,
) -> list[Border]:
    """Return the active borders, ordered by ``border_id``.

    Args:
        conn: Open psycopg2 connection (``search_path`` already pointing at
            the ``finance`` schema, as in ``BaseRunner.database_connection``).
        level: Only borders of this level (0 = CZ borders); None = all levels.
        kinds: Only these kinds (``'bidding_zone'``, ``'control_area'``);
            None = all kinds.
    """
    params = {"level": level, "kinds": list(kinds) if kinds is not None else None}
    with conn.cursor() as cur:
        cur.execute(_SELECT_BORDERS, params)
        rows = cur.fetchall()
    return [Border(*row) for row in rows]


def directions(border: Border) -> list[tuple[int, str, int, str]]:
    """Both directions of a border as ``(from_id, from_eic, to_id, to_eic)``.

    ``from`` is ENTSO-E ``out_Domain`` and ``to`` is ``in_Domain``: energy or
    capacity flows from -> to. The first tuple is a -> b, the second b -> a.
    """
    return [
        (border.area_a_id, border.area_a_eic, border.area_b_id, border.area_b_eic),
        (border.area_b_id, border.area_b_eic, border.area_a_id, border.area_a_eic),
    ]
