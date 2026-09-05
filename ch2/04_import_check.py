import unittest
import pandas as pd
import sqlalchemy
from sqlalchemy import text
from pandas.testing import assert_frame_equal

CSV_FILE_PATH = "processed/Traffic_Analysis_Novigrad_2026-08-20_2026-08-31_PROCESSED.csv"
DATABASE_URL = "postgresql+psycopg://adisspahic@localhost:5432/srp-traffic-analysis"

QUERY = """
SELECT m.id || '-' || v.seq   AS inference_id
, m.id                        AS motion_id
, m.ts                        AS ts
, m.file                      AS file
, cam.code                    AS camera_id
, m.cause                     AS cause
, m.peak_mass                 AS peak_mass
, m.dur_ms                    AS dur_ms
, m.pts_ms                    AS pts_ms
, vl.schema_v                 AS schema_v
, wo.temperature              AS temp_c
, wo.precipitation_mm         AS precip_mm
, wo.weather_code             AS weather_code
, m.lighting                  AS lighting
, i.road_condition             AS road_condition
, cl.name                     AS vehicle_class
, co.name                     AS color
, v.body                      AS body
, br.name                     AS make
, v.model                     AS model
, v.era                       AS era
, v.direction                 AS direction
, v.headlights_on             AS headlights_on
, v.commercial_markings       AS commercial_markings
, v.special_use               AS special_use
, v.truncated                 AS truncated
, v.plate_visible             AS plate_visible
FROM vehicle v
JOIN inference i            ON v.inference_fk = i.id
JOIN motion m               ON i.motion_fk = m.id
JOIN vlm vl                 ON i.vlm_fk = vl.id
JOIN camera cam             ON m.camera_fk = cam.id
JOIN weather_observation wo ON m.weather_observation_fk = wo.id
JOIN "class" cl              ON v.class_fk = cl.id
LEFT JOIN brand br          ON v.brand_fk = br.id
LEFT JOIN color co          ON v.color_fk = co.id
ORDER BY v.id ASC
"""

STRING_COLUMNS = [
    "inference_id",
    "file",
    "camera_id",
    "cause",
    "lighting",
    "road_condition",
    "vehicle_class",
    "color",
    "body",
    "make",
    "model",
    "era",
    "direction",
    "special_use",
]


def normalize(df):
    df = df.copy()
    df["ts"] = pd.to_datetime(df["ts"]).dt.tz_convert("UTC")
    df["temp_c"] = df["temp_c"].astype(float)
    df["precip_mm"] = df["precip_mm"].astype(float)
    df["pts_ms"] = df["pts_ms"].astype("Int64")
    for column in STRING_COLUMNS:
        df[column] = df[column].astype("string")
    return df


class TestDatabase(unittest.TestCase):
    def setUp(self):
        self.engine = sqlalchemy.create_engine(DATABASE_URL)
        self.connection = self.engine.connect()

        self.df = pd.read_csv(CSV_FILE_PATH)

        result = self.connection.execute(text(QUERY))
        self.db_df = pd.DataFrame(result.fetchall())
        self.db_df.columns = list(result.keys())

    def test_columns(self):
        self.assertListEqual(list(self.df.columns), list(self.db_df.columns))

    def test_dataframes(self):
        df = normalize(self.df).reset_index(drop=True)
        db_df = normalize(self.db_df).reset_index(drop=True)
        assert_frame_equal(df, db_df)

    def tearDown(self):
        self.connection.close()
        self.engine.dispose()


if __name__ == "__main__":
    unittest.main()
