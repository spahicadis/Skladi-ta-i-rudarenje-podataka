import json
from datetime import datetime
from decimal import Decimal
from typing import Optional

import pandas as pd
from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Integer,
    Numeric,
    SmallInteger,
    String,
    UniqueConstraint,
    create_engine,
    func,
    inspect,
    insert,
    select,
)
from sqlalchemy.orm import Mapped, Session, declarative_base, mapped_column

CSV_FILE_PATH = "processed/Traffic_Analysis_Novigrad_2026-08-20_2026-08-31_PROCESSED.csv"
CAMERAS_JSON_PATH = "../cameras.json"
VLM_JSON_PATH = "../vlm.json"

DATABASE_URL = "postgresql+psycopg://adisspahic@localhost:5432/srp-traffic-analysis"

# Boje se svrstavaju u nadskupine; ta grupa putuje sve do dimenzije vozila
# u skladištu, gdje služi kao viša razina prikaza od pojedinačne boje.
COLOR_GROUPS = {
    "white": "neutral",
    "black": "neutral",
    "gray": "neutral",
    "silver": "neutral",
    "beige": "neutral",
    "red": "warm",
    "orange": "warm",
    "yellow": "warm",
    "brown": "warm",
    "blue": "cool",
    "green": "cool",
    "purple": "cool",
    "unknown": "unknown",
}


# Relacijski model opisan je deklarativno: iz klasa koje slijede SQLAlchemy
# sam generira DDL sa svim ključevima i ograničenjima.
Base = declarative_base()


class Location(Base):
    __tablename__ = "location"
    id: Mapped[int] = mapped_column(primary_key=True)
    street: Mapped[Optional[str]] = mapped_column(String(80))
    house_number: Mapped[Optional[str]] = mapped_column(String(10))
    postal_code: Mapped[Optional[str]] = mapped_column(String(10))
    city: Mapped[Optional[str]] = mapped_column(String(60))
    county: Mapped[Optional[str]] = mapped_column(String(60))
    country: Mapped[Optional[str]] = mapped_column(String(60))


class Camera(Base):
    __tablename__ = "camera"
    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str] = mapped_column(String(30), nullable=False, unique=True)
    name: Mapped[Optional[str]] = mapped_column(String(60))
    device: Mapped[Optional[str]] = mapped_column(String(60))
    lat: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 6))
    lon: Mapped[Optional[Decimal]] = mapped_column(Numeric(9, 6))
    location_fk: Mapped[int] = mapped_column(ForeignKey("location.id"), nullable=False)


class Vlm(Base):
    __tablename__ = "vlm"
    id: Mapped[int] = mapped_column(primary_key=True)
    model: Mapped[str] = mapped_column(String(60), nullable=False)
    schema_v: Mapped[int] = mapped_column(SmallInteger, nullable=False, unique=True)
    prompt_v: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    valid_from: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class WeatherObservation(Base):
    __tablename__ = "weather_observation"
    id: Mapped[int] = mapped_column(primary_key=True)
    camera_fk: Mapped[int] = mapped_column(ForeignKey("camera.id"), nullable=False)
    hour_ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    temperature: Mapped[Optional[Decimal]] = mapped_column(Numeric(4, 1))
    precipitation_mm: Mapped[Optional[Decimal]] = mapped_column(Numeric(5, 2))
    weather_code: Mapped[Optional[int]] = mapped_column(SmallInteger)
    __table_args__ = (UniqueConstraint("camera_fk", "hour_ts"),)


class Motion(Base):
    __tablename__ = "motion"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=False)
    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    pts_ms: Mapped[Optional[int]] = mapped_column(BigInteger)
    peak_mass: Mapped[Optional[int]] = mapped_column(Integer)
    dur_ms: Mapped[Optional[int]] = mapped_column(Integer)
    cause: Mapped[Optional[str]] = mapped_column(String(10))
    file: Mapped[str] = mapped_column(String(255), nullable=False)
    lighting: Mapped[str] = mapped_column(String(5), nullable=False)
    camera_fk: Mapped[int] = mapped_column(ForeignKey("camera.id"), nullable=False)
    weather_observation_fk: Mapped[int] = mapped_column(ForeignKey("weather_observation.id"), nullable=False)
    __table_args__ = (
        CheckConstraint("cause IN ('QUIET','TIMEOUT','EOS')"),
        CheckConstraint("lighting IN ('day','dusk','night')"),
    )


class Inference(Base):
    __tablename__ = "inference"
    id: Mapped[int] = mapped_column(primary_key=True)
    motion_fk: Mapped[int] = mapped_column(ForeignKey("motion.id"), nullable=False)
    vlm_fk: Mapped[int] = mapped_column(ForeignKey("vlm.id"), nullable=False)
    road_condition: Mapped[Optional[str]] = mapped_column(String(10))
    __table_args__ = (
        UniqueConstraint("motion_fk", "vlm_fk"),
        CheckConstraint("road_condition IN ('dry','wet')"),
    )


class VehicleClass(Base):
    __tablename__ = "class"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(20), nullable=False, unique=True)


class Brand(Base):
    __tablename__ = "brand"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(40), nullable=False, unique=True)


class Color(Base):
    __tablename__ = "color"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(20), nullable=False, unique=True)
    color_group: Mapped[str] = mapped_column(String(20), nullable=False)


class Vehicle(Base):
    __tablename__ = "vehicle"
    id: Mapped[int] = mapped_column(primary_key=True)
    inference_fk: Mapped[int] = mapped_column(ForeignKey("inference.id"), nullable=False)
    seq: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    class_fk: Mapped[int] = mapped_column(ForeignKey("class.id"), nullable=False)
    brand_fk: Mapped[Optional[int]] = mapped_column(ForeignKey("brand.id"))
    model: Mapped[Optional[str]] = mapped_column(String(60))
    color_fk: Mapped[Optional[int]] = mapped_column(ForeignKey("color.id"))
    body: Mapped[Optional[str]] = mapped_column(String(15))
    era: Mapped[Optional[str]] = mapped_column(String(10))
    direction: Mapped[Optional[str]] = mapped_column(String(10))
    special_use: Mapped[str] = mapped_column(String(10), nullable=False)
    headlights_on: Mapped[bool] = mapped_column(Boolean, nullable=False)
    commercial_markings: Mapped[bool] = mapped_column(Boolean, nullable=False)
    truncated: Mapped[bool] = mapped_column(Boolean, nullable=False)
    plate_visible: Mapped[bool] = mapped_column(Boolean, nullable=False)
    __table_args__ = (
        UniqueConstraint("inference_fk", "seq"),
        CheckConstraint("body IN ('hatchback','sedan','suv','wagon','coupe','cabriolet','pickup','mpv')"),
        CheckConstraint("era IN ('pre_2000','2000s','2010s','2020s','unknown')"),
        CheckConstraint("direction IN ('left','right','toward','away','unknown')"),
        CheckConstraint("special_use IN ('taxi','emergency','delivery','none')"),
    )


# Prazne vrijednosti iz pandasa pretvara u NULL razumljiv bazi.
def nn(value):
    return None if pd.isna(value) else value


def main():
    df = pd.read_csv(CSV_FILE_PATH, delimiter=",")
    print(f"CSV size: {df.shape}")
    print(df.head())

    # Izvedeni stupci potrebni modelu: sat za vremensku opservaciju i redni
    # broj vozila unutar jedne detekcije.
    df["ts"] = pd.to_datetime(df["ts"])
    df["hour_ts"] = df["ts"].dt.floor("h")
    df["pts_ms"] = df["pts_ms"].astype("Int64")
    df["seq"] = df["inference_id"].str.rsplit("-", n=1).str[-1].astype(int)

    with open(CAMERAS_JSON_PATH, "r") as f:
        cameras_data = json.load(f)

    with open(VLM_JSON_PATH, "r") as f:
        vlm_data = json.load(f)

    engine = create_engine(DATABASE_URL, echo=False)

    # Shema se svaki put ruši i gradi iznova kako bi uvoz bio ponovljiv.
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    table_names = inspect(engine).get_table_names()
    print("Tables:", table_names)


    with Session(engine) as session:
        # Tablice se pune redoslijedom koji nalažu strani ključevi, a nakon
        # svakog umetanja čita se preslika prirodnog ključa u surogat ključ
        # kako bi se sljedeća tablica mogla povezati.
        camera_code, camera_info = next(iter(cameras_data.items()))

        location_list = [
            {
                "street": camera_info.get("street"),
                "house_number": camera_info.get("house_number"),
                "postal_code": camera_info.get("postal_code"),
                "city": camera_info.get("city"),
                "county": camera_info.get("county"),
                "country": camera_info.get("country"),
            }
        ]
        session.execute(insert(Location), location_list)
        session.commit()

        result = session.execute(select(Location.id))
        location_id = result.scalar_one()

        camera_list = [
            {
                "code": camera_code,
                "name": camera_info.get("name"),
                "device": camera_info.get("device"),
                "lat": camera_info.get("lat"),
                "lon": camera_info.get("lon"),
                "location_fk": location_id,
            }
        ]
        session.execute(insert(Camera), camera_list)
        session.commit()

        result = session.execute(select(Camera.code, Camera.id))
        camera_map = {code: id_ for code, id_ in result.all()}

        vlm_list = [
            {
                "model": entry["model"],
                "schema_v": entry["schema_v"],
                "prompt_v": entry["prompt_v"],
                "valid_from": pd.to_datetime(entry["valid_from"]),
            }
            for entry in vlm_data
        ]
        session.execute(insert(Vlm), vlm_list)
        session.commit()

        result = session.execute(select(Vlm.schema_v, Vlm.id))
        vlm_map = {schema_v: id_ for schema_v, id_ in result.all()}

        class_list = [{"name": name} for name in df["vehicle_class"].drop_duplicates()]
        session.execute(insert(VehicleClass), class_list)
        session.commit()

        result = session.execute(select(VehicleClass.name, VehicleClass.id))
        class_map = {name: id_ for name, id_ in result.all()}

        brand_list = [{"name": name} for name in df["make"].drop_duplicates()]
        session.execute(insert(Brand), brand_list)
        session.commit()

        result = session.execute(select(Brand.name, Brand.id))
        brand_map = {name: id_ for name, id_ in result.all()}

        color_list = [
            {"name": name, "color_group": COLOR_GROUPS[name]}
            for name in df["color"].drop_duplicates()
        ]
        session.execute(insert(Color), color_list)
        session.commit()

        result = session.execute(select(Color.name, Color.id))
        color_map = {name: id_ for name, id_ in result.all()}

        weather_df = df[["camera_id", "hour_ts", "temp_c", "precip_mm", "weather_code"]].drop_duplicates()
        weather_list = [
            {
                "camera_fk": camera_map[row.camera_id],
                "hour_ts": row.hour_ts,
                "temperature": nn(row.temp_c),
                "precipitation_mm": nn(row.precip_mm),
                "weather_code": nn(row.weather_code),
            }
            for row in weather_df.itertuples(index=False)
        ]
        session.execute(insert(WeatherObservation), weather_list)
        session.commit()

        result = session.execute(
            select(WeatherObservation.camera_fk, WeatherObservation.hour_ts, WeatherObservation.id)
        )
        weather_map = {(camera_fk, hour_ts): id_ for camera_fk, hour_ts, id_ in result.all()}

        # Jedan redak izvora je jedno vozilo, a ista detekcija pokreta može
        # sadržavati više vozila. Zato se motion i inference pune iz
        # jedinstvenih detekcija, a vehicle iz svih redaka.
        motion_df = df.drop_duplicates(subset="motion_id", keep="first")
        motion_list = [
            {
                "id": row.motion_id,
                "ts": row.ts,
                "pts_ms": int(row.pts_ms) if pd.notna(row.pts_ms) else None,
                "peak_mass": nn(row.peak_mass),
                "dur_ms": nn(row.dur_ms),
                "cause": nn(row.cause),
                "file": row.file,
                "lighting": row.lighting,
                "camera_fk": camera_map[row.camera_id],
                "weather_observation_fk": weather_map[(camera_map[row.camera_id], row.hour_ts)],
            }
            for row in motion_df.itertuples(index=False)
        ]
        session.execute(insert(Motion), motion_list)
        session.commit()

        inference_list = [
            {
                "motion_fk": row.motion_id,
                "vlm_fk": vlm_map[row.schema_v],
                "road_condition": nn(row.road_condition),
            }
            for row in motion_df.itertuples(index=False)
        ]
        session.execute(insert(Inference), inference_list)
        session.commit()

        result = session.execute(select(Inference.motion_fk, Inference.id))
        inference_map = {motion_fk: id_ for motion_fk, id_ in result.all()}

        vehicle_list = [
            {
                "inference_fk": inference_map[row.motion_id],
                "seq": row.seq,
                "class_fk": class_map[row.vehicle_class],
                "brand_fk": brand_map.get(row.make),
                "model": nn(row.model),
                "color_fk": color_map.get(row.color),
                "body": nn(row.body),
                "era": nn(row.era),
                "direction": nn(row.direction),
                "special_use": row.special_use,
                "headlights_on": bool(row.headlights_on),
                "commercial_markings": bool(row.commercial_markings),
                "truncated": bool(row.truncated),
                "plate_visible": bool(row.plate_visible),
            }
            for row in df.itertuples(index=False)
        ]
        session.execute(insert(Vehicle), vehicle_list)
        session.commit()

    with Session(engine) as session:
        counts = {}
        for model in (Location, Camera, Vlm, VehicleClass, Brand, Color, WeatherObservation, Motion, Inference, Vehicle):
            result = session.execute(select(func.count()).select_from(model))
            counts[model.__tablename__] = result.scalar_one()

    # Završna provjera: broj redaka u bazi mora odgovarati onome što se
    # iz izvornog skupa može izračunati unaprijed.
    expected = {
        "location": 1,
        "camera": df["camera_id"].nunique(),
        "vlm": df["schema_v"].nunique(),
        "class": df["vehicle_class"].nunique(),
        "brand": df["make"].nunique(),
        "color": df["color"].nunique(),
        "weather_observation": df[["camera_id", "hour_ts"]].drop_duplicates().shape[0],
        "motion": df["motion_id"].nunique(),
        "inference": df["motion_id"].nunique(),
        "vehicle": len(df),
    }

    for table_name, count in counts.items():
        target = expected[table_name]
        print(f"{table_name}: {count} (expected {target}){'' if count == target else '   <-- NE SLAZE SE'}")

    engine.dispose()


if __name__ == "__main__":
    main()
