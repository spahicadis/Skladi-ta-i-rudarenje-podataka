from datetime import date, datetime
from decimal import Decimal
from typing import Optional

from sqlalchemy import (
    BigInteger,
    Boolean,
    Date,
    DateTime,
    ForeignKey,
    Integer,
    Numeric,
    SmallInteger,
    String,
    create_engine,
    inspect,
)
from sqlalchemy.orm import Mapped, declarative_base, mapped_column

DATABASE_URL = "postgresql+psycopg://adisspahic@localhost:5432/srp-traffic-analysis"
# Skladište živi u zasebnoj shemi iste baze; izvorišni relacijski model
# ostaje netaknut u shemi public.
SCHEMA = "dw"


# Zvjezdasta shema: sedam dimenzija oko jedne tablice činjenica. Dimenzije
# su namjerno denormalizirane jer se u skladištu čita, a ne piše.
Base = declarative_base()


class DimDate(Base):
    __tablename__ = "dim_date"
    __table_args__ = {"schema": SCHEMA}

    date_tk: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    full_date: Mapped[date] = mapped_column(Date, nullable=False)
    day: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    day_of_week: Mapped[str] = mapped_column(String(10), nullable=False)
    is_weekend: Mapped[bool] = mapped_column(Boolean, nullable=False)
    month: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    quarter: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    year: Mapped[int] = mapped_column(SmallInteger, nullable=False)


class DimTime(Base):
    __tablename__ = "dim_time"
    __table_args__ = {"schema": SCHEMA}

    time_tk: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    hour: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    minute: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    part_of_day: Mapped[str] = mapped_column(String(10), nullable=False)


# Podaci o lokaciji spljošteni su u dimenziju kamere umjesto da ostanu
# u zasebnoj tablici kao u izvorišnom modelu.
class DimCamera(Base):
    __tablename__ = "dim_camera"
    __table_args__ = {"schema": SCHEMA}

    camera_tk: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    code: Mapped[str] = mapped_column(String(30))
    name: Mapped[Optional[str]] = mapped_column(String(60))
    device: Mapped[Optional[str]] = mapped_column(String(60))
    street: Mapped[Optional[str]] = mapped_column(String(80))
    house_number: Mapped[Optional[str]] = mapped_column(String(10))
    postal_code: Mapped[Optional[str]] = mapped_column(String(10))
    city: Mapped[Optional[str]] = mapped_column(String(60))
    county: Mapped[Optional[str]] = mapped_column(String(60))
    country: Mapped[Optional[str]] = mapped_column(String(60))
    lat: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 6))
    lon: Mapped[Optional[Decimal]] = mapped_column(Numeric(9, 6))


# Heterogena dimenzija: automobil, bicikl i vlak stoje u istoj tablici,
# pa su atributi koji ne vrijede za svaku klasu opcionalni.
class DimVehicle(Base):
    __tablename__ = "dim_vehicle"
    __table_args__ = {"schema": SCHEMA}

    vehicle_tk: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    vehicle_class: Mapped[str] = mapped_column(String(20))
    brand: Mapped[Optional[str]] = mapped_column(String(40))
    model: Mapped[Optional[str]] = mapped_column(String(60))
    color: Mapped[Optional[str]] = mapped_column(String(20))
    color_group: Mapped[Optional[str]] = mapped_column(String(20))
    body: Mapped[Optional[str]] = mapped_column(String(15))
    era: Mapped[Optional[str]] = mapped_column(String(10))


# Brojčani uvjeti iz izvora dolaze ovdje svedeni na razrede; sama pretvorba
# događa se u ETL procesu.
class DimConditions(Base):
    __tablename__ = "dim_conditions"
    __table_args__ = {"schema": SCHEMA}

    conditions_tk: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    weather_category: Mapped[Optional[str]] = mapped_column(String(20))
    temperature_bucket: Mapped[Optional[str]] = mapped_column(String(20))
    precipitation_bucket: Mapped[Optional[str]] = mapped_column(String(20))
    lighting: Mapped[Optional[str]] = mapped_column(String(5))


# Sporo mijenjajuća dimenzija tipa 2: promjena verzije modela bilježi se kao
# novi redak, a date_from i date_to omeđuju razdoblje njegove valjanosti.
class DimVlm(Base):
    __tablename__ = "dim_vlm"
    __table_args__ = {"schema": SCHEMA}

    vlm_tk: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    version: Mapped[Optional[int]] = mapped_column(Integer)
    date_from: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    date_to: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    vlm_id: Mapped[Optional[int]] = mapped_column(Integer, index=True)
    model: Mapped[Optional[str]] = mapped_column(String(60))
    schema_v: Mapped[Optional[int]] = mapped_column(SmallInteger)
    prompt_v: Mapped[Optional[int]] = mapped_column(SmallInteger)


# Kompozitna dimenzija: skuplja oznake i kratke pokazatelje koji sami ne bi
# opravdali vlastitu dimenziju, a u tablici činjenica bili bi osam stupaca.
class DimObservation(Base):
    __tablename__ = "dim_observation"
    __table_args__ = {"schema": SCHEMA}

    observation_tk: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    truncated: Mapped[Optional[bool]] = mapped_column(Boolean)
    plate_visible: Mapped[Optional[bool]] = mapped_column(Boolean)
    headlights_on: Mapped[Optional[bool]] = mapped_column(Boolean)
    commercial_markings: Mapped[Optional[bool]] = mapped_column(Boolean)
    cause: Mapped[Optional[str]] = mapped_column(String(10))
    direction: Mapped[Optional[str]] = mapped_column(String(10))
    special_use: Mapped[Optional[str]] = mapped_column(String(10))
    road_condition: Mapped[Optional[str]] = mapped_column(String(10))


# Zrnatost je jedno detektirano vozilo, ne jedan prolazak. Uz sedam stranih
# ključeva tu je motion_id kao degenerirana dimenzija i tri mjere.
class FactVehicleDetection(Base):
    __tablename__ = "fact_vehicle_detection"
    __table_args__ = {"schema": SCHEMA}

    fact_vehicle_detection_tk: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    date_tk: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey(f"{SCHEMA}.dim_date.date_tk"))
    time_tk: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey(f"{SCHEMA}.dim_time.time_tk"))
    camera_tk: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey(f"{SCHEMA}.dim_camera.camera_tk"))
    vehicle_tk: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey(f"{SCHEMA}.dim_vehicle.vehicle_tk"))
    conditions_tk: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey(f"{SCHEMA}.dim_conditions.conditions_tk"))
    vlm_tk: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey(f"{SCHEMA}.dim_vlm.vlm_tk"))
    observation_tk: Mapped[Optional[int]] = mapped_column(BigInteger, ForeignKey(f"{SCHEMA}.dim_observation.observation_tk"))
    motion_id: Mapped[Optional[int]] = mapped_column(Integer)
    peak_mass: Mapped[Optional[int]] = mapped_column(Integer)
    dur_ms: Mapped[Optional[int]] = mapped_column(Integer)
    objects_in_event: Mapped[Optional[int]] = mapped_column(SmallInteger)


engine = create_engine(DATABASE_URL, echo=False)

Base.metadata.create_all(engine)

print("Dimensional model tables created successfully!")
for table in sorted(inspect(engine).get_table_names(schema=SCHEMA)):
    print(f"   {SCHEMA}.{table}")

engine.dispose()
