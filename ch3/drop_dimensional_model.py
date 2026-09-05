from sqlalchemy import create_engine

from generate_star_schema import Base, DATABASE_URL, SCHEMA

engine = create_engine(DATABASE_URL, echo=False)

Base.metadata.drop_all(engine)

print(f"All dimensional model tables dropped from schema {SCHEMA}.")

engine.dispose()
