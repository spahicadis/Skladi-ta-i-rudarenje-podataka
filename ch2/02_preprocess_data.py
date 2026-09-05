import unicodedata
import pandas as pd

CSV_FILE_PATH = "../Traffic_Analysis_Novigrad_2026-08-20_2026-08-31.csv"

# Naziv marke dolazi iz VLM-a kao slobodan tekst, pa se ista marka javlja u više
# oblika. Rječnici pokrivaju tri slučaja: različiti nazivi istog proizvođača,
# vrijednosti koje uopće nisu proizvođač i iznimke u pisanju velikim slovom.
MAKE_ALIASES = {
    "vw": "volkswagen",
    "vw caddy": "volkswagen",
    "range rover evoque": "range rover",
    "doblo": "fiat",
    "infinite": "infiniti",
}

MAKE_NON_MANUFACTURER = {
    "suv": "unknown",
    "minivan": "unknown",
    "atv": "unknown",
    "candy": "unknown",
    "stummers": "unknown",
}

MAKE_DISPLAY_EXCEPTIONS = {
    "bmw": "BMW",
    "mg": "MG",
    "ktm": "KTM",
    "gmc": "GMC",
    "ram": "RAM",
}


# Svodi zapis na usporediv oblik kako bi se različito napisana ista marka
# prepoznala kao jedna vrijednost.
def make_key(value):
    if pd.isna(value):
        return value
    value = str(value).strip()
    value = " ".join(value.split())
    value = unicodedata.normalize("NFD", value)
    value = "".join(ch for ch in value if unicodedata.category(ch) != "Mn")
    return value.lower()


def make_display(key):
    if pd.isna(key):
        return key
    if key == "unknown":
        return key
    if key in MAKE_DISPLAY_EXCEPTIONS:
        return MAKE_DISPLAY_EXCEPTIONS[key]
    return key.title()


def canonicalize_make(series):
    keys = series.apply(make_key)
    keys = keys.replace(MAKE_ALIASES)
    keys = keys.replace(MAKE_NON_MANUFACTURER)
    return keys.apply(make_display)


df = pd.read_csv(CSV_FILE_PATH, delimiter=',')
print("CSV size before: ", df.shape)

make_before = df['make'].nunique()

df['make'] = canonicalize_make(df['make'])
# 'class' je rezervirana riječ, pa se stupac preimenuje u 'vehicle_class'.
df = df.rename(columns={'class': 'vehicle_class'})

make_after = df['make'].nunique()

print("CSV size after: ", df.shape)
print(df.head())

duplicates = df.duplicated().sum()
print(f"Number of duplicates: {duplicates}")

# Skup se dijeli na dva dijela kako bi ETL proces imao dva odvojena izvora:
# veći dio se učitava u relacijsku bazu, manji ostaje kao CSV datoteka.
# Fiksiran random_state čini podjelu ponovljivom.
df20 = df.sample(frac=0.2, random_state=1)
df = df.drop(df20.index)
print("CSV size 80: ", df.shape)
print("CSV size 20: ", df20.shape)

df.to_csv("processed/Traffic_Analysis_Novigrad_2026-08-20_2026-08-31_PROCESSED.csv", index=False)
df20.to_csv("processed/Traffic_Analysis_Novigrad_2026-08-20_2026-08-31_PROCESSED_20.csv", index=False)

print(f"Distinct 'make' before canonicalization: {make_before}")
print(f"Distinct 'make' after canonicalization: {make_after}")
print(f"80% + 20% = {df.shape[0] + df20.shape[0]}")
