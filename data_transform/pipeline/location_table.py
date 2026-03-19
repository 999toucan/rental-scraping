import pandas as pd
import ast

def city_from_coords(lat, lon):

    if lat is None or lon is None:
        return "Unknown"

    if 49.20 <= lat <= 49.32 and -123.23 <= lon <= -123.02:
        return "Vancouver"

    if 49.20 <= lat <= 49.29 and -122.98 <= lon <= -122.88:
        return "Burnaby"

    if 49.00 <= lat <= 49.20 and -122.95 <= lon <= -122.70:
        return "Surrey"

    if 49.10 <= lat <= 49.20 and -123.22 <= lon <= -123.05:
        return "Richmond"

    if 49.25 <= lat <= 49.32 and -122.90 <= lon <= -122.70:
        return "Coquitlam"

    if 49.00 <= lat <= 49.15 and -122.80 <= lon <= -122.45:
        return "Langley"

    if 49.18 <= lat <= 49.23 and -122.95 <= lon <= -122.88:
        return "New Westminster"

    return "Unknown"
    
def build_location_table(dfs):

    rows = []

    for df in dfs:

        if df is None or df.empty:
            continue

        source = df["source"].iloc[0]

        for _, row in df.iterrows():

            url = (
                row.get("url")
                or row.get("source_url")
                or row.get("listing_url")
            )

            lat = row.get("latitude")
            lon = row.get("longitude")

            if source == "craigslist" and pd.notnull(row.get("gps_coordinate")):
                try:
                    coords = ast.literal_eval(str(row["gps_coordinate"]))
                    lat, lon = float(coords[0]), float(coords[1])
                except:
                    pass

            # Address extraction
            raw_address = None

            if source == "craigslist":
                raw_address = row.get("region")

            elif source == "rew":
                raw_address = row.get("address")

            elif source in ["kijiji", "trovit", "padmapper"]:
                raw_address = row.get("location")

            elif source == "airbnb":
                neighbourhood = str(row.get("neighbourhood_cleansed", "")).strip()
                host_loc = str(row.get("host_location", "")).strip()

                parts = [p for p in [neighbourhood, host_loc] if p]
                raw_address = ", ".join(parts) if parts else None

            full_address = str(raw_address).strip() if raw_address else "Unknown"

            # Trovit cleanup (remove redundant first word)
            if source == "trovit" and full_address:
                parts = full_address.split(maxsplit=1)
                full_address = parts[1] if len(parts) > 1 else full_address

            # City detection
            city = "Unknown"
            cities = [
                "Vancouver", "Burnaby", "Surrey", "Richmond",
                "Coquitlam", "Langley", "New Westminster"
            ]

            search = f"{full_address} {row.get('region','')}".lower()

            for c in cities:
                if c.lower() in search:
                    city = c
                    break
                    
            if city == "Unknown":
                city = city_from_coords(lat, lon)

            rows.append({
                "hash": row.get("hash"),
                "source": row.get("source"),
                "url": url,
                "city": city,
                "latitude": lat,
                "longitude": lon,
                "full_address": full_address
            })

    df_master_location = pd.DataFrame(rows)

    df_master_location = df_master_location.drop_duplicates("hash")

    df_master_location.set_index("hash", inplace=True)

    return df_master_location