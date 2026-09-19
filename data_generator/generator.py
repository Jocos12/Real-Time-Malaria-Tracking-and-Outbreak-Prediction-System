import time
import uuid
import random
import argparse
import requests
from datetime import datetime, timezone
from faker import Faker

fake = Faker()

# --- Config des régions (Rwanda) ---
REGIONS = {
    "Kigali": {"district": ["Nyarugenge", "Gasabo", "Kicukiro"], "lat": -1.94, "lon": 30.06},
    "Northern": {"district": ["Musanze", "Burera", "Gicumbi"], "lat": -1.50, "lon": 29.63},
    "Southern": {"district": ["Huye", "Nyanza", "Muhanga"], "lat": -2.60, "lon": 29.74},
    "Eastern": {"district": ["Rwamagana", "Kayonza", "Nyagatare"], "lat": -1.95, "lon": 30.43},
    "Western": {"district": ["Rubavu", "Karongi", "Nyamasheke"], "lat": -1.70, "lon": 29.25},
}

API_URL = "http://127.0.0.1:8000/api/report-case/"


def build_fake_case():
    region = random.choice(list(REGIONS.keys()))
    region_info = REGIONS[region]
    district = random.choice(region_info["district"])

    # Saisonnalité simple : pluie/humidité plus hautes => plus de risques de positivité
    rainfall = round(random.uniform(0, 150), 1)
    humidity = round(random.uniform(40, 95), 1)
    ambient_temp = round(random.uniform(18, 32), 1)

    # Probabilité de test positif influencée par pluie/humidité (signal pour le futur ML)
    positivity_chance = 0.15 + (rainfall / 300) + (humidity / 500)
    result = "positive" if random.random() < positivity_chance else "negative"

    symptoms_pool = ["fever", "chills", "headache", "vomiting", "fatigue", "muscle_pain"]
    symptoms = random.sample(symptoms_pool, k=random.randint(1, 4))

    return {
        "case_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "region_id": region,
        "district": district,
        "gps_lat": region_info["lat"] + random.uniform(-0.05, 0.05),
        "gps_lon": region_info["lon"] + random.uniform(-0.05, 0.05),
        "patient_age": random.randint(1, 85),
        "gender": random.choice(["M", "F"]),
        "symptoms": symptoms,
        "rapid_test_result": result,
        "body_temperature": round(random.uniform(36.0, 40.5), 1),
        "ambient_temperature": ambient_temp,
        "humidity": humidity,
        "rainfall_mm": rainfall,
    }


def main():
    parser = argparse.ArgumentParser(description="Générateur de cas malaria en continu")
    parser.add_argument("--rate", type=float, default=1.0, help="Nombre d'enregistrements par seconde")
    args = parser.parse_args()

    interval = 1.0 / args.rate
    count = 0

    print(f"[Générateur démarré] rate={args.rate} records/sec (interval={interval:.3f}s)")

    while True:
        record = build_fake_case()
        try:
            response = requests.post(API_URL, json=record, timeout=5)
            count += 1
            print(f"[{count}] {response.status_code} - {record['region_id']} - {record['rapid_test_result']}")
        except requests.exceptions.RequestException as e:
            print(f"[ERREUR] Impossible d'envoyer : {e}")

        time.sleep(interval)


if __name__ == "__main__":
    main()