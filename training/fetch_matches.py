import json
import os
import requests

class FetchMatches:
    def __init__(self, api_key, keyTournaments, year):
        if not api_key or not api_key.strip():
            raise ValueError("Imposta la variabile d'ambiente API_TENNIS_KEY")
        self.api_key = api_key
        self.keyTournaments = keyTournaments
        self.year = year
        
    def fetch_matches(self, keyTournament, year):
        url = "https://api.api-tennis.com/tennis/"
        params = {
            "method": "get_fixtures",
            "APIkey": self.api_key,
            "date_start": f"{year}-01-01",
            "date_stop": f"{year}-12-31",
            "event_type_key": 265,
            "tournament_key": keyTournament,
            "tournament_season": year,
        }
        try:
            response = requests.get(url, params=params, timeout=60)
        except requests.RequestException:
            # Gli errori requests possono contenere l'URL con la chiave API.
            raise RuntimeError(
                f"Richiesta fallita per il torneo {keyTournament}"
            ) from None

        if response.status_code != 200:
            raise RuntimeError(f"Error fetching matches for tournament {keyTournament}: {response.status_code}")

        try:
            data = response.json()
        except ValueError:
            raise RuntimeError("La risposta API non contiene un JSON valido") from None

        if not isinstance(data, dict):
            raise RuntimeError("Formato della risposta API non valido")
        if data.get("success") != 1:
            raise RuntimeError(f"Risposta API non riuscita per il torneo {keyTournament}")

        matches = data.get("result")
        if matches is None or matches == {}:
            return []
        if not isinstance(matches, list):
            raise RuntimeError(f"Elenco incontri non valido per il torneo {keyTournament}")

        filtered_matches = []
        for match in matches:
            if not isinstance(match, dict):
                raise RuntimeError("Formato incontro non valido")

            qualification = match.get("event_qualification")
            pointbypoint = match.get("pointbypoint")

            # Teniamo solo il tabellone principale con punti disponibili.
            if qualification is False or qualification == "False":
                if isinstance(pointbypoint, list):
                    if len(pointbypoint) > 0:
                        filtered_matches.append(match)

        return filtered_matches
        
    
    def fetch_all_matches(self, output_dir="output"):
        os.makedirs(output_dir, exist_ok=True)
        for tournament in self.keyTournaments:
            keyTournament = tournament["tournament_key"]
            tournament_name = tournament["tournament_name"]
            matches = self.fetch_matches(keyTournament, self.year)
            if len(matches) == 0:
                print(f"{tournament_name}: nessun incontro trovato, salto il torneo")
                continue
            filename = os.path.join(
                output_dir, f"{tournament_name}_{self.year}.json"
            )
            self.save_matches_json(filename, matches)
            print(f"{tournament_name}: {len(matches)} matches saved to {filename}")
    
    def save_matches_json(self, filename, matches):
        with open(filename, 'w', encoding="utf-8") as f:
            json.dump({"tournament": matches}, f, indent=2)
            
    def main(self, output_dir="output"):
        self.fetch_all_matches(output_dir)
        
if __name__ == "__main__":
    api_key = os.environ.get("API_TENNIS_KEY")
    with open("training/config/tournaments.json", "r") as f:
        keyTournaments = json.load(f)
    year = int(input("Enter the year for which you want to fetch matches (e.g., 2023): "))
    fetcher = FetchMatches(api_key, keyTournaments, year)
    fetcher.main(f"training/data/raw/{year}")
