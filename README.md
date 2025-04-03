# DataZenith

### Mohamed Saidi - 40248103
### Miskat Mahmud - 40250110
### Beaudelaire Tsoungui Nzodoumkouo - 40216598
### Abdel-Rahman Khalifa - 40253332




# DataBase
run this code in the terminal to run the script

```bash
docker compose up
```

# Data Population
run this code in the terminal to populate the database

```bash
python3 -m pip install -r requirements.txt
python3 python_script_for_data_poplation.py
```

# DataSource

1st API: https://archive.org/advancedsearch.php?q=mediatype:texts&fl=identifier,title,creator,year,language,subject,downloads&rows=10000&page=1&output=json

2nd API: https://openlibrary.org/search.json?q=subject:Science+&limit=10000 

We're going to use Archive.org and openlibrary.org as our data sources
