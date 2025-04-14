# DataZenith

### Mohamed Saidi - 40248103
### Miskat Mahmud - 40250110
### Beaudelaire Tsoungui Nzodoumkouo - 40216598
### Abdel-Rahman Khalifa - 40253332




# DataBase
run this code in the terminal to run the script
make sure you have docker installed and running
```bash
docker compose up
```

# Data Population
run this code in the terminal to populate the database
before running the script, make sure you have updated the username and password in the python script file. 
```bash
python3 -m pip install -r requirements.txt
python3 python_script_for_data_poplation.py
```


# DataSource

1st API: https://archive.org/advancedsearch.php?q=mediatype:texts&fl=identifier,title,creator,year,language,subject,downloads&rows=10000&page=1&output=json

2nd API: https://openlibrary.org/search.json?q=subject:Science+&limit=10000 

We're going to use Archive.org and openlibrary.org as our data sources

---------------------------------------------------------------------
# Here is the link to our dump file that will allow you to download the DB after phase 1
https://drive.google.com/file/d/13UIT7632q6YmIrtrM6MXEerOMjaqZpQR/view?usp=sharing
