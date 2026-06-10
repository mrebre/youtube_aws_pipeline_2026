# ── UVOZ BIBLIOTEKA (Tvoj inženjerski alat) ──────────────────────────────────
# Biblioteka za rad sa JSON fajlovima (pretvaranje teksta u rečnike i obrnuto)
import json
# Omogućava nam komunikaciju sa operativnim sistemom (uzimanje promenljivih okruženja)
import os
# Alat za beleženje rada programa i grešaka (zamena za klasičan print)
import logging
# Alat za rad sa datumima i vremenskim zonama
from datetime import datetime, timezone
# Alat koji sređuje čudne karaktere u nazivima fajlova sa S3 (npr. menja "+" u razmak)
from urllib.parse import unquote_plus

# Zvanični Amazonov SDK za Python. Pomoću njega naš kod "razgovara" sa AWS servisima (S3, SNS...)
import boto3
# Genijalna biblioteka stvorena za Data Engineering. Povezuje Pandas, S3 i AWS Glue u par linija koda
import awswrangler as wr
# Osnovna biblioteka za analizu i manipulaciju podacima u obliku tabela (DataFrame)
import pandas as pd

# ── SISTEM ZA LOGOVANJE (Beleženje tragova) ──────────────────────────────────
# Kreiramo objekat koji će slati poruke u AWS CloudWatch logove, da vidimo šta Lambda radi u kom sekundu.
logger = logging.getLogger()
# Postavljamo nivo na INFO, što znači da bilježimo opšte važne informacije i greške
logger.setLevel(logging.INFO)

# ── KONFIGURACIJA (Preuzimanje parametara iz AWS okruženja) ───────────────────
# "os.environ" čita promenljive koje smo uneli u konfiguraciju same Lambde na AWS konzoli.
# Ako se promeni ime bucket-a, menjamo ga na AWS-u, ne diramo ovaj Python kod!
SILVER_BUCKET = os.environ["S3_BUCKET_SILVER"]

# "os.environ.get" radi istu stvar, ali ako ne nađe promenljivu, koristi podrazumevanu vrednost (drugi parametar)
GLUE_DB = os.environ.get("GLUE_DB_SILVER", "yt_pipeline_silver_dev")
GLUE_TABLE = os.environ.get("GLUE_TABLE_REFERENCE", "clean_reference_data")
SNS_TOPIC = os.environ.get("SNS_ALERT_TOPIC_ARN", "")

# Definišemo tačnu S3 putanju na koju će se upisivati Parquet fajlovi (Silver sloj)
SILVER_PATH = f"s3://{SILVER_BUCKET}/youtube/reference_data/"

# Kreiramo klijente (mostove) preko kojih naš kod šalje komande S3 skladištu i SNS sistemu za alarme
s3_client = boto3.client("s3")
sns_client = boto3.client("sns")


def read_json_from_s3(bucket: str, key: str) -> dict:
    """
    FUNKCIJA ZA ČITANJE SIROVOG FAJLA.
    Prima ime kofice (bucket) i tačnu putanju do fajla (key), a vraća Python rečnik (dict).
    """
    # 1. Šaljemo zahtev S3 servisu da nam dohvati fajl (objekat)
    response = s3_client.get_object(Bucket=bucket, Key=key)

    # 2. Fajl sa interneta stiže kao "strim" (bujica bajtova). Moramo ga pročitati (.read())
    # i pretvoriti u običan tekst koristeći UTF-8 standard (.decode('utf-8'))
    content = response["Body"].read().decode("utf-8")

    # 3. Pošto je taj tekst u JSON formatu, pretvaramo ga u Python rečnik pomoću json.loads
    return json.loads(content)


def validate_category_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    FUNKCIJA ZA PROVERU KVALITETA PODATAKA (Data Quality Check).
    Zadatak Data Engineer-a je da postavi odbrambene mehanizme kako loš fajl ne bi pokvario bazu.
    """
    # Provera 1: Da li je fajl prazan? Ako jeste, prekidamo sve i bacamo grešku.
    if df.empty:
        raise ValueError("Empty DataFrame — no category items found")

    # Provera 2: Da li imamo ključne kolone za biznis? (ID i Naziv kategorije)
    required_cols = {"id", "snippet.title"}
    # Uzimamo sve kolone koje su stvarno stigle u fajlu
    actual_cols = set(df.columns)
    # Gledamo da li neka od obaveznih fali u tom skupu
    missing = required_cols - actual_cols

    if missing:
        # Ako neka kolona fali, samo ispisujemo upozorenje u logove, ne prekidamo izvršavanje
        logger.warning(
            f"Missing expected columns: {missing}. Available: {actual_cols}")

    # ČIŠĆENJE DUPLIKATA: Proveravamo broj redova pre i posle uklanjanja duplih ID-jeva
    before = len(df)
    if "id" in df.columns:
        # drop_duplicates briše duple redove po koloni "id", a keep="last" zadržava onaj koji je stigao poslednji
        df = df.drop_duplicates(subset=["id"], keep="last")
    after = len(df)

    # Ako je broj redova manji nego na početku, znači da smo uspešno obrisali duplikate
    if before != after:
        logger.info(f"  Removed {before - after} duplicate categories")

    return df  # Vraćamo čistu, pročišćenu tabelu nazad u glavni program


def send_alert(subject: str, message: str):
    """
    FUNKCIJA ZA SLANJE ALARMA.
    Ako se desi haos u kodu, ova funkcija šalje poruku na AWS SNS (Simple Notification Service),
    koji je spojen sa tvojim email-om ili Slack-om.
    """
    if SNS_TOPIC:  # Provera da li smo uopšte uneli ARN adresu za alarm u konfiguraciji
        sns_client.publish(
            TopicArn=SNS_TOPIC,
            # Naslov poruke (AWS dozvoljava maksimalno 100 karaktera)
            Subject=subject[:100],
            Message=message         # Sadržaj poruke (tekst greške)
        )


def lambda_handler(event, context):
    """
    GLAVNA FUNKCIJA (Ulazna vrata za AWS).
    Nju AWS Lambda poziva kada je S3 okine. Parametar 'event' u sebi nosi sve podatke o fajlu koji je stigao.
    """
    # ── DETEKCIJA ULAZNIH PODATAKA ───────────────────────────────────────────
    # Izvlačimo listu zapisa (Records) iz događaja. S3 okidači spakuju podatke u "Records" listu.
    records = event.get("Records", [])

    if not records:
        # Ako je Lambda pozvana direktno (ručnim testom ili preko nekog drugog servisa),
        # struktura je ravna, pa ceo 'event' pretvaramo u listu od jednog elementa da nam petlja ne bi pukla.
        records = [event] if "s3" in event else []

    processed = []  # Prazna lista u koju ćemo beležiti fajlove koje smo uspešno obradili
    errors = []     # Prazna lista u koju ćemo beležiti podatke o greškama ako nešto pukne

    # ── GLAVNA PETLJA: Obrada fajlova ────────────────────────────────────────
    # Prolazimo kroz svaki zapis (obično S3 šalje jedan po jedan fajl, ali petlja obezbeđuje stabilnost)
    for record in records:
        try:
            # Izvlačimo metapodatke o fajlu iz S3 strukture
            s3_info = record["s3"]
            # Ime kofice (npr. bronze-yt-aws-pipeline-eu-dev)
            bucket = s3_info["bucket"]["name"]

            # Ključ (key) je zapravo puna putanja do fajla (npr. raw_data/region=US/category.json)
            # unquote_plus pretvara URL karaktere (poput %20 ili +) nazad u normalne razmake i tekst
            key = unquote_plus(s3_info["object"]["key"])

            logger.info(f"Processing: s3://{bucket}/{key}")

            # 1. KORAK: Čitamo sirovi JSON fajl preko naše funkcije
            raw_data = read_json_from_s3(bucket, key)

            # 2. KORAK: "Peglanje" (Normalizacija) ugnježdenog JSON-a u tabelu
            # YouTube API pakuje podatke tako da ključne informacije leže unutar liste pod imenom "items".
            if "items" in raw_data and isinstance(raw_data["items"], list):
                # pd.json_normalize uzima listu rečnika i pretvara je u ravan Pandas DataFrame (tabelu)
                df = pd.json_normalize(raw_data["items"])
            else:
                # Ako fajl nema "items" strukturu, pokušavamo da normalizujemo ceo fajl kao plan B
                df = pd.json_normalize(raw_data)

            # Logujemo dimenzije sirove tabele (broj redova, broj kolona)
            logger.info(f"  Raw shape: {df.shape}")

            # 3. KORAK: Validacija i čišćenje duplikata kroz našu funkciju
            df = validate_category_data(df)

            # 4. KORAK: Dodavanje sistemskih kolona (Data Lineage / Poreklo podataka)
            # Beležimo tačno vreme obrade u UTC zoni u ISO formatu
            df["_ingestion_timestamp"] = datetime.now(timezone.utc).isoformat()
            # Beležimo iz kog tačno fajla je ovaj podatak potekao, da bismo lakše otkrili greške u budućnosti
            df["_source_file"] = key

            # 5. KORAK: Izvlačenje regiona iz putanje fajla
            # Ako je fajl na S3 ležao u folderu "region=US/", želimo da izvučemo slova "US"
            region = "unknown"
            # Delimo putanju fajla na delove koristeći kosu crtu "/"
            for part in key.split("/"):
                if part.startswith("region="):
                    # Delimo tekst kod znaka "=" i uzimamo desni deo (indeks 1)
                    region = part.split("=")[1]
                    break                        # Našli smo region, prekidamo ovu malu petlju
            # Upisujemo vrednost regiona u potpuno novu kolonu unutar naše tabele
            df["region"] = region

            logger.info(f"  Clean shape: {df.shape}, region: {region}")

            # 6. KORAK: Magični upis u Silver sloj (Parquet format) i osvežavanje baze
            # Koristimo awswrangler koji završava ogroman posao jednim potezom:
            wr_response = wr.s3.to_parquet(
                df=df,                # Naša očišćena i spremna Pandas tabela
                path=SILVER_PATH,      # S3 lokacija gde se čuva Silver sloj
                # Označava da pravimo organizovan skup podataka (bazu), a ne samo jedan bačen fajl
                dataset=True,
                database=GLUE_DB,      # Ime baze podataka u AWS Glue katalogu
                table=GLUE_TABLE,      # Ime tabele u AWS Glue katalogu
                # PARTITIONING: Podaci će na S3 biti fizički razdvojeni u foldere po regionima (npr. region=US/)
                partition_cols=["region"],

                # IDEMPOTENTNOST (overwrite_partitions): Ako ponovo pustimo isti fajl za region US,
                # program neće dodati duple podatke, nego će obrisati stare fajlove u tom folderu i prepisati nove
                mode="overwrite_partitions",

                # EVOLUCIJA ŠEME: Ako u budućnosti sa YouTube API-ja stigne nova kolona,
                # AWS Wrangler će je automatski dodati u Glue katalog bez pucanja sistema
                schema_evolution=True,
            )

            logger.info(f"  Written to Silver: {SILVER_PATH}")
            # Ako je sve prošlo bez greške, dodajemo detalje u listu uspešno obrađenih fajlova
            processed.append({"key": key, "region": region, "rows": len(df)})

        except Exception as e:
            # ── BEZBEDNOSNA MREŽA (Error Handling) ───────────────────────────
            # Ako bilo šta pukne unutar "try" bloka za ovaj fajl, program ne doživljava krah (crash),
            # već hvatamo grešku, upisujemo je u logove sa kompletnim tragom (exc_info=True) i nastavljamo dalje
            logger.error(f"Error processing record: {e}", exc_info=True)

            # Beležimo putanju fajla koji je pukao i tekst greške
            errors.append({
                "key": key if "key" in dir() else "unknown",
                "error": str(e)
            })

    # ── FINALE: Slanje alarma ────────────────────────────────────────────────
    # Nakon što smo probali da obradimo sve fajlove, gledamo da li se u listi "errors" nalazi nešto
    if errors:
        # Ako ima grešaka, aktiviramo našu funkciju za slanje obaveštenja (SNS alarm)
        # Pretvaramo listu grešaka u čitljiv tekstualni format (JSON sa razmacima) pomoću json.dumps
        send_alert(
            subject="[YT Pipeline] Silver reference transform failed",
            message=json.dumps(errors, indent=2),
        )

    # Lambda na kraju vraća statusni odgovor (Response) – izveštaj o tome šta je urađeno, a šta je puklo
    return {
        "statusCode": 200,
        "processed": processed,
        "errors": errors,
    }
