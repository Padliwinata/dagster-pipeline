from dagster import (
    asset,
    repository,
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    ScheduleDefinition,
    define_asset_job,
)
import requests
from datetime import datetime, timedelta, timezone
import re
import os
from Sastrawi.Stemmer.StemmerFactory import StemmerFactory
from Sastrawi.StopWordRemover.StopWordRemoverFactory import StopWordRemoverFactory

# Configuration
NOCO_DB_URL = "https://noco.rpadliwinata.my.id"
NOCO_TOKEN = os.getenv("NOCO_TOKEN", "Qv3V7ZqqZXa1IMzxqskr3JtC3bEbrzZcGjq5htxF")
SOURCE_TABLE_ID = "m1v56phglixo9hr"
WORDCLOUD_TABLE_ID = "m58fmldudl81m2m"

def fetch_news_records(context: AssetExecutionContext) -> dict:
    """Fetch today's news records from NocoDB"""
    headers = {'xc-token': NOCO_TOKEN}
    url = f'{NOCO_DB_URL}/api/v3/data/pn7tso5s9rfw8g2/{SOURCE_TABLE_ID}/records?sort=%7B%22direction%22%3A%22desc%22%2C%20%22field%22%3A%22CreatedAt%22%7D&where=%28CreatedAt%2Ceq%2Ctoday%29&pageSize=35'

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        context.log.error(f"Error fetching news records: {e}")
        raise

def extract_current_hour_records_wib(raw_data):
    """Extract records from the current hour in WIB timezone"""
    WIB = timezone(timedelta(hours=7))

    now = datetime.now(WIB)
    hour_start = now.replace(minute=0, second=0, microsecond=0)
    hour_end = hour_start + timedelta(hours=1)

    flat_records = []

    for record in raw_data.get("records", []):
        fields = record.get("fields", {})

        created_utc = datetime.fromisoformat(fields.get("CreatedAt"))
        created_wib = created_utc.astimezone(WIB)

        if hour_start <= created_wib < hour_end:
            flat_records.append({
                "id": record.get("id"),
                "created_at_wib": created_wib.isoformat(),
                "media": fields.get("media"),
                "judul": fields.get("judul"),
                "url": fields.get("url"),
                "tag": fields.get("tag"),
                "topik": fields.get("topik"),
            })

    return flat_records

def build_wordcloud_text_from_records(records, context: AssetExecutionContext):
    """
    Process news records to create word cloud text
    Input  : list of flat records from extract_current_hour_records_wib
    Output : single clean string ready for word cloud
    """
    stemmer = StemmerFactory().create_stemmer()
    stopword_remover = StopWordRemoverFactory().create_stop_word_remover()

    cleaned_texts = []
    processed_count = 0

    for record in records:
        judul = record.get("judul")
        if not judul:
            continue

        text = judul.lower()

        # Remove URLs
        text = re.sub(r"http\S+|www\S+", "", text)

        # Remove punctuation & numbers
        text = re.sub(r"[^a-z\s]", " ", text)

        # Normalize spaces
        text = re.sub(r"\s+", " ", text).strip()

        # Remove stopwords
        text = stopword_remover.remove(text)

        # Stemming
        text = stemmer.stem(text)

        if text:
            cleaned_texts.append(text)
            processed_count += 1

    context.log.info(f"Processed {processed_count} news titles for word cloud")

    # Single string for word cloud
    return " ".join(cleaned_texts)

def save_wordcloud_to_noco(wordcloud_text: str, context: AssetExecutionContext) -> requests.Response:
    """Save word cloud data to NocoDB"""
    headers = {
        'xc-token': NOCO_TOKEN,
        'Content-Type': 'application/json'
    }

    url = f'{NOCO_DB_URL}/api/v3/data/pn7tso5s9rfw8g2/{WORDCLOUD_TABLE_ID}/records'

    wordcloud_data = {
        "fields": {
            "Text": wordcloud_text,
            "Time": datetime.now(timezone.utc).isoformat()
        }
    }

    try:
        response = requests.post(url, headers=headers, json=wordcloud_data)
        response.raise_for_status()
        return response
    except requests.exceptions.RequestException as e:
        context.log.error(f"Error saving word cloud data: {e}")
        raise


@asset(group_name="wordcloud_generation", compute_kind="python")
def hourly_wordcloud(context: AssetExecutionContext) -> MaterializeResult:
    """Generate word cloud from news articles from the current hour in WIB"""
    context.log.info("Starting hourly word cloud generation...")

    try:
        # Fetch today's news records
        raw_data = fetch_news_records(context)

        # Extract records from current hour (WIB)
        current_hour_records = extract_current_hour_records_wib(raw_data)
        context.log.info(f"Found {len(current_hour_records)} records from current hour")

        if not current_hour_records:
            context.log.warning("No records found for current hour")
            return MaterializeResult(
                metadata={
                    "records_found": 0,
                    "wordcloud_generated": False,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
            )

        # Process records to build word cloud text
        wordcloud_text = build_wordcloud_text_from_records(current_hour_records, context)

        if not wordcloud_text.strip():
            context.log.warning("No valid text generated for word cloud")
            return MaterializeResult(
                metadata={
                    "records_found": len(current_hour_records),
                    "wordcloud_generated": False,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
            )

        # Save word cloud data to NocoDB
        response = save_wordcloud_to_noco(wordcloud_text, context)
        context.log.info("Successfully saved word cloud data to NocoDB")

        return MaterializeResult(
            metadata={
                "records_found": len(current_hour_records),
                "wordcloud_generated": True,
                "text_length": len(wordcloud_text),
                "word_count": len(wordcloud_text.split()),
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "sources": list(set(record.get("media", "unknown") for record in current_hour_records))
            }
        )

    except Exception as e:
        context.log.error(f"Error generating word cloud: {str(e)}")
        raise

# Define job from asset
wordcloud_job = define_asset_job(
    name="hourly_wordcloud_job",
    selection="hourly_wordcloud",
    description="Job to generate hourly word cloud from news articles"
)

# Schedule definition - run 5 minutes past every hour
hourly_wordcloud_schedule = ScheduleDefinition(
    job=wordcloud_job,
    cron_schedule="5 * * * *",  # Run every hour at minute 5
    description="Generate word cloud 5 minutes past every hour"
)

@repository
def wordcloud_repository():
    """Dagster repository containing word cloud generation assets and jobs"""
    return [
        # Assets
        hourly_wordcloud,

        # Jobs
        wordcloud_job,

        # Schedules
        hourly_wordcloud_schedule,
    ]

if __name__ == "__main__":
    # For local testing
    from dagster import materialize

    # Materialize the word cloud asset
    materialize([hourly_wordcloud])