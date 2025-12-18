from dagster import (
    asset,
    job,
    repository,
    OpExecutionContext,
    MaterializeResult,
    MetadataValue,
    AssetExecutionContext,
    build_schedule_context,
    ScheduleDefinition,
    define_asset_job,
)
import requests
from bs4 import BeautifulSoup
from typing import List, Dict, Any
import datetime
import os
from datetime import timezone

# Configuration
NOCO_DB_URL = "https://noco.rpadliwinata.my.id"
NOCO_TOKEN = os.getenv("NOCO_TOKEN", "Qv3V7ZqqZXa1IMzxqskr3JtC3bEbrzZcGjq5htxF")
TABLE_ID = "m1v56phglixo9hr"

def save_to_noco(data: List[Dict[str, Any]], source: str) -> requests.Response:
    """
    Save scraped data to NocoDB

    Args:
        data (List[Dict]): List of news articles
        source (str): Source name for logging

    Returns:
        requests.Response: The response from NocoDB API
    """
    headers = {
        'xc-token': NOCO_TOKEN,
        'Content-Type': 'application/json'
    }

    url = f'{NOCO_DB_URL}/api/v2/tables/{TABLE_ID}/records'

    try:
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
        return response
    except requests.exceptions.RequestException as e:
        print(f"Error saving {source} data to NocoDB: {e}")
        raise

@asset(group_name="news_scraping", compute_kind="python")
def detik_news(context: AssetExecutionContext) -> MaterializeResult:
    """Scrape most popular and most commented news from Detik"""
    context.log.info("Starting Detik news scraping...")

    try:
        url = 'https://www.detik.com/'
        res = requests.get(url)
        res.raise_for_status()
        soup = BeautifulSoup(res.content, 'html.parser')

        data = []

        # Get most popular articles
        popular_box = soup.find('div', class_='box cb-mostpop')
        if popular_box:
            popular_articles = popular_box.find_all('div', class_='media')
            for article in popular_articles:
                link_elem = article.find("a", class_="media__link")
                if link_elem:
                    title = link_elem.text.strip()
                    data.append({
                        'media': 'detik',
                        'judul': title,
                        'url': link_elem["href"],
                        'tag': 'most_popular',
                        'scraped_at': datetime.datetime.now(timezone.utc).isoformat()
                    })

        # Get most commented articles
        most_comment_box = soup.find('div', class_='box cb-commented')
        if most_comment_box:
            most_commented_articles = most_comment_box.find_all('div', class_='media')
            for article in most_commented_articles:
                link_elem = article.find("a")
                if link_elem:
                    title = link_elem.text.strip()
                    data.append({
                        'media': 'detik',
                        'judul': title,
                        'url': link_elem["href"],
                        'tag': 'most_commented',
                        'scraped_at': datetime.datetime.now(timezone.utc).isoformat()
                    })

        # Save to NocoDB
        if data:
            response = save_to_noco(data, "Detik")
            context.log.info(f"Successfully saved {len(data)} Detik articles to NocoDB")
        else:
            context.log.warning("No Detik articles found")

        return MaterializeResult(
            metadata={
                "articles_scraped": len(data),
                "source": "Detik",
                "scraped_at": datetime.datetime.now(timezone.utc).isoformat(),
                "categories": list(set(item['tag'] for item in data))
            }
        )

    except Exception as e:
        context.log.error(f"Error scraping Detik: {str(e)}")
        raise

@asset(group_name="news_scraping", compute_kind="python")
def tribun_news(context: AssetExecutionContext) -> MaterializeResult:
    """Scrape most popular news from Tribun"""
    context.log.info("Starting Tribun news scraping...")

    try:
        url = 'https://www.tribunnews.com/'
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_5_6; en-US) AppleWebKit/534.35 (KHTML, like Gecko) Chrome/48.0.1150.159 Safari/602'
        }
        res = requests.get(url, headers=headers)
        res.raise_for_status()
        soup = BeautifulSoup(res.content, 'html.parser')

        data = []

        # Get most popular articles
        popular_box = soup.find('div', class_='mb20 populer')
        if popular_box:
            popular_articles = popular_box.find_all('div', class_='mt5 mr110')
            for article in popular_articles:
                link_elem = article.find("a")
                if link_elem:
                    title = link_elem.text.strip()
                    data.append({
                        'media': 'tribun',
                        'judul': title,
                        'url': link_elem["href"],
                        'tag': 'most_popular',
                        'scraped_at': datetime.datetime.now(timezone.utc).isoformat()
                    })

        # Save to NocoDB
        if data:
            response = save_to_noco(data, "Tribun")
            context.log.info(f"Successfully saved {len(data)} Tribun articles to NocoDB")
        else:
            context.log.warning("No Tribun articles found")

        return MaterializeResult(
            metadata={
                "articles_scraped": len(data),
                "source": "Tribun",
                "scraped_at": datetime.datetime.now(timezone.utc).isoformat()
            }
        )

    except Exception as e:
        context.log.error(f"Error scraping Tribun: {str(e)}")
        raise

@asset(group_name="news_scraping", compute_kind="python")
def cnn_indonesia_news(context: AssetExecutionContext) -> MaterializeResult:
    """Scrape most popular news from CNN Indonesia"""
    context.log.info("Starting CNN Indonesia news scraping...")

    try:
        url = 'https://www.cnnindonesia.com/'
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_5_6; en-US) AppleWebKit/534.35 (KHTML, like Gecko) Chrome/48.0.1150.159 Safari/602'
        }
        res = requests.get(url, headers=headers)
        res.raise_for_status()
        soup = BeautifulSoup(res.content, 'html.parser')

        data = []

        # Get most popular articles
        popular_box = soup.find('div', class_='overflow-y-auto relative h-[322px]')
        if popular_box:
            popular_articles = popular_box.find_all('article')
            for article in popular_articles:
                h2_elem = article.find("h2")
                link_elem = article.find("a")
                if h2_elem and link_elem:
                    title = h2_elem.text.strip()
                    data.append({
                        'media': 'cnnindonesia',
                        'judul': title,
                        'url': link_elem["href"],
                        'tag': 'most_popular',
                        'scraped_at': datetime.datetime.now(timezone.utc).isoformat()
                    })

        # Save to NocoDB
        if data:
            response = save_to_noco(data, "CNN Indonesia")
            context.log.info(f"Successfully saved {len(data)} CNN Indonesia articles to NocoDB")
        else:
            context.log.warning("No CNN Indonesia articles found")

        return MaterializeResult(
            metadata={
                "articles_scraped": len(data),
                "source": "CNN Indonesia",
                "scraped_at": datetime.datetime.now(timezone.utc).isoformat()
            }
        )

    except Exception as e:
        context.log.error(f"Error scraping CNN Indonesia: {str(e)}")
        raise

@asset(group_name="news_scraping", compute_kind="python")
def kompas_news(context: AssetExecutionContext) -> MaterializeResult:
    """Scrape most popular news from Kompas"""
    context.log.info("Starting Kompas news scraping...")

    try:
        url = 'https://www.kompas.com/'
        res = requests.get(url)
        res.raise_for_status()
        soup = BeautifulSoup(res.content, 'html.parser')

        data = []

        # Get most popular articles
        popular_box = soup.find('div', class_='mostWrap')
        if popular_box:
            popular_articles = popular_box.find_all('div', class_='mostItem')
            for article in popular_articles:
                h2_elem = article.find("h2", class_="mostTitle")
                link_elem = article.find("a")
                if h2_elem and link_elem:
                    title = h2_elem.text.strip()
                    data.append({
                        'media': 'kompas',
                        'judul': title,
                        'url': link_elem["href"],
                        'tag': 'most_popular',
                        'scraped_at': datetime.datetime.now(timezone.utc).isoformat()
                    })

        # Save to NocoDB
        if data:
            response = save_to_noco(data, "Kompas")
            context.log.info(f"Successfully saved {len(data)} Kompas articles to NocoDB")
        else:
            context.log.warning("No Kompas articles found")

        return MaterializeResult(
            metadata={
                "articles_scraped": len(data),
                "source": "Kompas",
                "scraped_at": datetime.datetime.now(timezone.utc).isoformat()
            }
        )

    except Exception as e:
        context.log.error(f"Error scraping Kompas: {str(e)}")
        raise

@asset(group_name="news_scraping", compute_kind="python")
def viva_news(context: AssetExecutionContext) -> MaterializeResult:
    """Scrape most popular news from Viva"""
    context.log.info("Starting Viva news scraping...")

    try:
        url = 'https://www.viva.co.id/'
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_5_6; en-US) AppleWebKit/534.35 (KHTML, like Gecko) Chrome/48.0.1150.159 Safari/602'
        }
        res = requests.get(url, headers=headers)
        res.raise_for_status()
        soup = BeautifulSoup(res.content, 'html.parser')

        data = []

        # Get most popular articles
        popular_box = soup.find('div', class_='column-small')
        if popular_box:
            popular_articles = popular_box.find_all('div', class_='article-list-row')
            for article in popular_articles:
                h2_elem = article.find("h2")
                link_elem = article.find("a", class_='article-list-title')
                if h2_elem and link_elem:
                    title = h2_elem.text.strip()
                    data.append({
                        'media': 'viva',
                        'judul': title,
                        'url': link_elem["href"],
                        'tag': 'most_popular',
                        'scraped_at': datetime.datetime.now(timezone.utc).isoformat()
                    })

        # Save to NocoDB
        if data:
            response = save_to_noco(data, "Viva")
            context.log.info(f"Successfully saved {len(data)} Viva articles to NocoDB")
        else:
            context.log.warning("No Viva articles found")

        return MaterializeResult(
            metadata={
                "articles_scraped": len(data),
                "source": "Viva",
                "scraped_at": datetime.datetime.now(timezone.utc).isoformat()
            }
        )

    except Exception as e:
        context.log.error(f"Error scraping Viva: {str(e)}")
        raise

# Define jobs from assets
scrape_all_news_job = define_asset_job(
    name="scrape_all_news_job",
    selection="*",
    description="Job to scrape news from all sources"
)

scrape_detik_only_job = define_asset_job(
    name="scrape_detik_only_job",
    selection="detik_news",
    description="Job to scrape only Detik news"
)

scrape_tribun_only_job = define_asset_job(
    name="scrape_tribun_only_job",
    selection="tribun_news",
    description="Job to scrape only Tribun news"
)

scrape_cnn_only_job = define_asset_job(
    name="scrape_cnn_only_job",
    selection="cnn_indonesia_news",
    description="Job to scrape only CNN Indonesia news"
)

scrape_kompas_only_job = define_asset_job(
    name="scrape_kompas_only_job",
    selection="kompas_news",
    description="Job to scrape only Kompas news"
)

scrape_viva_only_job = define_asset_job(
    name="scrape_viva_only_job",
    selection="viva_news",
    description="Job to scrape only Viva news"
)

# Schedule definitions
hourly_news_schedule = ScheduleDefinition(
    job=scrape_all_news_job,
    cron_schedule="0 * * * *",  # Run every hour at minute 0
    description="Scrape news from all sources every hour"
)

@repository
def news_scraper_repository():
    """Dagster repository containing all news scraping assets and jobs"""
    return [
        # Assets
        detik_news,
        tribun_news,
        cnn_indonesia_news,
        kompas_news,
        viva_news,

        # Jobs
        scrape_all_news_job,
        scrape_detik_only_job,
        scrape_tribun_only_job,
        scrape_cnn_only_job,
        scrape_kompas_only_job,
        scrape_viva_only_job,

        # Schedules
        hourly_news_schedule,
    ]

if __name__ == "__main__":
    # For local testing
    from dagster import materialize

    # Materialize all assets
    materialize(
        [
            detik_news,
            tribun_news,
            cnn_indonesia_news,
            kompas_news,
            viva_news,
        ]
    )