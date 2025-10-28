import pandas as pd
from apify_client import ApifyClient
import time
import re
import logging
import html
import unicodedata
import os

# Configurar logging más limpio
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

# --- PARÁMETROS DE CONFIGURACIÓN ---
APIFY_TOKEN = os.environ.get("APIFY_TOKEN")
SOLO_PRIMER_POST = False

# LISTA DE URLs A PROCESAR
URLS_A_PROCESAR = [
    # --- Facebook URLs ---
    "https://www.facebook.com/100064867445065/posts/1217600013745569/?dco_ad_token=AaoF89IFxE22tlbNX0I7jV7QlNZtp-TRp0V3C8Kboi6KvJj3uKPtSlgRZ1HV_nLUCdJ6lpAAZdbbbA7x&dco_ad_id=120231960561750767",
    "https://www.facebook.com/100064867445065/posts/1217600207078883/?dco_ad_token=AaqCuxpQRkElHpDWah_cxlKSxActg7DQPMl0hc0MAYyeOjCaPH_Ru4XNz8IgJeO5w2iT2H68pa7WF1X0&dco_ad_id=120231961149310767",
    "https://www.facebook.com/100064867445065/posts/1217600217078882/?dco_ad_token=AarYFtUT3TEv7kbLxhxPSwTpEuSgdNf8Oaq_BV0Fn8wi240t82uQ1jCWAdzTqETTIeetXL_e5JUMj8PO&dco_ad_id=120231961160580767",
    "https://www.facebook.com/100064867445065/posts/1217600220412215/?dco_ad_token=AaoRhcx8gzva-fvCYY408y211x18z0F_3Biw0iCSA3dWxoO8AjJQJQbXjNtRxpcUm3nMtpJDHjcgRzJV&dco_ad_id=120231961156780767",
    "https://www.facebook.com/100064867445065/posts/1217618077077096/?dco_ad_token=AaoYWtSKZUI9Sj3JS6ttVjbb2qJRfctdqSnLprRVeY2aPoN7aTsjcyuOHDMpoczyvuVKR53hPaXEKTSP&dco_ad_id=120231961518270767",
    "https://www.facebook.com/100064867445065/posts/1217618253743745/?dco_ad_token=Aar6YkXNqqjfbtdLljovvAcbZ2pPFcu_oNBLy_eA0ELHW06fzdym-vbpBhzARthhL-LcPhDssUZTbBmc&dco_ad_id=120231961430560767",
    "https://www.facebook.com/100064867445065/posts/1217618257077078/?dco_ad_token=Aapo8zuvcXtK-zKs3fq54hhriR3rW_eNBkTvSpqpJmFZzN_KxLLBaUVQuVYZP9JCMqteMuXxfnLyMoQ2&dco_ad_id=120231960556670767",
    "https://www.facebook.com/100064867445065/posts/1217618263743744/?dco_ad_token=Aap-WxnXyBHXDz7rMydr4nUTndfmSaZlLDifE-L2DqUC4-KJ1gIjBVIuIKSPBApNV4equQoJNGjvueY4&dco_ad_id=120231961490130767",
    "https://www.facebook.com/100064867445065/posts/1229679052537665/?dco_ad_token=Aaqetf784RumLkbscgHZXNSRF0TZIO9n2Vt1_9uwZO6mpYTzysZ_WfEF3CH19-WBugGRBO39K-5V4ma7&dco_ad_id=120232527495410767",
    "https://www.facebook.com/100064867445065/posts/1231479782357592/?dco_ad_token=AaodirgHyfjdrRtg5kbM2UKuwTLaktdQ0eZi1UcYqEqYVNmBNKrkN8I9cMgqskgOxr_VQDBovS29M4Dl&dco_ad_id=120232625186890767",
    "https://www.facebook.com/100064867445065/posts/1231508129021424/",
    "https://www.facebook.com/100064867445065/posts/pfbid022RtBGRNcwvtaFeqPpAZNM1zoPWsxoSyvTpX3iDUzDbuo9DATtjD1fxRewU7zbfnjl?dco_ad_id=120233417640040767",
    "https://www.facebook.com/100064867445065/posts/pfbid02J5RGNbDmhZQPmwZ49LQe9a1PYixZYfa47ur3cPRFH6SB7zmn7cKUEzXAKu4uPxZ6l?dco_ad_id=120233417640000767",
    "https://www.facebook.com/100064867445065/posts/pfbid02FeY97uyQGMgPn2dTL7zhj9K1PsNxcY9rYYykTDsoi2LUQdnaJA1rfaoUHKHY9m5sl?dco_ad_id=120233417640070767",
    "https://www.facebook.com/100064867445065/posts/pfbid02nRaSVkknJakMLr1pbahBdWB3YrFGzcB9nGPfyvYM6JdpjygctLCTrL1WwsavLunpl?dco_ad_id=120233417640540767",
    "https://www.facebook.com/100064867445065/posts/pfbid02ybCAXaLKY9ubQ7eLNEvefGr6ur7WbezUmgbUmxtmGKx5a67zRM3s39cNLSBDfAQHl?dco_ad_id=120233417640110767",
    "https://www.facebook.com/100064867445065/posts/pfbid034hb9Tcgf8fNPvajC1QAoyuS7i91FdvBQCMSrvCkhrLda5uBX7FP3ensWhnoBJgbPl?dco_ad_id=120233417640090767",
    "https://www.facebook.com/100064867445065/posts/pfbid035nVdxMQqsW9NxddtJYAVd1LLrBaS8dgk1bBGhF9Y3uQiLNvqTCcPd6cYutzPPd9gl?dco_ad_id=120233372422600767",
    "https://www.facebook.com/100064867445065/posts/pfbid051W5BJG2FNRJ64njCDbs5m9oSVoaPp9mcbh6aaUqovRYCge1xi7BnNWGzxx3PUpHl?dco_ad_id=120233417640430767",
    "https://www.facebook.com/100064867445065/posts/pfbid07bStLKsV4VPYjTQT6qqHSgXnfrkMLQShTpX5PgJjjDHVjsqZQLm9CALjsG6nxUMxl?dco_ad_id=120233417640450767",
    "https://www.facebook.com/100064867445065/posts/pfbid0azzzVPgHKPkqDmoKFVWumSvkMzkMPXwxG3AptAmqteBwVG6bfPnaMoAV8sZnijSZl?dco_ad_id=120233417640060767",
    "https://www.facebook.com/100064867445065/posts/pfbid0Le48QQg6oZjcCdieicDexGNW6VRmx5mKyL1j5VvHqxAofD54W7nHFUvyL2ENPfXFl?dco_ad_id=120233417640410767",
    "https://www.facebook.com/100064867445065/posts/pfbid0LP6NPsv5TQKFjy3ySQhpKz7uDuprTZRAE26G9Eya6ip8YhY3vtrwVAwpzSyZW7wgl?dco_ad_id=120233417640030767",
    "https://www.facebook.com/100064867445065/posts/pfbid0MdHEzcA1GYXEEJipVD7xxvDhQSyEXJ6AvpS4XUgFjYhYFBdfMPZPbSTbL47BjAxNl?dco_ad_id=120233417640010767",
    "https://www.facebook.com/100064867445065/posts/pfbid0N3R82dbJH8TK4stdDiYZPingWQpJ7eRMfk2XATEGWg2GpKDfFTJmbQ1mRbMbi9Pvl?dco_ad_id=120233372150830767",
    "https://www.facebook.com/100064867445065/posts/pfbid0nPPpJvZTAk9Ji92nyy34A1mekB9HjbL2ojotiqjdG1MQrBvyuDyFc9UHst9yLneUl?dco_ad_id=120233371761820767",
    "https://www.facebook.com/100064867445065/posts/pfbid0tUoDdXF3XnyK9y7Yyzc2JZSBKoz7bFZ6chWnARxhbqRRAiLx4heaCjCKx2G6FS4Fl?dco_ad_id=120233417640020767",
    "https://www.facebook.com/100064867445065/posts/pfbid0upLVRR72g8wYmQfhFkEBZVGX3hVqV4gQp3VahAes9gWWFdkjLhpordeQjcTAReW6l?dco_ad_id=120233417640080767",
    "https://www.facebook.com/100064867445065/posts/pfbid0WMUCHyMQGEqhQ1MSKyf3fWkmxkNWKTd32jngqUDKwmDTmVcQbEbdguriMpj9cSfAl?dco_ad_id=120233417640100767",
    "https://www.facebook.com/100064867445065/posts/pfbid0YZApPYi3eGapbpDvHzvoGpQinZxZH5Hk8i4BTd7M3YE6LzCFdsNhuK5UFLLEUNjl?dco_ad_id=120233372061270767",
    "https://www.facebook.com/reel/1178692000784865/",
    "https://www.facebook.com/reel/1206917587929517/",
    "https://www.facebook.com/reel/1343171207221314/",
    "https://www.facebook.com/reel/1542606723825791/",
    "https://www.facebook.com/reel/1791844461437602/",
    "https://www.facebook.com/reel/793063333529226/",
    "https://www.facebook.com/reel/807708398851924/",
    "https://www.facebook.com/reel/820221664284123/",
    "https://www.facebook.com/reel/832078625827688/",
    # --- Instagram URLs ---
    "http://instagram.com/p/DOekIj5DPYE/#advertiser",
    "https://www.instagram.com/p/DPHW-YyjJlv/#advertiser",
    "https://www.instagram.com/p/DPHbZ51jBH_/#advertiser",
    "https://www.instagram.com/p/DOekIASjMIz/#advertiser",
    "https://www.instagram.com/p/DOekIQHDI6q/#advertiser",
    "https://www.instagram.com/p/DOekId4DBos/#advertiser",
    "https://www.instagram.com/p/DOemd2qDGPk/#advertiser",
    "https://www.instagram.com/p/DOemgJTjIVE/#advertiser",
    "https://www.instagram.com/p/DOemgJYDMpG/#advertiser",
    "https://www.instagram.com/p/DOemgKVjL8u/#advertiser",
    "https://www.instagram.com/p/DPpTQ9ODIWC/",
    "https://www.instagram.com/p/DPpVC6UjLlp/",
    "https://www.instagram.com/p/DPpWL69jF1R/",
    "https://www.instagram.com/p/DPpWgTEjKY7/",
    "https://www.instagram.com/p/DPpWwM1jM6r/",
    "https://www.instagram.com/p/DPpVx3xjLX2/",
    "https://www.instagram.com/p/DPpXA4uDDh4/",
    "https://www.instagram.com/p/DPpXFV9DGnM/",
    "https://www.instagram.com/p/DPpXWD4jNcx/",
    "https://www.instagram.com/p/DPpXWIlDOxY/",
    "https://www.instagram.com/p/DPpXWpHjMX2/",
    "https://www.instagram.com/p/DPrJPHUjKA6/",
    "https://www.instagram.com/p/DPrJQEODNrJ/",
    "https://www.instagram.com/p/DPrJQMYjDBB/",
    "https://www.instagram.com/p/DPrJQcfDNe_/",
    "https://www.instagram.com/p/DPrJQmaDHEZ/",
    "https://www.instagram.com/p/DPrJQsVjHMW/",
    "https://www.instagram.com/p/DPrJQ4-DIjt/",
    "https://www.instagram.com/p/DPrJRp4jPvq/",
    "https://www.instagram.com/p/DPrJRoDjIzx/",
    "https://www.instagram.com/p/DPrJR_GDHSU/",
    "https://www.instagram.com/p/DPrJSDYDEKI/",
    "https://www.instagram.com/p/DPrJShhjNDc/",
    "https://www.instagram.com/p/DPrJTIsDFbt/",
    "https://www.instagram.com/p/DPrJTRDjOMk/",
    "https://www.instagram.com/p/DPrJUk_DFiM/",
    "https://www.instagram.com/p/DPrJURUDACm/",
    "https://www.instagram.com/p/DPzKNF0DIqm/",
    # --- Facebook URLs Faltantes ---
    "https://www.facebook.com/100064867445065/posts/1139081728264065/",
    "https://www.facebook.com/100064867445065/posts/1136206128551625/",
    "https://www.facebook.com/100064867445065/posts/1159592922879612/",
    "https://www.facebook.com/100064867445065/posts/1159593356212902/",
    "https://www.facebook.com/100064867445065/posts/1159593509546220/",
    "https://www.facebook.com/100064867445065/posts/1159593362879568/",
    # --- Instagram URLs Faltantes ---
    "https://www.instagram.com/p/DKiPruOA4FL/",
    "https://www.instagram.com/p/DLsjOfCAwZJ/",
    "https://www.instagram.com/p/DLsjNd8AM3R/",
    "https://www.instagram.com/p/DLsjPVcgnt6/",
    "https://www.instagram.com/p/DLsjNViAcFf/",
    # --- URLs que faltaban de la primera lista ---
    "https://www.facebook.com/100064867445065/posts/1250895837082653/",
    "https://www.instagram.com/p/DP7-ZoujKxq/",
]

# INFORMACIÓN DE CAMPAÑA
CAMPAIGN_INFO = {
    'campaign_name': 'CAMPAÑA_MANUAL_MULTIPLE',
    'campaign_id': 'MANUAL_002',
    'campaign_mes': 'Septiembre 2025',
    'campaign_marca': 'TU_MARCA',
    'campaign_referencia': 'REF_MANUAL',
    'campaign_objetivo': 'Análisis de Comentarios'
}

class SocialMediaScraper:
    def __init__(self, apify_token):
        self.client = ApifyClient(apify_token)

    def detect_platform(self, url):
        if pd.isna(url) or not url: return None
        url = str(url).lower()
        if any(d in url for d in ['facebook.com', 'fb.com']): return 'facebook'
        if 'instagram.com' in url: return 'instagram'
        if 'tiktok.com' in url: return 'tiktok'
        return None

    def clean_url(self, url):
        return str(url).split('?')[0] if '?' in str(url) else str(url)

    def fix_encoding(self, text):
        if pd.isna(text) or text == '': return ''
        try:
            text = str(text)
            text = html.unescape(text)
            text = unicodedata.normalize('NFKD', text)
            return text.strip()
        except Exception as e:
            logger.warning(f"Could not fix encoding: {e}")
            return str(text)

    def _wait_for_run_finish(self, run):
        logger.info("Scraper initiated, waiting for results...")
        max_wait_time = 300
        start_time = time.time()
        while True:
            run_status = self.client.run(run["id"]).get()
            if run_status["status"] in ["SUCCEEDED", "FAILED", "TIMED-OUT"]:
                return run_status
            if time.time() - start_time > max_wait_time:
                logger.error("Timeout reached while waiting for scraper.")
                return None
            time.sleep(10)

    def scrape_facebook_comments(self, url, max_comments=500, campaign_info=None, post_number=1):
        try:
            logger.info(f"Processing Facebook Post {post_number}: {url}")
            run_input = {"startUrls": [{"url": self.clean_url(url)}], "maxComments": max_comments}
            run = self.client.actor("apify/facebook-comments-scraper").call(run_input=run_input)
            run_status = self._wait_for_run_finish(run)
            if not run_status or run_status["status"] != "SUCCEEDED":
                logger.error(f"Facebook extraction failed. Status: {run_status.get('status', 'UNKNOWN')}")
                return []
            items = self.client.dataset(run["defaultDatasetId"]).list_items().items
            logger.info(f"Extraction complete: {len(items)} items found.")
            return self._process_facebook_results(items, url, post_number, campaign_info)
        except Exception as e:
            logger.error(f"Fatal error in scrape_facebook_comments: {e}")
            return []

    def scrape_instagram_comments(self, url, max_comments=500, campaign_info=None, post_number=1):
        try:
            logger.info(f"Processing Instagram Post {post_number}: {url}")
            run_input = {"directUrls": [url], "resultsType": "comments", "resultsLimit": max_comments}
            run = self.client.actor("apify/instagram-scraper").call(run_input=run_input)
            run_status = self._wait_for_run_finish(run)
            if not run_status or run_status["status"] != "SUCCEEDED":
                logger.error(f"Instagram extraction failed. Status: {run_status.get('status', 'UNKNOWN')}")
                return []
            items = self.client.dataset(run["defaultDatasetId"]).list_items().items
            logger.info(f"Extraction complete: {len(items)} items found.")
            return self._process_instagram_results(items, url, post_number, campaign_info)
        except Exception as e:
            logger.error(f"Fatal error in scrape_instagram_comments: {e}")
            return []

    def scrape_tiktok_comments(self, url, max_comments=500, campaign_info=None, post_number=1):
        try:
            logger.info(f"Processing TikTok Post {post_number}: {url}")
            run_input = {"postURLs": [self.clean_url(url)], "maxCommentsPerPost": max_comments}
            run = self.client.actor("clockworks/tiktok-comments-scraper").call(run_input=run_input)
            run_status = self._wait_for_run_finish(run)
            if not run_status or run_status["status"] != "SUCCEEDED":
                logger.error(f"TikTok extraction failed. Status: {run_status.get('status', 'UNKNOWN')}")
                return []
            items = self.client.dataset(run["defaultDatasetId"]).list_items().items
            logger.info(f"Extraction complete: {len(items)} comments found.")
            return self._process_tiktok_results(items, url, post_number, campaign_info)
        except Exception as e:
            logger.error(f"Fatal error in scrape_tiktok_comments: {e}")
            return []

    def _process_facebook_results(self, items, url, post_number, campaign_info):
        processed = []
        # <-- CORRECCIÓN: Usando tu lista de campos de fecha más completa
        possible_date_fields = ['createdTime', 'timestamp', 'publishedTime', 'date', 'createdAt', 'publishedAt']
        for comment in items:
            # <-- CORRECCIÓN: Usando tu bucle for original para máxima compatibilidad
            created_time = None
            for field in possible_date_fields:
                if field in comment and comment[field]:
                    created_time = comment[field]
                    break
            comment_data = {**campaign_info, 'post_url': url, 'post_number': post_number, 'platform': 'Facebook', 'author_name': self.fix_encoding(comment.get('authorName')), 'author_url': comment.get('authorUrl'), 'comment_text': self.fix_encoding(comment.get('text')), 'created_time': created_time, 'likes_count': comment.get('likesCount', 0), 'replies_count': comment.get('repliesCount', 0), 'is_reply': False, 'parent_comment_id': None, 'created_time_raw': str(comment)}
            processed.append(comment_data)
        logger.info(f"Processed {len(processed)} Facebook comments.")
        return processed

    def _process_instagram_results(self, items, url, post_number, campaign_info):
        processed = []
        # <-- CORRECCIÓN: Usando tu lista de campos de fecha más completa
        possible_date_fields = ['timestamp', 'createdTime', 'publishedAt', 'date', 'createdAt', 'taken_at']
        for item in items:
            comments_list = item.get('comments', [item]) if item.get('comments') is not None else [item]
            for comment in comments_list:
                # <-- CORRECCIÓN: Usando tu bucle for original
                created_time = None
                for field in possible_date_fields:
                    if field in comment and comment[field]:
                        created_time = comment[field]
                        break
                author = comment.get('ownerUsername', '')
                comment_data = {**campaign_info, 'post_url': url, 'post_number': post_number, 'platform': 'Instagram', 'author_name': self.fix_encoding(author), 'author_url': f"https://instagram.com/{author}", 'comment_text': self.fix_encoding(comment.get('text')), 'created_time': created_time, 'likes_count': comment.get('likesCount', 0), 'replies_count': 0, 'is_reply': False, 'parent_comment_id': None, 'created_time_raw': str(comment)}
                processed.append(comment_data)
        logger.info(f"Processed {len(processed)} Instagram comments.")
        return processed

    def _process_tiktok_results(self, items, url, post_number, campaign_info):
        processed = []
        for comment in items:
            author_id = comment.get('user', {}).get('uniqueId', '')
            comment_data = {**campaign_info, 'post_url': url, 'post_number': post_number, 'platform': 'TikTok', 'author_name': self.fix_encoding(comment.get('user', {}).get('nickname')), 'author_url': f"https://www.tiktok.com/@{author_id}", 'comment_text': self.fix_encoding(comment.get('text')), 'created_time': comment.get('createTime'), 'likes_count': comment.get('diggCount', 0), 'replies_count': comment.get('replyCommentTotal', 0), 'is_reply': 'replyToId' in comment, 'parent_comment_id': comment.get('replyToId'), 'created_time_raw': str(comment)}
            processed.append(comment_data)
        logger.info(f"Processed {len(processed)} TikTok comments.")
        return processed

def save_to_excel(df, filename):
    try:
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Comentarios', index=False)
            if 'post_number' in df.columns:
                summary = df.groupby(['post_number', 'platform', 'post_url']).agg(Total_Comentarios=('comment_text', 'count'), Total_Likes=('likes_count', 'sum')).reset_index()
                summary.to_excel(writer, sheet_name='Resumen_Posts', index=False)
        logger.info(f"Excel file saved successfully: {filename}")
        return True
    except Exception as e:
        logger.error(f"Error saving Excel file: {e}")
        return False

def process_datetime_columns(df):
    if 'created_time' not in df.columns: return df
    logger.info("Processing datetime columns...")
    # Intenta convertir todo tipo de formatos (timestamps, ISO, etc.) a un datetime unificado
    df['created_time_processed'] = pd.to_datetime(df['created_time'], errors='coerce', utc=True, unit='s')
    mask = df['created_time_processed'].isna()
    df.loc[mask, 'created_time_processed'] = pd.to_datetime(df.loc[mask, 'created_time'], errors='coerce', utc=True)
    if df['created_time_processed'].notna().any():
        df['created_time_processed'] = df['created_time_processed'].dt.tz_localize(None)
        df['fecha_comentario'] = df['created_time_processed'].dt.date
        df['hora_comentario'] = df['created_time_processed'].dt.time
    return df

def run_extraction():
    logger.info("--- STARTING COMMENT EXTRACTION PROCESS ---")
    if not APIFY_TOKEN:
        logger.error("APIFY_TOKEN not found in environment variables. Aborting.")
        return

    valid_urls = [url.strip() for url in URLS_A_PROCESAR if url.strip()]
    if not valid_urls:
        logger.warning("No valid URLs to process. Exiting.")
        return

    scraper = SocialMediaScraper(APIFY_TOKEN)
    all_comments = []
    post_counter = 0

    for url in valid_urls:
        post_counter += 1
        platform = scraper.detect_platform(url)
        comments = []
        if platform == 'facebook':
            comments = scraper.scrape_facebook_comments(url, campaign_info=CAMPAIGN_INFO, post_number=post_counter)
        elif platform == 'instagram':
            comments = scraper.scrape_instagram_comments(url, campaign_info=CAMPAIGN_INFO, post_number=post_counter)
        elif platform == 'tiktok':
            comments = scraper.scrape_tiktok_comments(url, campaign_info=CAMPAIGN_INFO, post_number=post_counter)
        else:
            logger.warning(f"Unknown platform for URL: {url}")
        
        all_comments.extend(comments)
        if not SOLO_PRIMER_POST and post_counter < len(valid_urls):
            logger.info("Pausing for 30 seconds between posts...")
            time.sleep(30)

    if not all_comments:
        logger.warning("No comments were extracted. Process finished.")
        return

    logger.info("--- PROCESSING FINAL RESULTS ---")
    df_comments = pd.DataFrame(all_comments)
    df_comments = process_datetime_columns(df_comments)
    
    final_columns = ['post_number', 'platform', 'campaign_name', 'post_url', 'author_name', 'comment_text', 'created_time_processed', 'fecha_comentario', 'hora_comentario', 'likes_count', 'replies_count', 'is_reply', 'author_url', 'created_time_raw']
    existing_cols = [col for col in final_columns if col in df_comments.columns]
    df_comments = df_comments[existing_cols]

    filename = "Comentarios Campaña.xlsx"
    save_to_excel(df_comments, filename)
    logger.info("--- EXTRACTION PROCESS FINISHED ---")

if __name__ == "__main__":
    run_extraction()












