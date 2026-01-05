# -*- coding: utf-8 -*-
"""
High Volume News Scraper
Optimized to get 1000+ articles by increasing limits and adding more sources
"""

import feedparser
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from datetime import datetime
import trafilatura
import re
import random
from urllib.parse import urljoin, urlparse
import json
import os
import threading

# Import the enhanced extraction functions from the main scraper
from Article_Scrapper import (
    extract_text_from_url, 
    get_common_rss_feeds, 
    get_news_organizations_by_category,
    get_random_headers,
    NEWS_SELECTORS
)

# -------------------------------
# ADDITIONAL HIGH-VOLUME SOURCES
# -------------------------------
ADDITIONAL_SOURCES = {
    # More RSS feeds for higher volume
    'CNN Politics': 'http://rss.cnn.com/rss/edition_politics.rss',
    'CNN Business': 'http://rss.cnn.com/rss/money_news_international.rss',
    'CNN Technology': 'http://rss.cnn.com/rss/edition_technology.rss',
    'CNN World': 'http://rss.cnn.com/rss/edition_world.rss',
    'BBC Politics': 'http://feeds.bbci.co.uk/news/politics/rss.xml',
    'BBC Business': 'http://feeds.bbci.co.uk/news/business/rss.xml',
    'BBC Technology': 'http://feeds.bbci.co.uk/news/technology/rss.xml',
    'BBC World': 'http://feeds.bbci.co.uk/news/world/rss.xml',
    'Reuters Business': 'https://feeds.reuters.com/reuters/businessNews',
    'Reuters Politics': 'https://feeds.reuters.com/news/politics',
    'Reuters Technology': 'https://feeds.reuters.com/reuters/technologyNews',
    'Reuters World': 'https://feeds.reuters.com/reuters/worldNews',
    'AP Politics': 'https://feeds.apnews.com/apnews/politics',
    'AP Business': 'https://feeds.apnews.com/apnews/business',
    'AP Technology': 'https://feeds.apnews.com/apnews/technology',
    'AP World': 'https://feeds.apnews.com/apnews/world',
    'NYT Politics': 'https://rss.nytimes.com/services/xml/rss/nyt/Politics.xml',
    'NYT Business': 'https://rss.nytimes.com/services/xml/rss/nyt/Business.xml',
    'NYT Technology': 'https://rss.nytimes.com/services/xml/rss/nyt/Technology.xml',
    'NYT World': 'https://rss.nytimes.com/services/xml/rss/nyt/World.xml',
    'Guardian Politics': 'https://www.theguardian.com/politics/rss',
    'Guardian Business': 'https://www.theguardian.com/business/rss',
    'Guardian Technology': 'https://www.theguardian.com/technology/rss',
    'Guardian World': 'https://www.theguardian.com/world/rss',
    'Politico EU': 'https://www.politico.eu/feed/',
    'Politico Pro': 'https://www.politico.com/rss/politicopicks.xml',
    'HuffPost Politics': 'https://www.huffpost.com/section/politics/feed',
    'HuffPost Business': 'https://www.huffpost.com/section/business/feed',
    'Vox Politics': 'https://www.vox.com/rss/politics/index.xml',
    'Vox World': 'https://www.vox.com/rss/world/index.xml',
    'Slate Politics': 'https://slate.com/feeds/politics.rss',
    'Slate Business': 'https://slate.com/feeds/business.rss',
    'Mother Jones Politics': 'https://www.motherjones.com/politics/feed/',
    'Rolling Stone Politics': 'https://www.rollingstone.com/politics/feed/',
    'Time Politics': 'https://time.com/politics/feed/',
    'Time Business': 'https://time.com/business/feed/',
    'Time World': 'https://time.com/world/feed/',
    'Newsweek Politics': 'https://www.newsweek.com/politics/rss',
    'Newsweek Business': 'https://www.newsweek.com/business/rss',
    'Forbes Business': 'https://www.forbes.com/business/feed/',
    'Fortune Business': 'https://fortune.com/feed/',
    'Bloomberg Politics': 'https://feeds.bloomberg.com/politics/news.rss',
    'Bloomberg Technology': 'https://feeds.bloomberg.com/technology/news.rss',
    'WSJ Politics': 'https://feeds.a.dj.com/rss/RSSPolitics.xml',
    'WSJ Business': 'https://feeds.a.dj.com/rss/RSSMarketsMain.xml',
    'WSJ Technology': 'https://feeds.a.dj.com/rss/RSSWSJD.xml',
    'Financial Times Politics': 'https://www.ft.com/politics?format=rss',
    'Financial Times Business': 'https://www.ft.com/business?format=rss',
    'Economist Politics': 'https://www.economist.com/politics/rss.xml',
    'Economist Business': 'https://www.economist.com/business/rss.xml',
    'Economist World': 'https://www.economist.com/world/rss.xml',
    'NPR Politics': 'https://feeds.npr.org/1014/rss.xml',
    'NPR Business': 'https://feeds.npr.org/1007/rss.xml',
    'NPR World': 'https://feeds.npr.org/1004/rss.xml',
    'USA Today Politics': 'https://rssfeeds.usatoday.com/UsatodaycomPolitics-TopStories',
    'USA Today Business': 'https://rssfeeds.usatoday.com/UsatodaycomMoney-TopStories',
    'USA Today World': 'https://rssfeeds.usatoday.com/UsatodaycomWorld-TopStories',
    'ABC Politics': 'https://abcnews.go.com/abcnews/politicsheadlines',
    'ABC Business': 'https://abcnews.go.com/abcnews/moneyheadlines',
    'ABC World': 'https://abcnews.go.com/abcnews/internationalheadlines',
    'CBS Politics': 'https://www.cbsnews.com/latest/rss/politics',
    'CBS Business': 'https://www.cbsnews.com/latest/rss/business',
    'CBS World': 'https://www.cbsnews.com/latest/rss/world',
    'NBC Politics': 'https://feeds.nbcnews.com/nbcnews/public/politics',
    'NBC Business': 'https://feeds.nbcnews.com/nbcnews/public/business',
    'NBC World': 'https://feeds.nbcnews.com/nbcnews/public/world',
    'MSNBC Politics': 'https://www.msnbc.com/politics/rss',
    'FOX Politics': 'https://feeds.foxnews.com/foxnews/politics',
    'FOX Business': 'https://feeds.foxnews.com/foxnews/business',
    'FOX World': 'https://feeds.foxnews.com/foxnews/world',
    'Daily Mail Politics': 'https://www.dailymail.co.uk/news/politics/index.rss',
    'Daily Mail Business': 'https://www.dailymail.co.uk/money/index.rss',
    'Daily Mail World': 'https://www.dailymail.co.uk/news/worldnews/index.rss',
    'Telegraph Politics': 'https://www.telegraph.co.uk/politics/rss.xml',
    'Telegraph Business': 'https://www.telegraph.co.uk/business/rss.xml',
    'Telegraph World': 'https://www.telegraph.co.uk/world-news/rss.xml',
    'Independent Politics': 'https://www.independent.co.uk/news/uk/politics/rss',
    'Independent Business': 'https://www.independent.co.uk/news/business/rss',
    'Independent World': 'https://www.independent.co.uk/news/world/rss',
}

# -------------------------------
# TIMEOUT SAFETY FEATURES
# -------------------------------
def safe_extract_text_with_timeout(article_url, timeout_seconds=10):
    """Extract text with shorter timeout for faster processing"""
    result = [None]
    error = [None]
    
    def extract_worker():
        try:
            result[0] = extract_text_from_url(article_url)
        except Exception as e:
            error[0] = e
    
    thread = threading.Thread(target=extract_worker)
    thread.daemon = True
    thread.start()
    thread.join(timeout_seconds)
    
    if thread.is_alive():
        print(f"⏰ Timeout after {timeout_seconds}s")
        return None
    elif error[0]:
        print(f"❌ Error: {str(error[0])[:30]}")
        return None
    else:
        return result[0]

def safe_rss_parse_with_timeout(rss_url, timeout_seconds=10):
    """Parse RSS with shorter timeout"""
    result = [None]
    error = [None]
    
    def parse_worker():
        try:
            result[0] = feedparser.parse(rss_url)
        except Exception as e:
            error[0] = e
    
    thread = threading.Thread(target=parse_worker)
    thread.daemon = True
    thread.start()
    thread.join(timeout_seconds)
    
    if thread.is_alive():
        print(f"⏰ RSS timeout after {timeout_seconds}s")
        return None
    elif error[0]:
        print(f"❌ RSS error: {str(error[0])[:30]}")
        return None
    else:
        return result[0]

# -------------------------------
# BIAS CLASSIFICATION
# -------------------------------
def classify_bias(source_name):
    """Classify news source bias"""
    source_lower = source_name.lower()
    
    left_sources = ['huffpost', 'vox', 'msnbc', 'slate', 'mother jones', 'rolling stone', 'guardian', 'independent']
    right_sources = ['fox', 'daily mail', 'telegraph', 'breitbart', 'daily caller', 'national review']
    center_sources = ['bbc', 'reuters', 'ap', 'bloomberg', 'wsj', 'financial times', 'economist', 'npr', 'time', 'newsweek']
    
    for source in left_sources:
        if source in source_lower:
            return 'Left'
    for source in right_sources:
        if source in source_lower:
            return 'Right'
    for source in center_sources:
        if source in source_lower:
            return 'Center'
    
    return 'Unknown'

def classify_article_type(article_text, title, source):
    """Classify article type"""
    if not article_text:
        return 'Unknown'
    
    text_lower = (article_text + ' ' + title).lower()
    
    if any(word in text_lower for word in ['election', 'vote', 'campaign', 'president', 'congress', 'political', 'biden', 'trump']):
        return 'Politics'
    elif any(word in text_lower for word in ['stock', 'market', 'economy', 'business', 'financial', 'money', 'investment']):
        return 'Business'
    elif any(word in text_lower for word in ['technology', 'tech', 'ai', 'software', 'digital', 'internet']):
        return 'Technology'
    elif any(word in text_lower for word in ['international', 'foreign', 'global', 'world', 'china', 'russia', 'ukraine']):
        return 'International'
    elif any(word in text_lower for word in ['health', 'medical', 'covid', 'vaccine', 'hospital']):
        return 'Health'
    elif any(word in text_lower for word in ['climate', 'environment', 'energy', 'pollution']):
        return 'Environment'
    else:
        return 'General'

# -------------------------------
# HIGH VOLUME SCRAPING
# -------------------------------
def scrape_high_volume(max_articles_per_source=50, max_total_articles=5000):
    """High volume scraping with optimized settings"""
    print("🚀 Starting HIGH VOLUME scraping...")
    print(f"🎯 Target: {max_total_articles} articles")
    print("="*70)
    
    # Get all sources including additional ones
    base_feeds = get_common_rss_feeds()
    all_feeds = {**base_feeds, **ADDITIONAL_SOURCES}
    
    print(f"📊 Total sources: {len(all_feeds)}")
    
    results = []
    stats = {
        'sources_processed': 0,
        'sources_failed': 0,
        'total_articles': 0,
        'processing_time': 0,
        'articles_by_type': {},
        'articles_by_bias': {}
    }
    
    start_time = time.time()
    source_count = 0
    
    for source_name, rss_url in all_feeds.items():
        source_count += 1
        source_start_time = time.time()
        
        print(f"\n📰 [{source_count}/{len(all_feeds)}] {source_name}")
        
        # Process with timeout
        source_result = [None]
        source_error = [None]
        
        def source_worker():
            try:
                source_result[0] = scrape_from_rss_fast(rss_url, max_articles_per_source, source_name)
            except Exception as e:
                source_error[0] = e
        
        source_thread = threading.Thread(target=source_worker)
        source_thread.daemon = True
        source_thread.start()
        source_thread.join(120)  # 2 minute timeout for 50 articles
        
        if source_thread.is_alive():
            print(f"⏰ Timeout: {source_name}")
            stats['sources_failed'] += 1
            continue
        elif source_error[0]:
            print(f"❌ Error: {source_name}")
            stats['sources_failed'] += 1
            continue
        else:
            source_articles = source_result[0]
        
        if source_articles:
            for article in source_articles:
                article_type = classify_article_type(article.get('text', ''), article.get('title', ''), source_name)
                bias = classify_bias(source_name)
                
                formatted_article = {
                    'News Article Type': article_type,
                    'Website': source_name,
                    'Article Text': article.get('text', ''),
                    'Article Summary': article.get('summary', ''),
                    'Class': 'News',
                    'Bias': bias
                }
                
                results.append(formatted_article)
                
                stats['articles_by_type'][article_type] = stats['articles_by_type'].get(article_type, 0) + 1
                stats['articles_by_bias'][bias] = stats['articles_by_bias'].get(bias, 0) + 1
            
            stats['sources_processed'] += 1
            stats['total_articles'] += len(source_articles)
            
            print(f"✅ {len(source_articles)} articles in {time.time() - source_start_time:.1f}s")
            print(f"📊 Total so far: {len(results)} articles")
            
        else:
            stats['sources_failed'] += 1
            print(f"❌ No articles: {source_name}")
        
        # Check if we've reached the target
        if len(results) >= max_total_articles:
            print(f"🎯 Target reached: {len(results)} articles")
            break
        
        # Minimal delay for faster processing
        time.sleep(0.2)
    
    stats['processing_time'] = time.time() - start_time
    
    print("\n" + "="*70)
    print("📊 HIGH VOLUME SCRAPING SUMMARY")
    print("="*70)
    print(f"✅ Sources processed: {stats['sources_processed']}")
    print(f"❌ Sources failed: {stats['sources_failed']}")
    print(f"📰 Total articles: {stats['total_articles']}")
    print(f"⏱️  Total time: {stats['processing_time']:.1f} seconds")
    
    if stats['total_articles'] > 0:
        print(f"\n📊 Articles by Type:")
        for article_type, count in sorted(stats['articles_by_type'].items(), key=lambda x: x[1], reverse=True):
            print(f"  • {article_type}: {count}")
        
        print(f"\n📊 Articles by Bias:")
        for bias, count in sorted(stats['articles_by_bias'].items(), key=lambda x: x[1], reverse=True):
            print(f"  • {bias}: {count}")
    
    return results, stats

def scrape_from_rss_fast(rss_url, max_articles=50, source_name="Unknown"):
    """Fast RSS scraping with reduced delays"""
    feed = safe_rss_parse_with_timeout(rss_url, timeout_seconds=10)
    
    if not feed or not feed.entries:
        return []
    
    results = []
    
    for i, entry in enumerate(feed.entries[:max_articles]):
        try:
            title = entry.get('title', 'No Title').strip()
            url = entry.get('link', '').strip()
            
            if not url:
                continue
            
            summary = entry.get('summary', entry.get('description', 'No summary'))
            if summary and len(summary) > 300:  # Shorter summaries
                summary = summary[:300] + "..."
            
            # Extract text with shorter timeout
            text = safe_extract_text_with_timeout(url, timeout_seconds=10)
            
            if not text or len(text.split()) < 30:  # Lower word threshold
                continue
            
            results.append({
                'title': title,
                'url': url,
                'summary': summary,
                'text': text,
                'source': source_name
            })
            
            # Minimal delay for speed
            time.sleep(0.1)
            
        except Exception as e:
            continue
    
    return results

def save_results(data, filename='high_volume_news.csv'):
    """Save results"""
    if not data:
        print("❌ No data to save")
        return None
    
    try:
        df = pd.DataFrame(data)
        
        for col in ['Article Text', 'Article Summary']:
            if col in df.columns:
                df[col] = df[col].astype(str).str.replace('\n', ' ').str.replace('\r', ' ')
        
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        
        print(f"✅ Saved {len(data)} articles to {filename}")
        return filename
        
    except Exception as e:
        print(f"❌ Error saving CSV: {str(e)}")
        return None

# -------------------------------
# MAIN EXECUTION
# -------------------------------
if __name__ == "__main__":
    print("="*70)
    print("🚀 ULTRA HIGH VOLUME NEWS SCRAPER")
    print("Optimized for 5000+ articles")
    print("="*70)
    
    # High volume settings
    max_per_source = 50
    max_total = 5000
    
    print(f"📊 Settings:")
    print(f"  • Max per source: {max_per_source}")
    print(f"  • Target total: {max_total}")
    print(f"  • Timeouts: 10s for articles, 45s per source")
    print("="*70)
    
    articles, stats = scrape_high_volume(max_per_source, max_total)
    
    if articles:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f'high_volume_news_{timestamp}.csv'
        save_results(articles, filename)
        
        print(f"\n🎉 SUCCESS!")
        print(f"📁 File: {filename}")
        print(f"📊 Articles: {len(articles)}")
        print(f"⏱️  Time: {stats['processing_time']:.1f}s")
        
    else:
        print("❌ No articles processed")

    print("\n" + "="*70)
    print("🎉 High volume scraping completed!")
    print("="*70)
