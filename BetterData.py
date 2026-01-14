import pandas as pd
import re
from datetime import datetime
import os
import glob

def split_into_sentences(text):
    if not text or pd.isna(text):
        return []
    text = str(text).strip()
    if not text:
        return []

    text = re.sub(r'\s+', ' ', text)
    
    sentence_endings = r'(?<=[.!?])\s+(?=[A-Z])'
    sentences = re.split(sentence_endings, text)
    
    if len(sentences) == 1:
        sentences = re.split(r'([.!?]+)\s+', text)
        combined = []
        for i in range(0, len(sentences) - 1, 2):
            if i + 1 < len(sentences):
                combined.append(sentences[i] + sentences[i + 1])
        if combined:
            sentences = combined
    
    cleaned_sentences = []
    for sentence in sentences:
        sentence = sentence.strip()
        if len(sentence) > 10:
            if not sentence[-1] in '.!?':
                sentence += '.'
            cleaned_sentences.append(sentence)
    
    return cleaned_sentences


def find_latest_high_volume_database():
    csv_files = glob.glob('high_volume_news_*.csv')
    if not csv_files:
        raise FileNotFoundError("No high volume database CSV files found")
    
    latest_file = max(csv_files, key=os.path.getmtime)
    return latest_file

def process_articles(csv_path=None, max_articles=None):
    """Split articles into sentences and save to CSV"""
    
    if csv_path is None:
        csv_path = find_latest_high_volume_database()
    
    print(f"Loading database: {csv_path}")
    df = pd.read_csv(csv_path)
    
    print(f"Found {len(df)} articles")
    
    if max_articles:
        df = df.head(max_articles)
        print(f"Processing first {len(df)} articles")
    
    all_sentences = []
    for article_idx, (idx, row) in enumerate(df.iterrows(), 1):
        article_type = row.get('News Article Type', 'Unknown')
        website = row.get('Website', 'Unknown')
        article_text = row.get('Article Text', '')
        
        sentences = split_into_sentences(article_text)
        for sentence_idx, sentence in enumerate(sentences, 1):
            all_sentences.append({
                'article_id': article_idx,
                'article_type': article_type,
                'website': website,
                'sentence_index': sentence_idx,
                'sentence': sentence
            })
    
    total_sentences = len(all_sentences)
    print(f"Total sentences: {total_sentences}")
    print("="*70)
    
    # Create DataFrame and save to CSV
    sentences_df = pd.DataFrame(all_sentences)
    output_file = f'sentences_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
    sentences_df.to_csv(output_file, index=False, encoding='utf-8-sig')
    
    print("\n" + "="*70)
    print(f"Processing complete!")
    print(f"Processed {total_sentences} sentences from {len(df)} articles")
    print(f"CSV saved: {output_file}")
    print("="*70)
    
    return output_file

if __name__ == "__main__":
    print("="*70)
    print("BetterData: Article to Sentences Converter")
    print("="*70)
    
    csv_path = process_articles(max_articles=None)
    
    print(f"\nAll done! CSV saved to: {csv_path}")

