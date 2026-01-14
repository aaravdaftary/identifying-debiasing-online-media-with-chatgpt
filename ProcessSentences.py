import pandas as pd
import re
import json
from datetime import datetime
from google import genai
import time

GEMINI_API_KEY = "AIzaSyAO33ADc6PKE0dVo-8x_bD26QTjw1jTYQQ"
client = genai.Client(api_key=GEMINI_API_KEY)

def process_batch_with_gemini(sentences_batch, batch_num, total_batches):
    """Process a batch of sentences: filter, score, rewrite, and score rewritten - all in one call"""
    
    sentences_text = "\n".join([f"{idx+1}. {s['sentence']}" for idx, s in enumerate(sentences_batch)])
    
    prompt = f"""Process {len(sentences_batch)} news article sentences. For EACH sentence:

STEP 1: FILTER - Remove if:
- Requires context (e.g., "He said", "The company added", pronouns without clear referents)
- Not article content (navigation, ads, metadata, author names, dates)

STEP 2: If NOT filtered, SCORE (1-10, 10=highest):
1. Emotional: Negative emotions (anger/fear) that counter objective reality
2. Framing: Influencing opinions via presentation rather than facts
3. Divisive: Us-vs-Them tribalistic language reducing empathy

STEP 3: If NOT filtered, REWRITE to minimize all three scores while keeping facts

STEP 4: If NOT filtered, SCORE the rewritten version

SENTENCES:
{sentences_text}

Output ONLY valid JSON in this exact format:
{{
  "results": [
    {{
      "sentence_number": 1,
      "filtered": true,
      "filter_reason": "requires context",
      "original_scores": {{"emotional": 0, "framing": 0, "divisive": 0}},
      "rewritten": "",
      "rewritten_scores": {{"emotional": 0, "framing": 0, "divisive": 0}}
    }},
    {{
      "sentence_number": 2,
      "filtered": false,
      "filter_reason": "",
      "original_scores": {{"emotional": 5, "framing": 3, "divisive": 2}},
      "rewritten": "Neutral rewritten version here",
      "rewritten_scores": {{"emotional": 1, "framing": 1, "divisive": 0}}
    }}
  ]
}}

CRITICAL: Return valid JSON for all {len(sentences_batch)} sentences. No markdown, no code blocks."""

    try:
        print(f"   Batch {batch_num}/{total_batches}: Processing {len(sentences_batch)} sentences...")
        response = client.models.generate_content(
            model="gemini-2.5-flash",
            contents=prompt
        )
        
        result_text = response.text.strip()
        
        # Try to extract JSON from the response
        json_match = re.search(r'\{.*\}', result_text, re.DOTALL)
        if json_match:
            result_text = json_match.group(0)
        
        # Clean up common JSON issues
        result_text = result_text.replace('```json', '').replace('```', '').strip()
        
        try:
            result_data = json.loads(result_text)
            return result_data.get('results', [])
        except json.JSONDecodeError as e:
            print(f"   Error parsing JSON: {e}")
            print(f"   Response preview: {result_text[:500]}")
            # Return default structure if JSON parsing fails
            return [{
                "sentence_number": i+1,
                "filtered": False,
                "filter_reason": "",
                "original_scores": {"emotional": 0, "framing": 0, "divisive": 0},
                "rewritten": sentences_batch[i]['sentence'],
                "rewritten_scores": {"emotional": 0, "framing": 0, "divisive": 0}
            } for i in range(len(sentences_batch))]
    
    except Exception as e:
        print(f"   Error processing batch: {str(e)}")
        # Return default structure on error
        return [{
            "sentence_number": i+1,
            "filtered": False,
            "filter_reason": f"Error: {str(e)}",
            "original_scores": {"emotional": 0, "framing": 0, "divisive": 0},
            "rewritten": sentences_batch[i]['sentence'],
            "rewritten_scores": {"emotional": 0, "framing": 0, "divisive": 0}
        } for i in range(len(sentences_batch))]

def process_sentences_csv(csv_path, max_api_calls=20):
    """Process sentences CSV through Gemini API in limited calls"""
    
    print(f"Loading sentences from: {csv_path}")
    df = pd.read_csv(csv_path)
    print(f"Total sentences: {len(df)}")
    
    # Calculate batch size to process all sentences in max_api_calls
    # Start with dividing evenly, but cap at reasonable limit for token constraints
    sentences_per_batch = (len(df) + max_api_calls - 1) // max_api_calls
    
    # Cap at 4000 sentences per batch to avoid token limits
    # If sentences are long, this might need adjustment
    sentences_per_batch = min(sentences_per_batch, 4000)
    
    # Calculate actual number of batches needed
    actual_batches = (len(df) + sentences_per_batch - 1) // sentences_per_batch
    actual_batches = min(actual_batches, max_api_calls)
    
    print(f"Processing in up to {actual_batches} API calls (max {max_api_calls})")
    print(f"Batch size: {sentences_per_batch} sentences per batch")
    print(f"Will process approximately {actual_batches * sentences_per_batch} sentences")
    print("="*70)
    
    all_results = []
    api_call_count = 0
    
    for i in range(0, len(df), sentences_per_batch):
        if api_call_count >= max_api_calls:
            print(f"\nReached maximum of {max_api_calls} API calls. Stopping.")
            break
        
        batch_df = df.iloc[i:i+sentences_per_batch]
        sentences_batch = []
        
        for _, row in batch_df.iterrows():
            sentences_batch.append({
                'article_id': row['article_id'],
                'article_type': row['article_type'],
                'website': row['website'],
                'sentence_index': row['sentence_index'],
                'sentence': row['sentence']
            })
        
        api_call_count += 1
        batch_results = process_batch_with_gemini(sentences_batch, api_call_count, actual_batches)
        
        # Merge results with original data
        for j, result in enumerate(batch_results):
            if j < len(sentences_batch):
                sentence_data = sentences_batch[j]
                all_results.append({
                    'article_id': sentence_data['article_id'],
                    'article_type': sentence_data['article_type'],
                    'website': sentence_data['website'],
                    'sentence_index': sentence_data['sentence_index'],
                    'original_sentence': sentence_data['sentence'],
                    'filtered': result.get('filtered', False),
                    'filter_reason': result.get('filter_reason', ''),
                    'emotional_score_original': result.get('original_scores', {}).get('emotional', 0),
                    'framing_score_original': result.get('original_scores', {}).get('framing', 0),
                    'divisive_score_original': result.get('original_scores', {}).get('divisive', 0),
                    'rewritten_sentence': result.get('rewritten', ''),
                    'emotional_score_rewritten': result.get('rewritten_scores', {}).get('emotional', 0),
                    'framing_score_rewritten': result.get('rewritten_scores', {}).get('framing', 0),
                    'divisive_score_rewritten': result.get('rewritten_scores', {}).get('divisive', 0)
                })
        
        # Small delay to avoid rate limiting
        if api_call_count < max_api_calls:
            time.sleep(1)
    
    # Create output DataFrame
    results_df = pd.DataFrame(all_results)
    
    # Save results
    output_file = f'processed_sentences_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
    results_df.to_csv(output_file, index=False, encoding='utf-8-sig')
    
    # Print statistics
    total_processed = len(all_results)
    filtered_count = results_df['filtered'].sum()
    kept_count = total_processed - filtered_count
    
    print("\n" + "="*70)
    print("Processing Complete!")
    print(f"Total sentences processed: {total_processed}")
    print(f"Sentences filtered out: {filtered_count}")
    print(f"Sentences kept and analyzed: {kept_count}")
    print(f"API calls used: {api_call_count}/{max_api_calls}")
    print(f"Results saved to: {output_file}")
    print("="*70)
    
    # Print score statistics for kept sentences
    if kept_count > 0:
        kept_df = results_df[~results_df['filtered']]
        print("\nScore Statistics (for kept sentences):")
        print(f"Original - Emotional: avg={kept_df['emotional_score_original'].mean():.2f}, max={kept_df['emotional_score_original'].max()}")
        print(f"Original - Framing: avg={kept_df['framing_score_original'].mean():.2f}, max={kept_df['framing_score_original'].max()}")
        print(f"Original - Divisive: avg={kept_df['divisive_score_original'].mean():.2f}, max={kept_df['divisive_score_original'].max()}")
        print(f"Rewritten - Emotional: avg={kept_df['emotional_score_rewritten'].mean():.2f}, max={kept_df['emotional_score_rewritten'].max()}")
        print(f"Rewritten - Framing: avg={kept_df['framing_score_rewritten'].mean():.2f}, max={kept_df['framing_score_rewritten'].max()}")
        print(f"Rewritten - Divisive: avg={kept_df['divisive_score_rewritten'].mean():.2f}, max={kept_df['divisive_score_rewritten'].max()}")
    
    return output_file

if __name__ == "__main__":
    print("="*70)
    print("Sentence Processing with Gemini API")
    print("="*70)
    
    csv_path = 'sentences_20260104_100127.csv'
    output_file = process_sentences_csv(csv_path, max_api_calls=20)
    
    print(f"\nAll done! Results saved to: {output_file}")

