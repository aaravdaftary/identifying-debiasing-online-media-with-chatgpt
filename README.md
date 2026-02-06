---
# Evaluating and Debiasing Media Bias with Large Language Models

## Overview
This project investigates the effectiveness of large language models (LLMs), specifically ChatGPT, as both evaluators of media bias and as automated assistants for debiasing news content. We introduce a three-phase pipeline—**Identify, Rewrite, Evaluate**—to assess whether model-generated rewrites reduce perceived bias while preserving journalistic quality.

The study analyzes a stratified dataset of news articles drawn from outlets across the political spectrum, selected using **Ad Fontes Media bias ratings**. We propose a novel scoring framework to quantify bias across three dimensions: **Framing**, **Emotional language**, and **Divisive (Us-vs-Them) language**, collectively referred to as **F.E.D. scores**.

## Methodology
1. **Identify**  
   High-bias sentences are identified using an automated evaluation framework that scores text along the three F.E.D. dimensions.

2. **Rewrite**  
   Sentences exceeding a predefined bias threshold (F.E.D. ≥ 6) are rewritten by ChatGPT with the goal of reducing bias while preserving semantic content.

3. **Evaluate**  
   Rewritten sentences are evaluated using:
   - Quantitative model-based bias scoring
   - Validation against established media bias ratings
   - Inter-model consistency checks
   - Human evaluation of perceived bias and engagement

## Key Findings
- Automated rewrites resulted in substantial reductions in perceived bias:
  - **Framing:** 79% reduction  
  - **Emotional language:** 69% reduction  
  - **Divisive language:** 78% reduction
- Human evaluation confirmed that rewritten texts were perceived as less biased (356 selections favoring original texts as more biased vs. 90 for rewrites).
- A trade-off was observed: participants preferred the original, more biased content for engagement (90 selections) over debiased rewrites (48).

## Conclusions
The results suggest that while LLMs are effective at identifying and reducing linguistic bias, naive automated debiasing can diminish reader engagement by removing stylistic and rhetorical elements central to journalism. We argue that the optimal role of LLMs is **assistive rather than autonomous**—flagging potentially biased content and offering suggestions for human editors, rather than performing end-to-end debiasing.

## Repository Contents
- `paper/` – Research paper (PDF / LaTeX)
- `code/` – Scripts used for bias scoring, rewriting, and evaluation
- `data/` – Dataset metadata and access instructions
- `figures/` – Figures and visualizations used in the paper

## Reproducibility
Where possible, datasets and intermediate outputs are referenced or linked in accordance with licensing and platform constraints. Full experimental details are provided in the paper.

## Dataset
The dataset used in this study consists of approximately 100,000 news text samples drawn from outlets across the political spectrum.  
Due to size considerations, the full dataset is hosted on Hugging Face:

- **Dataset:** [https://huggingface.co/datasets/AaravDaftary/identifying-debiasing-online-media-with-chatgpt](https://huggingface.co/datasets/AaravDaftary/identifying-debiasing-online-media-with-chatgpt)

The repository contains metadata and scripts required to reproduce the preprocessing and analysis described in the paper.

## Status
This repository accompanies a research preprint. The work is under active review and may be updated in future versions.
---
