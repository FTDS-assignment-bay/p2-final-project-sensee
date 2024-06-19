[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-24ddc0f5d75046c5622901739e7c5dd533143b0c8e959d652212380cedb1ea36.svg)](https://classroom.github.com/a/4g4dJXpF)
[![Open in Visual Studio Code](https://classroom.github.com/assets/open-in-vscode-718a45dd9cf7e7f842a935f5ebbe5719a5e09af4491e668f4dbf3b35d5cca122.svg)](https://classroom.github.com/online_ide?assignment_repo_id=15228806&assignment_repo_type=AssignmentRepo)

# SenSee (Sentiment Analysis of Sunscreen)
<p align="center">
    <img src="https://github.com/FTDS-assignment-bay/p2-final-project-sensee/assets/162549243/ecde7944-446c-4419-99d9-1829b89b39a3" alt="Centered Image">
</p>

## 🌞 Project Overview
**SenSee** leverages Natural Language Processing (NLP) to analyze customer sentiment towards sunscreen products from Twitter. The aim is to distinguish between affiliated (e.g., sponsored) and non-affiliated sentiments, providing invaluable insights for consumer analytics and strategic decision-making.

## 🎯 Project Objective
To create a robust NLP model capable of accurately identifying and categorizing customer sentiment towards sunscreen products from Twitter tweets.

## 🛠️ Background and Rationale
Sunscreen products are essential for daily skincare routines due to their protective properties against UV rays. With the rise of social media, consumers continuously share their experiences and opinions online. This project aims to automate sentiment analysis to help companies understand customer sentiment and improve their products and strategies.

## 📋 Scope of the Project
1. **Data Collection**: Scraped 5210 tweets using Node.js tweet harvest technology.
2. **Data Preprocessing**: 
   - Convert all characters to lowercase.
   - Remove mentions, hashtags, spaces, repeated letters, non-letter characters, and common words.
   - Convert words to their root forms and remove words that appear only once.
3. **Model Development**: Used LSTM, GRU, Bidirectional LSTM, and Bidirectional GRU models, and performed tuning.
4. **Model Evaluation**: 
   - Distinguished between advertisement and non-advertisement tweets with 89% precision.
   - Distinguished between positive and negative sentiment tweets with 68% accuracy.
5. **Automation Framework**: Designed and implemented a system to clean data, preprocess it, and analyze sentiment monthly.

## 🗓️ Timeline
| Milestone                  | Estimated Completion |
|----------------------------|----------------------|
| Data Collection            | 3 days               |
| Data Preprocessing         | 1 day                |
| Model Development          | 3 days               |
| Model Evaluation           | 1 day                |
| Automation Framework       | 2 days               |
| Reporting and Visualization| 1 day                |
| **Total Duration**         | 1 week               |

## 👥 Team
- Kumala Cantika Ainun Maya
- Putra Rizqa Yasira

## 🎯 Success Criteria
1. A fully trained and validated NLP model for sentiment analysis.
2. An automated system for monthly sentiment analysis.
3. Monthly sentiment reports with visualizations and insights.


## 📞 Contact
For any questions or inquiries, please contact:
- Kumala Cantika Ainun Maya (kumala.cantika12@gmail.com)
- Putra Rizqa Yasira (ptryasira@gmail.com)

---

**SenSee** - Leveraging NLP to decode the true sentiment of sunscreen product users! 🌞🕶️
