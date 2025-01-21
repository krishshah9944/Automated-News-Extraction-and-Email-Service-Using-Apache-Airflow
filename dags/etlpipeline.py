from airflow.decorators import dag, task
from airflow.providers.smtp.hooks.smtp import SmtpHook
import requests
from datetime import datetime, timedelta
from airflow.models import Variable

# Define your GNews API key here

GNEWS_API_KEY = Variable.get("GNEWS_API_KEY")
# DAG configuration
@dag(
    schedule_interval=timedelta(hours=6),  # Run the pipeline every 6 hours 
    start_date=datetime(2025, 1, 21),
    catchup=False,
    tags=["news_etl"],
)
def news_etl_pipeline():

    # Task 1: Extract news from GNews API
    @task
    def extract_news():
        # GNews API endpoint
        url = f'https://gnews.io/api/v4/top-headlines?token={GNEWS_API_KEY}&lang=en'

        
        response = requests.get(url)
        news_data = response.json()

        
        news_summary = []
        for article in news_data['articles']:
            news_summary.append(f"Title: {article['title']}\nLink: {article['url']}\n")

        return news_summary

    # Task 2: Transform news data (optional formatting)
    # @task
    # def transform_news(news_data):
    #     # Here, you could transform or format the news data as required
    #     transformed_data = "\n\n".join(news_data)
    #     return transformed_data
    # Transform the raw news data into HTML format
    @task
    def transform_news(news_data):
        transformed_data = []

        for news in news_data:
            
            if isinstance(news, dict):
                title = news.get('title', 'No title')
                link = news.get('url', '#')

            
            else:
                
                try:
                    parts = news.split(' - ')
                    title = parts[0] if len(parts) > 0 else 'No title'
                    link = parts[1] if len(parts) > 1 else '#'
                except Exception as e:
                    title = 'No title'
                    link = '#'

            
            news_item = f"""
            <div class="news-item">
                <p>{title}</p>
                <p><a href="{link}"></a></p>
            </div>
            """
            transformed_data.append(news_item)

        return "\n".join(transformed_data)



    # Task 3: Load and send the news via email
    @task
    def send_email(news_content):
       
        smtp_hook = SmtpHook(smtp_conn_id='smtp_default')  # Uses the SMTP connection defined in Airflow

        smtp_hook.get_conn()

        
        subject = "Latest News Updates"
        
        body = f"""
                <html>
                <head>
                    <style>
                        body {{
                            font-family: Arial, sans-serif;
                            line-height: 1.6;
                            background-color: #f4f4f4;
                            padding: 20px;
                        }}
                        h2 {{
                            color: #333;
                        }}
                        p {{
                            color: #555;
                        }}
                        .content {{
                            background-color: #fff;
                            padding: 20px;
                            border-radius: 8px;
                            box-shadow: 0 0 10px rgba(0, 0, 0, 0.1);
                            margin-top: 20px;
                        }}
                        .news-item {{
                            margin-bottom: 20px;
                            border-bottom: 1px solid #ddd;
                            padding-bottom: 15px;
                        }}
                        .news-item:last-child {{
                            border-bottom: none;
                        }}
                        .news-item a {{
                            font-weight: bold;
                            color: #007bff;
                            text-decoration: none;
                        }}
                        .news-item a:hover {{
                            text-decoration: underline;
                        }}
                    </style>
                </head>
                <body>
                    <h2>Latest News Updates</h2>
                    <p>Dear User,</p>
                    <p>Here are the latest news updates:</p>
                    <div class="content">
                        {news_content}
                    </div>
                    <p>Best regards,<br>Your News Service</p>
                </body>
                </html>
                """

        # Send the email using Gmail SMTP
        smtp_hook.send_email_smtp(
            to='krishshah6090@gmail.com',  
            subject=subject,
            html_content=body,
            dryrun=False
        )

    # Set up the task dependencies
    news_data = extract_news() 
    transformed_data = transform_news(news_data) 
    send_email(transformed_data)  

# Instantiate the DAG
news_etl_pipeline = news_etl_pipeline()
