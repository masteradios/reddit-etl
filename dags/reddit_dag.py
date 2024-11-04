from datetime import datetime, timedelta
from airflow.operators.dummy import DummyOperator
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import io
default_args = {
    'owner': 'masteradios',
    'retry': 2,
    'retry_delay': timedelta(minutes=5)
}

def get_reddit_data(**kwargs):
    import praw
    from datetime import datetime
    import pandas as pd

    # Initialize the Reddit instance with your credentials
    reddit = praw.Reddit(
        client_id='acc',
        client_secret='secc',
        user_agent='learn_airflow/1.0 by /u/master_adios (contact: reachadikush@gmail.com)'
    )
    # Choose the subreddit you want to get posts from
    subreddit_names=['bollywoodmemes','Cricket','unitedstatesofindia','ipl']
    dfs = []
    # Loop through each subreddit name
    for subreddit_name in subreddit_names:
        subreddit = reddit.subreddit(subreddit_name)
        
        # Fetch the top 10 posts from the subreddit and convert to a list
        top_posts = list(subreddit.top(limit=10))
        
        # Create a list of dictionaries for each post's attributes
        posts_data = []
        
        for post in top_posts:
            post_info = {
                'title': post.title,
                'url': post.url,
                'id': post.id,
                'author': str(post.author),
                'score': post.score,
                'upvote_ratio': post.upvote_ratio,
                'subreddit': str(post.subreddit),
                'selftext': post.selftext,
                'created': datetime.fromtimestamp(post.created_utc).strftime('%Y-%m-%d %H:%M:%S'),
                'num_comments': post.num_comments,
                'flair': post.link_flair_text,
                'thumbnail': post.thumbnail,
                'nsfw': post.over_18,
                'stickied': post.stickied,
                'permalink': post.permalink,
                'distinguished': post.distinguished
            }
            posts_data.append(post_info)
        
        # Create a DataFrame from the list of dictionaries
        subreddit_df = pd.DataFrame(posts_data)
        
        # Append the subreddit DataFrame to the list of DataFrames
        dfs.append(subreddit_df)

    # Concatenate all DataFrames in the list
    all_posts_df = pd.concat(dfs, ignore_index=True)
    kwargs['ti'].xcom_push(key='dataframe', value=all_posts_df)
    # all_posts_df.to_csv('s3://learn-airflow-reddit/reddit_data/reddit.csv',index=False)




def upload_to_s3(**kwargs):
    ti = kwargs['ti']
    all_posts = ti.xcom_pull(key='dataframe', task_ids='get_reddit_data')
    csv_buffer = io.StringIO()
    all_posts.to_csv(csv_buffer, index=False)
    s3_hook = S3Hook(aws_conn_id='s3_conn')
    s3_bucket='learn-airflow-reddit'
    s3_key = 'reddit_data/data.csv'
    s3_hook.load_string(
        string_data=csv_buffer.getvalue(),
        key=s3_key,
        bucket_name=s3_bucket,
        replace=True
    )
    print(f"DataFrame saved to s3://{s3_bucket}/{s3_key}")





with DAG(
    default_args=default_args,
    dag_id="sample_dag_v20",
    start_date=datetime(2024, 5, 22),
    schedule_interval='@daily'
) as dag:
    start_dag=DummyOperator(
        task_id='start-pipeline'
    )
    get_reddit = PythonOperator(
        task_id='get_reddit_data',
        python_callable=get_reddit_data,
        provide_context=True,
    )
    upload_data_to_s3=PythonOperator(
        task_id='upload-to-s3',
        python_callable=upload_to_s3,
        provide_context=True,
    )

    create_table=PostgresOperator(
        
        task_id='create_table_in_postgres',
        postgres_conn_id='postgres_conn',
        sql='''
        CREATE TABLE reddit_posts (
   
    title TEXT NOT NULL,
    url TEXT,
    id TEXT ,
    author VARCHAR(255),
    score INTEGER,
    upvote_ratio DECIMAL(5, 2),
    subreddit VARCHAR(255),
    selftext TEXT,
    created TIMESTAMP,
    num_comments INTEGER,
    flair TEXT,
    thumbnail TEXT,
    nsfw BOOLEAN,
    stickied BOOLEAN,
    permalink TEXT,
    distinguished VARCHAR(255)
);
        '''
    )


    create_extension_task = PostgresOperator(
        task_id='create_extension_task_for_postgres',
        postgres_conn_id='postgres_conn',
        sql='CREATE EXTENSION IF NOT EXISTS aws_s3 CASCADE;'
    )

    upload_to_postgres=PostgresOperator(
        task_id='load_from_csv_to_postgres_table',
        postgres_conn_id='postgres_conn',

    # SQL command to import CSV from S3
    sql = '''
            SELECT aws_s3.table_import_from_s3(
            'reddit_posts', '', '(format csv,header true)',
            aws_commons.create_s3_uri(
                'learn-airflow-reddit',
                'reddit_data/data.csv',
                'us-east-1'
            ),
            aws_commons.create_aws_credentials('access', 'secretaccess', '')
        );
    '''
    )

    start_dag>>get_reddit>>upload_data_to_s3>>create_table>>create_extension_task>>upload_to_postgres
    