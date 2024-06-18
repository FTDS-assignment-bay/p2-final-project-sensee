import datetime as dt
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup

import pandas as pd
import psycopg2 as db
from elasticsearch import Elasticsearch
import re
import nltk
import string
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

from nltk.corpus import stopwords

from Sastrawi.Stemmer.StemmerFactory import StemmerFactory
nltk.download('stopwords')
nltk.download('punkt')
from tensorflow.keras.layers import TextVectorization
from tensorflow.keras.layers import Embedding
from tensorflow.keras.callbacks import EarlyStopping

import tensorflow as tf

from nltk.tokenize import word_tokenize
from sklearn.model_selection import train_test_split
from sklearn.feature_extraction.text import CountVectorizer

from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, LSTM, Bidirectional, GRU, Dropout, Reshape
from tensorflow.keras.utils import to_categorical
from tensorflow.keras.callbacks import ModelCheckpoint
from tensorflow.keras.metrics import Precision

from sklearn.metrics import classification_report, precision_score
  


def queryPostgresql():
    conn_string="dbname='airflow' host='postgres' user='airflow' password='airflow'"
    conn=db.connect(conn_string)
    df=pd.read_sql("select * from final_project",conn)
    df.to_csv('final_project.csv', index=False)
    print("-------Data Saved------")
 
  
def cleanScooter():
    df=pd.read_csv('final_project.csv')
    # Handling missing value and duplicates
    df.dropna(inplace=True)
    df.drop_duplicates(subset='full_text', keep="first")
    # Text Preprocessing
    stpwds_id = list(set(stopwords.words('indonesian')))
    stpwds_id = stpwds_id + ['ygy', 'please', 'blm', 'and', 'ml', 'hanya', 'lah', '(...)', 'u', 'aja', 'kl', 'gw', 'yaaa', 'uns', 'lgsg', 'rep', 'yuk', 'tau', 'ajaa', 'garn&amp;&amp;&amp;',
                  '(sisaan)', 'gua', 'yaallah', 'guaaa', 'rip', 'my', '200k', 'bgt', 'ss', 'n', 'kak', 'bbrp', 'cod', 'tanya', '&amp;', 'ttp', 'tp', 'kl', 'krn', 'sblm', 'sih',
                  'bbjirr', 'kuy', 'laa', 'tpi', 'hadalabo', 'pyunkang', 'yul', 'azarine', 'cosrx', 'sunscreen', 'gokujyun', 'brand', 'paragon', 'pro', 'palestine', 'wardah', 'emina',
                  'kahf', 'skintific', 'maybelline', 'vinyl', 'ink', 'vaseline', 'gluta', 'hya', 'somethinc', 'loreal', 'physical', 'skin1004', 'madagascar', 'sm', 'org', 'loreal',
                  'paris', 'defender', 'invisible', 'fluid', 'serum', 'skincare', '30ml', 'toner', 'essence', 'snail', 'micellar', 'water', 'originate', 'lorealparisid', 'reply',
                  'checkout', 'makeover', 'powerstay', 'foundation', 'c31', 'glossy', 'stain', 'sariayu', 'oil', 'revitalift', 'ha', 'panorama', 'lipmatte', 'wkwk', 'rm75++', 'niacinamide',
                  'sea', 'makeup', 'foundie', 'cushion', 'copypaste', 'nina', 'centella', 'poremizing', 'glycolic', 'bright', 'cream', 'hmps', 'whitelab', 'anessa', 'moisturizer', 'lanbena',
                  'marine', 'collagen', 'hyacross', 'cerave', 'retinol', 'el formula', 'pink', 'npure', 'skinaqua', 'karena', 'biore', 'implora', 'elformula', 'powder', 'esqa', 'lip',
                  'dettol', 'red', 'jelly', 'exfoliating', 'ungu', 'scrub', 'hyaluron', 'peptides', 'sunbrella', 'isriwil', 'fot', 'zonaba', 'zonabu', 'zonajajan', 'fss', 'skinitip',
                  'skintype', 'hydrium', 'layered', 'illiyoon', 'ceramide', 'dexskin', 'derna', 'express', '3w', 'cli0nic', 'somebymi', 'senka', 'eye', 'citra', 'pepsodent', 'lipstick',
                  'handbody', 'pata', 'usruk2', 'tjoyy', 'softymo', 'cetaphil', 'phisical', 'my', 'salicylic', 'acid', 'congested', 'invisible', 'fluid', 'kecit', 'yutup', 'tt', 'sunblock',
                  'hybrid', 'spf', 'yagesya', 'maba', 'mahasiswi', 'indomar*t', '100rbun', 'fw', 'eyeshadow', 'liquid', 'eneles', 'discontinue', 'pixy', 'yuja', 'niacin', 'witch', 'hazel',
                  'rojukiss', 'gold', 'cyclopentasiloxane', 'holyshield', 'corrector', 'ingridientnya', 'copy', 'paste', 'tinted', 'co1', 'perle', 'available', 'carousell', '(cont)', 'c02',
                  'serene', 'zonauang', 'butter', 'w01', 'w03', 'pons', 'himalaya', 'facetology', 'nivea', 'avoskin', 'airnderm', 'animate', 'corxs', 'sebamed', 'buttered', 'rose', 'madame',
                  'gie', 'natrep', 'garnier', 'hale', 'comfort', 'zone', 'tribiotic', 'elsheskim', 'phantenol/smtc', 'calmdown', 'nderr', 'shinzui', 'marina', 'dayang2mu', 'princess',
                  'prajurit', 'care&amp;protect', 'larang', 'wae', 'sing', 'pokoke', 'hv', 'paling2', 'scarlett', 'klinik', 'receptionist', 'xiu', 'xiu(?)', 'eco', 'tan', 'safi', 'dermasafe',
                  'skinker', 'cica', 'waterfit', 'manyo', 'toba', 'metal', 'fortis', 'hanasui', 'dermies', 'alba', 'laneige', 'dermatologist', 'koji', 'speed', 'cappadocia', '⬇️', 'ertos',
                  'dirgie', 'originot', 'amaterasun', 'jf', 'sulfur', '⭐', '🆕', 'belia', 'carasun', 'bpom', '8ml', '70ml', 'kita', 'loose', 'matte', 'yellow', 'btw', 'beat', 'the', 'sun',
                  'with', '3-in-1', 'bring', 'green', 'acnes', 'scora', 'mask', 'aha', 'bha', 'ultra', 'aloe', 'vera', '4d', 'msh', 'selebgram', 'fahiram', 'miraa', 'tiktokers', 'sachet',
                  'blue', 'light', 'buddies!', 'tts', 'ktnya', '4tahunan', 'spray', 'hi', 'beudd', '#carasunbuddies', 'e*em', 'elshe', '⤵️', '&gt;', '[kotak', 'hadiah]', 'mr. diy', 'labore',
                  'diskon', '6.6', 's/d', '60%', 'anyg', 'watson(?)', 'bjrotlah', 'spray', 'bnb', 'rebiu', 'tak', 'berbayar:', 'vs', 'y.o.u', 'cloud', 'touch', 'loreal', '5.5', 'golden',
                  'sand', 'routine:', 'jinot', 'skintific/kleveru', 'wkakkwkwkaakwka', 'mist', 'rp77.000', 'skunk', '[nicholas', 'saputra', 'choice]', 'spf50', 'pa++++', 'zarzou', 'fuji',
                  'sandra', 'dewi', 'nahan', 'geby', '2minggu', 'pipiqiu', 'cinamoroll', 'dekahnya', 'pure', 'paw', 'shopee', 'lazada', 'fdx', 'jastip', 'jxb', 'blibli', 'tele', '(jar', 'ijo)',
                  '⭐⭐', 'nyahh', 'om-om', 'hatewavenya', 'anthelios', 'uvmune', '400', 'oil', 'control', 'fluid', 'spf50+', 'pa++++', '50ml', 'barenbliss', 'r-cover', 'cicaplast', 'baume',
                  'b5', 'b5+', 'oxy 5 browcara', 'yogurt', 'kiehls', 'mcm2', 'tocobo', 'isntree', 'effaclar', 'bhumi', 'skinceuticals', 'menstruasi', 'januari', 'spearmint', 'tea', 'lrp',
                  'payar', 'allie', 'kanebo', 'kose', 'torriden', 'dive', 'purito', 'innisfree', 'gel-cream', 'obgynku', 'gweh', 'dr.', 'barbara', 'sturm', 'dressing', 'estée', 'lauder', 'eta',
                  'sept', 'sleman', 'cmiiw', 'bluemarine', 'ampoule', 'kayman', 'moisturiser', 'ceradan', 'gemfou', 'petersons', 'dokter', 'spkk', 'himalayan', '4jt', 'tretinoin', 'segestam',
                  'mois', 'ss-nya', 'tranexamic', 'boj', 'snail~', 'avoskin!', 'niacin/alpha', 'arbutin', 'ref.toner', 'go-to', 'complexion', 'pad!', 'nct', 'istj', 'cercaode+sheetmask',
                  'gerlam/fib/hergarmanah/ciseke', '2-3x/minggu', 'ordinary', 'rich', 'gokyujun', 'oranye', 'ariul', '7days', 'mediklin', 'tr', 'st', 'kinda', 'vitacid', '0.025%', 'skingame',
                  'azarine/facetology', 'glad2glow', 'rojukis', 'hyalucross', 'neem', 'oxy', 'jennskin', 'elsheskin', 'misha', 'bioacne', 'tompi', 'toner-essence', 'panaaas2nya', 'mursida',
                  'oiya', 'adu-duh', 'gabisa', 'deh', 'plis', 'sama', 'gak', 'doang', 'jadi', 'facial', 'foam', 'tapi', 'masih', 'skrg', 'tu', 'gaboleh', 'smp', 'ben', 'pas', 'eh', 'bjirr',
                  'yang', '#kulitsensitif', '#sunscreenaman', '#zincoxide', '#reefsafe', 'ni', 'yak', 'dowoon', 'mau', 'gt', 'ko', 'ini', 'di', 'ter-ringan', 'lgs', 'mbak', 'cmn', 'sgla', 'iye',
                  'zuzur', 'palg', 'fondi', 'sumpa', 'guefak', 'gase', 'kon', 'supergoop', 'gendistalk', 'chocolatos', 'jajan/debit', '3️⃣', 'diemplok', 'jyujyur', 'co', 'atau', 'jg', 'ga', 'si',
                  'pilih', 'guys', 'dong', 'juga', 'sorry', 'oot', 'ada', 'ya?', 'aku', '1x', '(n01)', 'swatch', 'on', 'by', 'request', 'ini!', 'untuk', 'dari', 'bnr2', 'akan', 'nah', 'ah', 'iya',
                  'itu', 'lain?', 'pad!', 'ciws', 'klo', 'thx', 'uu', 'hai', 'kdg', 'kmrn', 'ᯓ', 'dasdes', "that's", 'why', 'asliii', 'ssnya', '-dips!', '->', 'insyallah', 'u__u', 'dkk', 'jd',
                  '(w)', ':->', 'update', '!!', 'b1g1', 'mascara', 'nextguele', 'bijoux', 'shade', 'naavagreen', 'bondak', 'wtt', 'exfo', 'nder', '^__^', 'dom', 'punten', 'haul', 'foundi', 'loose',
                  'fcdabf', 'loreal', 'ignorant', 'klairs', 'grace&amp;glow', 'wkwi', '-ness', 'stku', 'lrp', 'efwk', 'gasngeng', 'originote', 'hyuuk', 'syopi', 'berbagu', 'beraeti', 'bucim', 'hiks',
                  'bolong2', 'ngelupas', 'pyunkang', 'mwehehe', '87rb', 'bgttt', 'r&amp;d', 'sistur', 'congg', 'ndoro', 'ehe', 'lazada', 'bjirrr', 'w', 'yhh', 'dear', 'me', 'beauty', 'centela',
                  '#shopeeid', 'colorfit~', 'viva~', 'dll.', 'brand', 'paragon', 'amaterasun', 'omg', 'pk', '/beauty/', 'tmi', 'bukin', 'unil*ever', 'oreo', 'nucin', 'moist', 'face', 'wash', 'toner',
                  'serum', 'skin', 'day', 'night', 'essence', 'bedak', 'setting', 'gapyear', 'jog', '⅕', '>>>', 'liptint', 'panthenol', 'oake', 'shdhdgsjgsfshsh', 'compact', 'bcz', '>_<', 'ermnnn',
                  'je', 'luqfa', 'mr', 'lippen', 'nan', 'y.o.u', 'mcmana', 'ntu', "d'alba", 'sksnzksbzksnsz', '-rl', 'imo', 'mintq', 'minboss', 'girlboss', 'harlett*', 'tps', 'axis', 'y', 'nak',
                  '-ns', 't___t', 'cbaki', 'cariki', 'wash&gt;moisturize&gt;sunscreen', 'mm', 'clinic', 'ancrit', 'bet', 'drpd', '3w', 'clinic', 'g2g', 'xyah', 'x', 'st', 'lol!', 'tgk', 'kat',
                  'haihh', 'syok', 'tweeps', 'sbb', 'dab2', 'pastu', 'dorang', 'bla', 'lps', 'pkai', 'mkin', 'akn', 'jer', 'ae', '<3', 'iunik', 'sefil', 'koenji', 'plua', 'fw+moist+sunscreen',
                  'hehe', 'keanya', 'pol', 'originoteee', 'ntuuu', 'wktu', 'xiu', 'xiu', '(?)', 'true', 'to', 'skin', 'besties!', 'ausie', 'masiih', 'niih', 'woolworths', 'yeu', 'wkwkwk', 'mediheal',
                  'forebie', 'azarineee', 'ituu', 'bebb', 'kesayangankuuuu', 'yorobun!!!!', 'i', 'yyyh', 'hvft', 'for', 'watsons', 'jap', 'mmg', 'tbh', 'tekstyrnya', '[help', 'rt]', 'syafa', 'ngl',
                  'em.', 'avoskin']
    stemmer = StemmerFactory().create_stemmer()
    # Create A Function for Text Preprocessing
    def text_preprocessing(text):
    # Case folding
        text = text.lower()
        # Mention removal
        text = re.sub("@[A-Za-z0-9_]+", " ", text)
        # Hashtags removal
        text = re.sub("#[A-Za-z0-9_]+", " ", text)
        # Newline removal (\n)
        text = re.sub(r"\\n", " ",text)
        # Whitespace removal
        text = text.strip()
        # repeate letters removal
        text = re.sub(r'(\w)\1+\b', r'\1', text)
        # URL removal
        text = re.sub(r"http\S+", " ", text)
        text = re.sub(r"www.\S+", " ", text)
        # Non-letter removal (such as emoticon, symbol (like μ, $, 兀), etc
        text = re.sub("[^A-Za-z\s']", " ", text)
        # Tokenization
        tokens = word_tokenize(text)
        # Stopwords removal
        tokens = [word for word in tokens if word not in stpwds_id]
        # Stemming
        tokens = [stemmer.stem(word) for word in tokens]
        # Combining Tokens
        text = ' '.join(tokens)
        return text
    df['text_processed'] = df['full_text'].apply(lambda x: text_preprocessing(x))
    df.to_csv('/opt/airflow/dags/final_project_data_clean.csv', index=False)
 
def insertCleantoPostgres():
    creating_experiment_tracking_table = PostgresOperator(
        task_id="creating_experiment_tracking_table",
        postgres_conn_id='postgres_default',
        sql='sql/create_experiments.sql'
    )



def AffiliateModel():
    df = pd.read_csv('/opt/airflow/dags/P2M3_kumala_data_clean.csv', index=False)
    X_train_val, X_test, y_train_val, y_test = train_test_split(df.text_processed,
                                                            df.Affiliate,
                                                            test_size=0.15,
                                                            random_state=20,
                                                            stratify=df.Affiliate)
    X_train, X_val, y_train, y_val = train_test_split(X_train_val,
                                                    y_train_val,
                                                    test_size=0.10,
                                                    random_state=20,
                                                    stratify=y_train_val)
    # Text Vectorization
    Vectorize = CountVectorizer()
    X_train_vec = Vectorize.fit_transform(X_train)
    X_test_vec = Vectorize.transform(X_test)
    total_vocab = len(Vectorize.vocabulary_.keys())
    max_sen_len = max([len(i.split(" ")) for i in X_train])
    text_vectorization = TextVectorization(max_tokens=total_vocab,
                                       standardize="lower_and_strip_punctuation",
                                       split="whitespace",
                                       ngrams=None,
                                       output_mode="int",
                                       output_sequence_length=max_sen_len,
                                       input_shape=(1,)) # Only use in Sequential API
    text_vectorization.adapt(X_train)
    text_vectorization.set_vocabulary(text_vectorization.get_vocabulary()[295:5311])
    # Word Embedding
    embedding = Embedding(input_dim=total_vocab,
                      output_dim=128,
                      embeddings_initializer="uniform",
                      input_length=max_sen_len)
    ######################################################

# def insertModelAffiliatetoPostgres():
#     with TaskGroup('creating_storage_structures') as creating_storage_structures:

#         # task: 1.1
#         creating_experiment_tracking_table = PostgresOperator(
#             task_id="creating_experiment_tracking_table",
#             postgres_conn_id='postgres_default',
#             sql='sql/create_experiments.sql'
#         )

#         # task: 1.2
#         creating_batch_data_table = PostgresOperator(
#             task_id="creating_batch_data_table",
#             postgres_conn_id='postgres_default',
#             sql='sql/create_batch_data_table.sql'
#         )

def SentimentModel():
    df = pd.read_csv('/opt/airflow/dags/P2M3_kumala_data_clean.csv', index=False)
    df_sentiment = df[(df['Affiliate']==0) & (df['Sentiment'] != 2) & (df['Sentiment'] != 3)]
    X_train_val, X_test, y_train_val, y_test = train_test_split(df_sentiment.text_processed,
                                                            df_sentiment.Sentiment,
                                                            test_size=0.15,
                                                            random_state=20,
                                                            stratify=df_sentiment.Sentiment)
    X_train, X_val, y_train, y_val = train_test_split(X_train_val,
                                                    y_train_val,
                                                    test_size=0.10,
                                                    random_state=20,
                                                    stratify=y_train_val)
    # Text Vectorization
    Vectorize = CountVectorizer()
    X_train_vec = Vectorize.fit_transform(X_train)
    X_test_vec = Vectorize.transform(X_test)
    total_vocab = len(Vectorize.vocabulary_.keys())
    max_sen_len = max([len(i.split(" ")) for i in X_train])
    text_vectorization = TextVectorization(max_tokens=total_vocab,
                                       standardize="lower_and_strip_punctuation",
                                       split="whitespace",
                                       ngrams=None,
                                       output_mode="int",
                                       output_sequence_length=max_sen_len,
                                       input_shape=(1,)) # Only use in Sequential API
    text_vectorization.adapt(X_train)
    text_vectorization.set_vocabulary(text_vectorization.get_vocabulary()[295:5311])
    # Word Embedding
    embedding = Embedding(input_dim=total_vocab,
                      output_dim=128,
                      embeddings_initializer="uniform",
                      input_length=max_sen_len)
    ################################################3

def insertElasticsearch():
    '''
    Fungsi ini dilakukan untuk automisasi memasukkan data ke ElasticSearch
    '''
    es = Elasticsearch("http://elasticsearch:9200/") 
    df=pd.read_csv('/opt/airflow/dags/P2M3_kumala_data_clean.csv')
    for i,r in df.iterrows(): 
        doc=r.to_json()
        res=es.index(index="milestone3",doc_type="doc",body=doc)
        print(res)	 
     
 
default_args = {
    'owner': 'kumala',
    'start_date': dt.datetime(2024, 5, 24)- timedelta(hours=7),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=15)
}
 
with DAG('milestone3', 
         default_args=default_args,
         schedule_interval='30 6 * * *', 
         ) as dag:

    fetchData = PythonOperator(task_id='fetchdata',
                                 python_callable=queryPostgresql)
    
    cleanData = PythonOperator(task_id='cleandata',
                                 python_callable=cleanScooter)
    
    insertData = PythonOperator(task_id='insert_to_elasticsearch',
                                 python_callable=insertElasticsearch)
    

fetchData >> cleanData >> insertData