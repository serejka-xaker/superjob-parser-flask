from flask import Flask, request,render_template, redirect, jsonify
import requests
import sqlite3
from datetime import datetime
import pandas
from bs4 import BeautifulSoup
import configparser
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import threading
import os 


def get_secret_key():
    global CLIENT_SECRET,CLIENT_ID

    config = configparser.ConfigParser()
    config.read('Файлы/config.ini')
    CLIENT_SECRET = config.get('Settings', 'secret_key')
    CLIENT_ID = config.get('Settings', 'client_id')


def get_checks():
    duties = []
    requirements = []
    conditions = []
    professions = []
    code_words = []
    try:
        df = pandas.read_excel('Файлы/Опции.xlsx')
        professions_column = df['Профессии'].dropna()
        for profession in professions_column:
            professions.append(str(profession).strip())
        
        code_words_column = df['Кодовые слова'].dropna()
        for code_word in code_words_column:
            code_words.append(str(code_word).strip())


        requirements_column = df['Требования'].dropna()
        for requirement in requirements_column:
            requirements.append(str(requirement).strip())
        
        conditions_column = df['Условия'].dropna()
        for condition in conditions_column:
            conditions.append(str(condition).strip())
        
        duties_column = df['Обязанности'].dropna()
        for duty in duties_column:
            duties.append(str(duty).strip())

    except FileNotFoundError:
        print("Файл Опции.xlsx не найден.")


    keywords = {
        "обязанности": duties,
        "требования": requirements,
        "условия": conditions
    }
    return keywords,professions,code_words


def parse_vacancies(date_from_timestamp,date_to_timestamp,professions,code_words,keywords):
    num = 1
    ids = []
    items = []
    session = requests.Session()
    retry_strategy = Retry(total=5, backoff_factor=1, status_forcelist=[403,404,408,429, 500, 502, 503, 504], allowed_methods=["GET"])
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    access_token, refresh_token, ttl = load_tokens()

    headers = {'X-Api-App-Id':CLIENT_SECRET,
               'Authorization':f'Bearer {access_token}'}

    for profession in professions:
        try:
            params = {
                'keyword': profession, 
                'c':[1],
                'no_agreement': 1,
                'date_published_from':date_from_timestamp,
                'date_published_to':date_to_timestamp
                }
            url = 'https://api.superjob.ru/2.0/vacancies/'
            response = session.get(url,headers=headers,params=params)
            response.raise_for_status()
            # print(response.status_code,response.text)
            if response.status_code == 200:
                if response.json()['total'] != 0:
                    data = response.json()['objects']
                    # print(response.json())
                    for item in data:
                        
                        # print(item,'\n')
                        id = item.get('id','-')

                        is_closed = item.get('is_closed')
                        if is_closed:
                            continue

                        is_archive = item.get('is_archive')
                        if is_archive:
                            continue

                        is_storage = item.get('is_storage')
                        if is_storage:
                            continue

                        name = item.get('contact','-')

                        email = item.get('email','-')

                        phone = item.get('phone','-')

                        link = item.get('link')

                        vacancy_name = item.get('profession')
                        is_true_professional = False
                        for word in code_words:
                            if word in vacancy_name or word in vacancy_name.lower():
                                is_true_professional = True
                        if not is_true_professional:
                            continue

                        salary_min = item.get('payment_from','-')

                        salary_max = item.get('payment_to','-')

                        publish_timestamp = item.get('date_published','-')
                        publish_date = datetime.fromtimestamp(publish_timestamp).date()

                        employer_name = item.get('firm_name')

                        city = item.get('town').get('title')

                        description = item.get('candidat')

                        description_with_html = item.get('vacancyRichText')

                        duties, requirements, conditions = '-', '-', '-'

                        soup = BeautifulSoup(description_with_html, "lxml")

                        bs = soup.find_all('b')
                        duties, requirements, conditions = '-', '-', '-'
                        if bs:
                            for b in bs:
                                b_lower = str(b.text).lower()
                                for key in ['обязанности', 'требования', 'условия']:
                                    for word in keywords[key]:
                                        if word.lower() in b_lower:
                                            ul_tag = b.find_next('ul')
                                            if ul_tag:
                                                if key == 'обязанности':
                                                    duties = ' '.join(li.get_text() for li in ul_tag.find_all('li'))
                                                elif key == 'требования':
                                                    requirements = ' '.join(li.get_text() for li in ul_tag.find_all('li'))
                                                elif key == 'условия':
                                                    conditions = ' '.join(li.get_text() for li in ul_tag.find_all('li'))
                        else:
                            ps = soup.find_all('p')
                            duties, requirements, conditions = '-', '-', '-'
                            if ps:
                                for p in ps:
                                    p_lower = str(p.text).lower()
                                    for key in ['обязанности', 'требования', 'условия']:
                                        for word in keywords[key]:
                                            if word.lower() in p_lower:
                                                ul_tag = p.find_next('ul')
                                                if ul_tag:
                                                    if key == 'обязанности':
                                                        duties = ' '.join(li.get_text() for li in ul_tag.find_all('li'))
                                                    elif key == 'требования':
                                                        requirements = ' '.join(li.get_text() for li in ul_tag.find_all('li'))
                                                    elif key == 'условия':
                                                        conditions = ' '.join(li.get_text() for li in ul_tag.find_all('li'))


                        if duties == requirements or requirements == conditions or duties == conditions:
                            duties, requirements, conditions = '-', '-', '-'

                        if id not in ids:
                            ids.append(id)
                            items.append([link,publish_date,vacancy_name,name,email,phone,salary_min,salary_max,employer_name,city,duties,requirements,conditions,description])
                            print(f'Успешно выполнил {num} итераций')
                            num += 1
            else:
                print(f'Запрос не удался, код ответа от сервера - {response.status_code}')
                print(f'Ошибка - {response.text}')
                continue
        
        except requests.exceptions.ConnectionError as e:
            print(f"Error connecting to the server: {e}")
        except requests.exceptions.HTTPError as e:
            print(f"HTTP error occurred: {e}")
        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")


    return items


def pr_date_to_timestamp(date,type):
    try:
        date_object = datetime.strptime(date, '%Y-%m-%d')
        if type == 'from':
            date_object = date_object.replace(hour=0, minute=0, second=0)
        else:
            date_object = date_object.replace(hour=23, minute=59, second=59)  
        return int(date_object.timestamp())
    except ValueError:
        print(f"Неверный формат даты: {date}. Ожидается ГГГГ-ММ-ДД.")
        return None


def save_to_excel(items):
    # df = pandas.DataFrame(items, columns=['Ссылка', 'Название вакансии','Зарплата от','Зарплата до','ФИО','Email','Номер телефона','Название компании','Город','Описание'])
    df = pandas.DataFrame(items, columns=['Ссылка','Дата публикации','Название вакансии','Имя','Email','Номер телефона','Зарплата от','Зарплата до','Название компании','Город','Обязанности','Требования','Условия','Описание'])
    df.to_excel('Вакансии.xlsx', sheet_name='Вакансии')
    print('Файл успешно сохранен!')




app = Flask(__name__, template_folder=os.path.join(os.getcwd(), 'Файлы'))


REDIRECT_URI = 'http://127.0.0.1:8000/callback'
DB_NAME = 'Файлы/tokens.db'



def init_db():
    with sqlite3.connect(DB_NAME) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tokens (
                id INTEGER PRIMARY KEY,
                access_token TEXT,
                refresh_token TEXT,
                ttl INTEGER
            )
        ''')
        conn.commit()
        # Заполняем начальные значения, если таблица пустая
        cursor.execute('SELECT COUNT(*) FROM tokens')
        if cursor.fetchone()[0] == 0:
            cursor.execute('INSERT INTO tokens (id, access_token, refresh_token, ttl) VALUES (1, NULL, NULL, NULL)')
            conn.commit()

def update_tokens(access_token, refresh_token, ttl):
    with sqlite3.connect(DB_NAME) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE tokens 
            SET access_token = ?, refresh_token = ?, ttl = ?
            WHERE id = 1
        ''', (access_token, refresh_token, ttl))
        conn.commit()

def get_tokens():
    with sqlite3.connect(DB_NAME) as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT access_token, refresh_token, ttl FROM tokens WHERE id = 1')
        return cursor.fetchone()

def load_tokens():
    tokens = get_tokens()
    if tokens:
        return tokens
    return None, None, None

@app.route('/')
def home():
    access_token, refresh_token, ttl = load_tokens()
    if ttl is None:
        date = 'не указано'
    else:
        date = datetime.fromtimestamp(ttl)
    return f'''
    <style>
        body {{
            font-family: Arial, sans-serif;
            background-color: #f0f8ff;
            color: #333;
            margin: 0;
            padding: 20px;
            display: flex;
            flex-direction: column;
            align-items: center;
        }}

        .container {{
            background-color: #ffffff;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 10px rgba(0, 0, 0, 0.1);
            text-align: center;
            max-width: 400px; /* Максимальная ширина блока */
            width: 100%;      /* Занимает всю ширину контейнера */
            margin-top: 0;    /* Убран отступ сверху для блока */
            margin-bottom: 20px; /* Отступ снизу для блока */
        }}

        h1 {{
            font-size: 24px;
            margin-bottom: 10px; /* Отступ снизу для заголовка */
        }}

        p {{
            font-size: 18px;
            margin: 10px 0; /* Убран отдельный нижний отступ */
        }}

        a {{
            text-decoration: none;
            color: #4CAF50;
            font-weight: bold;
            margin: 5px 0;
        }}

        a:hover {{
            text-decoration: underline;
        }}
    </style>
    <div class="container">
        <h1>Главная</h1>
        <p>Токен действителен до: {date}</p>
        <a href="/authorize">Получить токен</a><br>
        <a href="/parse">Спарсить вакансии</a>
    </div>
'''


@app.route('/authorize')
def authorize():
    auth_url = f'https://www.superjob.ru/authorize/?client_id={CLIENT_ID}&redirect_uri={REDIRECT_URI}'
    return redirect(auth_url)

@app.route('/callback')
def callback():
    code = request.args.get('code')
    if not code:
        return jsonify({'error': 'Не удалось получить код авторизации'}), 400

    token_url = 'https://api.superjob.ru/2.0/oauth2/access_token/'
    response = requests.post(token_url, data={
        'code': code,
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'redirect_uri': REDIRECT_URI
    })

    if response.status_code == 200:
        token_data = response.json()
        update_tokens(token_data['access_token'], token_data['refresh_token'], token_data['ttl'])

        return '''
    <style>
        body {
            font-family: Arial, sans-serif;
            background-color: #f0f8ff;
            color: #333;
            margin: 0;
            padding: 20px;
            display: flex;
            flex-direction: column;
            align-items: center;
        }

        .container {
            background-color: #ffffff;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 10px rgba(0, 0, 0, 0.1);
            text-align: center;
            max-width: 400px; /* Максимальная ширина блока */
            width: 100%;      /* Занимает всю ширину контейнера */
        }

        h1 {
            font-size: 24px;
            margin-bottom: 20px;
        }

        p {
            font-size: 18px;
            margin-bottom: 20px;
        }

        a {
            text-decoration: none;
            color: #4CAF50;
            font-weight: bold;
        }

        a:hover {
            text-decoration: underline;
        }
    </style>
    <div class="container">
        <h1>Успешная авторизация!</h1>
        <p>Токен успешно получен.</p>
        <a href="/">Вернуться на главную страницу</a>
    </div>
'''

    return jsonify({'error': 'Ошибка получения Access Token', 'details': response.text}), response.status_code


def parse_vacancies_task(date_from_timestamp, date_to_timestamp, professions, code_words, keywords, callback):
    # Начало парсинга
    callback("Парсинг начался...")
    items = parse_vacancies(date_from_timestamp,date_to_timestamp,professions,code_words,keywords)
    save_to_excel(items)
    callback("Парсинг завершён. Ссылка на страницу http://127.0.0.1:8000/")
    # raise SystemExit(1)


@app.route('/parse', methods=['GET', 'POST'])
def parse():
    if request.method == 'POST':
        date_from = request.form['date_from']
        
        date_to = request.form['date_to']
        
        date_from_timestamp = pr_date_to_timestamp(date_from, type='from')
        date_to_timestamp = pr_date_to_timestamp(date_to, type='to')
        # Проверка на валидность
        if date_from_timestamp is None or date_to_timestamp is None:
            return "Неверный формат даты. Пожалуйста, попробуйте снова."

        
        get_secret_key()
        # print(secret_key)
        keywords,professions,code_words = get_checks()
        # print(keywords)
        # print(professions)
        # print(code_words)

        # Создаём поток для парсинга
        thread = threading.Thread(target=parse_vacancies_task, args=(date_from_timestamp, date_to_timestamp, professions, code_words, keywords, lambda message: print(message)))
        thread.start()
        
        return f'''
        <style>
        body {{
            font-family: Arial, sans-serif;
            background-color: #f0f8ff;
            color: #333;
            margin: 0;
            padding: 20px;
            display: flex;
            flex-direction: column;
            align-items: center;
        }}

        h1 {{
            font-size: 24px;
            margin-bottom: 20px;
        }}

        .message {{
            background-color: #ffffff;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 10px rgba(0, 0, 0, 0.1);
            text-align: center;
            width: 300px; /* Ширина блока сообщения */
            margin-top: 20px; /* Отступ сверху для расположения ниже заголовка */
        }}
        </style>
        <div class="message">
        <h1>Парсинг начался</h1>
        <p>Проверьте приложение для информации.</p>
        <a href="/">Вернуться на главную страницу</a>
        </div>
        '''



    return render_template('form.html')
    


if __name__ == '__main__':
    init_db()
    get_secret_key()
    app.run(port=8000)
