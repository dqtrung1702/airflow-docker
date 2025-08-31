****Install Apache-Airflow****

```
sudo apt-get install software-properties-common
sudo apt-add-repository universe
sudo apt-get update
sudo apt-get install python-setuptools
sudo apt-get install libmysqlclient-dev
sudo apt-get install libssl-dev
sudo apt-get install libkrb5-dev
sudo pip3 install apache-airflow
sudo pip3 install typing_extensions
```

Thiết lập một biến môi trường export AIRFLOW_HOME=~/airflow. Để lưu biến môi trường này vĩnh viễn, bạn cần thêm nó vào một tệp cấu hình khởi động của shell, chẳng hạn như .bashrc, .bash_profile, hoặc tệp tương tự tùy thuộc vào shell bạn dùng (ở đây dùng .bashrc trên Ubuntu).

```
nano ~/.bashrc — thêm "export AIRFLOW_HOME=~/airflow" vào cuối file và save vào
source ~/.bashrc
```

Sau khi cài apache-airflow phải khởi tạo database back-end(Repository data) để lưu cấu hình. Mặc định khi cài Apache-Airflow sẽ sử dụng SQLite để làm database back-end . Mình muốn sử dụng Postgres để làm database back-end.

Install postgres:

```
sudo apt install postgresql postgresql-contrib
sudo -i -u postgres
pg_lsclusters — Hiển thị thông tin các clusters PostgreSQL
pg_ctlcluster {Ver} main start — start một cluster PostgreSQL
```

truy cập giao diện dòng lệnh PostgreSQL
```
psql
```
tạo user và database vd:
```
CREATE USER airflow;
CREATE DATABASE airflow;
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
ALTER USER airflow WITH ENCRYPTED PASSWORD 'admin';
```
thoát khỏi giao diện dòng lệnh PostgreSQL
```
\q
```

Trong AIRFLOW_HOME có file airflow.cfg sử lại tham số sql_alchemy_conn để trỏ tới serv Postgres:

```
sql_alchemy_conn = postgresql+psycopg2://airflow:admin@localhost:5432/airflow
```

Khởi tạo database-backend:

```
airflow db init
```
xóa toàn bộ cơ sở dữ liệu nếu cần
```
airflow db reset
```
upgrade cơ sở dữ liệu nếu cần
```
airflow db upgrade
```

Tạo acc vào Airflow:

```
airflow users create \
--username admin \
--firstname Trump \
--lastname Donald \
--role Admin \
--email trumpd@gmail.com
```

****Tips 4 begin with Apache-Airflow****

Trước khi lunch Apache-Airflow kiểm tra xem db back-end postgres đang online không(Khi reboot host db bị down). Nếu đang down thì phải start lại:

```
sudo service postgresql restart
```

Start scheduler instance(service để execute các requests) và Airflow webserver instance(UI để xem và thực thi các DAGs) trên 2 remote session khác nhau cùng lúc:

```
airflow scheduler
airflow webserver [-p 8080]
```



****Extract — Load data from/to database with Apache-Airflow****

Để connect tới db thì cần cài các provider tương ứng.

Danh sách providers pakage:
https://airflow.apache.org/docs/apache-airflow-providers/packages-ref.html#providers-packages-reference

1. Oracle

Cài provider :

```
pip3 install apache-airflow-providers-oracle
```

Đối với database Oracle, cần cài thêm oracle client để có thể connect tới database server:

download instant client package:
```
https://download.oracle.com/otn_software/linux/instantclient/19800/instantclient-basic-linux.x64-19.8.0.0.0dbru.zip?xd_co_f=fae07a5a-ae5f-4c1a-8ce0-f41ed4a394c5
```

download sqlplus package:
```
https://download.oracle.com/otn_software/linux/instantclient/19800/instantclient-sqlplus-linux.x64-19.8.0.0.0dbru.zip
```
setup:
```
sudo apt update && sudo apt upgrade
sudo apt-get install -y unzip
sudo unzip <download>/instantclient-basic-linux.x64–19.8.0.0.0dbru.zip -d /opt/oracle
sudo unzip <download>/instantclient-sqlplus-linux.x64–19.8.0.0.0dbru.zip -d /opt/oracle/
```

Add PATH:
```
nano ~/.profile
export PATH=”$PATH:/opt/oracle/instantclient_19_8"
export LD_LIBRARY_PATH=”$LD_LIBRARY_PATH:/opt/oracle/instantclient_19_8"
source ~/.profile
```

**DAG example:**
```
from datetime import datetime
from airflow import DAG
from airflow.operators.oracle_operator import OracleOperator
from airflow.hooks.oracle_hook import OracleHook
from airflow.operators.python_operator import PythonOperator

import requests
import json

from bs4 import BeautifulSoup

def get_data_api():
db = OracleHook(oracle_conn_id=’oracle_test’)
db_conn = db.get_conn()
db_cursor = db_conn.cursor()

url = “https://khcn.vista.gov.vn/vuejx/”
payload="{\"query\":\"\\n query search($token: String, $body: JSON, $db: String, $collection: String) {\\n results: search(token: $token, body: $body, db: $db, collection: $collection )\\n }\\n \”,\”variables\”:\”{\\\”body\\\”:{\\\”size\\\”:30,\\\”query\\\”:{\\\”bool\\\”:{\\\”filter\\\”:{\\\”match\\\”:{\\\”site\\\”:\\\”guest\\\”}},\\\”must\\\”:[{\\\”match\\\”:{\\\”storage\\\”:\\\”regular\\\”}}]}},\\\”highlight\\\”:{\\\”pre_tags\\\”:[\\\”<es_em>\\\”],\\\”post_tags\\\”:[\\\”</es_em>\\\”],\\\”fields\\\”:{\\\”*\\\”:{}}},\\\”_source\\\”:{\\\”includes\\\”:[\\\”id\\\”,\\\”Collection\\\”,\\\”Type\\\”,\\\”Title\\\”,\\\”Authors\\\”,\\\”PublicationDate\\\”,\\\”Authors\\\”,\\\”Keyword\\\”,\\\”Publisher\\\”,\\\”Subject\\\”,\\\”Journal\\\”,\\\”Number\\\”,\\\”ISSN\\\”,\\\”StartPage\\\”,\\\”EndPage\\\”,\\\”Updated_Date\\\”]},\\\”sort\\\”:[{\\\”_score\\\”:\\\”desc\\\”},{\\\”PublicationDate\\\”:\\\”desc\\\”}],\\\”from\\\”:0},\\\”db\\\”:\\\”khcn\\\”,\\\”collection\\\”:\\\”Publication\\\”,\\\”token\\\”:null}\”}”
headers = {‘Content-Type’: ‘application/json’}
res = requests.post(url, headers=headers, data=payload,verify=False)
if res.status_code == 200:
# Kiểm tra được certificate
result = json.loads(res.content)
# for h in (result[‘data’][‘results’][‘hits’][‘hits’]):
# print(h[‘_id’])
# print(h[‘_source’][‘Title’])
# print(h)
# break
for item in result[‘data’][‘results’][‘hits’][‘hits’]:
_id = (item.get(‘_source’,None).get(‘id’,None))
_value = (item.get(‘_source’,None).get(‘Title’,None).get(‘_source’,None).get(‘_value’,None))
print(_id,’:’,_value)
db_cursor.execute(‘insert into TPR_POC_DB.test_vuejx (“_id”,”Title”) VALUES (:1,:2)’, (_id,_value))
else:
# Certificate lỗi
print(res.status_code)
db_conn.commit()
db_conn.close
def get_data_html():
db = OracleHook(oracle_conn_id=’oracle_test’)
db_conn = db.get_conn()
db_cursor = db_conn.cursor()

url = ‘http://thaibinh.edu.vn/?&module=Content.Listing&moduleId=1014&service=Content.Decl.DataSet.Grouping.select&itemId=5d04f1e130817e0576046952&layout=Decl.DataSet.Detail.default&keyword=&gridModuleParentId=14&site=18853'

# Make a GET request to fetch the raw HTML content
html_content = requests.get(url).text

# Parse the html content
soup = BeautifulSoup(html_content, “lxml”)

table = soup.find(“table”, attrs={“class”: “table”})
header = table.thead.find_all(“th”)
detail = table.tbody.find_all(“tr”)

print(table)
print(‘______________________________________________’)
for th in header:
print(th.get_text())
print(‘______________________________________________’)
for tr in detail:
# print(tr)
_values = []
for td in tr.find_all(“td”):
_values.append(td.get_text())
db_cursor.execute(‘INSERT INTO TPR_POC_DB.TEST_DIEM(“stt”, “truong”, “diem”, “ghichu”) VALUES(:1, :2, :3, :4)’, tuple(_values))
db_conn.commit()
db_conn.close

dag = DAG(
dag_id = ‘get_data’,
start_date = datetime(2021, 1, 1),
schedule_interval = ‘*/10 * * * *’,
)
task_1 = PythonOperator(
task_id=’get_data_api’,
provide_context=True,
python_callable=get_data_api,
dag=dag,
)

task_2 = PythonOperator(
task_id=’get_data_html’,
provide_context=True,
python_callable=get_data_html,
dag=dag,
)

[task_1,task_2]
```

Miscellaneous commands
airflow cheat-sheet | Display cheat sheet
airflow info | Show information about current Airflow and environment
airflow kerberos | Start a kerberos ticket renewer
airflow plugins | Dump information about loaded plugins
airflow rotate-fernet-key | Rotate encrypted connection credentials and variables
airflow scheduler | Start a scheduler instance
airflow sync-perm | Update permissions for existing roles and DAGs
airflow version | Show the version
airflow webserver | Start a Airflow webserver instance

Celery components
airflow celery flower | Start a Celery Flower
airflow celery stop | Stop the Celery worker gracefully
airflow celery worker | Start a Celery worker node

View configuration
airflow config get-value | Print the value of the configuration
airflow config list | List options for the configuration

Manage connections
airflow connections add | Add a connection
airflow connections delete | Delete a connection
airflow connections export | Export all connections
airflow connections get | Get a connection
airflow connections list | List connections

Manage DAGs
airflow dags backfill | Run subsections of a DAG for a specified date range
airflow dags delete | Delete all DB records related to the specified DAG
airflow dags list | List all the DAGs
airflow dags list-jobs | List the jobs
airflow dags list-runs | List DAG runs given a DAG id
airflow dags next-execution | Get the next execution datetimes of a DAG
airflow dags pause | Pause a DAG
airflow dags report | Show DagBag loading report
airflow dags show | Displays DAG’s tasks with their dependencies
airflow dags state | Get the status of a dag run
airflow dags test | Execute one single DagRun
airflow dags trigger | Trigger a DAG run
airflow dags unpause | Resume a paused DAG

Database operations
airflow db check | Check if the database can be reached
airflow db check-migrations | Check if migration have finished
airflow db init | Initialize the metadata database
airflow db reset | Burn down and rebuild the metadata database
airflow db shell | Runs a shell to access the database
airflow db upgrade | Upgrade the metadata database to latest version

Tools to help run the KubernetesExecutor
airflow kubernetes cleanup-pods | Clean up Kubernetes pods in evicted/failed/succeeded states
airflow kubernetes generate-dag-yaml | Generate YAML files for all tasks in DAG. Useful for debugging tasks without launching into a cluster

Manage pools
airflow pools delete | Delete pool
airflow pools export | Export all pools
airflow pools get | Get pool size
airflow pools import | Import pools
airflow pools list | List pools
airflow pools set | Configure pool

Display providers
airflow providers behaviours | Get information about registered connection types with custom behaviours
airflow providers get | Get detailed information about a provider
airflow providers hooks | List registered provider hooks
airflow providers links | List extra links registered by the providers
airflow providers list | List installed providers
airflow providers widgets | Get information about registered connection form widgets

Manage roles
airflow roles create | Create role
airflow roles list | List roles

Manage tasks
airflow tasks clear | Clear a set of task instance, as if they never ran
airflow tasks failed-deps | Returns the unmet dependencies for a task instance
airflow tasks list | List the tasks within a DAG
airflow tasks render | Render a task instance’s template(s)
airflow tasks run | Run a single task instance
airflow tasks state | Get the status of a task instance
airflow tasks states-for-dag-run | Get the status of all task instances in a dag run
airflow tasks test | Test a task instance

Manage users
airflow users add-role | Add role to a user
airflow users create | Create a user
airflow users delete | Delete a user
airflow users export | Export all users
airflow users import | Import users
airflow users list | List users
airflow users remove-role | Remove role from a user

Manage variables
airflow variables delete | Delete variable
airflow variables export | Export all variables
airflow variables get | Get variable
airflow variables import | Import variables
airflow variables list | List variables
airflow variables set | Set variable

B. Cài Apache-Airflow đầy đủ cho việc phát triển.

I. Cài đặt Docker trên Ubuntu

Xóa, dọn dẹp các cài đặt cũ:
```
sudo apt — purge autoremove docker docker-engine docker.io containerd runc
```

Cài các lib cần
```
sudo apt update && sudo apt install software-properties-common gnupg2 curl ca-certificates apt-transport-https
```

Thêm key fingerprint GPG vào danh sách tin cây của APT
```
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
```

Thêm kho lưu trữ Docker cho Ubuntu vào nguồn phần mềm.
```
sudo add-apt-repository “deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable”
```

Cài đặt Docker.
```
sudo apt update && sudo apt install docker-ce
sudo apt install docker-compose
sudo adduser $USER docker
```

II. Cài Docker Compose.
```
COMPOSE_VERSION=”$(curl -s https://api.github.com/repos/docker/compose/releases/latest | grep ‘“tag_name”:’\
| cut -d ‘“‘ -f 4)”

COMPOSE_URL=”https://github.com/docker/compose/releases/download/${COMPOSE_VERSION}/\
docker-compose-$(uname -s)-$(uname -m)”

sudo curl -L “${COMPOSE_URL}” -o /usr/local/bin/docker-compose

sudo chmod +x /usr/local/bin/docker-compose
```

verify.
```
docker-compose — version
```
III. Cài đặt Pyenv

Checking required packages
```
sudo add-apt-repository ppa:deadsnakes/ppa && sudo apt update
sudo apt-get install -y build-essential libssl-dev zlib1g-dev libbz2-dev libreadline-dev libsqlite3-dev wget curl llvm libncurses5-dev libncursesw5-dev xz-utils tk-dev libffi-dev liblzma-dev python-openssl git
sudo apt install build-essential python3.6-dev python3.7-dev python3.8-dev python-dev openssl sqlite3 libsqlite3-dev default-libmysqlclient-dev libmariadbd-dev libmariadb-dev libmysqlclient-dev:i386 libmysqlclient-dev postgresql
```

Install pyenv
```
curl https://pyenv.run | bash
```

thêm các dòng suggested vào cuối ~/.bashrc

Restart your shell
```
exec $SHELL
```

installing required Python version to pyenv and verifying it
```
pyenv install — list
pyenv install 3.8.5
pyenv versions
```

Creating new virtual environment named airflow-env
```
pyenv virtualenv 3.8.5 airflow-env
```

IV. Cài Apache-Airflow với Breeze
clone source code về: (vd)
```
git clone https://github.com/dqtrung1702/airflow.git
```

Thêm dòng sau vào ~/.bashrc để call breeze cho tiện: (vd)
export PATH=${PATH}:”/home/trung/Apache-Airflow/airflow”
→
```
source ~/.bashrc
```

activate venv lên:
```
pyenv activate airflow-env
```

mở tới project directory: (vd)
```
cd /home/trung/Apache-Airflow/airflow
```

Init sth:
```
breeze setup-autocomplete
~/.bash_completion.d/breeze-complete
breeze — python 3.8 — backend postgres
airflow db reset
airflow users create — role Admin — username admin — password admin — email trung@dang — firstname trung — lastname dang
exit
```

chạy airflow map vào db đã tạo:
```
breeze start-airflow initialize-local-virtualenv — python 3.8 — backend postgres
```

**NOTE**

Port Forwarding

When you run Airflow Breeze, the following ports are automatically forwarded:

28080 -> forwarded to Airflow webserver -> airflow:8080
25555 -> forwarded to Flower dashboard -> airflow:5555
25433 -> forwarded to Postgres database -> postgres:5432
23306 -> forwarded to MySQL database -> mysql:3306
26379 -> forwarded to Redis broker -> redis:6379

