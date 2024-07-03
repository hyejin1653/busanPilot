import psycopg2 
import configparser
import datetime
import time
import schedule
import socket
import json
import re
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import logging
import traceback
import chromedriver_autoinstaller
logging.basicConfig(filename='./test.log', level=logging.ERROR)

# 설정 파일 객체 생성
config = configparser.ConfigParser()
# 설정 파일 로드
config.read('config.ini')
# 설정 정보 읽기
db_host = config.get('database', 'host')
db_dbname = config.get('database', 'dbname')
db_user = config.get('database', 'user')
db_port = config.get('database', 'port')
db_password = config.get('database', 'password')

#udp ip, port 정보
udp_ip = config.get('socketInfo', 'ip')
udp_port = config.get('socketInfo', 'port')
udp_port = int(udp_port)

udp_port = 10353
udp_addr_port = (udp_ip, udp_port)


# 부산 도선예보현황 홈페이지
url_pusan = 'http://busanpilot.co.kr/popup/monitoring?index=0&type=bspilot'

# 울산 도선예보현황 홈페이지
url_ulsan = 'http://www.ulsanpilot.co.kr/main/pilot_forecast.php'


# Chrome 드라이버 서비스 생성 
chromedriver_autoinstaller.install()
#chrome_service = chromedriver_autoinstaller.install()

# Chrome 드라이버 서비스 생성 구버전(아래처럼 하면 크롬 버전이 바뀔때마다 크롬드라이버 버전도 맞춰줘야하기 때문에 chromedriver_autoinstaller 사용하는 것으로 변경)
# chrome_service = Service(executable_path = '/usr/local/bin/chromedriver')
#chrome_service = Service(ChromeDriverManager(version="114.0.5735.90").install())

# Chrome 드라이버 옵션 설정 (선택사항)
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--headless')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')

# Chrome 드라이버 실행
driver_pusan = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
driver_ulsan = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)


#driver_pusan = webdriver.Chrome('/usr/local/bin/chromedriver');
#driver_ulsan = webdriver.Chrome('/usr/local/bin/chromedriver');


# postgresql db설정 정보
conn = psycopg2.connect( 
    host = db_host,
    dbname = db_dbname,
    user = db_user,
    port = db_port,
    password = db_password
)

conn.autocommit = True
cursor = conn.cursor();

def clean_characters(text):
    # replace() 메소드로 특수문자 제거
    special_characters = ['\t','\n']
    for char in special_characters:
        text = text.replace(char, '')
    return text

def remove_special_characters(text):
    # replace() 메소드로 특수문자 제거
    special_characters = ["'", '(', ')','\t','+','\n',' ']
    for char in special_characters:
        text = text.replace(char, '')
    return text

def convert_date(date_string):
    # 날짜 형식에서 '년', '월', '일' 및 요일 제거
    date_string = re.sub(r'[년월]', '-', date_string)
    date_string = re.sub(r'[일]', '', date_string)
    date_string = re.sub(r'\([^)]*\)', '', date_string)
    date_string = date_string.replace(' ', '')
    
    # 날짜 문자열을 날짜 객체로 변환
    date_obj = datetime.datetime.strptime(date_string, '%Y-%m-%d')
    
    # 원하는 날짜 형식으로 변환하여 반환
    formatted_date = date_obj.strftime('%Y-%m-%d')
    return formatted_date


# 부산 도선 예보현황 데이터 조회
def getPusanData():
  # 현재 날짜와 시간을 가져옴
  now = datetime.datetime.now()
  print("부산항 데이터 조회: ", now.strftime("%Y-%m-%d %H:%M:%S"))

  html = driver_pusan.page_source
  soup = BeautifulSoup(html, 'html.parser')
  tables = soup.select('tbody')

  record_date = ""
  rows = tables[2].select('tr')
  tableData = []

  for row in rows:
        obj = []
        if row.find_all('th'):
          record_date = row.find_all('th')[0].text.strip()
              
        cells = row.find_all('td')
        if cells:
            # 각 셀의 데이터 출력
            obj.insert(0, record_date)
            for index, cell in enumerate(cells):
                  text = cell.text.strip()
                  if index == 3: 
                      values = text.split("/")
                      obj.append(clean_characters(values[0]))
                      obj.append(values[1].lstrip())
                  elif index == 11:
                      values = text.split("(")
                      obj.append(clean_characters(values[0]))
                      obj.append(clean_characters(values[1][:-1]))
                  elif index != 0:
                      obj.append(clean_characters(text))

        if len(obj)>0:
          obj.insert(17,'')
          tableData.append(obj)
          send_udp_row_data(obj, "부산")
        
        #print(tableData)
  
  #send_udp_table_data(tableData, "부산")
  #insert_crawl_data_pusan(tableData)

#udp server로 row data 전송
def send_udp_row_data(data, flag):
    header_list = []
    
    if(flag == "부산"):
        header_list = [
            "pilot_date", "pilot_time", "ship_name", "gross_ton", "loa", "dft", "from_", "to_", "side", "pilot", 
            "tugs" ,"ds","call_sign","imo", "agent","status", "entry", "ps", "rmk_com","rmk_agent", "rmk_pilot"
        ]
    else :
        header_list = [
            "status", "pilot_flag", "pilot_date", "pilot_time", "ship_name", "pilot", "call_sign", "gross_ton", "loa", "dft", 
            "from_" , "to_", "local_agent", "ca", "side", "berth", "t", "l", "remarks"
        ]
    
    json_object = {}
    
    for i in range(len(header_list)):
        json_object[header_list[i]] = data[i]
    
    json_object["flag"] = flag
    json_string = json.dumps(json_object, ensure_ascii=False)
    
    bytes_to_send = json_string.encode("utf-8")
    
    
    udp_client_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    #print(json_string, bytes_to_send)
    udp_client_socket.sendto(bytes_to_send, udp_addr_port)


#udp server로 array 전송 
def send_udp_table_data(data, flag):
    # 빈 항목을 찾아내고 삭제
    data = [item for item in data if item]
    
    for i in range(len(data)):
        send_udp_row_data(data[i], flag)
        
    
    

# 데이터 db에 insert
def insert_crawl_data_pusan(dataList):
    
    # 빈 항목을 찾아내고 삭제
    dataList = [item for item in dataList if item]

    header_list = [
        "pilot_date", "pilot_time", "ship_name", "gross_ton", "loa", "dft", "from_", "to_", "side", "pilot", 
        "tugs" ,"ds","call_sign","imo", "agent","status", "entry", "ps", "rmk_com","rmk_agent", "rmk_pilot"
    ]
    

    for i in range(len(header_list)):
        header_list[i] = header_list[i] + " = excluded." +  header_list[i]
        

    header_update = tuple(header_list);

    args_str = ",".join(cursor.mogrify('(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',row).decode('utf-8')
                        for row in dataList)
    
    update_str = '{header_update}'.format(header_update = header_update);
    update_str = remove_special_characters(update_str).lower();

    #upsert
    sql = "insert into opendata.busan_pilot_forecast values {data} on conflict on constraint busan_pilot_forecast_pkey do update set {update_str};".format(data = args_str, update_str=update_str);

    try:
        cursor.execute(sql)
        print("pusan update success")
    except Exception as e:
          print("pusan Insert error: ", e)


# 울산항 도선 예보현황 데이터 조회
def getUlsanData():
  # 현재 날짜와 시간을 가져옴
  now = datetime.datetime.now()
  print("울산항 데이터 조회: ", now.strftime("%Y-%m-%d %H:%M:%S"))

  html = driver_ulsan.page_source
  soup = BeautifulSoup(html, 'html.parser')
  tables = soup.select('tbody');

  date_arr = soup.select('.bgtab');
  date = [date.get_text(strip=True) for date in date_arr]

  # 날짜 형식 변경
  date = [convert_date(item) for item in date]

  for i in range(2):
    tableData = []
    rows = tables[i].select('tr')

    for row in rows:
        obj = []
        cells = row.find_all('td')
        if cells:
            # 각 셀의 데이터 출력
            for cell in cells:
                  obj.append(cell.text.strip())
        obj.pop(0)
        obj.insert(2, date[i])
        tableData.append(obj)
        send_udp_row_data(obj, "울산")
    
    #send_udp_table_data(tableData, "울산")
    #insert_crawl_data_ulsan(tableData);


# 데이터 db에 insert
def insert_crawl_data_ulsan(dataList):
    
    header_list = [
        "status", "pilot_flag", "pilot_date", "pilot_time", "ship_name", "pilot", "call_sign", "gross_ton", "loa", "dft", 
        "from_" , "to_", "local_agent", "ca", "side", "berth", "t", "l", "remarks"
    ]
    

    for i in range(len(header_list)):
        header_list[i] = header_list[i] + " = excluded." +  header_list[i]
        

    header_update = tuple(header_list);

    args_str = ",".join(cursor.mogrify('(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',row).decode('utf-8')
                        for row in dataList)
    
    update_str = '{header_update}'.format(header_update = header_update);
    update_str = remove_special_characters(update_str).lower();
    #upsert
    sql = "insert into opendata.ulsan_pilot_forecast values {data} on conflict on constraint ulsan_pilot_forecast_pkey do update set {update_str};".format(data = args_str, update_str=update_str);

    try:
        cursor.execute(sql)
        print("ulsan update success")
    except Exception as e:
          print("ulsan Insert error: ", e)



try:
  print("부산항, 울선항 도선예보현황 수집서버 start")
  driver_pusan.get(url_pusan);
  driver_ulsan.get(url_ulsan);

  getPusanData()
  getUlsanData()


  # 2분 간격으로 데이터 요청 및 업데이트
  schedule.every(2).minutes.do(getPusanData)
  schedule.every(2).minutes.do(getUlsanData)


  while True:
      schedule.run_pending()
      time.sleep(1)
except:
    logging.error(traceback.format_exc())
  

